use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::audio::AudioConfig;
use crate::base::{AppError, debug_err_log, debug_log};
use crate::transport::{
    NotifyingSender, OutboundControl, OutboundTarget, PeerId, PeerSource, RoutedOutbound,
};

// WsPeerHub 是“session 内所有网络出口的统一抽象”。
//
// 它主要承担三类职责：
// 1. 维护当前有哪些 peer 还活着
// 2. 按 Broadcast / ToPeer 的语义分发出站控制消息
// 3. 维护录音订阅集合，决定录音流应该发给谁
//
// 这样 router / recorder fanout / supervisor 都不需要自己维护一份 peer 表。
pub struct WsPeerHub {
    state: Mutex<WsPeerHubState>,
}

// WsPeerHubState 是 WsPeerHub 在锁内维护的真实状态。
//
// 它把“当前在线 peer 集合”和“当前录音订阅集合”统一放在一起维护，
// 这样 peer 上下线时就能顺手同步更新录音订阅状态。
struct WsPeerHubState {
    // peers 保存“当前 session 还在线”的所有 peer 发送句柄。
    peers: HashMap<PeerId, PeerSenders>,
    // recording_subscribers 只记录“当前需要收到录音流”的 peer。
    recording_subscribers: HashSet<PeerId>,
    // active_recording_config 用来落实“首个订阅者决定录音配置”的规则。
    active_recording_config: Option<AudioConfig>,
}

#[derive(Clone)]
// PeerSenders 表示“一个 peer 对应的一组出站发送句柄”。
//
// 它不关心 websocket 连接本身怎么实现，只关心：
// - 控制消息应该往哪个队列发
// - 录音音频应该往哪个队列发
struct PeerSenders {
    source: PeerSource,
    control_sender: NotifyingSender<OutboundControl>,
    audio_sender: NotifyingSender<Vec<u8>>,
}

// PeerRemoval 是 remove_peer 返回给上层的收尾摘要。
//
// 它告诉调用方三件事：
// - 当前移除的是哪种来源的 peer
// - 移除后还剩多少 peer
// - 是否应该顺手把全局 recorder 停掉
pub struct PeerRemoval {
    // source 会回传给 supervisor，便于它决定是否需要重新打开 outbound connector。
    pub source: PeerSource,
    pub remaining_peers: usize,
    // 只有“这个 peer 本来就在录音订阅集合里，并且它离开后订阅集合空了”时才为 true。
    pub stop_recording: bool,
}

#[derive(Debug)]
// RecordingSubscription 描述一次 start_recording 请求对当前 session 录音状态的影响。
pub enum RecordingSubscription {
    // 表示这是首个有效订阅者，调用方需要真正启动 recorder。
    Start(AudioConfig),
    // 表示 recorder 已经在运行，当前请求只需要加入订阅集合或按幂等处理即可。
    AlreadyActive,
}

impl WsPeerHub {
    // 创建一个空的 WsPeerHub。
    //
    // 初始状态下：
    // - 没有任何在线 peer
    // - 没有任何录音订阅者
    // - 也没有被锁定的 active_recording_config
    //
    // 入参说明：
    // - 无
    pub fn new() -> Self {
        Self {
            state: Mutex::new(WsPeerHubState {
                peers: HashMap::new(),
                recording_subscribers: HashSet::new(),
                active_recording_config: None,
            }),
        }
    }

    // 向当前 session 注册一个新 peer 的发送句柄。
    //
    // 一旦 add_peer 成功，后续的广播、单播和录音 fanout 都能找到这个 peer。
    //
    // 入参说明：
    // - self：当前 session 的 peer hub
    // - peer_id：该 peer 在当前 session 内的唯一编号
    // - source：该 peer 来自 listener 还是 outbound connect
    // - control_sender：发往该 peer 的控制消息发送端
    // - audio_sender：发往该 peer 的音频流发送端
    pub fn add_peer(
        &self,
        peer_id: PeerId,
        source: PeerSource,
        control_sender: NotifyingSender<OutboundControl>,
        audio_sender: NotifyingSender<Vec<u8>>,
    ) {
        // add_peer 在 session attach 成功时调用。
        // 一旦这里插入成功，后面的广播 / 定向发送 / 录音 fanout 就都能看到这个 peer。
        self.state.lock().expect("peer hub poisoned").peers.insert(
            peer_id,
            // 参数说明：
            // - source：记录该 peer 来自 listen 还是 outbound connect
            // - control_sender：向该 peer 发送文本控制消息的出口
            // - audio_sender：向该 peer 发送二进制音频流的出口
            PeerSenders {
                source,
                control_sender,
                audio_sender,
            },
        );
    }

    // 从当前 session 删除一个 peer，并返回它带来的副作用摘要。
    //
    // remove_peer 不只是删发送句柄，还会顺便：
    // - 把它从录音订阅集合中移除
    // - 判断是否因为它离开而需要停止全局 recorder
    //
    // 入参说明：
    // - self：当前 session 的 peer hub
    // - peer_id：要移除的 peer 编号
    pub fn remove_peer(&self, peer_id: PeerId) -> Option<PeerRemoval> {
        // remove_peer 不只是删掉网络出口；
        // 它还顺手把这个 peer 从录音订阅集合中清掉，并判断要不要停全局 recorder。
        let mut state = self.state.lock().expect("peer hub poisoned");
        let peer = state.peers.remove(&peer_id)?;
        let removed_subscriber = state.recording_subscribers.remove(&peer_id);
        let stop_recording = removed_subscriber && state.recording_subscribers.is_empty();
        if stop_recording {
            state.active_recording_config = None;
        }
        Some(PeerRemoval {
            // 参数说明：
            // - peer.source：把该 peer 的来源回传给上层，便于处理 outbound 状态
            // - state.peers.len()：移除后 session 内剩余的 peer 数量
            // - stop_recording：是否应该由调用方真正停掉 recorder
            source: peer.source,
            remaining_peers: state.peers.len(),
            stop_recording,
        })
    }

    // 给当前 session 内所有 peer 尽量发送一条 Close 指令。
    //
    // 这是“优雅关闭”的第一步，不保证一定发送成功；
    // 如果队列已满或 peer 已经坏掉，上层后续还会做强制关闭。
    //
    // 入参说明：
    // - self：当前 session 的 peer hub
    pub fn close_all_peers(&self) {
        // session 整体关闭时，先尽量给所有 writer 发一个 close 指令。
        // 如果某些队列已经满了也没关系，supervisor 后面还会走强制 close 兜底。
        let senders = {
            let state = self.state.lock().expect("peer hub poisoned");
            state
                .peers
                .values()
                .map(|peer| peer.control_sender.clone())
                .collect::<Vec<_>>()
        };

        for sender in senders {
            let _ = sender.try_send(OutboundControl::Close);
        }
    }

    // 尝试只给某一个 peer 发送一条 Close 指令。
    //
    // 这是“单 peer 收尾”时的最佳努力路径：
    // - 如果它的控制队列还活着，就先给 writer 一个发送 websocket close frame 的机会
    // - 如果队列已满或已断开，就返回 false，让上层直接走强制关闭兜底
    //
    // 入参说明：
    // - self：当前 session 的 peer hub
    // - peer_id：目标 peer 编号
    pub fn try_close_peer(&self, peer_id: PeerId) -> bool {
        let sender = {
            let state = self.state.lock().expect("peer hub poisoned");
            state
                .peers
                .get(&peer_id)
                .map(|peer| peer.control_sender.clone())
        };

        let Some(sender) = sender else {
            debug_log(
                "peer-hub",
                format!("Skip graceful close because peer {peer_id} is already gone"),
            );
            return false;
        };

        match sender.try_send(OutboundControl::Close) {
            Ok(()) => {
                debug_log(
                    "peer-hub",
                    format!("Graceful close frame queued for peer {peer_id}"),
                );
                true
            }
            Err(err) => {
                debug_err_log(
                    "peer-hub",
                    format!("Failed to queue graceful close for peer {peer_id}: {err}"),
                );
                false
            }
        }
    }

    // 返回当前 session 内仍然在线的 peer 数量。
    //
    // 入参说明：
    // - self：当前 session 的 peer hub
    pub fn peer_count(&self) -> usize {
        self.state.lock().expect("peer hub poisoned").peers.len()
    }

    // 按目标语义分发一条控制消息。
    //
    // 目标可以是：
    // - Broadcast：广播给当前所有 peer
    // - ToPeer(peer_id)：只发给某一个 peer
    //
    // 入参说明：
    // - self：当前 session 的 peer hub
    // - target：目标范围，决定是广播还是单播
    // - outbound：要发送的控制消息
    pub fn send_control(
        &self,
        target: OutboundTarget,
        outbound: OutboundControl,
    ) -> Result<(), AppError> {
        match target {
            OutboundTarget::Broadcast => {
                // 广播时先把 sender clone 出来，避免在真正 send 时长时间持锁。
                let senders = {
                    let state = self.state.lock().expect("peer hub poisoned");
                    state
                        .peers
                        .values()
                        .map(|peer| peer.control_sender.clone())
                        .collect::<Vec<_>>()
                };
                for sender in senders {
                    // 参数说明：
                    // - outbound.clone()：广播模式下，每个 peer 都要拿到一份独立的消息副本
                    sender.send(outbound.clone()).map_err(|err| {
                        debug_err_log(
                            "peer-hub",
                            format!("Failed to broadcast outbound control message: {err}"),
                        );
                        anyhow::Error::from(err)
                    })?;
                }
            }
            OutboundTarget::ToPeer(peer_id) => {
                // 单播如果目标已经不存在，不把它视作致命错误。
                // 这是因为 peer 可能刚好在 response 返回前就退出了。
                let sender = {
                    let state = self.state.lock().expect("peer hub poisoned");
                    state
                        .peers
                        .get(&peer_id)
                        .map(|peer| peer.control_sender.clone())
                };
                if let Some(sender) = sender {
                    // 参数说明：
                    // - outbound：只发给目标 peer 的控制消息
                    sender.send(outbound).map_err(|err| {
                        debug_err_log(
                            "peer-hub",
                            format!(
                                "Failed to send outbound control message to peer {peer_id}: {err}"
                            ),
                        );
                        anyhow::Error::from(err)
                    })?;
                } else {
                    debug_log(
                        "peer-hub",
                        format!("Outbound control message dropped because peer {peer_id} is gone"),
                    );
                }
            }
        }
        Ok(())
    }

    // 提供给 router 使用的更业务化出站入口。
    //
    // router 只需要给出“目标 + 消息”，具体如何找到 peer 并分发，由 WsPeerHub 内部负责。
    //
    // 入参说明：
    // - self：当前 session 的 peer hub
    // - outbound：已经封装好目标和消息体的出站动作
    pub fn dispatch_outbound(&self, outbound: RoutedOutbound) -> Result<(), AppError> {
        // 给 router 一个更贴近业务边界的入口：它只关心“目标 + 消息”，
        // 不需要知道 WsPeerHub 内部是怎么分发的。
        self.send_control(outbound.target, outbound.message)
    }

    // 处理某个 peer 发起的 start_recording 请求。
    //
    // 录音语义是“一个 session 一个 recorder，多 peer 订阅”：
    // - 第一个有效订阅者真正启动 recorder
    // - 第一条生效请求锁定 active_recording_config
    // - 后续相同配置请求加入订阅
    // - 后续不同配置请求直接报错
    //
    // 入参说明：
    // - self：当前 session 的 peer hub
    // - peer_id：发起 start_recording 的 peer 编号
    // - requested：该请求携带的录音配置，可能为空
    pub fn subscribe_recording(
        &self,
        peer_id: PeerId,
        requested: Option<AudioConfig>,
    ) -> Result<RecordingSubscription, AppError> {
        let mut state = self.state.lock().expect("peer hub poisoned");
        if !state.peers.contains_key(&peer_id) {
            return Err(anyhow::anyhow!("peer not found: {peer_id}"));
        }

        // 规则 1：未显式给配置时，使用项目默认录音配置。
        let requested = requested.unwrap_or_else(|| crate::audio::AUDIO_CONFIG.clone());
        // 规则 2：同一 peer 重复 start_recording 按幂等处理。
        if state.recording_subscribers.contains(&peer_id) {
            return Ok(RecordingSubscription::AlreadyActive);
        }

        if let Some(active) = state.active_recording_config.as_ref() {
            // 规则 3：一旦已有激活配置，后续不同配置直接报错，不做隐式重启。
            if active != &requested {
                return Err(anyhow::anyhow!(
                    "recording is already active with a different AudioConfig"
                ));
            }
            // 配置一致时，只把这个 peer 加入订阅集合，不需要再次启动 recorder。
            state.recording_subscribers.insert(peer_id);
            return Ok(RecordingSubscription::AlreadyActive);
        }

        // 走到这里说明当前还没有任何活跃录音订阅者；
        // 当前请求会成为“首个订阅者 + 配置制定者”。
        state.recording_subscribers.insert(peer_id);
        state.active_recording_config = Some(requested.clone());
        Ok(RecordingSubscription::Start(requested))
    }

    // 取消某个 peer 的录音订阅。
    //
    // 返回值语义是：调用方是否应该进一步真正停止 recorder。
    //
    // 入参说明：
    // - self：当前 session 的 peer hub
    // - peer_id：要取消订阅的 peer 编号
    pub fn unsubscribe_recording(&self, peer_id: PeerId) -> bool {
        // 返回值语义是：“移除当前 peer 后，是否应该由调用方真正停掉 recorder”。
        let mut state = self.state.lock().expect("peer hub poisoned");
        let removed = state.recording_subscribers.remove(&peer_id);
        if removed && state.recording_subscribers.is_empty() {
            state.active_recording_config = None;
            return true;
        }
        false
    }

    // 把 recorder 产出的单路音频扇出给当前所有录音订阅者。
    //
    // 这里采用“先收集 sender，再逐个 try_send”的方式，
    // 避免在发送过程中长时间持有内部锁。
    //
    // 入参说明：
    // - self：当前 session 的 peer hub
    // - payload：一块已经编码好的录音流负载
    pub fn fan_out_record_audio(&self, payload: Vec<u8>) {
        // recorder 输出的是 session 级原始录音流。
        // 到这里再根据 recording_subscribers 做“多播”，这样 recorder 本身就不需要知道 peer 概念。
        let senders = {
            let state = self.state.lock().expect("peer hub poisoned");
            state
                .recording_subscribers
                .iter()
                .filter_map(|peer_id| {
                    state
                        .peers
                        .get(peer_id)
                        .map(|peer| peer.audio_sender.clone())
                })
                .collect::<Vec<_>>()
        };

        for sender in senders {
            // 参数说明：
            // - payload.clone()：每个订阅者都要拿到完整的一份音频块
            if let Err(err) = sender.try_send(payload.clone()) {
                debug_err_log(
                    "peer-hub",
                    format!("Failed to fan out record stream to peer audio queue: {err}"),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, mpsc};

    use crate::audio::AudioConfig;
    use crate::transport::{OutboundControl, OutboundTarget, PeerSource, WriteSignal};

    use super::{RecordingSubscription, WsPeerHub};

    // 构造一组用于测试的控制消息发送端和接收端。
    fn make_control_sender() -> (
        crate::transport::NotifyingSender<OutboundControl>,
        mpsc::Receiver<OutboundControl>,
    ) {
        let (tx, rx) = mpsc::sync_channel(8);
        (
            crate::transport::NotifyingSender::new(tx, Arc::new(WriteSignal::new())),
            rx,
        )
    }

    // 构造一组用于测试的音频消息发送端和接收端。
    fn make_audio_sender() -> (
        crate::transport::NotifyingSender<Vec<u8>>,
        mpsc::Receiver<Vec<u8>>,
    ) {
        let (tx, rx) = mpsc::sync_channel(8);
        (
            crate::transport::NotifyingSender::new(tx, Arc::new(WriteSignal::new())),
            rx,
        )
    }

    // 生成一份便于测试的标准 AudioConfig。
    //
    // 入参说明：
    // - rate：采样率
    fn sample_config(rate: u32) -> AudioConfig {
        AudioConfig {
            pcm: "noop".to_string(),
            channels: 1,
            bits_per_sample: 16,
            sample_rate: rate,
            period_size: 160,
            buffer_size: 480,
        }
    }

    #[test]
    // 验证广播和单播控制消息的路由语义正确。
    fn routes_broadcast_and_single_peer_messages() {
        let hub = WsPeerHub::new();
        let (control_1, rx_1) = make_control_sender();
        let (control_2, rx_2) = make_control_sender();
        let (audio_1, _) = make_audio_sender();
        let (audio_2, _) = make_audio_sender();

        hub.add_peer(1, PeerSource::Listener, control_1, audio_1);
        hub.add_peer(2, PeerSource::Listener, control_2, audio_2);

        hub.send_control(
            OutboundTarget::Broadcast,
            OutboundControl::Text("broadcast".to_string()),
        )
        .unwrap();
        hub.send_control(
            OutboundTarget::ToPeer(2),
            OutboundControl::Text("target".to_string()),
        )
        .unwrap();

        assert!(matches!(
            rx_1.recv().unwrap(),
            OutboundControl::Text(ref text) if text == "broadcast"
        ));
        assert!(matches!(
            rx_2.recv().unwrap(),
            OutboundControl::Text(ref text) if text == "broadcast"
        ));
        assert!(matches!(
            rx_2.recv().unwrap(),
            OutboundControl::Text(ref text) if text == "target"
        ));
        assert!(rx_1.try_recv().is_err());
    }

    #[test]
    // 验证首个录音订阅者会锁定 active_recording_config，后续不同配置会被拒绝。
    fn recording_subscription_keeps_first_config() {
        let hub = WsPeerHub::new();
        let (control_1, _) = make_control_sender();
        let (control_2, _) = make_control_sender();
        let (control_3, _) = make_control_sender();
        let (audio_1, _) = make_audio_sender();
        let (audio_2, _) = make_audio_sender();
        let (audio_3, _) = make_audio_sender();

        hub.add_peer(1, PeerSource::Listener, control_1, audio_1);
        hub.add_peer(2, PeerSource::Listener, control_2, audio_2);
        hub.add_peer(3, PeerSource::Listener, control_3, audio_3);

        let first = hub
            .subscribe_recording(1, Some(sample_config(16000)))
            .unwrap();
        assert!(matches!(first, RecordingSubscription::Start(_)));

        let second = hub
            .subscribe_recording(2, Some(sample_config(16000)))
            .unwrap();
        assert!(matches!(second, RecordingSubscription::AlreadyActive));

        let err = hub
            .subscribe_recording(3, Some(sample_config(8000)))
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("recording is already active with a different AudioConfig")
        );
    }

    #[test]
    // 验证最后一个订阅者离开时会触发“应停止 recorder”的返回值。
    fn stop_recording_when_last_subscriber_leaves() {
        let hub = WsPeerHub::new();
        let (control_1, _) = make_control_sender();
        let (control_2, _) = make_control_sender();
        let (audio_1, _) = make_audio_sender();
        let (audio_2, _) = make_audio_sender();

        hub.add_peer(1, PeerSource::Listener, control_1, audio_1);
        hub.add_peer(2, PeerSource::Listener, control_2, audio_2);
        hub.subscribe_recording(1, Some(sample_config(16000)))
            .unwrap();
        hub.subscribe_recording(2, Some(sample_config(16000)))
            .unwrap();

        assert!(!hub.unsubscribe_recording(1));
        assert!(hub.unsubscribe_recording(2));
    }

    #[test]
    // 验证录音流只会发给已订阅录音的 peer。
    fn record_stream_only_hits_subscribers() {
        let hub = WsPeerHub::new();
        let (control_1, _) = make_control_sender();
        let (control_2, _) = make_control_sender();
        let (audio_1, rx_1) = make_audio_sender();
        let (audio_2, rx_2) = make_audio_sender();

        hub.add_peer(1, PeerSource::Listener, control_1, audio_1);
        hub.add_peer(2, PeerSource::Listener, control_2, audio_2);
        hub.subscribe_recording(2, Some(sample_config(16000)))
            .unwrap();

        hub.fan_out_record_audio(vec![1, 2, 3]);

        assert_eq!(rx_2.recv().unwrap(), vec![1, 2, 3]);
        assert!(rx_1.try_recv().is_err());
    }
}
