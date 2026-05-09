use std::collections::HashMap;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use crate::audio::{AudioPlayer, AudioRecorder};
use crate::base::{AppError, debug_err_log, debug_log};
use crate::transport::{
    NotifyingSender, OutboundControl, OutboundTarget, PeerId, PeerSource, RoutedOutbound,
    WsPeerHandle,
};

// peer_context 模块收敛“单个 peer 在 session 内需要持有的全部本地资源”。
//
// 这样做的目标是：
// 1. 避免 player/recorder 和网络 sender 分散在多张表里
// 2. 让 command handler / router / session 收尾都只查一张表
// 3. 把“这个 peer 的完整上下文”当成一个稳定对象来思考

// PeerContext 表示“一个 peer 在当前 session 内的完整本地上下文”。
//
// 它把原先分散在两个表里的资源合并到一起：
// - player / recorder：本地媒体设备
// - control_sender / audio_sender：发往该 peer 的网络出口
// - source：该 peer 的接入来源
// - ws_handle：强制关闭该 peer 时需要的 websocket 控制句柄
#[derive(Clone)]
pub(crate) struct PeerContext {
    pub(crate) source: PeerSource,
    pub(crate) player: Arc<AudioPlayer>,
    pub(crate) recorder: Arc<AudioRecorder>,
    control_sender: NotifyingSender<OutboundControl>,
    audio_sender: NotifyingSender<Vec<u8>>,
    ws_handle: Option<Arc<WsPeerHandle>>,
}

impl PeerContext {
    // 为一个新挂入 session 的 peer 构造完整上下文。
    //
    // 入参说明：
    // - source：该 peer 的接入来源
    // - control_sender：发往该 peer 的文本控制消息出口
    // - audio_sender：发往该 peer 的音频流消息出口
    fn new(
        source: PeerSource,
        control_sender: NotifyingSender<OutboundControl>,
        audio_sender: NotifyingSender<Vec<u8>>,
    ) -> Self {
        Self {
            source,
            player: Arc::new(AudioPlayer::new()),
            recorder: Arc::new(AudioRecorder::new()),
            control_sender,
            audio_sender,
            ws_handle: None,
        }
    }

    // 返回当前 peer 对应的音频发送句柄，供 recorder 直接回送录音流。
    //
    // 入参说明：
    // - self：当前 peer 的完整上下文
    pub(crate) fn audio_sender(&self) -> NotifyingSender<Vec<u8>> {
        self.audio_sender.clone()
    }

    pub(crate) fn clear_audio_queue(&self) -> Result<usize, AppError> {
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        self.control_sender
            .send(OutboundControl::ClearAudioQueue(ack_tx))
            .map_err(|err| anyhow::anyhow!("failed to queue audio clear command: {err}"))?;
        ack_rx
            .recv_timeout(Duration::from_secs(2))
            .map_err(|err| anyhow::anyhow!("timed out waiting for audio queue clear: {err}"))
    }

    // 返回当前 peer 的 websocket 强制关闭句柄。
    //
    // 入参说明：
    // - self：当前 peer 的完整上下文
    pub(crate) fn ws_handle(&self) -> Option<Arc<WsPeerHandle>> {
        self.ws_handle.clone()
    }
}

// RemovedPeer 是 remove_peer 返回给上层的收尾摘要。
//
// 它同时携带：
// - 被移除 peer 的完整上下文，供上层 stop 媒体或强制 close
// - 移除后还剩多少 peer，供 session 判断是否需要整体结束
pub(crate) struct RemovedPeer {
    pub(crate) context: PeerContext,
    pub(crate) remaining_peers: usize,
}

// PeerContextRegistry 按 peer_id 管理本轮 session 内所有 peer 的完整上下文。
//
// 这样命令处理器、router 和 session 收尾逻辑都只需要查这一张表，
// 不再分别维护“媒体表”和“网络出口表”。
pub(crate) struct PeerContextRegistry {
    peers: Mutex<HashMap<PeerId, PeerContext>>,
}

impl PeerContextRegistry {
    // 创建一个空的 peer context 注册表。
    //
    // 初始状态下当前 session 内还没有任何在线 peer。
    //
    // 入参说明：
    // - 无
    pub(crate) fn new() -> Self {
        Self {
            peers: Mutex::new(HashMap::new()),
        }
    }

    // 为一个新接入的 peer 创建完整上下文。
    //
    // 入参说明：
    // - peer_id：该 peer 在当前 session 内的唯一编号
    // - source：该 peer 的接入来源
    // - control_sender：发往该 peer 的控制消息出口
    // - audio_sender：发往该 peer 的音频流出口
    pub(crate) fn add_peer(
        &self,
        peer_id: PeerId,
        source: PeerSource,
        control_sender: NotifyingSender<OutboundControl>,
        audio_sender: NotifyingSender<Vec<u8>>,
    ) {
        // 参数说明：
        // - peer_id：作为当前 session 内的路由键
        // - PeerContext::new(...)：一次性组装该 peer 的媒体对象和网络出口
        self.peers
            .lock()
            .expect("peer context registry poisoned")
            .insert(
                peer_id,
                PeerContext::new(source, control_sender, audio_sender),
            );
    }

    // 在 websocket reader / writer 成功启动后，把它们对应的强制关闭句柄补进 peer context。
    //
    // 入参说明：
    // - peer_id：要安装 ws_handle 的目标 peer
    // - ws_handle：该 peer 对应的 websocket 强制关闭句柄
    pub(crate) fn install_ws_handle(
        &self,
        peer_id: PeerId,
        ws_handle: Arc<WsPeerHandle>,
    ) -> Result<(), AppError> {
        let mut peers = self.peers.lock().expect("peer context registry poisoned");
        let peer = peers
            .get_mut(&peer_id)
            .ok_or_else(|| anyhow::anyhow!("peer context not found: {peer_id}"))?;
        if peer.ws_handle.is_some() {
            return Err(anyhow::anyhow!(
                "ws handle already installed for peer {peer_id}"
            ));
        }
        peer.ws_handle = Some(ws_handle);
        Ok(())
    }

    // 返回某个 peer 对应的完整上下文。
    //
    // 入参说明：
    // - peer_id：目标 peer 编号
    pub(crate) fn get(&self, peer_id: PeerId) -> Option<PeerContext> {
        self.peers
            .lock()
            .expect("peer context registry poisoned")
            .get(&peer_id)
            .cloned()
    }

    // 移除某个 peer，并把它的上下文交还给调用方做收尾。
    //
    // 入参说明：
    // - peer_id：要移除的目标 peer 编号
    pub(crate) fn remove_peer(&self, peer_id: PeerId) -> Option<RemovedPeer> {
        let mut peers = self.peers.lock().expect("peer context registry poisoned");
        let context = peers.remove(&peer_id)?;
        Some(RemovedPeer {
            context,
            remaining_peers: peers.len(),
        })
    }

    // 在 session 整体关闭时一次性取出所有 peer 的上下文，交给上层统一 stop / close。
    //
    // 入参说明：
    // - self：当前 session 的 peer context 注册表
    pub(crate) fn drain(&self) -> Vec<PeerContext> {
        self.peers
            .lock()
            .expect("peer context registry poisoned")
            .drain()
            .map(|(_, context)| context)
            .collect()
    }

    // 返回当前 session 内仍然在线的 peer 数量。
    //
    // 入参说明：
    // - self：当前 session 的 peer context 注册表
    pub(crate) fn peer_count(&self) -> usize {
        self.peers
            .lock()
            .expect("peer context registry poisoned")
            .len()
    }

    // 统计当前 session 内处于 outbound connect 来源的 peer 数量。
    //
    // 入参说明：
    // - self：当前 session 的 peer context 注册表
    pub(crate) fn outbound_peer_count(&self) -> usize {
        self.peers
            .lock()
            .expect("peer context registry poisoned")
            .values()
            .filter(|peer| matches!(peer.source, PeerSource::OutboundConnect { .. }))
            .count()
    }

    // 给当前 session 内所有 peer 尽量发送一条 Close 指令。
    //
    // 入参说明：
    // - self：当前 session 的 peer context 注册表
    pub(crate) fn close_all_peers(&self) {
        let senders = {
            let peers = self.peers.lock().expect("peer context registry poisoned");
            peers
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
    // 入参说明：
    // - peer_id：目标 peer 编号
    pub(crate) fn try_close_peer(&self, peer_id: PeerId) -> bool {
        let sender = {
            let peers = self.peers.lock().expect("peer context registry poisoned");
            peers.get(&peer_id).map(|peer| peer.control_sender.clone())
        };

        let Some(sender) = sender else {
            debug_log(
                "peer-context",
                format!("Skip graceful close because peer {peer_id} is already gone"),
            );
            return false;
        };

        match sender.try_send(OutboundControl::Close) {
            Ok(()) => {
                debug_log(
                    "peer-context",
                    format!("Graceful close frame queued for peer {peer_id}"),
                );
                true
            }
            Err(err) => {
                debug_err_log(
                    "peer-context",
                    format!("Failed to queue graceful close for peer {peer_id}: {err}"),
                );
                false
            }
        }
    }

    // 按目标语义分发一条控制消息。
    //
    // 入参说明：
    // - target：目标范围，决定是广播还是单播
    // - outbound：要分发的控制消息
    pub(crate) fn send_control(
        &self,
        target: OutboundTarget,
        outbound: OutboundControl,
    ) -> Result<(), AppError> {
        match target {
            OutboundTarget::Broadcast => {
                // 广播时先把 sender clone 出来，避免在真正 send 时长时间持锁。
                let senders = {
                    let peers = self.peers.lock().expect("peer context registry poisoned");
                    peers
                        .values()
                        .map(|peer| peer.control_sender.clone())
                        .collect::<Vec<_>>()
                };
                for sender in senders {
                    // 参数说明：
                    // - outbound.clone()：广播模式下，每个 peer 都要拿到一份独立消息副本
                    sender.send(outbound.clone()).map_err(|err| {
                        debug_err_log(
                            "peer-context",
                            format!("Failed to broadcast outbound control message: {err}"),
                        );
                        anyhow::Error::from(err)
                    })?;
                }
            }
            OutboundTarget::ToPeer(peer_id) => {
                // 单播时先尝试查找目标 peer；如果 peer 已经离开，这里按“最佳努力”静默丢弃。
                let sender = {
                    let peers = self.peers.lock().expect("peer context registry poisoned");
                    peers.get(&peer_id).map(|peer| peer.control_sender.clone())
                };
                if let Some(sender) = sender {
                    // 参数说明：
                    // - outbound：只发给目标 peer 的控制消息
                    sender.send(outbound).map_err(|err| {
                        debug_err_log(
                            "peer-context",
                            format!(
                                "Failed to send outbound control message to peer {peer_id}: {err}"
                            ),
                        );
                        anyhow::Error::from(err)
                    })?;
                } else {
                    debug_log(
                        "peer-context",
                        format!("Outbound control message dropped because peer {peer_id} is gone"),
                    );
                }
            }
        }
        Ok(())
    }

    // 提供给 router 使用的更业务化出站入口。
    //
    // 入参说明：
    // - outbound：已经封装好目标和消息体的出站动作
    pub(crate) fn dispatch_outbound(&self, outbound: RoutedOutbound) -> Result<(), AppError> {
        // 参数说明：
        // - outbound.target：决定广播还是单播
        // - outbound.message：真正要发往 websocket writer 的控制消息
        self.send_control(outbound.target, outbound.message)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, mpsc};

    use crate::transport::{OutboundControl, OutboundTarget, PeerSource, WriteSignal};

    use super::PeerContextRegistry;

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

    #[test]
    fn routes_broadcast_and_single_peer_messages() {
        let peers = PeerContextRegistry::new();
        let (control_1, rx_1) = make_control_sender();
        let (control_2, rx_2) = make_control_sender();
        let (audio_1, _) = make_audio_sender();
        let (audio_2, _) = make_audio_sender();

        peers.add_peer(1, PeerSource::Listener, control_1, audio_1);
        peers.add_peer(2, PeerSource::Listener, control_2, audio_2);

        peers
            .send_control(
                OutboundTarget::Broadcast,
                OutboundControl::Text("broadcast".to_string()),
            )
            .unwrap();
        peers
            .send_control(
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
    fn returns_audio_sender_for_active_peer() {
        let peers = PeerContextRegistry::new();
        let (control_2, _) = make_control_sender();
        let (audio_1, _) = make_audio_sender();
        let (audio_2, rx_2) = make_audio_sender();

        peers.add_peer(2, PeerSource::Listener, control_2, audio_2);
        peers.add_peer(1, PeerSource::Listener, make_control_sender().0, audio_1);

        let sender = peers.get(2).expect("peer 2 context").audio_sender();
        sender.try_send(vec![1, 2, 3]).unwrap();

        assert_eq!(rx_2.recv().unwrap(), vec![1, 2, 3]);
        assert!(peers.remove_peer(2).is_some());
        assert!(peers.get(2).is_none());
    }
}
