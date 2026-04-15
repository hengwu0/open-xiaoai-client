use std::collections::HashMap;
use std::sync::Mutex;

use crate::base::{AppError, debug_err_log, debug_log};
use crate::transport::{
    NotifyingSender, OutboundControl, OutboundTarget, PeerId, PeerSource, RoutedOutbound,
};

// WsPeerHub 是“session 内所有网络出口的统一抽象”。
//
// 它主要承担三类职责：
// 1. 维护当前有哪些 peer 还活着
// 2. 按 Broadcast / ToPeer 的语义分发出站控制消息
// 3. 提供每个 peer 对应的音频发送句柄，供 peer 自己的 recorder 直连写出
//
// 这样 router / command handler / supervisor 都不需要自己维护一份 peer 表。
pub struct WsPeerHub {
    state: Mutex<WsPeerHubState>,
}

// WsPeerHubState 是 WsPeerHub 在锁内维护的真实状态。
//
// 当前只维护“当前在线 peer 集合”。
struct WsPeerHubState {
    // peers 保存“当前 session 还在线”的所有 peer 发送句柄。
    peers: HashMap<PeerId, PeerSenders>,
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
pub struct PeerRemoval {
    // source 会回传给 supervisor，便于它决定是否需要重新打开 outbound connector。
    pub source: PeerSource,
    pub remaining_peers: usize,
}

impl WsPeerHub {
    // 创建一个空的 WsPeerHub。
    //
    // 初始状态下：
    // - 没有任何在线 peer
    //
    // 入参说明：
    // - 无
    pub fn new() -> Self {
        Self {
            state: Mutex::new(WsPeerHubState {
                peers: HashMap::new(),
            }),
        }
    }

    // 向当前 session 注册一个新 peer 的发送句柄。
    //
    // 一旦 add_peer 成功，后续的广播、单播和当前 peer 自己的录音回传都能找到这个 peer。
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
        // 一旦这里插入成功，后面的广播 / 定向发送 / 录音回传就都能看到这个 peer。
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
    // - 回传剩余 peer 数量，供上层决定是否结束整轮 session
    //
    // 入参说明：
    // - self：当前 session 的 peer hub
    // - peer_id：要移除的 peer 编号
    pub fn remove_peer(&self, peer_id: PeerId) -> Option<PeerRemoval> {
        let mut state = self.state.lock().expect("peer hub poisoned");
        let peer = state.peers.remove(&peer_id)?;
        Some(PeerRemoval {
            // 参数说明：
            // - peer.source：把该 peer 的来源回传给上层，便于处理 outbound 状态
            // - state.peers.len()：移除后 session 内剩余的 peer 数量
            source: peer.source,
            remaining_peers: state.peers.len(),
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

    // 返回某个 peer 对应的音频发送句柄，供该 peer 自己的 recorder 使用。
    pub fn audio_sender(&self, peer_id: PeerId) -> Option<NotifyingSender<Vec<u8>>> {
        let state = self.state.lock().expect("peer hub poisoned");
        state
            .peers
            .get(&peer_id)
            .map(|peer| peer.audio_sender.clone())
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
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, mpsc};

    use crate::transport::{OutboundControl, OutboundTarget, PeerSource, WriteSignal};

    use super::WsPeerHub;

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
    // 验证可以拿到单个 peer 的音频发送句柄，且移除后查询不到。
    fn returns_audio_sender_for_active_peer() {
        let hub = WsPeerHub::new();
        let (control_2, _) = make_control_sender();
        let (audio_1, _) = make_audio_sender();
        let (audio_2, rx_2) = make_audio_sender();

        hub.add_peer(2, PeerSource::Listener, control_2, audio_2);
        hub.add_peer(1, PeerSource::Listener, make_control_sender().0, audio_1);

        let sender = hub.audio_sender(2).expect("peer 2 audio sender");
        sender.try_send(vec![1, 2, 3]).unwrap();

        assert_eq!(rx_2.recv().unwrap(), vec![1, 2, 3]);
        assert!(hub.remove_peer(2).is_some());
        assert!(hub.audio_sender(2).is_none());
    }
}
