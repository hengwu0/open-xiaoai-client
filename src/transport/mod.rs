// transport 模块承载 websocket 适配、协议帧编解码，以及 session 内部消息边界定义。
pub mod codec;
pub mod control;
pub mod ws_pump;

// control 负责定义 session 内部消息边界，以及 ws writer 的通知原语。
pub use control::{
    InboundMessage, NotifyingSender, OutboundControl, OutboundTarget, PeerId, PeerSource,
    RoutedInbound, RoutedOutbound, SessionControl, WriteSignal, WriteSignalWake,
};
// ws_pump 负责 websocket accept/connect 与每个 peer 的读写任务生命周期。
pub use ws_pump::{
    PendingPeer, WsPeerHandle, accept_pending_peer, connect_pending_peer, spawn_peer_tasks,
};
