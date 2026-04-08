// transport 模块承载 websocket 适配、协议帧编解码，以及 session 内部消息边界定义。
pub mod codec;
pub mod control;
pub mod ws_pump;

pub use control::{
    InboundMessage, NotifyingSender, OutboundControl, OutboundTarget, PeerId, PeerSource,
    RoutedInbound, RoutedOutbound, SessionControl, WriteSignal, WriteSignalWake,
};
pub use ws_pump::{
    PendingPeer, WsPeerHandle, accept_pending_peer, connect_pending_peer, spawn_peer_tasks,
};
