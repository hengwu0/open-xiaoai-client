// app 模块收敛客户端进程级编排逻辑：
// - supervisor：全局生命周期主循环
// - session / ws_peer_hub / peer_media：session、peer 与本地音频设备管理
// - commands / fanout / ws_ingress：辅助编排组件
pub mod commands;
pub mod fanout;
pub mod peer_media;
pub mod session;
pub mod supervisor;
pub mod ws_ingress;
pub mod ws_peer_hub;

pub use supervisor::AppSupervisor;
