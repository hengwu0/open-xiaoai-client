// app 模块收敛客户端进程级编排逻辑：
// - supervisor：全局生命周期主循环
// - session_peer / ws_peer_hub：session 与 peer 资源管理
// - commands / fanout / ws_ingress：辅助编排组件
pub mod commands;
pub mod fanout;
pub mod session_peer;
pub mod supervisor;
pub mod ws_ingress;
pub mod ws_peer_hub;

pub use supervisor::AppSupervisor;
