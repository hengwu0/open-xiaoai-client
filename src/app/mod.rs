// app 模块收敛客户端进程级编排逻辑：
// - supervisor：全局生命周期主循环
// - session / peer_context：session、peer 与本地音频设备管理
// - commands / fanout / ws_ingress：辅助编排组件
pub mod capabilities;
pub mod commands;
pub mod fanout;
// peer_context：一张表统一托管单个 peer 的媒体对象、网络 sender 和 ws 关闭句柄。
pub mod peer_context;
pub mod session;
pub mod supervisor;
pub mod ws_ingress;

pub use supervisor::AppSupervisor;
