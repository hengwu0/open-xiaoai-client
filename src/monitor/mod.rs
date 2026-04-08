// monitor 模块承载设备侧本地状态观测能力：
// - file：通用 inotify 文件追踪基础设施
// - instruction / kws / playing：具体业务 monitor
mod file;
pub mod instruction;
pub mod kws;
pub mod playing;

pub(crate) use file::FileMonitorHandle;
