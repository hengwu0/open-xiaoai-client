// shell 模块承载项目里的本地 shell 调用封装。
pub mod command;
// device：面向当前设备环境的本地识别码与地址辅助逻辑。
pub mod device;
// lx06_audio：LX06 设备 fast_recording 所需 ALSA 配置初始化。
pub mod lx06_audio;
