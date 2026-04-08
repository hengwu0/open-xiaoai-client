// audio 模块收敛项目里的本地音频能力：
// - config：统一音频配置结构
// - player：播放链路
// - recorder：录音链路
pub mod config;
pub mod player;
pub mod recorder;

pub use config::{AUDIO_CONFIG, AudioConfig};
pub use player::AudioPlayer;
pub use recorder::AudioRecorder;
pub(crate) use recorder::RecordOutputSender;
