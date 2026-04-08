use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

// 音频输入输出的统一配置。
// 服务端既可以显式下发，也可以走这里的默认值。
//
// 这些字段会同时影响：
// - player 启动 aplay 的参数
// - recorder 启动 arecord 的参数
// - recorder 内部的分块聚合粒度
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioConfig {
    pub pcm: String,
    pub channels: u16,
    pub bits_per_sample: u16,
    pub sample_rate: u32,
    pub period_size: u32,
    pub buffer_size: u32,
}

// 默认参数尽量对齐旧项目和设备侧常用录放音格式。
// 如果服务端没有下发自定义配置，客户端就会使用这里这套默认值。
pub static AUDIO_CONFIG: LazyLock<AudioConfig> = LazyLock::new(|| AudioConfig {
    pcm: "noop".into(),
    channels: 1,
    bits_per_sample: 16,
    sample_rate: 16000,
    period_size: 160,
    buffer_size: 480,
});
