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
    // pcm：底层 aplay/arecord 使用的 PCM 设备名，例如 `default`、`hw:0,0`、`noop`
    pub pcm: String,
    // channels：声道数
    pub channels: u16,
    // bits_per_sample：单个样本位宽，例如 16 或 32
    pub bits_per_sample: u16,
    // sample_rate：采样率，单位 Hz
    pub sample_rate: u32,
    // period_size：底层音频设备的 period 帧数
    pub period_size: u32,
    // buffer_size：底层音频设备的 buffer 帧数，也是 recorder 对外聚合粒度的重要依据
    pub buffer_size: u32,
}

// 默认参数尽量对齐旧项目和设备侧常用录放音格式。
// 如果服务端没有下发自定义配置，客户端就会使用这里这套默认值。
pub static AUDIO_CONFIG: LazyLock<AudioConfig> = LazyLock::new(|| AudioConfig {
    // 参数说明：
    // - pcm：默认走 noop，交由具体设备兼容逻辑在运行时决定真实采集/播放参数
    pcm: "noop".into(),
    // 参数说明：
    // - channels：默认单声道
    channels: 1,
    // 参数说明：
    // - bits_per_sample：默认使用 32bit，便于兼容当前设备侧录音链路
    bits_per_sample: 32,
    // 参数说明：
    // - sample_rate：默认 48k，与 fast/normal 链路保持一致
    sample_rate: 48000,
    // 参数说明：
    // - period_size：底层读写粒度
    period_size: 384,
    // 参数说明：
    // - buffer_size：上层聚合与设备缓冲目标
    buffer_size: 6144,
});
