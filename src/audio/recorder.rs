use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::{AudioConfig, AUDIO_CONFIG};
use crate::base::{debug_err_log, debug_log, debug_log_limited, AppError};
use crate::protocol::Stream;
use crate::transport::NotifyingSender;

const A113_CAPTURE_BITS_PER_SAMPLE: u16 = 32;
const FAST_RECORDING_PCM: &str = "lx06_aec_2ch";
const FAST_RECORDING_BITS_PER_SAMPLE: u16 = 16;
const FAST_RECORDING_SAMPLE_RATE: u32 = 16_000;
const FAST_RECORDING_KWS_CHANNELS: u16 = 1;
const FAST_RECORDING_LLM_CHANNELS: u16 = 2;
const FAST_RECORDING_PERIOD_SIZE: u32 = 320;
const FAST_RECORDING_BUFFER_SIZE: u32 = 2_048;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArecordProfile {
    Standard,
    FastKws,
    FastLlm,
}

impl ArecordProfile {
    fn is_standard(self) -> bool {
        matches!(self, Self::Standard)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FastRecordingMode {
    Kws = 0,
    LlmRaw = 1,
}

impl FastRecordingMode {
    fn channel_count(self) -> u16 {
        match self {
            Self::Kws => FAST_RECORDING_KWS_CHANNELS,
            Self::LlmRaw => FAST_RECORDING_LLM_CHANNELS,
        }
    }
}

fn fast_recording_profile_for_mode(mode: FastRecordingMode) -> ArecordProfile {
    match mode {
        FastRecordingMode::Kws => ArecordProfile::FastKws,
        FastRecordingMode::LlmRaw => ArecordProfile::FastLlm,
    }
}

fn fast_recording_config_for_mode(mode: FastRecordingMode) -> AudioConfig {
    AudioConfig {
        pcm: FAST_RECORDING_PCM.to_string(),
        channels: mode.channel_count(),
        bits_per_sample: FAST_RECORDING_BITS_PER_SAMPLE,
        sample_rate: FAST_RECORDING_SAMPLE_RATE,
        period_size: FAST_RECORDING_PERIOD_SIZE,
        buffer_size: FAST_RECORDING_BUFFER_SIZE,
    }
}

// AudioRecorder 通过 `arecord` 采集麦克风 PCM 数据。
// 当前实现会把采集到的音频编码成协议里的 Stream 二进制包，交给 ws 线程统一发送。
//
// 这条链路的关键点是：
// 1. arecord 只负责原始采集
// 2. recorder 线程负责聚合，并按 profile 做格式转换 / 重采样
// 3. 编码后的 Stream 二进制包先进入本地 audio 通道
// 4. 最终由 ws-writer 统一发到网络
//
// 这样录音线程就不需要直接感知网络状态，也不会因为网络抖动阻塞采集。
pub struct AudioRecorder {
    child: Mutex<Option<Child>>,
    task: Mutex<Option<JoinHandle<()>>>,
    profile: Mutex<Option<ArecordProfile>>,
}

impl AudioRecorder {
    // new 创建一个尚未启动的录音器实例。
    //
    // 入参说明：
    // - 无
    pub fn new() -> Self {
        // 和播放器一样，录音器也是按需启动，而不是进程启动时就常驻拉起。
        Self {
            child: Mutex::new(None),
            task: Mutex::new(None),
            profile: Mutex::new(None),
        }
    }

    // start_recording 按给定配置启动本地录音链路。
    //
    // 它会：
    // 0. 如果当前 peer 已经在录音，则先完整停止旧录音链路
    // 1. 确定业务请求配置和底层真实采集配置
    // 2. 拉起 arecord 子进程
    // 3. 启动一个录音线程持续读取 stdout
    // 4. 把录到的 PCM 聚合、必要时转换并编码成 Stream，再投到当前 peer 的音频输出队列
    //
    // 入参说明：
    // - self：当前 peer 自己的录音器实例
    // - config：可选业务录音配置；为空时退回全局默认 AUDIO_CONFIG
    // - audio_output：当前 peer 的音频输出发送端，录音线程会把编码后的 payload 发到这里
    pub fn start_recording(
        &self,
        config: Option<AudioConfig>,
        audio_output: NotifyingSender<Vec<u8>>,
    ) -> Result<(), AppError> {
        let requested = config.unwrap_or_else(|| (*AUDIO_CONFIG).clone());
        self.start_recording_with_profile(requested, ArecordProfile::Standard, audio_output)
    }

    // start_fast_recording 按固定 fast profile 启动本地录音链路。
    //
    // fast profile 初始对应 KWS 阶段，启动参数固定为：
    // - `arecord --quiet -D lx06_aec_2ch -t raw -f S16_LE -r 16000 -c 1`
    //
    // 入参说明：
    // - self：当前 peer 自己的录音器实例
    // - audio_output：当前 peer 的音频输出发送端，录音线程会把编码后的 payload 发到这里
    pub fn start_fast_recording(
        &self,
        audio_output: NotifyingSender<Vec<u8>>,
    ) -> Result<(), AppError> {
        self.start_fast_recording_for_mode(FastRecordingMode::Kws, audio_output)
    }

    fn start_fast_recording_for_mode(
        &self,
        mode: FastRecordingMode,
        audio_output: NotifyingSender<Vec<u8>>,
    ) -> Result<(), AppError> {
        self.start_recording_with_profile(
            fast_recording_config_for_mode(mode),
            fast_recording_profile_for_mode(mode),
            audio_output,
        )
    }

    // switch_to_llm_start_audio 将 fast_recording 从 KWS 单通道采集切到 LLM 会话双通道采集。
    //
    // 入参说明：
    // - self：当前 peer 自己的录音器实例
    // - audio_output：当前 peer 的音频输出发送端，重启后的录音线程继续把音频发回这里
    pub fn switch_to_llm_start_audio(
        &self,
        audio_output: NotifyingSender<Vec<u8>>,
    ) -> Result<(), AppError> {
        self.start_fast_recording_for_mode(FastRecordingMode::LlmRaw, audio_output)
    }

    // switch_to_llm_stop_audio 将 fast_recording 切回唤醒前 KWS 单通道采集。
    //
    // 入参说明：
    // - self：当前 peer 自己的录音器实例
    // - audio_output：当前 peer 的音频输出发送端，重启后的录音线程继续把音频发回这里
    pub fn switch_to_llm_stop_audio(
        &self,
        audio_output: NotifyingSender<Vec<u8>>,
    ) -> Result<(), AppError> {
        self.start_fast_recording_for_mode(FastRecordingMode::Kws, audio_output)
    }

    fn start_recording_with_profile(
        &self,
        requested: AudioConfig,
        profile: ArecordProfile,
        audio_output: NotifyingSender<Vec<u8>>,
    ) -> Result<(), AppError> {
        // start/stop 必须幂等：同一 profile 已经在跑时直接成功，禁止重复 reopen ALSA。
        {
            let profile_guard = self.profile.lock().expect("recorder profile poisoned");
            // 判断是否需要真正打开录音设备。
            // - *profile_guard：当前已经运行的录音 profile
            // - profile：本次请求的录音 profile
            if *profile_guard == Some(profile) {
                debug_log(
                    "audio-recorder",
                    format!(
                        "Recorder already running for profile {:?}; duplicate start ignored",
                        profile
                    ),
                );
                return Ok(());
            }
        }

        // 不同 profile 的 start 仍然先停旧链路，再按新配置启动。
        self.stop_recording()?;
        // 标准录音仍然保留设备兼容转换；fast profile 则直接按固定参数采集。
        let capture = match profile {
            ArecordProfile::Standard => capture_config_for_recording(&requested),
            ArecordProfile::FastKws | ArecordProfile::FastLlm => requested.clone(),
        };
        debug_log(
            "audio-recorder",
            format!(
                "Starting recorder: profile={:?}, requested_bits={}, capture_bits={}, rate={}, channels={}",
                profile,
                requested.bits_per_sample,
                capture.bits_per_sample,
                capture.sample_rate,
                capture.channels
            ),
        );
        // 参数说明：
        // - &capture：底层 arecord 实际使用的采集配置，可能与业务请求配置不同
        let mut child = spawn_arecord(&capture, profile)?;
        let mut stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                let err = anyhow::anyhow!("missing arecord stdout");
                debug_err_log("audio-recorder", err.to_string());
                return Err(err);
            }
        };
        // 参数说明：
        // - stdout：arecord 的标准输出，持续产出原始 PCM 数据
        // - requested：业务层真正期望的输出格式
        // - capture：底层真实采集格式，用于决定是否需要转换
        // - audio_output：编码后的录音块最终直接写到当前 peer 的音频队列
        let handle = thread::Builder::new().name("audio-recorder-thread".to_string()).spawn(move || {
            let mut dropped_audio_frames = 0_u64;
            // 底层按 period_size 读数据，但对外按 buffer_size 聚合后再发，减少消息碎片。
            // 可以把它理解成：
            // - period_size：底层采集粒度
            // - buffer_size：对外上报粒度
            //
            // 先小粒度读，再大粒度发，可以兼顾采集稳定性和网络消息数量。
            let bytes_per_sample = (capture.bits_per_sample.max(8) / 8) as usize;
            let bytes_per_frame = bytes_per_sample * capture.channels.max(1) as usize;
            let target_frames = capture.buffer_size.max(1) as usize;
            let read_frames = capture.period_size.max(1) as usize;
            let target_size = target_frames * bytes_per_frame;
            let read_size = read_frames * bytes_per_frame;
            let mut accumulated = Vec::with_capacity(target_size * 2);
            let mut buffer = vec![0u8; read_size];
            loop {
                match stdout.read(&mut buffer) {
                    Ok(0) => break,
                    Err(err) => {
                        debug_err_log("audio-recorder", format!("Failed to read from arecord stdout: {err}"));
                        break;
                    }
                    Ok(size) => {
                        accumulated.extend_from_slice(&buffer[..size]);
                        while accumulated.len() >= target_size {
                            let chunk = accumulated.drain(..target_size).collect::<Vec<u8>>();
                            // 如果设备底层采的是 32bit，而上层需要 16bit，这里统一做一次转换。
                            // 参数说明：
                            // - chunk：一块按 target_size 聚合好的原始 PCM 数据
                            // - &requested：业务层要求的输出格式
                            // - &capture：底层真实采集格式
                            let bytes = match profile {
                                ArecordProfile::Standard => {
                                    transform_stream_chunk(chunk, &requested, &capture)
                                }
                                ArecordProfile::FastKws | ArecordProfile::FastLlm => chunk,
                            };
                            if !bytes.is_empty() {
                                debug_log_limited(
                                    "audio-recorder",
                                    "prepared-record-chunk",
                                    Duration::from_secs(60),
                                    format!(
                                        "Prepared record chunk: profile={:?}, {} bytes",
                                        profile,
                                        bytes.len()
                                    ),
                                );
                                // 这里编码成协议里的 Stream 对象，而不是裸 PCM。
                                // 这样 ws-writer 收到后不需要再知道“这是什么音频语义”，直接发即可。
                                // 参数说明：
                                // - Stream::new("record", bytes, None)：把录音块包装成统一协议流对象
                                // - serde_json::to_vec(...)：编码成 websocket binary frame 的负载字节
                                let payload = serde_json::to_vec(&Stream::new("record", bytes, None)).unwrap();
                                // 队列满时不阻塞录音线程，而是记一次丢帧统计。
                                // 参数说明：
                                // - audio_output.try_send(payload)：尝试把录音块投递给当前 peer 的音频队列
                                match audio_output.try_send(payload) {
                                    Ok(()) => {}
                                    Err(mpsc::TrySendError::Full(_payload)) => {
                                        dropped_audio_frames += 1;
                                        debug_err_log(
                                            "audio-recorder",
                                            format!("Dropped record chunk because queue is full; total_dropped={}", dropped_audio_frames),
                                        );
                                    }
                                    Err(mpsc::TrySendError::Disconnected(_payload)) => {
                                        debug_err_log("audio-recorder", "Audio stream channel disconnected; recorder thread will exit");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            debug_log("audio-recorder", "Recorder thread exited");
        })?;
        *self.child.lock().expect("recorder child poisoned") = Some(child);
        *self.task.lock().expect("recorder task poisoned") = Some(handle);
        *self.profile.lock().expect("recorder profile poisoned") = Some(profile);
        Ok(())
    }

    // stop_recording 停止当前录音链路。
    //
    // 它会先结束 arecord 子进程，再等待录音线程自然退出。
    //
    // 入参说明：
    // - self：当前 peer 自己的录音器实例
    pub fn stop_recording(&self) -> Result<(), AppError> {
        // 录音停止时先结束子进程，再等待线程退出。
        // 因为线程本身依赖 arecord.stdout 持续读数据，子进程结束后它会自然走到退出路径。
        debug_log("audio-recorder", "Stopping recorder");
        if let Some(mut child) = self.child.lock().expect("recorder child poisoned").take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(handle) = self.task.lock().expect("recorder task poisoned").take() {
            let _ = handle.join();
        }
        *self.profile.lock().expect("recorder profile poisoned") = None;
        debug_log("audio-recorder", "Recorder stopped");
        Ok(())
    }
}

// capture_config_for_recording 根据业务请求录音配置，计算底层真实采集配置。
//
// 某些设备底层更适合以 32bit 采集，再在用户态压回 16bit；这里就是这层兼容的收口点。
//
// 入参说明：
// - requested：业务层想要的录音输出配置
fn capture_config_for_recording(requested: &AudioConfig) -> AudioConfig {
    // 这里返回的是“底层真实采集参数”，不一定等于业务上想要的最终输出参数。
    let mut capture = requested.clone();
    // A113 设备的采集链路更适合先用 32bit 抓，再在用户态压回 16bit。
    if requested.bits_per_sample == 16 {
        capture.bits_per_sample = A113_CAPTURE_BITS_PER_SAMPLE;
    }
    capture
}

// transform_stream_chunk 负责把一块采集到的 PCM 数据转换成业务层期望的格式。
//
// 当前只处理一类特殊兼容路径：A113 设备用 32bit 采集，但业务侧仍然要 16bit 输出。
//
// 入参说明：
// - chunk：一块原始采集数据
// - requested：业务层期望的最终输出格式
// - capture：底层实际采集格式
fn transform_stream_chunk(
    chunk: Vec<u8>,
    requested: &AudioConfig,
    capture: &AudioConfig,
) -> Vec<u8> {
    // 只有在“业务想要 16bit，但设备底层更适合采 32bit”这个组合下，
    // 才需要做位宽转换。其他情况直接透传即可。
    if requested.bits_per_sample != 16 || capture.bits_per_sample != A113_CAPTURE_BITS_PER_SAMPLE {
        return chunk;
    }
    convert_a113_s32_to_s16(&chunk)
}

// convert_a113_s32_to_s16 把 A113 设备采集得到的 32bit little-endian PCM 转换成 16bit PCM。
//
// 入参说明：
// - chunk：按 32bit little-endian 样本排列的原始 PCM 字节序列
fn convert_a113_s32_sample_to_s16(sample: i32) -> i16 {
    // A113 的有效 PDM 数据在低 24 位，因此这里右移 8 位而不是 16 位。
    (sample >> 8).clamp(i16::MIN as i32, i16::MAX as i32) as i16
}

fn convert_a113_s32_to_s16(chunk: &[u8]) -> Vec<u8> {
    // 输入数据必须是完整的 32bit little-endian 样本序列。
    if !chunk.len().is_multiple_of(4) {
        return Vec::new();
    }
    let mut out = vec![0u8; (chunk.len() / 4) * 2];
    for frame in 0..(chunk.len() / 4) {
        let base = frame * 4;
        let sample = i32::from_le_bytes([
            chunk[base],
            chunk[base + 1],
            chunk[base + 2],
            chunk[base + 3],
        ]);
        let mapped = convert_a113_s32_sample_to_s16(sample);
        let out_base = frame * 2;
        out[out_base..out_base + 2].copy_from_slice(&mapped.to_le_bytes());
    }
    out
}

// spawn_arecord 负责按给定采集参数拉起 `arecord` 子进程。
//
// 入参说明：
// - config：底层要传给 arecord 的音频采集配置
fn spawn_arecord(config: &AudioConfig, profile: ArecordProfile) -> Result<Child, AppError> {
    // arecord 的 stdout 会被录音线程持续读取。
    // 这里把 arecord.stderr 直接丢弃，是因为调试信息已经统一走应用层日志；
    // 如果后面要排查驱动层异常，再单独把它接出来即可。
    debug_log(
        "audio-recorder",
        format!(
            "Spawning arecord: profile={:?}, pcm={}, rate={}, channels={}, bits={}",
            profile, config.pcm, config.sample_rate, config.channels, config.bits_per_sample
        ),
    );
    let args = build_arecord_args(config, profile);
    // 参数说明：
    // - Command::new("arecord")：启动系统录音子进程
    // - .args(args)：按当前录音 profile 生成采样参数
    // - .stdout(Stdio::piped())：让录音线程能够持续读取 PCM 数据
    Ok(Command::new("arecord")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| {
            debug_err_log("audio-recorder", format!("Failed to spawn arecord: {err}"));
            anyhow::Error::from(err)
        })?)
}

fn build_arecord_args(config: &AudioConfig, profile: ArecordProfile) -> Vec<String> {
    let mut args = vec![
        "--quiet".to_string(),
        "-t".to_string(),
        "raw".to_string(),
        "-D".to_string(),
        config.pcm.clone(),
        "-f".to_string(),
        format!("S{}_LE", config.bits_per_sample),
        "-r".to_string(),
        config.sample_rate.to_string(),
        "-c".to_string(),
        config.channels.to_string(),
    ];
    if profile.is_standard() {
        args.push("--buffer-size".to_string());
        args.push(config.buffer_size.to_string());
        args.push("--period-size".to_string());
        args.push(config.period_size.to_string());
    }
    args
}

#[cfg(test)]
mod tests {
    use super::{
        build_arecord_args, fast_recording_config_for_mode, fast_recording_profile_for_mode,
        ArecordProfile, FastRecordingMode,
    };
    use crate::audio::AudioConfig;

    #[test]
    fn standard_arecord_args_include_buffer_and_period_sizes() {
        let config = AudioConfig {
            pcm: "noop".to_string(),
            channels: 1,
            bits_per_sample: 16,
            sample_rate: 16_000,
            period_size: 160,
            buffer_size: 480,
        };

        let args = build_arecord_args(&config, ArecordProfile::Standard);

        assert_eq!(
            args,
            vec![
                "--quiet",
                "-t",
                "raw",
                "-D",
                "noop",
                "-f",
                "S16_LE",
                "-r",
                "16000",
                "-c",
                "1",
                "--buffer-size",
                "480",
                "--period-size",
                "160",
            ]
        );
    }

    #[test]
    fn fast_kws_arecord_args_match_expected_profile() {
        let config = fast_recording_config_for_mode(FastRecordingMode::Kws);

        let args = build_arecord_args(
            &config,
            fast_recording_profile_for_mode(FastRecordingMode::Kws),
        );

        assert_eq!(
            args,
            vec![
                "--quiet",
                "-t",
                "raw",
                "-D",
                "lx06_aec_2ch",
                "-f",
                "S16_LE",
                "-r",
                "16000",
                "-c",
                "1",
            ]
        );
    }

    #[test]
    fn fast_llm_arecord_args_match_expected_profile() {
        let config = fast_recording_config_for_mode(FastRecordingMode::LlmRaw);

        let args = build_arecord_args(
            &config,
            fast_recording_profile_for_mode(FastRecordingMode::LlmRaw),
        );

        assert_eq!(
            args,
            vec![
                "--quiet",
                "-t",
                "raw",
                "-D",
                "lx06_aec_2ch",
                "-f",
                "S16_LE",
                "-r",
                "16000",
                "-c",
                "2",
            ]
        );
    }

    #[test]
    fn fast_llm_and_kws_use_distinct_profiles_so_arecord_restarts() {
        assert_ne!(
            fast_recording_profile_for_mode(FastRecordingMode::Kws),
            fast_recording_profile_for_mode(FastRecordingMode::LlmRaw)
        );
        assert_eq!(
            fast_recording_profile_for_mode(FastRecordingMode::Kws),
            ArecordProfile::FastKws
        );
        assert_eq!(
            fast_recording_profile_for_mode(FastRecordingMode::LlmRaw),
            ArecordProfile::FastLlm
        );
    }
}
