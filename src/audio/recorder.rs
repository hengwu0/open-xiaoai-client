use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, LazyLock, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use speexdsp_resampler::State as SpeexResampler;

use super::{AUDIO_CONFIG, AudioConfig};
use crate::base::{AppError, debug_err_log, debug_log, debug_log_limited};
use crate::protocol::Stream;
use crate::transport::NotifyingSender;

const A113_CAPTURE_BITS_PER_SAMPLE: u16 = 32;
const FAST_RECORDING_INPUT_CHANNELS: usize = 8;
const FAST_RECORDING_KWS_CHANNELS: usize = 3;
const FAST_RECORDING_RAW_CHANNELS: usize = 2;
const FAST_RECORDING_KWS_SELECTED_CHANNELS: [usize; FAST_RECORDING_KWS_CHANNELS] = [0, 2, 4];
const FAST_RECORDING_RAW_SELECTED_CHANNELS: [usize; FAST_RECORDING_RAW_CHANNELS] = [0, 6];
const FAST_RECORDING_INPUT_RATE: usize = 48_000;
const FAST_RECORDING_OUTPUT_RATE: usize = 16_000;
const FAST_RECORDING_RESAMPLE_QUALITY: usize = 8;
static FAST_RECORDING_CONFIG: LazyLock<AudioConfig> = LazyLock::new(|| AudioConfig {
    pcm: "noop".into(),
    channels: FAST_RECORDING_INPUT_CHANNELS as u16,
    bits_per_sample: 32,
    sample_rate: FAST_RECORDING_INPUT_RATE as u32,
    period_size: 384,
    buffer_size: 6_144,
});

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArecordProfile {
    Standard,
    Fast,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FastRecordingMode {
    Kws = 0,
    LlmRaw = 1,
}

impl FastRecordingMode {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::LlmRaw,
            _ => Self::Kws,
        }
    }
}

struct FastRecordingPipeline {
    resamplers: [SpeexResampler; FAST_RECORDING_KWS_CHANNELS],
}

impl FastRecordingPipeline {
    fn new() -> Result<Self, AppError> {
        let mut resamplers = [
            create_fast_resampler()?,
            create_fast_resampler()?,
            create_fast_resampler()?,
        ];
        // 首包时跳过 resampler 内部前导零，避免刚起录音时额外插入静音。
        for resampler in &mut resamplers {
            resampler.skip_zeros();
        }
        Ok(Self { resamplers })
    }

    fn process_chunk(
        &mut self,
        chunk: &[u8],
        mode: FastRecordingMode,
    ) -> Result<Vec<u8>, AppError> {
        match mode {
            FastRecordingMode::Kws => {
                let extracted_channels = extract_fast_channels_as_s16(chunk)?;
                let mut resampled_channels = Vec::with_capacity(FAST_RECORDING_KWS_CHANNELS);
                for (index, samples) in extracted_channels.into_iter().enumerate() {
                    resampled_channels.push(resample_fast_channel(
                        &mut self.resamplers[index],
                        &samples,
                    )?);
                }
                Ok(interleave_s16_channels(&resampled_channels))
            }
            FastRecordingMode::LlmRaw => extract_fast_raw_channels_s32(chunk),
        }
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
    fast_mode: Arc<AtomicU8>,
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
            fast_mode: Arc::new(AtomicU8::new(FastRecordingMode::Kws as u8)),
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
    // fast profile 对应的 arecord 启动参数固定为：
    // - `arecord -D noop -f S32_LE -r 48000 -c 8 --quiet -t raw`
    //
    // 入参说明：
    // - self：当前 peer 自己的录音器实例
    // - audio_output：当前 peer 的音频输出发送端，录音线程会把编码后的 payload 发到这里
    pub fn start_fast_recording(
        &self,
        audio_output: NotifyingSender<Vec<u8>>,
    ) -> Result<(), AppError> {
        self.start_recording_with_profile(
            (*FAST_RECORDING_CONFIG).clone(),
            ArecordProfile::Fast,
            audio_output,
        )
    }

    // switch_to_llm_start_audio 将 fast_recording 从 KWS 三通道输出切到 LLM 会话 raw 双通道输出。
    //
    // 入参说明：
    // - self：当前 peer 自己的录音器实例
    pub fn switch_to_llm_start_audio(&self) {
        self.fast_mode
            .store(FastRecordingMode::LlmRaw as u8, Ordering::SeqCst);
    }

    // switch_to_llm_stop_audio 将 fast_recording 切回唤醒前 KWS 三通道输出。
    //
    // 入参说明：
    // - self：当前 peer 自己的录音器实例
    pub fn switch_to_llm_stop_audio(&self) {
        self.fast_mode
            .store(FastRecordingMode::Kws as u8, Ordering::SeqCst);
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
        if matches!(profile, ArecordProfile::Fast) {
            self.fast_mode
                .store(FastRecordingMode::Kws as u8, Ordering::SeqCst);
        }
        // 标准录音仍然保留设备兼容转换；fast profile 则直接按固定参数采集。
        let capture = match profile {
            ArecordProfile::Standard => capture_config_for_recording(&requested),
            ArecordProfile::Fast => requested.clone(),
        };
        let mut fast_pipeline = match profile {
            ArecordProfile::Standard => None,
            ArecordProfile::Fast => Some(FastRecordingPipeline::new()?),
        };
        let fast_mode = self.fast_mode.clone();
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
                                ArecordProfile::Fast => {
                                    let mode = FastRecordingMode::from_u8(fast_mode.load(Ordering::SeqCst));
                                    match fast_pipeline
                                        .as_mut()
                                        .expect("fast pipeline must exist for fast profile")
                                        .process_chunk(&chunk, mode)
                                    {
                                        Ok(bytes) => bytes,
                                        Err(err) => {
                                            debug_err_log(
                                                "audio-recorder",
                                                format!(
                                                    "Failed to process fast recording chunk: {err}"
                                                ),
                                            );
                                            break;
                                        }
                                    }
                                }
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

fn create_fast_resampler() -> Result<SpeexResampler, AppError> {
    SpeexResampler::new(
        1,
        FAST_RECORDING_INPUT_RATE,
        FAST_RECORDING_OUTPUT_RATE,
        FAST_RECORDING_RESAMPLE_QUALITY,
    )
    .map_err(|err| anyhow::anyhow!("create speex resampler failed: {err:?}"))
}

fn extract_fast_channels_as_s16(
    chunk: &[u8],
) -> Result<[Vec<i16>; FAST_RECORDING_KWS_CHANNELS], AppError> {
    let bytes_per_sample = std::mem::size_of::<i32>();
    let input_frame_size = FAST_RECORDING_INPUT_CHANNELS * bytes_per_sample;
    if !chunk.len().is_multiple_of(input_frame_size) {
        return Err(anyhow::anyhow!(
            "fast recording chunk size {} is not aligned to {}-channel S32 frames",
            chunk.len(),
            FAST_RECORDING_INPUT_CHANNELS
        ));
    }

    let frame_count = chunk.len() / input_frame_size;
    let mut channels = std::array::from_fn(|_| Vec::with_capacity(frame_count));
    for frame in chunk.chunks_exact(input_frame_size) {
        for (output_channel_index, input_channel_index) in FAST_RECORDING_KWS_SELECTED_CHANNELS
            .iter()
            .copied()
            .enumerate()
        {
            let base = input_channel_index * bytes_per_sample;
            let sample = i32::from_le_bytes([
                frame[base],
                frame[base + 1],
                frame[base + 2],
                frame[base + 3],
            ]);
            channels[output_channel_index].push(convert_a113_s32_sample_to_s16(sample));
        }
    }
    Ok(channels)
}

fn extract_fast_raw_channels_s32(chunk: &[u8]) -> Result<Vec<u8>, AppError> {
    let bytes_per_sample = std::mem::size_of::<i32>();
    let input_frame_size = FAST_RECORDING_INPUT_CHANNELS * bytes_per_sample;
    if !chunk.len().is_multiple_of(input_frame_size) {
        return Err(anyhow::anyhow!(
            "fast recording raw chunk size {} is not aligned to {}-channel S32 frames",
            chunk.len(),
            FAST_RECORDING_INPUT_CHANNELS
        ));
    }

    let frame_count = chunk.len() / input_frame_size;
    let mut out = Vec::with_capacity(frame_count * FAST_RECORDING_RAW_CHANNELS * bytes_per_sample);
    for frame in chunk.chunks_exact(input_frame_size) {
        for input_channel_index in FAST_RECORDING_RAW_SELECTED_CHANNELS {
            let base = input_channel_index * bytes_per_sample;
            out.extend_from_slice(&frame[base..base + bytes_per_sample]);
        }
    }
    Ok(out)
}

fn resample_fast_channel(
    resampler: &mut SpeexResampler,
    input: &[i16],
) -> Result<Vec<i16>, AppError> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let input_f32 = input
        .iter()
        .map(|&sample| sample as f32)
        .collect::<Vec<_>>();
    let mut consumed_total = 0usize;
    let mut output_i16 = Vec::with_capacity(estimate_fast_output_samples(input.len()));

    while consumed_total < input_f32.len() {
        let remaining = &input_f32[consumed_total..];
        let mut output =
            vec![
                0.0_f32;
                estimate_fast_output_samples(remaining.len()) + resampler.get_output_latency() + 32
            ];
        let (consumed, produced) = resampler
            .process_float(0, remaining, &mut output)
            .map_err(|err| anyhow::anyhow!("speex resample failed: {err:?}"))?;
        if consumed == 0 {
            return Err(anyhow::anyhow!(
                "speex resampler made no progress while processing fast recording"
            ));
        }
        consumed_total += consumed;
        output_i16.extend(
            output[..produced]
                .iter()
                .map(|&sample| convert_fast_f32_to_s16(sample)),
        );
    }

    Ok(output_i16)
}

fn estimate_fast_output_samples(input_samples: usize) -> usize {
    (input_samples * FAST_RECORDING_OUTPUT_RATE).div_ceil(FAST_RECORDING_INPUT_RATE)
}

fn convert_fast_f32_to_s16(sample: f32) -> i16 {
    sample.round().clamp(i16::MIN as f32, i16::MAX as f32) as i16
}

fn interleave_s16_channels(channels: &[Vec<i16>]) -> Vec<u8> {
    let Some(frame_count) = channels.iter().map(|channel| channel.len()).min() else {
        return Vec::new();
    };

    if channels.iter().any(|channel| channel.len() != frame_count) {
        debug_err_log(
            "audio-recorder",
            "Fast recording channel lengths diverged during resampling; trimming to shortest",
        );
    }

    let mut out = Vec::with_capacity(frame_count * channels.len() * std::mem::size_of::<i16>());
    for frame_index in 0..frame_count {
        for channel in channels {
            out.extend_from_slice(&channel[frame_index].to_le_bytes());
        }
    }
    out
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
    if matches!(profile, ArecordProfile::Standard) {
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
        ArecordProfile, AudioRecorder, FAST_RECORDING_CONFIG, FAST_RECORDING_INPUT_CHANNELS,
        FAST_RECORDING_KWS_CHANNELS, FastRecordingMode, FastRecordingPipeline, build_arecord_args,
        extract_fast_channels_as_s16, extract_fast_raw_channels_s32,
    };
    use crate::audio::AudioConfig;
    use std::sync::atomic::Ordering;

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
    fn fast_arecord_args_match_expected_profile() {
        let config = (*FAST_RECORDING_CONFIG).clone();

        let args = build_arecord_args(&config, ArecordProfile::Fast);

        assert_eq!(
            args,
            vec![
                "--quiet", "-t", "raw", "-D", "noop", "-f", "S32_LE", "-r", "48000", "-c", "8",
            ]
        );
    }

    #[test]
    fn fast_channel_extraction_keeps_kws_channels_and_converts_to_s16() {
        let samples = [
            [1_i32, 2, 3, 4, 5, 6, 7, 8],
            [11_i32, 12, 13, 14, 15, 16, 17, 18],
        ];
        let mut chunk = Vec::new();
        for frame in samples {
            for sample in frame {
                chunk.extend_from_slice(&(sample << 8).to_le_bytes());
            }
        }

        let extracted = extract_fast_channels_as_s16(&chunk).unwrap();

        assert_eq!(extracted[0], vec![1, 11]);
        assert_eq!(extracted[1], vec![3, 13]);
        assert_eq!(extracted[2], vec![5, 15]);
    }

    #[test]
    fn fast_raw_extraction_keeps_ch0_and_ch6_as_s32_without_shift() {
        let samples = [
            [0x0001_0203_i32, 2, 3, 4, 5, 6, 0x0007_0809, 8],
            [0x0011_1213_i32, 12, 13, 14, 15, 16, 0x0017_1819, 18],
        ];
        let mut chunk = Vec::new();
        for frame in samples {
            for sample in frame {
                chunk.extend_from_slice(&sample.to_le_bytes());
            }
        }

        let raw = extract_fast_raw_channels_s32(&chunk).unwrap();

        let expected = [0x0001_0203_i32, 0x0007_0809, 0x0011_1213, 0x0017_1819];
        let actual = raw
            .chunks_exact(std::mem::size_of::<i32>())
            .map(|bytes| {
                let sample: [u8; 4] = bytes.try_into().unwrap();
                i32::from_le_bytes(sample)
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn fast_pipeline_outputs_three_channel_kws_pcm_before_llm_start() {
        let frames = FAST_RECORDING_CONFIG.buffer_size as usize;
        let mut chunk =
            Vec::with_capacity(frames * FAST_RECORDING_INPUT_CHANNELS * std::mem::size_of::<i32>());
        for _ in 0..frames * FAST_RECORDING_INPUT_CHANNELS {
            chunk.extend_from_slice(&0_i32.to_le_bytes());
        }

        let mut pipeline = FastRecordingPipeline::new().unwrap();
        let pcm = pipeline
            .process_chunk(&chunk, FastRecordingMode::Kws)
            .unwrap();

        assert!(!pcm.is_empty());
        assert!(pcm.len().is_multiple_of(FAST_RECORDING_KWS_CHANNELS * 2));
        assert!(pcm.iter().all(|&byte| byte == 0));
    }

    #[test]
    fn fast_pipeline_outputs_raw_two_channel_s32_after_llm_start() {
        let frames = 2usize;
        let mut chunk =
            Vec::with_capacity(frames * FAST_RECORDING_INPUT_CHANNELS * std::mem::size_of::<i32>());
        for frame in 0..frames {
            for channel in 0..FAST_RECORDING_INPUT_CHANNELS {
                let sample = (frame as i32) * 100 + channel as i32;
                chunk.extend_from_slice(&sample.to_le_bytes());
            }
        }

        let mut pipeline = FastRecordingPipeline::new().unwrap();
        let pcm = pipeline
            .process_chunk(&chunk, FastRecordingMode::LlmRaw)
            .unwrap();

        let actual = pcm
            .chunks_exact(std::mem::size_of::<i32>())
            .map(|bytes| {
                let sample: [u8; 4] = bytes.try_into().unwrap();
                i32::from_le_bytes(sample)
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![0, 6, 100, 106]);
    }

    #[test]
    fn fast_mode_returns_to_kws_after_llm_stop() {
        // 验证 llm_start 后切 raw 双通道，llm_stop 后必须回到三通道 KWS 模式。
        let recorder = AudioRecorder::new();

        recorder.switch_to_llm_start_audio();
        assert_eq!(
            FastRecordingMode::from_u8(recorder.fast_mode.load(Ordering::SeqCst)),
            FastRecordingMode::LlmRaw
        );

        recorder.switch_to_llm_stop_audio();
        assert_eq!(
            FastRecordingMode::from_u8(recorder.fast_mode.load(Ordering::SeqCst)),
            FastRecordingMode::Kws
        );
    }
}
