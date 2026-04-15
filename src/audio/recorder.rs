use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::{AUDIO_CONFIG, AudioConfig};
use crate::base::{AppError, debug_err_log, debug_log, debug_log_limited};
use crate::protocol::Stream;
use crate::transport::NotifyingSender;

const A113_CAPTURE_BITS_PER_SAMPLE: u16 = 32;

// AudioRecorder 通过 `arecord` 采集麦克风 PCM 数据。
// 当前实现会把采集到的音频编码成协议里的 Stream 二进制包，交给 ws 线程统一发送。
//
// 这条链路的关键点是：
// 1. arecord 只负责原始采集
// 2. recorder 线程负责聚合、必要时做位宽转换
// 3. 编码后的 Stream 二进制包先进入本地 audio 通道
// 4. 最终由 ws-writer 统一发到网络
//
// 这样录音线程就不需要直接感知网络状态，也不会因为网络抖动阻塞采集。
pub struct AudioRecorder {
    child: Mutex<Option<Child>>,
    task: Mutex<Option<JoinHandle<()>>>,
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
        // 和播放器保持一致：重复 start_recording 时，先停旧链路，再按新配置重启。
        self.stop_recording()?;
        let requested = config.unwrap_or_else(|| (*AUDIO_CONFIG).clone());
        // 某些设备实际采集格式与请求格式不一致，这里先算出底层采集参数。
        let capture = capture_config_for_recording(&requested);
        debug_log(
            "audio-recorder",
            format!(
                "Starting recorder: requested_bits={}, capture_bits={}, rate={}, channels={}",
                requested.bits_per_sample,
                capture.bits_per_sample,
                capture.sample_rate,
                capture.channels
            ),
        );
        // 参数说明：
        // - &capture：底层 arecord 实际使用的采集配置，可能与业务请求配置不同
        let mut child = spawn_arecord(&capture)?;
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
                            let bytes = transform_stream_chunk(chunk, &requested, &capture);
                            if !bytes.is_empty() {
                                debug_log_limited(
                                    "audio-recorder",
                                    "prepared-record-chunk",
                                    Duration::from_secs(60),
                                    format!("Prepared record chunk: {} bytes", bytes.len()),
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

// convert_a113_s32_to_s16 把 A113 设备采集得到的 32bit little-endian PCM 压缩成 16bit PCM。
//
// 入参说明：
// - chunk：按 32bit little-endian 样本排列的原始 PCM 字节序列
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
        // A113 的有效 PDM 数据在低 24 位，因此这里右移 8 位而不是 16 位。
        let mapped = (sample >> 8).clamp(i16::MIN as i32, i16::MAX as i32) as i16;
        let out_base = frame * 2;
        out[out_base..out_base + 2].copy_from_slice(&mapped.to_le_bytes());
    }
    out
}

// spawn_arecord 负责按给定采集参数拉起 `arecord` 子进程。
//
// 入参说明：
// - config：底层要传给 arecord 的音频采集配置
fn spawn_arecord(config: &AudioConfig) -> Result<Child, AppError> {
    // arecord 的 stdout 会被录音线程持续读取。
    // 这里把 arecord.stderr 直接丢弃，是因为调试信息已经统一走应用层日志；
    // 如果后面要排查驱动层异常，再单独把它接出来即可。
    debug_log(
        "audio-recorder",
        format!(
            "Spawning arecord: pcm={}, rate={}, channels={}, bits={}",
            config.pcm, config.sample_rate, config.channels, config.bits_per_sample
        ),
    );
    // 参数说明：
    // - Command::new("arecord")：启动系统录音子进程
    // - .args([...])：按 AudioConfig 生成采样参数
    // - .stdout(Stdio::piped())：让录音线程能够持续读取 PCM 数据
    Ok(Command::new("arecord")
        .args([
            "--quiet",
            "-t",
            "raw",
            "-D",
            &config.pcm,
            "-f",
            &format!("S{}_LE", config.bits_per_sample),
            "-r",
            &config.sample_rate.to_string(),
            "-c",
            &config.channels.to_string(),
            "--buffer-size",
            &config.buffer_size.to_string(),
            "--period-size",
            &config.period_size.to_string(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|err| {
            debug_err_log("audio-recorder", format!("Failed to spawn arecord: {err}"));
            anyhow::Error::from(err)
        })?)
}
