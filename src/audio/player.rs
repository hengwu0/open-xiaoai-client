use std::io::Write;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::base::{AppError, debug_err_log, debug_log, debug_log_limited};

use super::{AUDIO_CONFIG, AudioConfig};

// AudioPlayer 通过子进程 `aplay` 播放 PCM 原始流。
// 外部只需要先 start，再不断 enqueue 音频块即可。
//
// 内部结构分成两层：
// 1. `aplay` 子进程：真正负责把 PCM 数据交给设备播放
// 2. `audio-player-thread`：专门把上层送来的字节块持续写入 aplay.stdin
//
// 这样做的原因是，上层收到播放流时不应该直接在调用线程里写 stdin，
// 否则一旦写入阻塞，就会把 router 或其他业务线程拖慢。
pub struct AudioPlayer {
    stdin: Mutex<Option<ChildStdin>>,
    child: Mutex<Option<Child>>,
    sender: Mutex<Option<mpsc::SyncSender<Vec<u8>>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl AudioPlayer {
    // new 创建一个尚未启动的播放器实例。
    //
    // 它只准备好内部状态，不会立刻拉起 `aplay` 子进程。
    //
    // 入参说明：
    // - 无
    pub fn new() -> Self {
        // 初始时什么都不启动，等收到 start_play 命令再真正创建播放器。
        Self {
            stdin: Mutex::new(None),
            child: Mutex::new(None),
            sender: Mutex::new(None),
            task: Mutex::new(None),
        }
    }

    // start 负责按给定音频配置启动本地 `aplay` 播放链路。
    //
    // 它会先停止旧播放器，再重新拉起：
    // - 一个 `aplay` 子进程
    // - 一个专门向 aplay.stdin 持续写数据的播放线程
    // - 一个内部小缓冲队列
    //
    // 入参说明：
    // - self：当前共享播放器实例
    // - config：可选播放配置；为空时退回全局默认 AUDIO_CONFIG
    pub fn start(&self, config: Option<AudioConfig>) -> Result<(), AppError> {
        // 每次重新 start 都先做一次完整 stop，避免遗留旧的 aplay 进程和写线程。
        self.stop()?;
        // 服务端可以选择显式传入 AudioConfig，也可以直接复用默认配置。
        let config = config.unwrap_or_else(|| (*AUDIO_CONFIG).clone());
        debug_log(
            "audio-player",
            format!(
                "Starting player: pcm={}, rate={}, channels={}, bits={}",
                config.pcm, config.sample_rate, config.channels, config.bits_per_sample
            ),
        );
        // 参数说明：
        // - Command::new("aplay")：启动系统播放器子进程
        // - .args([...])：按 AudioConfig 拼出 PCM 格式、采样率、声道和缓冲区参数
        // - .stdin(Stdio::piped())：后续由播放线程持续往标准输入写 PCM 数据
        let mut child = Command::new("aplay")
            .args([
                "--quiet",
                "-t",
                "raw",
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
                "-",
            ])
            .stdin(Stdio::piped())
            .spawn()
            .map_err(|err| {
                debug_err_log("audio-player", format!("Failed to spawn aplay: {err}"));
                anyhow::Error::from(err)
            })?;
        let stdin = match child.stdin.take() {
            Some(stdin) => stdin,
            None => {
                // 理论上 stdin(Stdio::piped()) 后这里应该总能拿到，
                // 如果拿不到，说明 aplay 进程状态已经不符合预期。
                let err = anyhow::anyhow!("missing aplay stdin");
                debug_err_log("audio-player", err.to_string());
                return Err(err);
            }
        };
        // 播放队列是“播放器内部的小缓冲”，用来承接服务端持续下发的播放块。
        let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(50);
        // 参数说明：
        // - stdin：当前 aplay 子进程的标准输入写端
        // - rx：播放器内部接收队列；上层 enqueue 后的数据最终会在这里被消费
        let handle = thread::Builder::new()
            .name("audio-player-thread".to_string())
            .spawn(move || {
                // 单独线程负责把接收到的字节块持续写入 aplay 的标准输入。
                let mut stdin = stdin;
                while let Ok(bytes) = rx.recv() {
                    debug_log_limited(
                        "audio-player",
                        "writing-aplay-stdin",
                        Duration::from_secs(60),
                        format!("Writing {} bytes to aplay stdin", bytes.len()),
                    );
                    if let Err(err) = stdin.write_all(&bytes) {
                        debug_err_log(
                            "audio-player",
                            format!(
                                "Failed to write to aplay stdin: {err}; playback thread will exit"
                            ),
                        );
                        break;
                    }
                }
                debug_log("audio-player", "Playback thread exited");
            })?;
        *self.sender.lock().expect("player sender poisoned") = Some(tx);
        *self.child.lock().expect("player child poisoned") = Some(child);
        *self.task.lock().expect("player task poisoned") = Some(handle);
        Ok(())
    }

    // enqueue 把一块播放音频放入播放器内部队列。
    //
    // 它不会直接写 aplay.stdin，而是交给独立播放线程处理，避免阻塞调用方。
    //
    // 入参说明：
    // - self：当前共享播放器实例
    // - bytes：一块待播放的 PCM 原始字节
    pub fn enqueue(&self, bytes: Vec<u8>) -> Result<(), AppError> {
        // 播放未启动时静默忽略，避免因为时序问题让上层直接失败。
        // 例如：
        // - 服务端刚开始推 play 流，但 start_play 还没完全执行完
        // - 当前 session 正在收尾，旧的播放数据仍有少量尾包进入
        if let Some(sender) = self.sender.lock().expect("player sender poisoned").as_ref() {
            debug_log_limited(
                "audio-player",
                "queueing-play-chunk",
                Duration::from_secs(60),
                format!("Queueing play chunk: {} bytes", bytes.len()),
            );
            // 参数说明：
            // - sender.send(bytes)：把播放块送进播放器内部队列，供写线程异步消费
            sender.send(bytes).map_err(|err| {
                debug_err_log(
                    "audio-player",
                    format!("Failed to enqueue play chunk: {err}"),
                );
                anyhow::Error::from(err)
            })?;
        } else {
            debug_log_limited(
                "audio-player",
                "dropping-play-chunk-because-player-is-not-started",
                Duration::from_secs(60),
                format!(
                    "Dropping play chunk because player is not started: {} bytes",
                    bytes.len()
                ),
            );
        }
        Ok(())
    }

    // stop 负责完整停止当前播放器链路。
    //
    // 它会依次：
    // 1. 断开发送端，让播放线程自然退出
    // 2. join 播放线程
    // 3. 回收 aplay 子进程和 stdin
    //
    // 入参说明：
    // - self：当前共享播放器实例
    pub fn stop(&self) -> Result<(), AppError> {
        // 先断开发送端，让写线程自然退出，再回收线程和子进程。
        // 这个顺序比“先 kill 子进程，再停写线程”更稳，
        // 因为写线程会先停止继续往一个无效 stdin 上写数据。
        debug_log("audio-player", "Stopping player");
        self.sender.lock().expect("player sender poisoned").take();
        if let Some(handle) = self.task.lock().expect("player task poisoned").take() {
            let _ = handle.join();
        }
        self.stdin.lock().expect("player stdin poisoned").take();
        if let Some(mut child) = self.child.lock().expect("player child poisoned").take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        debug_log("audio-player", "Player stopped");
        Ok(())
    }
}
