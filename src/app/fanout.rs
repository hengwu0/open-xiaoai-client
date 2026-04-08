use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};

use crate::app::ws_peer_hub::WsPeerHub;
use crate::audio::{AudioPlayer, AudioRecorder};
use crate::base::{AppError, debug_err_log, debug_log};
use crate::monitor::{
    FileMonitorHandle,
    instruction::spawn_instruction_monitor,
    kws::spawn_kws_monitor,
    playing::{PlayingMonitorHandle, spawn_playing_monitor},
};
use crate::transport::SessionControl;

// MediaDevices 表示一轮 session 内共享的本地音频设备句柄。
//
// 它们不属于某个具体 peer，而是整轮 session 共享：
// - player 负责消费远端下发的播放流
// - recorder 负责产生本地录音流
pub(crate) struct MediaDevices {
    pub(crate) player: Arc<AudioPlayer>,
    pub(crate) recorder: Arc<AudioRecorder>,
}

impl MediaDevices {
    // 创建一组新的 session 级音频设备句柄。
    //
    // 入参说明：
    // - 无
    pub(crate) fn new() -> Self {
        Self {
            player: Arc::new(AudioPlayer::new()),
            recorder: Arc::new(AudioRecorder::new()),
        }
    }
}

// MonitorHandles 用来统一托管一轮 session 内启动的 monitor 线程句柄。
pub(crate) struct MonitorHandles {
    instruction: FileMonitorHandle,
    playing: PlayingMonitorHandle,
    kws: FileMonitorHandle,
}

impl MonitorHandles {
    // 启动当前 session 需要的三个 monitor，并返回它们的线程句柄。
    //
    // 入参说明：
    // - route_channel_writer：monitor 产出的本地事件统一通过它回写到 session 总线
    pub(crate) fn spawn(route_channel_writer: mpsc::SyncSender<SessionControl>) -> Self {
        Self {
            // 参数说明：
            // - route_channel_writer.clone()：instruction 事件通过它写回 session 总线
            // - instruction monitor 通过 close() 关闭内部 inotify fd 来退出
            instruction: spawn_instruction_monitor(route_channel_writer.clone()),
            // 参数说明：
            // - route_channel_writer.clone()：playing 事件通过它写回 session 总线
            // - playing monitor 自己在关闭时会通过 kill `ubus listen` 子进程退出，
            //   因此这里不再依赖通用 monitor_stop。
            playing: spawn_playing_monitor(route_channel_writer.clone()),
            // 参数说明：
            // - route_channel_writer：kws 事件通过它写回 session 总线
            // - kws monitor 通过 close() 关闭内部 inotify fd 来退出
            kws: spawn_kws_monitor(route_channel_writer),
        }
    }

    // 依次 join 三个 monitor 线程，并统一处理它们的退出日志。
    //
    // 入参说明：
    // - self：当前 session 持有的 monitor 句柄集合
    pub(crate) fn join(self) {
        let Self {
            instruction,
            playing,
            kws,
        } = self;

        // playing monitor 现在阻塞在 `ubus listen` 的 stdout 上；
        // join 前先显式 kill 它当前持有的 listener 子进程，把读循环唤醒出来。
        instruction.close();
        playing.request_stop();
        kws.close();

        join_monitor_thread("instruction", instruction.join());
        join_monitor_thread("playing", playing.join());
        join_monitor_thread("kws", kws.join());
    }
}

// 启动录音 fanout 线程。
//
// recorder 只负责产出单路音频块；
// fanout 线程负责根据 WsPeerHub 里的订阅集合，把音频复制给需要的 peer。
//
// 入参说明：
// - peer_hub：当前 session 的 peer 集合和录音订阅表
// - record_output_reader：接收 recorder 单路输出的 session 级录音队列
pub(crate) fn spawn_record_fanout_thread(
    peer_hub: Arc<WsPeerHub>,
    record_output_reader: mpsc::Receiver<Vec<u8>>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("record-fanout-thread".to_string())
        .spawn(move || {
            while let Ok(payload) = record_output_reader.recv() {
                // 参数说明：
                // - payload：recorder 产出的单块录音数据
                peer_hub.fan_out_record_audio(payload);
            }
            debug_log("supervisor", "Record fanout thread exited");
        })
        .expect("spawn record fanout thread")
}

// 统一 join monitor 线程，并在异常退出时补充分级日志。
//
// 入参说明：
// - monitor_name：用于日志标记的 monitor 名称
// - handle：要 join 的 monitor 线程句柄
fn join_monitor_thread(monitor_name: &'static str, handle: JoinHandle<Result<(), AppError>>) {
    match handle.join() {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            debug_err_log(
                "supervisor",
                format!("Monitor thread {monitor_name} exited with error: {err}"),
            );
        }
        Err(_) => {
            debug_err_log(
                "supervisor",
                format!("Monitor thread {monitor_name} panicked"),
            );
        }
    }
}
