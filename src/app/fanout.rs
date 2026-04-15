use std::sync::mpsc;
use std::thread::JoinHandle;

use crate::base::{AppError, debug_err_log};
use crate::monitor::{
    FileMonitorHandle,
    instruction::spawn_instruction_monitor,
    kws::spawn_kws_monitor,
    playing::{PlayingMonitorHandle, spawn_playing_monitor},
};
use crate::transport::SessionControl;

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
            // - instruction monitor 通过 close() 写 shutdown eventfd，唤醒内部 poll 退出
            instruction: spawn_instruction_monitor(route_channel_writer.clone()),
            // 参数说明：
            // - route_channel_writer.clone()：playing 事件通过它写回 session 总线
            // - playing monitor 自己在关闭时会通过 kill `ubus listen` 子进程退出，
            //   因此这里不再依赖通用 monitor_stop。
            playing: spawn_playing_monitor(route_channel_writer.clone()),
            // 参数说明：
            // - route_channel_writer：kws 事件通过它写回 session 总线
            // - kws monitor 通过 close() 写 shutdown eventfd，唤醒内部 poll 退出
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
