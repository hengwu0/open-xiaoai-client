use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::mpsc::SyncSender;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Deserialize;

use crate::base::{AppError, debug_err_log, debug_log};
use crate::protocol::{AppMessage, Event};
use crate::shell::command::run_shell;
use crate::transport::{OutboundControl, SessionControl};

// `ubus listen` 监听的事件名。
// 设备侧一旦有播放状态变化，就会产出形如：
// `{ "play_status_broadcast_event": {"status":1} }`
// 的单行 JSON。
const PLAY_STATUS_EVENT_NAME: &str = "play_status_broadcast_event";

// 事件到来后，我们再去查询一次“当前真实状态”。
// 之所以不直接信任广播里的 `status` 字段，是因为本次需求明确要求：
// “当收到广播时，再执行 `ubus -t 5 call mediaplayer player_play_status`，
//  把它返回的 status 视为旧算法 `mphelper mute_stat` 的等价语义。”
const PLAY_STATUS_QUERY_COMMAND: &str = "ubus -t 5 call mediaplayer player_play_status";

// 主监听线程不应该因为一次瞬时异常就永久退出，所以这里把重试节奏固定下来。
const LISTENER_RESTART_DELAY: Duration = Duration::from_secs(1);

type SharedListenerChild = Arc<Mutex<Option<Child>>>;

// 统一托管一轮 `ubus listen` 监听所需的资源。
// `child` 句柄本身放在共享容器里，原因是：
// - monitor 线程负责阻塞读取 stdout
// - session 收尾线程需要能从外部主动 kill 这个子进程
//
// 两边都需要触达同一个 `Child`，所以这里用 `Arc<Mutex<Option<Child>>>` 托管。
struct PlayStatusListener {
    child: SharedListenerChild,
    stdout: BufReader<ChildStdout>,
}

// PlayingMonitorHandle 是 playing monitor 对外暴露的控制句柄。
// 它除了包含 join handle，还额外持有：
// - shutdown_requested：标记这次退出是不是“外部主动收尾”
// - child：当前正在运行的 `ubus listen` 子进程句柄
//
// 这样 session 关闭时，就可以先 kill 子进程，把阻塞在 `read_line()` 上的线程唤醒，
// 然后再 join 线程，而不需要在读循环里做 timeout/poll。
pub struct PlayingMonitorHandle {
    join_handle: JoinHandle<Result<(), AppError>>,
    shutdown_requested: Arc<AtomicBool>,
    child: SharedListenerChild,
}

impl PlayingMonitorHandle {
    // request_stop 请求当前 playing monitor 停止，并主动杀掉阻塞中的 listener 子进程。
    //
    // 入参说明：
    // - self：当前 playing monitor 的控制句柄
    pub fn request_stop(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        kill_current_listener_process(&self.child);
    }

    // join 取出内部 monitor 线程句柄，交给上层自行等待收尾。
    //
    // 入参说明：
    // - self：当前 playing monitor 的控制句柄
    pub fn join(self) -> JoinHandle<Result<(), AppError>> {
        self.join_handle
    }
}

// `ubus listen` 输出的事件结构。
// 当前算法只需要确认“是不是 play_status_broadcast_event 事件”，
// 因此只保留最小字段即可，不把业务逻辑绑定到广播体细节上。
#[derive(Debug, Deserialize)]
struct PlayStatusBroadcastEnvelope {
    #[serde(default)]
    play_status_broadcast_event: Option<PlayStatusBroadcastPayload>,
}

#[derive(Debug, Deserialize)]
struct PlayStatusBroadcastPayload {
    #[allow(dead_code)]
    status: Option<i64>,
}

// `ubus -t 5 call mediaplayer player_play_status` 的第一层返回值：
// {
//     "code": 0,
//     "info": "{ \"status\": 0 }"
// }
//
// 注意 `info` 本身还是一个 JSON 字符串，所以需要再解一层。
#[derive(Debug, Deserialize)]
struct PlayerPlayStatusResponse {
    code: i64,
    info: String,
}

#[derive(Debug, Deserialize)]
struct PlayerPlayStatusInfo {
    status: i64,
}

// 监听设备播放状态，并在状态变化时上报。
//
// 新算法分成两层：
// 1. 常驻启动 `ubus listen play_status_broadcast_event`
// 2. 每当监听到一条对应事件时，再查询一次 `player_play_status`
//
// 这样做相比旧版 10ms 高频轮询有两个好处：
// 1. 平时没有状态变化时几乎不消耗额外 shell 调用
// 2. 真正有状态变化时仍然能快速拿到最新状态
//
// 入参说明：
// - route_channel_writer：把 playing 事件写回 session 总线的发送端
pub fn spawn_playing_monitor(
    route_channel_writer: SyncSender<SessionControl>,
) -> PlayingMonitorHandle {
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    let child = Arc::new(Mutex::new(None));
    let shutdown_requested_for_thread = shutdown_requested.clone();
    let child_for_thread = child.clone();

    let join_handle = thread::Builder::new()
        .name("playing-monitor-thread".to_string())
        .spawn(move || {
            debug_log("playing-monitor", "Playing monitor thread started");
            let mut last = String::new();

            while !shutdown_requested_for_thread.load(Ordering::SeqCst) {
                let listener = match spawn_play_status_listener(child_for_thread.clone()) {
                    Ok(listener) => listener,
                    Err(err) => {
                        debug_err_log(
                            "playing-monitor",
                            format!(
                                "Failed to start `ubus listen {PLAY_STATUS_EVENT_NAME}`: {err}"
                            ),
                        );
                        if shutdown_requested_for_thread.load(Ordering::SeqCst) {
                            break;
                        }
                        thread::sleep(LISTENER_RESTART_DELAY);
                        continue;
                    }
                };

                debug_log(
                    "playing-monitor",
                    format!("`ubus listen {PLAY_STATUS_EVENT_NAME}` started"),
                );

                // 旧实现会在 monitor 启动后很快主动上报一次当前状态。
                // 新实现如果只依赖后续广播，可能会出现“程序刚启动但当前已经在播放，
                // 却迟迟等不到第一条 playing event”的退化。
                //
                // 因此这里在每次成功拉起监听器后，先主动同步一次状态：
                // - 首次启动时：补齐初始状态
                // - 监听器异常重启后：补齐监听中断窗口里可能错过的状态变化
                // 参数说明：
                // - query_playing_state_from_query()：以主动查询结果作为当前播放状态真值
                match query_playing_state_from_query() {
                    Ok(next) => {
                        emit_playing_state_if_changed(&route_channel_writer, &mut last, next)?
                    }
                    Err(err) => {
                        debug_err_log(
                            "playing-monitor",
                            format!("Initial play status sync failed after listener start: {err}"),
                        );
                    }
                }

                // 参数说明：
                // - listener：当前活跃的 `ubus listen` 监听资源
                // - &route_channel_writer：播放状态变化时通过它上报事件
                // - &mut last：用于做状态去重
                monitor_play_status_listener(listener, &route_channel_writer, &mut last)?;

                if !shutdown_requested_for_thread.load(Ordering::SeqCst) {
                    debug_err_log(
                        "playing-monitor",
                        format!(
                            "`ubus listen {PLAY_STATUS_EVENT_NAME}` exited unexpectedly; restarting"
                        ),
                    );
                    thread::sleep(LISTENER_RESTART_DELAY);
                }
            }
            debug_log("playing-monitor", "Playing monitor thread exiting");
            Ok(())
        })
        .expect("spawn playing monitor");

    PlayingMonitorHandle {
        join_handle,
        shutdown_requested,
        child,
    }
}

// spawn_play_status_listener 拉起一个 `ubus listen play_status_broadcast_event` 子进程，
// 并返回当前监听会话需要的 stdout 读取器和共享 child 句柄。
//
// 入参说明：
// - child_slot：共享 child 槽位，供 monitor 线程与外部收尾线程共同访问
fn spawn_play_status_listener(
    child_slot: SharedListenerChild,
) -> Result<PlayStatusListener, AppError> {
    // 这里不用 `run_shell`，因为我们需要长期持有子进程并持续读取它的 stdout。
    let mut child = Command::new("ubus")
        .args(["listen", PLAY_STATUS_EVENT_NAME])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        // stderr 不参与业务决策，直接丢弃，避免没人消费时撑满管道。
        .stderr(Stdio::null())
        .spawn()?;

    // 参数说明：
    // - child.stdout.take()：拿走 listener 子进程 stdout，供 monitor 线程阻塞读取
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("listener stdout was not captured"))?;

    {
        let mut child_guard = child_slot
            .lock()
            .map_err(|_| anyhow::anyhow!("listener child mutex poisoned"))?;
        *child_guard = Some(child);
    }

    Ok(PlayStatusListener {
        child: child_slot,
        stdout: BufReader::new(stdout),
    })
}

// monitor_play_status_listener 持续读取 `ubus listen` 输出，并在收到目标事件时刷新播放状态。
//
// 入参说明：
// - listener：当前正在运行的 `ubus listen` 监听资源
// - route_channel_writer：把 playing 事件写回 session 总线的发送端
// - last：上一次已经上报出去的播放状态，用于去重
fn monitor_play_status_listener(
    mut listener: PlayStatusListener,
    route_channel_writer: &SyncSender<SessionControl>,
    last: &mut String,
) -> Result<(), AppError> {
    let loop_result = loop {
        let mut line = String::new();
        match listener.stdout.read_line(&mut line) {
            Ok(0) => {
                debug_err_log(
                    "playing-monitor",
                    format!("`ubus listen {PLAY_STATUS_EVENT_NAME}` stdout closed"),
                );
                break Ok(());
            }
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                debug_log(
                    "playing-monitor",
                    format!("Received listener line: {trimmed}"),
                );

                if !is_play_status_broadcast_event_line(trimmed) {
                    // 理论上 `ubus listen play_status_broadcast_event` 只会产出目标事件；
                    // 如果收到了别的内容，说明设备侧输出格式发生了漂移，
                    // 这属于异常观测路径，应该直接进入 stderr。
                    debug_err_log(
                        "playing-monitor",
                        "Ignored listener line because it is not a play_status_broadcast_event",
                    );
                    continue;
                }

                // 按需求：收到广播后，再去查询一次 `player_play_status`，
                // 把查询结果视为最终真值来源。
                match query_playing_state_from_query() {
                    Ok(next) => emit_playing_state_if_changed(route_channel_writer, last, next)?,
                    Err(err) => {
                        debug_err_log(
                            "playing-monitor",
                            format!("Failed to refresh play status after broadcast event: {err}"),
                        );
                    }
                }
            }
            Err(err) => break Err(err.into()),
        }
    };

    stop_play_status_listener(listener);
    loop_result
}

// stop_play_status_listener 回收一轮 listener 监听会话持有的子进程资源。
//
// 入参说明：
// - listener：当前要被清理的监听资源集合
fn stop_play_status_listener(listener: PlayStatusListener) {
    // 无论是正常 stop，还是异常重启，退出当前 listener 时都要把 child 从共享槽位里取出并回收。
    // 如果 session 收尾线程之前已经对它调用了 kill，这里负责最终 wait/reap。
    let PlayStatusListener { child, stdout: _ } = listener;

    let child = match child.lock() {
        Ok(mut child_guard) => child_guard.take(),
        Err(_) => {
            debug_err_log(
                "playing-monitor",
                "Listener child mutex poisoned during cleanup",
            );
            None
        }
    };

    let Some(mut child) = child else {
        return;
    };

    match child.try_wait() {
        Ok(Some(status)) => {
            debug_log(
                "playing-monitor",
                format!("Listener process already exited before cleanup: {status}"),
            );
        }
        Ok(None) => {
            if let Err(err) = child.wait() {
                debug_err_log(
                    "playing-monitor",
                    format!("Failed to wait listener process during cleanup: {err}"),
                );
            }
        }
        Err(err) => {
            debug_err_log(
                "playing-monitor",
                format!("Failed to inspect listener process during cleanup: {err}"),
            );
        }
    }
}

// kill_current_listener_process 从共享槽位里找到当前 listener 子进程，并尝试 kill 它。
//
// 入参说明：
// - child：共享 child 槽位
fn kill_current_listener_process(child: &SharedListenerChild) {
    let mut child_guard = match child.lock() {
        Ok(guard) => guard,
        Err(_) => {
            debug_err_log(
                "playing-monitor",
                "Listener child mutex poisoned while requesting stop",
            );
            return;
        }
    };

    let Some(child) = child_guard.as_mut() else {
        return;
    };

    if let Err(err) = child.kill() {
        debug_err_log(
            "playing-monitor",
            format!("Failed to kill listener process while requesting stop: {err}"),
        );
    }
}

// emit_playing_state_if_changed 在播放状态发生变化时，把新状态编码成事件并写回 session 总线。
//
// 入参说明：
// - route_channel_writer：把 playing 事件写回 session 总线的发送端
// - last：上一次已上报状态；函数内部会在变化时更新它
// - next：当前查询或监听得到的新状态
fn emit_playing_state_if_changed(
    route_channel_writer: &SyncSender<SessionControl>,
    last: &mut String,
    next: &'static str,
) -> Result<(), AppError> {
    // 协议层只关心“状态是否变化”，因此继续保留旧实现的去重语义。
    if last == next {
        return Ok(());
    }

    *last = next.to_string();

    // 参数说明：
    // - Event::new("playing", Some(json!(next)))：把当前播放状态包装成统一事件对象
    let text = serde_json::to_string(&AppMessage::Event(Event::new(
        "playing",
        Some(serde_json::json!(next)),
    )))?;
    route_channel_writer.send(SessionControl::Outbound(crate::transport::RoutedOutbound {
        target: crate::transport::OutboundTarget::Broadcast,
        message: OutboundControl::Text(text),
    }))?;
    debug_log(
        "playing-monitor",
        format!("Outbound playing event queued: state={next}"),
    );
    Ok(())
}

// query_playing_state_from_query 通过主动查询播放器状态来得到当前 playing 语义。
//
// 入参说明：
// - 无
fn query_playing_state_from_query() -> Result<&'static str, AppError> {
    let status = query_player_play_status_value()?;
    let next = map_status_to_playing_state(status);

    debug_log(
        "playing-monitor",
        format!("Resolved play status from query: raw_status={status}, mapped_state={next}"),
    );

    Ok(next)
}

// query_player_play_status_value 执行设备侧查询命令，并解析出原始 status 数值。
//
// 入参说明：
// - 无
fn query_player_play_status_value() -> Result<i64, AppError> {
    // 参数说明：
    // - PLAY_STATUS_QUERY_COMMAND：设备侧用于查询当前播放状态的 shell 命令
    let res = run_shell(PLAY_STATUS_QUERY_COMMAND)?;
    if res.exit_code != 0 {
        return Err(anyhow::anyhow!(
            "`{PLAY_STATUS_QUERY_COMMAND}` exited with code {}: stdout={}, stderr={}",
            res.exit_code,
            res.stdout.trim(),
            res.stderr.trim(),
        ));
    }
    parse_player_play_status_output(&res.stdout)
}

// parse_player_play_status_output 解析 `ubus call mediaplayer player_play_status` 的输出文本。
//
// 入参说明：
// - stdout：命令原始标准输出文本
fn parse_player_play_status_output(stdout: &str) -> Result<i64, AppError> {
    // `ubus call` 返回的是一个 JSON 对象，但真正的 `status` 被包在 `info` 字符串里。
    // 所以这里做两次反序列化：
    // 1. 先解析外层 `{ code, info }`
    // 2. 再把 `info` 当成 JSON 字符串解析出 `{ status }`
    let response: PlayerPlayStatusResponse =
        serde_json::from_str(stdout.trim()).map_err(|err| {
            anyhow::anyhow!(
                "failed to parse outer player_play_status response: {err}; stdout={}",
                stdout.trim()
            )
        })?;

    if response.code != 0 {
        return Err(anyhow::anyhow!(
            "player_play_status returned non-zero code {}: info={}",
            response.code,
            response.info
        ));
    }

    let info: PlayerPlayStatusInfo = serde_json::from_str(&response.info).map_err(|err| {
        anyhow::anyhow!(
            "failed to parse inner player_play_status info: {err}; info={}",
            response.info
        )
    })?;

    Ok(info.status)
}

// is_play_status_broadcast_event_line 判断一行 listener 输出是否可视为播放状态广播事件。
//
// 入参说明：
// - line：`ubus listen` 输出的一整行文本
fn is_play_status_broadcast_event_line(line: &str) -> bool {
    // 正常情况下，这里应当总能被解析成 JSON。
    // 但为了兼容设备侧偶发格式漂移，这里加一个字符串兜底：
    // 只要行里出现了事件名，也视为一次状态变化通知。
    match serde_json::from_str::<PlayStatusBroadcastEnvelope>(line) {
        Ok(envelope) => envelope.play_status_broadcast_event.is_some(),
        Err(err) => {
            debug_err_log(
                "playing-monitor",
                format!("Failed to parse listener line as JSON: {err}; line={line}"),
            );
            line.contains(PLAY_STATUS_EVENT_NAME)
        }
    }
}

// map_status_to_playing_state 把设备侧原始 status 数值映射成协议层约定的字符串状态。
//
// 入参说明：
// - status：设备侧原始播放状态数值
fn map_status_to_playing_state(status: i64) -> &'static str {
    // 这里继续严格沿用旧项目 `mphelper mute_stat` 的约定：
    // 1 = Playing
    // 2 = Paused
    // 其他 = Idle
    match status {
        1 => "Playing",
        2 => "Paused",
        _ => "Idle",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        is_play_status_broadcast_event_line, map_status_to_playing_state,
        parse_player_play_status_output,
    };

    #[test]
    // 验证 `player_play_status` 的双层 JSON 输出能被正确解析出 status。
    fn parse_player_play_status_output_extracts_nested_status() {
        let stdout = r#"
        {
            "code": 0,
            "info": "{ \"status\": 2 }"
        }
        "#;

        assert_eq!(parse_player_play_status_output(stdout).unwrap(), 2);
    }

    #[test]
    // 验证 listener 输出行的事件识别规则符合预期。
    fn play_status_event_line_detection_matches_expected_shape() {
        assert!(is_play_status_broadcast_event_line(
            r#"{ "play_status_broadcast_event": {"status":1} }"#
        ));
        assert!(!is_play_status_broadcast_event_line(
            r#"{ "some_other_event": {"status":1} }"#
        ));
    }

    #[test]
    // 验证 raw status 到 Playing/Paused/Idle 的映射保持与旧约定一致。
    fn status_mapping_matches_legacy_mute_stat_contract() {
        assert_eq!(map_status_to_playing_state(1), "Playing");
        assert_eq!(map_status_to_playing_state(2), "Paused");
        assert_eq!(map_status_to_playing_state(0), "Idle");
        assert_eq!(map_status_to_playing_state(999), "Idle");
    }
}
