use std::sync::Arc;

use super::ws_peer_hub::{RecordingSubscription, WsPeerHub};
use crate::audio::{AudioPlayer, AudioRecorder, RecordOutputSender};
use crate::base::{VERSION, debug_log};
use crate::protocol::Response;
use crate::protocol::registry::CommandRegistry;
use crate::shell::command::run_shell;

// 注册一轮 session 可用的远端命令。
//
// 这里做的是“能力装配”而不是“消息分发”：
// - 命令表按 session 重建，避免跨 session 共享旧资源
// - handler 只负责本地动作，不直接关心 websocket 回包
// - router 负责把 handler 的结果再包装成 Response 回给请求来源
//
// 入参说明：
// - registry：当前 session 专属的命令注册表
// - player：共享播放器实例，播放相关命令通过它控制设备播放
// - recorder：共享录音器实例，录音相关命令通过它控制设备采集
// - peer_hub：当前 session 的 peer/订阅管理中心
// - record_output_writer：录音器产出的单路音频先写到这里，再交给 fanout 线程分发
pub(crate) fn register_session_commands(
    registry: &Arc<CommandRegistry>,
    player: Arc<AudioPlayer>,
    recorder: Arc<AudioRecorder>,
    peer_hub: Arc<WsPeerHub>,
    record_output_writer: RecordOutputSender,
) {
    // 这张表定义的是“远端能驱动本机做什么”。
    // 约束保持不变：
    // - handler 只做本地业务动作
    // - router 负责把结果包装成 Response 并回源
    // - peer_hub 负责多对端下的网络分发细节
    debug_log("supervisor", "Registering inbound command: get_version");
    // 参数说明：
    // - "get_version"：远端请求里使用的命令名
    // - |_context, _request| ...：该命令对应的本地处理逻辑
    registry.register("get_version", |_context, _request| {
        Ok(Response::from_data(serde_json::json!(VERSION.to_string())))
    });

    debug_log("supervisor", "Registering inbound command: run_shell");
    // 参数说明：
    // - "run_shell"：远端请求里使用的命令名
    // - |_context, request| ...：读取 payload 中的 shell 脚本并在本机执行
    registry.register("run_shell", |_context, request| {
        let script = match request.payload {
            Some(payload) => serde_json::from_value::<String>(payload)?,
            None => return Err(anyhow::anyhow!("empty command")),
        };
        debug_log(
            "supervisor",
            format!(
                "Executing inbound command run_shell: {}",
                script.replace('\n', " ")
            ),
        );
        // 参数说明：
        // - run_shell(&script)：在本机同步执行 shell，并返回完整 stdout/stderr/exit_code
        Ok(Response::from_data(serde_json::json!(run_shell(&script)?)))
    });

    debug_log("supervisor", "Registering inbound command: start_play");
    // 参数说明：
    // - "start_play"：远端发起播放的命令名
    // - move |_context, request| ...：读取可选 AudioConfig 并启动共享播放器
    registry.register("start_play", {
        let player = player.clone();
        move |_context, request| {
            let config = request
                .payload
                .and_then(|payload| serde_json::from_value(payload).ok());
            debug_log(
                "supervisor",
                format!(
                    "Executing inbound command start_play; custom_config={}",
                    config.is_some()
                ),
            );
            // 参数说明：
            // - config：如果 payload 能解析成 AudioConfig，就按该配置启动播放器
            player.start(config)?;
            Ok(Response::success())
        }
    });

    debug_log("supervisor", "Registering inbound command: stop_play");
    // 参数说明：
    // - "stop_play"：远端停止播放的命令名
    // - move |_context, _request| ...：直接停止共享播放器
    registry.register("stop_play", {
        let player = player.clone();
        move |_context, _request| {
            debug_log("supervisor", "Executing inbound command stop_play");
            player.stop()?;
            Ok(Response::success())
        }
    });

    debug_log("supervisor", "Registering inbound command: start_recording");
    // 参数说明：
    // - "start_recording"：远端开始录音订阅的命令名
    // - move |context, request| ...：根据请求方 peer_id 和可选配置维护录音订阅
    registry.register("start_recording", {
        let recorder = recorder.clone();
        let peer_hub = peer_hub.clone();
        let record_output_writer = record_output_writer.clone();
        move |context, request| {
            let config = request
                .payload
                .and_then(|payload| serde_json::from_value(payload).ok());
            debug_log(
                "supervisor",
                format!(
                    "Executing inbound command start_recording; peer={}, custom_config={}",
                    context.peer_id,
                    config.is_some()
                ),
            );
            // 参数说明：
            // - context.peer_id：当前发起 start_recording 的对端
            // - config：本次请求携带的录音配置，可能为空
            // - peer_hub.subscribe_recording(...)：统一决定这次请求是首个订阅还是幂等复用
            match peer_hub.subscribe_recording(context.peer_id, config)? {
                RecordingSubscription::Start(active_config) => {
                    // 只有首个订阅者才真正启动全局 recorder。
                    // 参数说明：
                    // - Some(active_config)：本轮被锁定下来的录音配置
                    // - record_output_writer.clone()：recorder 输出先进入 session 级 fanout 队列
                    recorder.start_recording(Some(active_config), record_output_writer.clone())?;
                }
                RecordingSubscription::AlreadyActive => {}
            }
            Ok(Response::success())
        }
    });

    debug_log("supervisor", "Registering inbound command: stop_recording");
    // 参数说明：
    // - "stop_recording"：远端取消录音订阅的命令名
    // - move |context, _request| ...：移除当前 peer 的录音订阅，必要时停掉 recorder
    registry.register("stop_recording", {
        let recorder = recorder.clone();
        let peer_hub = peer_hub.clone();
        move |context, _request| {
            debug_log(
                "supervisor",
                format!(
                    "Executing inbound command stop_recording; peer={}",
                    context.peer_id
                ),
            );
            // 参数说明：
            // - peer_hub.unsubscribe_recording(context.peer_id)：移除当前 peer 的录音订阅
            if peer_hub.unsubscribe_recording(context.peer_id) {
                recorder.stop_recording()?;
            }
            Ok(Response::success())
        }
    });
}
