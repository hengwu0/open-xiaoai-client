use std::sync::Arc;

use super::peer_media::PeerMediaRegistry;
use super::ws_peer_hub::WsPeerHub;
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
// - peer_media：当前 session 的 per-peer 音频设备表
// - peer_hub：当前 session 的 peer 网络出口管理中心
pub(crate) fn register_session_commands(
    registry: &Arc<CommandRegistry>,
    peer_media: Arc<PeerMediaRegistry>,
    peer_hub: Arc<WsPeerHub>,
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
    // - move |context, request| ...：读取可选 AudioConfig 并启动当前 peer 自己的播放器
    registry.register("start_play", {
        let peer_media = peer_media.clone();
        move |context, request| {
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
            let media = peer_media
                .get(context.peer_id)
                .ok_or_else(|| anyhow::anyhow!("peer media not found: {}", context.peer_id))?;
            media.player.start(config)?;
            Ok(Response::success())
        }
    });

    debug_log("supervisor", "Registering inbound command: stop_play");
    // 参数说明：
    // - "stop_play"：远端停止播放的命令名
    // - move |context, _request| ...：直接停止当前 peer 自己的播放器
    registry.register("stop_play", {
        let peer_media = peer_media.clone();
        move |context, _request| {
            debug_log(
                "supervisor",
                format!(
                    "Executing inbound command stop_play; peer={}",
                    context.peer_id
                ),
            );
            let media = peer_media
                .get(context.peer_id)
                .ok_or_else(|| anyhow::anyhow!("peer media not found: {}", context.peer_id))?;
            media.player.stop()?;
            Ok(Response::success())
        }
    });

    debug_log("supervisor", "Registering inbound command: start_recording");
    // 参数说明：
    // - "start_recording"：远端启动当前 peer 本地录音的命令名
    // - move |context, request| ...：按当前 peer 的独立 recorder 启动录音
    registry.register("start_recording", {
        let peer_media = peer_media.clone();
        let peer_hub = peer_hub.clone();
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
            let media = peer_media
                .get(context.peer_id)
                .ok_or_else(|| anyhow::anyhow!("peer media not found: {}", context.peer_id))?;
            let audio_sender = peer_hub.audio_sender(context.peer_id).ok_or_else(|| {
                anyhow::anyhow!("peer audio sender not found: {}", context.peer_id)
            })?;
            media.recorder.start_recording(config, audio_sender)?;
            Ok(Response::success())
        }
    });

    debug_log("supervisor", "Registering inbound command: fast_recording");
    // 参数说明：
    // - "fast_recording"：远端启动当前 peer 固定 fast profile 录音的命令名
    // - move |context, _request| ...：按固定 fast 处理链启动当前 peer 的独立 recorder
    registry.register("fast_recording", {
        let peer_media = peer_media.clone();
        let peer_hub = peer_hub.clone();
        move |context, _request| {
            debug_log(
                "supervisor",
                format!(
                    "Executing inbound command fast_recording; peer={}",
                    context.peer_id
                ),
            );
            let media = peer_media
                .get(context.peer_id)
                .ok_or_else(|| anyhow::anyhow!("peer media not found: {}", context.peer_id))?;
            let audio_sender = peer_hub.audio_sender(context.peer_id).ok_or_else(|| {
                anyhow::anyhow!("peer audio sender not found: {}", context.peer_id)
            })?;
            media.recorder.start_fast_recording(audio_sender)?;
            Ok(Response::success())
        }
    });

    debug_log("supervisor", "Registering inbound command: stop_recording");
    // 参数说明：
    // - "stop_recording"：远端停止当前 peer 本地录音的命令名
    // - move |context, _request| ...：停止当前 peer 自己的 recorder
    registry.register("stop_recording", {
        let peer_media = peer_media.clone();
        move |context, _request| {
            debug_log(
                "supervisor",
                format!(
                    "Executing inbound command stop_recording; peer={}",
                    context.peer_id
                ),
            );
            let media = peer_media
                .get(context.peer_id)
                .ok_or_else(|| anyhow::anyhow!("peer media not found: {}", context.peer_id))?;
            media.recorder.stop_recording()?;
            Ok(Response::success())
        }
    });
}
