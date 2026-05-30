use std::sync::Arc;

use super::capabilities::AppCapabilities;
use super::peer_context::PeerContextRegistry;
use crate::base::{debug_log, VERSION};
use crate::protocol::registry::CommandRegistry;
use crate::protocol::Response;
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
// - peer_contexts：当前 session 的 per-peer 完整上下文表
// - capabilities：进程启动阶段探测到的本机能力，用于决定是否注册 fast_recording 相关命令
pub(crate) fn register_session_commands(
    registry: &Arc<CommandRegistry>,
    peer_contexts: Arc<PeerContextRegistry>,
    capabilities: AppCapabilities,
) {
    // 这张表定义的是“远端能驱动本机做什么”。
    // 约束保持不变：
    // - handler 只做本地业务动作
    // - router 负责把结果包装成 Response 并回源
    // - peer_contexts 负责多对端下的资源查找与网络分发细节
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

    debug_log("supervisor", "Registering inbound command: xiaoai_exit");
    // 参数说明：
    // - "xiaoai_exit"：远端请求退出/重启小爱原生会话
    // - |_context, _request| ...：执行固定的 mico_aivs_lab 重启命令，不读取远端 payload
    registry.register("xiaoai_exit", |_context, _request| {
        const XIAOAI_EXIT_SCRIPT: &str = "/etc/init.d/mico_aivs_lab restart >/dev/null 2>&1";
        debug_log(
            "supervisor",
            format!("Executing inbound command xiaoai_exit: {XIAOAI_EXIT_SCRIPT}"),
        );
        // 参数说明：
        // - 固定命令由本地定义，避免复用 run_shell 的任意远端脚本能力
        // - 返回 shell 执行结果，便于联调时观察 exit_code
        Ok(Response::from_data(serde_json::json!(run_shell(
            XIAOAI_EXIT_SCRIPT
        )?)))
    });

    debug_log("supervisor", "Registering inbound command: start_play");
    // 参数说明：
    // - "start_play"：远端发起播放的命令名
    // - move |context, request| ...：读取可选 AudioConfig 并启动当前 peer 自己的播放器
    registry.register("start_play", {
        let peer_contexts = peer_contexts.clone();
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
            // 参数说明：
            // - context.peer_id：当前命令来源 peer
            // - peer_contexts.get(...)：拿到这个 peer 独享的 player / recorder / sender 集合
            let peer = peer_contexts
                .get(context.peer_id)
                .ok_or_else(|| anyhow::anyhow!("peer context not found: {}", context.peer_id))?;
            // 参数说明：
            // - config：远端可选下发的播放参数；为空时退回本地默认配置
            peer.player.start(config)?;
            Ok(Response::success())
        }
    });

    debug_log("supervisor", "Registering inbound command: stop_play");
    // 参数说明：
    // - "stop_play"：远端停止播放的命令名
    // - move |context, _request| ...：直接停止当前 peer 自己的播放器
    registry.register("stop_play", {
        let peer_contexts = peer_contexts.clone();
        move |context, _request| {
            debug_log(
                "supervisor",
                format!(
                    "Executing inbound command stop_play; peer={}",
                    context.peer_id
                ),
            );
            // 参数说明：
            // - context.peer_id：当前命令来源 peer
            let peer = peer_contexts
                .get(context.peer_id)
                .ok_or_else(|| anyhow::anyhow!("peer context not found: {}", context.peer_id))?;
            peer.player.stop()?;
            Ok(Response::success())
        }
    });

    debug_log("supervisor", "Registering inbound command: start_recording");
    // 参数说明：
    // - "start_recording"：远端启动当前 peer 本地录音的命令名
    // - move |context, request| ...：按当前 peer 的独立 recorder 启动录音
    registry.register("start_recording", {
        let peer_contexts = peer_contexts.clone();
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
            // - context.peer_id：当前命令来源 peer
            // - peer.audio_sender()：该 peer 自己的音频回传出口
            let peer = peer_contexts
                .get(context.peer_id)
                .ok_or_else(|| anyhow::anyhow!("peer context not found: {}", context.peer_id))?;
            // 参数说明：
            // - config：远端可选下发的录音参数
            // - peer.audio_sender()：把录到的音频只发回当前 peer
            peer.recorder.start_recording(config, peer.audio_sender())?;
            Ok(Response::success())
        }
    });

    if capabilities.fast_recording_enabled {
        debug_log("supervisor", "Registering inbound command: fast_recording");
        // 参数说明：
        // - "fast_recording"：远端启动当前 peer 固定 fast profile 录音的命令名
        // - move |context, _request| ...：按固定 fast 处理链启动当前 peer 的独立 recorder
        registry.register("fast_recording", {
            let peer_contexts = peer_contexts.clone();
            move |context, _request| {
                debug_log(
                    "supervisor",
                    format!(
                        "Executing inbound command fast_recording; peer={}",
                        context.peer_id
                    ),
                );
                // 参数说明：
                // - context.peer_id：当前命令来源 peer
                // - peer.audio_sender()：当前 peer 的录音流回传出口
                let peer = peer_contexts
                    .get(context.peer_id)
                    .ok_or_else(|| anyhow::anyhow!("peer context not found: {}", context.peer_id))?;
                peer.recorder.start_fast_recording(peer.audio_sender())?;
                Ok(Response::success())
            }
        });

        debug_log("supervisor", "Registering inbound command: llm_start");
        // 参数说明：
        // - "llm_start"：服务端本地 KWS 命中后，请求当前 peer 切到 LLM 会话双通道录音模式
        // - move |context, _request| ...：先切 recorder 模式，再清空旧单通道音频队列并返回确认
        registry.register("llm_start", {
            let peer_contexts = peer_contexts.clone();
            move |context, _request| {
                debug_log(
                    "supervisor",
                    format!(
                        "Executing inbound command llm_start; peer={}",
                        context.peer_id
                    ),
                );
                let peer = peer_contexts
                    .get(context.peer_id)
                    .ok_or_else(|| anyhow::anyhow!("peer context not found: {}", context.peer_id))?;
                peer.recorder
                    .switch_to_llm_start_audio(peer.audio_sender())?;
                let cleared = peer.clear_audio_queue()?;
                debug_log(
                    "supervisor",
                    format!("llm_start prepared 2ch recording mode; cleared_audio_frames={cleared}"),
                );
                Ok(Response::success_msg("llm_start_ok"))
            }
        });

        debug_log("supervisor", "Registering inbound command: llm_stop");
        // 参数说明：
        // - "llm_stop"：服务端一轮 LLM 会话结束后，请求当前 peer 回到 KWS 单通道录音模式
        // - move |context, _request| ...：先切 recorder 模式，再清空旧双通道音频队列并返回确认
        registry.register("llm_stop", {
            let peer_contexts = peer_contexts.clone();
            move |context, _request| {
                debug_log(
                    "supervisor",
                    format!(
                        "Executing inbound command llm_stop; peer={}",
                        context.peer_id
                    ),
                );
                let peer = peer_contexts
                    .get(context.peer_id)
                    .ok_or_else(|| anyhow::anyhow!("peer context not found: {}", context.peer_id))?;
                peer.recorder
                    .switch_to_llm_stop_audio(peer.audio_sender())?;
                let cleared = peer.clear_audio_queue()?;
                debug_log(
                    "supervisor",
                    format!("llm_stop restored 1ch KWS recording mode; cleared_audio_frames={cleared}"),
                );
                Ok(Response::success_msg("llm_stop_ok"))
            }
        });
    } else {
        debug_log(
            "supervisor",
            "Skipping inbound commands fast_recording/llm_start/llm_stop; LX06 audio capability unavailable",
        );
    }

    debug_log("supervisor", "Registering inbound command: stop_recording");
    // 参数说明：
    // - "stop_recording"：远端停止当前 peer 本地录音的命令名
    // - move |context, _request| ...：停止当前 peer 自己的 recorder
    registry.register("stop_recording", {
        let peer_contexts = peer_contexts.clone();
        move |context, _request| {
            debug_log(
                "supervisor",
                format!(
                    "Executing inbound command stop_recording; peer={}",
                    context.peer_id
                ),
            );
            // 参数说明：
            // - context.peer_id：当前命令来源 peer
            let peer = peer_contexts
                .get(context.peer_id)
                .ok_or_else(|| anyhow::anyhow!("peer context not found: {}", context.peer_id))?;
            peer.recorder.stop_recording()?;
            Ok(Response::success())
        }
    });
}
