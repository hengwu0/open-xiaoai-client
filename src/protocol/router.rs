use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};

use crate::app::ws_peer_hub::WsPeerHub;
use crate::audio::AudioPlayer;
use crate::base::{AppError, debug_err_log, debug_log};
use crate::protocol::registry::{CommandContext, CommandRegistry};
use crate::transport::{OutboundControl, OutboundTarget, RoutedOutbound, SessionControl};

use super::AppMessage;

// router 线程是“统一会话分发器”：
// 1. monitor 产出的本地事件会先进入 route_channel
// 2. WebSocket 读线程解码后的入站消息也会进入同一条 route_channel
// 3. router 再决定哪些消息要本地执行，哪些消息要继续转发给 ws 写线程
//
// 如果把一次 session 看成一个小型消息总线，那么 router 就是这条总线的“中央交换机”：
// - monitor -> router -> ws-writer
// - ws-reader -> router -> 本地 handler / player
// - handler -> router -> ws-writer
//
// 这样设计后，monitor、命令处理器、播放器、ws 写线程之间都不需要两两互相认识，
// 耦合度会明显下降。
pub struct RouterThread {
    pub router_thread_handle: JoinHandle<Result<(), AppError>>,
}

// spawn_router 负责启动当前 session 的 router 线程。
//
// router 是会话内唯一的“统一消息分发点”：
// - monitor 侧主动上报的 Outbound 会先进入它
// - ws reader 读到的 Inbound 也会先进入它
// - supervisor 发出的 Close 同样通过它收尾
//
// 入参说明：
// - registry：本地命令注册表；处理入站 Request 时从这里查找对应 handler
// - route_channel_reader：router 的统一输入队列
// - player：本地播放器；收到 `tag=play` 的入站流时把音频交给它
// - peer_hub：当前 session 内所有 peer 的统一出站分发器
pub fn spawn_router(
    registry: Arc<CommandRegistry>,
    route_channel_reader: mpsc::Receiver<SessionControl>,
    player: Arc<AudioPlayer>,
    peer_hub: Arc<WsPeerHub>,
) -> Result<RouterThread, AppError> {
    // 参数说明：
    // - registry：本轮 session 的命令表，收到 Request 时从这里查 handler
    // - route_channel_reader：router 的统一输入队列，收敛 monitor / ws-reader / supervisor 的消息
    // - player：收到 `tag=play` 的入站流时，把音频字节交给本地播放器
    // - peer_hub：router 通过它做广播或单播，不直接碰 websocket
    let router_thread_handle = thread::Builder::new()
        .name("router-thread".to_string())
        .spawn(move || {
            debug_log("router", "Router thread started");

            // router 是一个典型的“阻塞消费 + 分支分发”模型：
            // - recv 阻塞等待下一条 session 消息
            // - 根据消息类型决定是本地执行，还是转发给 ws-writer
            //
            // 注意这里故意把所有入口都收敛到 route_channel_reader：
            // - monitor 的出站事件
            // - ws-reader 的入站协议消息
            // - supervisor 的 Close 信号
            //
            // 这样 router 的状态始终只需要围绕“一条队列”来思考。
            while let Ok(message) = route_channel_reader.recv() {
                match message {
                    SessionControl::Outbound(outbound) => {
                        // 参数说明：
                        // - outbound：已经确定好目标和消息体的出站动作
                        // - peer_hub.dispatch_outbound(outbound)：按 target 语义把消息路由到对应 peer
                        peer_hub.dispatch_outbound(outbound)?;
                    }
                    SessionControl::Inbound(inbound) => match inbound.message {
                        crate::transport::InboundMessage::Event(event) => {
                            // 旧客户端保留了入站 Event 的处理入口。
                            // 当前重构版继续保留解析能力，这里先做观测日志，不参与业务分发。
                            debug_log("router", format!("Inbound event received and ignored: event={}, id={}", event.event, event.id));
                        }
                        crate::transport::InboundMessage::Response(response) => {
                            debug_err_log(
                                "router",
                                format!("Unexpected inbound response ignored: id={}, code={:?}, msg={:?}", response.id, response.code, response.msg),
                            );
                        }
                        crate::transport::InboundMessage::Request(request) => {
                            // Request 是“真正需要本地执行”的入口。
                            // router 在这里做两件事：
                            // 1. 查 registry 找到对应 handler
                            // 2. 把执行结果包装成 Response，再交回 ws-writer
                            debug_log("router", format!("Inbound request received: command={}, id={}", request.command, request.id));
                            // 参数说明：
                            // - CommandContext { peer_id: inbound.peer_id }：
                            //   告诉 handler 当前请求来自哪个 peer
                            // - request.clone()：把原始协议请求交给具体命令处理器
                            let response = match registry.handle(
                                CommandContext {
                                    peer_id: inbound.peer_id,
                                },
                                request.clone(),
                            ) {
                                Ok(mut response) => {
                                    // handler 只返回业务结果，不负责回填 request id；
                                    // 这里由 router 统一补齐协议层字段。
                                    response.id = request.id;
                                    response
                                }
                                Err(err) => {
                                    debug_err_log(
                                        "router",
                                        format!("Inbound request handler failed: command={}, id={}, err={err}", request.command, request.id),
                                    );
                                    super::Response::from_error(&request.id, &err)
                                }
                            };
                            // 参数说明：
                            // - AppMessage::Response(response)：把本地 handler 结果重新包装成协议层 Response
                            // - serde_json::to_string(...)：编码成 websocket 文本帧承载的 JSON 文本
                            let text = serde_json::to_string(&AppMessage::Response(response))?;
                            // 参数说明：
                            // - target: OutboundTarget::ToPeer(inbound.peer_id)：
                            //   强制把这条 Response 回给请求来源 peer
                            // - message: OutboundControl::Text(text)：
                            //   已序列化好的 Response 文本包
                            peer_hub.dispatch_outbound(RoutedOutbound {
                                target: OutboundTarget::ToPeer(inbound.peer_id),
                                message: OutboundControl::Text(text),
                            })?;
                        }
                        crate::transport::InboundMessage::Stream(stream) => {
                            if stream.tag == "play" {
                                // 当前入站流里真正会被消费的只有播放流。
                                // 因此这里直接把字节交给 AudioPlayer，不再额外引入更泛化的流调度器。
                                debug_log("router", format!("Inbound play stream received: id={}, bytes={}", stream.id, stream.bytes.len()));
                                // 参数说明：
                                // - stream.bytes：服务端下发的 PCM 播放数据
                                // - player.enqueue(stream.bytes)：把播放数据交给本地 aplay 写线程
                                player.enqueue(stream.bytes)?;
                            }
                        }
                    },
                    SessionControl::Close => {
                        // 这是 supervisor 的“会话结束”信号。
                        // 一旦收到，就说明当前 session 的其他组件已经准备进入回收阶段。
                        debug_log("router", "Router close signal received");
                        break;
                    }
                }
            }

            debug_log("router", "Router thread exiting because control channel was closed");
            Ok(())
        })
        .map_err(|err| anyhow::anyhow!("spawn router thread: {err}"))?;

    Ok(RouterThread {
        router_thread_handle,
    })
}
