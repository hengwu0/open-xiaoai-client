use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use anyhow::Result;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio::task::{AbortHandle, JoinHandle};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::header::{AUTHORIZATION, HeaderValue};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, accept_async, accept_hdr_async, connect_async,
};

use crate::base::{AppError, debug_err_log, debug_log, debug_log_limited};
use crate::transport::codec::{DecodeResult, decode_message, encode_outbound};
use crate::transport::{
    OutboundControl, PeerId, PeerSource, RoutedInbound, SessionControl, WriteSignal,
    WriteSignalWake,
};

// transport::ws_pump 现在不再代表“一整轮 session 的唯一 websocket”，
// 而是退化成“单个 peer 的 ws 适配层”：
// - PendingPeer 表示“已经握手完成、但还没挂到 session”
// - spawn_peer_tasks 表示“把这个 peer 接进 session 的读写体系”
// - WsPeerHandle 表示“后续 supervisor 可以如何强制终止这个 peer”
pub type ClientWsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsReadHalf = SplitStream<ClientWsStream>;
type WsWriteHalf = SplitSink<ClientWsStream, Message>;

pub struct PendingPeer {
    // 用 source 标记这个 peer 来自哪里，方便 supervisor 统一管理 listen / connect 两种来源。
    pub source: PeerSource,
    pub ws_stream: ClientWsStream,
}

pub struct WsPeerTasks {
    // peer_handle 暴露给 supervisor，用于后续在清理阶段打断本 peer 的 reader / writer。
    pub peer_handle: Arc<WsPeerHandle>,
    pub ws_read_thread_handle: JoinHandle<Result<(), AppError>>,
    pub ws_write_thread_handle: JoinHandle<Result<(), AppError>>,
}

pub struct WsPeerHandle {
    // close_state 采用延迟安装，是因为只有 split 并成功 spawn 出 reader / writer 之后，
    // 才真正具备“可关闭本 peer”的条件。
    close_state: Mutex<Option<WsPeerCloseState>>,
}

struct WsPeerCloseState {
    ws_reader_abort_handle: AbortHandle,
    ws_writer_abort_handle: AbortHandle,
    ws_send_signal: Arc<WriteSignal>,
}

impl WsPeerHandle {
    // 创建一个“尚未安装关闭能力”的空句柄。
    //
    // 这个构造函数只负责先把壳子建出来，真正的 close 资源要等 reader / writer
    // 都成功启动后，才能在 install_close_state() 里补进去。
    //
    // 入参说明：
    // - 无
    fn new() -> Self {
        Self {
            close_state: Mutex::new(None),
        }
    }

    // 把当前 peer 的实际关闭资源安装到 WsPeerHandle 中。
    //
    // 这个步骤之所以拆出来，是因为只有在 websocket 已经 split、并且 reader /
    // writer 任务都已经成功 spawn 之后，当前 peer 才真正具备“可被外部关闭”的条件。
    //
    // 入参说明：
    // - self：当前 peer 对应的关闭控制句柄
    // - close_state：本 peer 的关闭状态集合，里面包含：
    //   - ws_reader_abort_handle：用于中断 reader task
    //   - ws_writer_abort_handle：用于中断 writer task
    //   - ws_send_signal：用于把阻塞中的 writer 从 wait() 中唤醒出来
    fn install_close_state(&self, close_state: WsPeerCloseState) -> Result<(), AppError> {
        let mut slot = self
            .close_state
            .lock()
            .expect("ws close state poisoned while installing");
        if slot.is_some() {
            return Err(anyhow::anyhow!(
                "ws close state has already been initialized"
            ));
        }
        *slot = Some(close_state);
        Ok(())
    }

    // 主动关闭当前 peer。
    //
    // 这里的目标不是尽可能优雅地完成 websocket close 握手，而是优先保证：
    // 1. 阻塞中的 writer 能被唤醒
    // 2. reader / writer 两个本地任务最终一定退出
    // 3. supervisor 在回收 session 时不会留下悬挂任务
    //
    // 入参说明：
    // - self：当前 peer 的关闭控制句柄
    pub fn close(&self) {
        let mut close_state = self.close_state.lock().expect("ws close state poisoned");
        let Some(close_state) = close_state.take() else {
            debug_log(
                "ws",
                "WsPeerHandle close request ignored because peer is already closing",
            );
            return;
        };

        // 这里优先保证“本地任务一定退出”，而不是执着于完整 websocket close 握手。
        // 因为对 supervisor 来说，更重要的是不要留下悬挂 reader / writer。
        debug_log("ws", "WsPeerHandle force close requested");
        // 参数说明：
        // - close_state.ws_send_signal.close()：先把阻塞在 wait() 上的 writer 唤醒，
        //   让它有机会自行走到退出路径，而不是直接被 abort 打断在任意位置。
        close_state.ws_send_signal.close();
        // 给 writer 留一个很短的时间窗口，优先尝试自己感知 close 并收尾。
        std::thread::sleep(std::time::Duration::from_millis(100));
        // 如果 reader / writer 还没有自然退出，这里再做强制打断兜底。
        close_state.ws_reader_abort_handle.abort();
        close_state.ws_writer_abort_handle.abort();
    }
}

// connect_pending_peer 负责走“主动外连”方向的 websocket 建连，
// 并把已经完成握手、但还没正式挂进 session 的连接包装成 PendingPeer 返回。
//
// 它只负责把“网络连接”准备好，不负责：
// - 给这个 peer 分配 peer_id
// - 启动 reader / writer 任务
// - 把它接入当前 session 的 peer context
//
// 上面这些后续动作都由 supervisor 再往下推进。
//
// 入参说明：
// - handle：supervisor 持有的 tokio runtime handle；这里借它执行 async connect
// - url：目标 websocket 地址，通常来自运行配置里的 server_url
// - ws_token：可选 Bearer token；为空时保持旧的无认证握手逻辑
pub fn connect_pending_peer(
    handle: &Handle,
    url: &str,
    ws_token: Option<&str>,
) -> Result<PendingPeer, AppError> {
    // outbound connect 成功后，只返回一个 PendingPeer；
    // 真正把它挂进当前 session，是 supervisor 的职责，不在这里完成。
    debug_log(
        "ws",
        format!("Attempting outbound WebSocket connection to {url}"),
    );
    let request = build_connect_request(url, ws_token)?;
    // 参数说明：
    // - handle.block_on(...)：复用已有 runtime 执行 async connect，避免这里额外起 runtime
    // - connect_async(request)：按给定 websocket 请求发起握手；有 token 时会自动带 Authorization
    let (ws_stream, _) = handle.block_on(async { connect_async(request).await })?;
    debug_log("ws", format!("Outbound WebSocket connected: {url}"));
    Ok(PendingPeer {
        source: PeerSource::OutboundConnect {
            url: url.to_string(),
        },
        ws_stream,
    })
}

// build_connect_request 构造 outbound websocket 握手请求。
//
// 入参说明：
// - url：目标 websocket 地址
// - ws_token：可选 Bearer token；为空时不附带任何 Authorization 头
fn build_connect_request(
    url: &str,
    ws_token: Option<&str>,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, AppError> {
    let mut request = url.into_client_request()?;
    if let Some(token) = ws_token.filter(|token| !token.is_empty()) {
        let value = HeaderValue::from_str(&format!("Bearer {token}"))?;
        request.headers_mut().insert(AUTHORIZATION, value);
        debug_log(
            "ws",
            "Outbound WebSocket request will include Authorization header",
        );
    }
    Ok(request)
}

#[cfg(test)]
mod tests {
    use tokio_tungstenite::tungstenite::http::Request;
    use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;

    use super::{build_connect_request, verify_listener_bearer_token};

    #[test]
    fn build_connect_request_includes_bearer_token_when_present() {
        let request = build_connect_request("ws://127.0.0.1:9000", Some("secret-token")).unwrap();

        assert_eq!(
            request.headers().get(AUTHORIZATION).unwrap(),
            "Bearer secret-token"
        );
    }

    #[test]
    fn build_connect_request_keeps_old_behavior_without_token() {
        let request = build_connect_request("ws://127.0.0.1:9000", None).unwrap();

        assert!(request.headers().get(AUTHORIZATION).is_none());
    }

    #[test]
    fn verify_listener_bearer_token_accepts_matching_token() {
        let request = Request::builder()
            .header(AUTHORIZATION, "Bearer 123456")
            .body(())
            .unwrap();
        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(101)
            .body(())
            .unwrap();

        assert!(verify_listener_bearer_token(&request, response, "123456").is_ok());
    }

    #[test]
    fn verify_listener_bearer_token_rejects_missing_or_mismatched_token() {
        let request = Request::builder().body(()).unwrap();
        let response = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(101)
            .body(())
            .unwrap();

        let error = verify_listener_bearer_token(&request, response, "123456").unwrap_err();
        assert_eq!(error.status(), 401);
    }
}

// accept_pending_peer 负责把 listener 刚 accept 到的 TCP 连接升级成 websocket，
// 并返回一个“已完成握手、尚未接入 session”的 PendingPeer。
//
// 这个函数是 inbound listener 方向与 session 管理层之间的边界：
// - 上游只需要提供原始 TCP 连接
// - 下游拿到的就是统一形态的 PendingPeer
//
// 入参说明：
// - stream：listener 刚 accept 到的原始 TCP 连接
// - expected_token：listener 侧要求的可选 Bearer token；为空时保持旧逻辑
pub async fn accept_pending_peer(
    stream: TcpStream,
    expected_token: Option<&str>,
) -> Result<PendingPeer, AppError> {
    // listener accept 到 TCP 后，在这里补 websocket 握手。
    // 参数说明：
    // - MaybeTlsStream::Plain(stream)：当前 listener 侧接入的是明文 TCP 连接
    // - accept_async(...)：在现有 TCP 连接上完成 websocket 握手升级
    let ws_stream = if let Some(token) = expected_token.filter(|token| !token.is_empty()) {
        accept_hdr_async(
            MaybeTlsStream::Plain(stream),
            move |req: &Request, response: Response| {
                verify_listener_bearer_token(req, response, token)
            },
        )
        .await?
    } else {
        accept_async(MaybeTlsStream::Plain(stream)).await?
    };
    debug_log("ws", "Inbound listener WebSocket accepted");
    Ok(PendingPeer {
        source: PeerSource::Listener,
        ws_stream,
    })
}

fn verify_listener_bearer_token(
    req: &Request,
    response: Response,
    expected_token: &str,
) -> std::result::Result<Response, tokio_tungstenite::tungstenite::handshake::server::ErrorResponse>
{
    let actual = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if actual == format!("Bearer {expected_token}") {
        return Ok(response);
    }

    Err(tokio_tungstenite::tungstenite::http::Response::builder()
        .status(401)
        .body(Some("Unauthorized".to_string()))
        .expect("build unauthorized response"))
}

// spawn_peer_tasks 负责把一个 PendingPeer 真正接入当前 session 的读写体系。
//
// 它会完成四件事：
// 1. 把 websocket split 成读半边和写半边
// 2. 启动 reader task，负责读取入站消息并回投 router
// 3. 启动 writer task，负责消费本 peer 的 control/audio 出站队列
// 4. 组装一个 WsPeerHandle，供 supervisor 后续做强制关闭
//
// 入参说明：
// - handle：当前 session 复用的 tokio runtime handle，reader / writer 都运行在其上
// - pending_peer：已经完成 websocket 握手、但尚未挂入 session 的对端
// - peer_id：supervisor 分配给该 peer 的会话内唯一编号
// - route_channel_writer：reader 解码后把入站消息投递回 router 的统一入口
// - control_ws_reader：消费发往该 peer 的控制消息队列
// - audio_ws_reader：消费发往该 peer 的录音流二进制队列
// - ws_send_signal：writer 的共享阻塞通知器，用于“有数据可发”和“需要退出”两类唤醒
pub fn spawn_peer_tasks(
    handle: &Handle,
    pending_peer: PendingPeer,
    peer_id: PeerId,
    route_channel_writer: mpsc::SyncSender<SessionControl>,
    control_ws_reader: mpsc::Receiver<OutboundControl>,
    audio_ws_reader: mpsc::Receiver<Vec<u8>>,
    ws_send_signal: Arc<WriteSignal>,
) -> Result<WsPeerTasks, AppError> {
    // 每个 peer 都拥有自己独立的一套 control/audio 队列消费侧和 reader/writer task。
    // 这样任何一个 peer 出问题，都只影响它自己，不会直接拖垮其他 peer。
    let peer_handle = Arc::new(WsPeerHandle::new());
    // 参数说明：
    // - pending_peer.ws_stream：当前 peer 对应的 websocket 全双工连接
    // - split()：拆成独立的读半边 / 写半边，便于 reader 与 writer 并发运行
    let (write_half, read_half) = pending_peer.ws_stream.split();
    // 参数说明：
    // - handle：让 ws-reader 运行在 supervisor 的 tokio runtime 上
    // - peer_id：把“这条入站消息来自谁”带回 router
    // - read_half：当前 peer 的 websocket 只读半边
    // - route_channel_writer：reader 解码后把 RoutedInbound 投递回 session 总线
    let ws_read_thread_handle = spawn_ws_reader(handle, peer_id, read_half, route_channel_writer);
    // 参数说明：
    // - handle：让 ws-writer 运行在 supervisor 的 tokio runtime 上
    // - write_half：当前 peer 的 websocket 只写半边
    // - control_ws_reader：消费发往本 peer 的文本控制消息
    // - audio_ws_reader：消费发往本 peer 的音频二进制消息
    // - ws_send_signal.clone()：writer 空闲时阻塞等待，关闭时也通过它被唤醒
    let ws_write_thread_handle = spawn_ws_writer(
        handle,
        write_half,
        control_ws_reader,
        audio_ws_reader,
        ws_send_signal.clone(),
    );
    // 参数说明：
    // - ws_read_thread_handle.abort_handle()：后续需要时可强制中断 reader
    // - ws_write_thread_handle.abort_handle()：后续需要时可强制中断 writer
    // - ws_send_signal：关闭时先用它把 writer 从阻塞 wait() 中唤醒
    peer_handle.install_close_state(WsPeerCloseState {
        ws_reader_abort_handle: ws_read_thread_handle.abort_handle(),
        ws_writer_abort_handle: ws_write_thread_handle.abort_handle(),
        ws_send_signal,
    })?;
    Ok(WsPeerTasks {
        peer_handle,
        ws_read_thread_handle,
        ws_write_thread_handle,
    })
}

// spawn_ws_reader 负责启动“单个 peer 的 websocket 入站读取任务”。
//
// reader 的职责非常单一：
// - 从 websocket 连续读取 frame
// - 调 codec 把 frame 还原成 InboundMessage
// - 把消息连同来源 peer_id 一起回投给 router
//
// 它不在这里做业务分发，也不关心请求会被谁处理。
//
// 入参说明：
// - handle：运行 reader async task 的 tokio runtime handle
// - peer_id：当前 reader 所属 peer 的编号；回投 router 时要带上它
// - read_half：当前 peer websocket 的只读半边
// - route_channel_writer：把解码后的入站消息送回 router 的同步通道
fn spawn_ws_reader(
    handle: &Handle,
    peer_id: PeerId,
    mut read_half: WsReadHalf,
    route_channel_writer: mpsc::SyncSender<SessionControl>,
) -> JoinHandle<Result<(), AppError>> {
    handle.spawn(async move {
        debug_log("ws", format!("WebSocket reader task started for peer {peer_id}"));
        loop {
            // 参数说明：
            // - read_half.next().await：异步等待下一帧 websocket 输入；没有新数据时这里会挂起
            match read_half.next().await {
                Some(Ok(message)) => {
                    // 参数说明：
                    // - message：底层 websocket 原始 frame
                    // - decode_message(message)：把 frame 还原成当前客户端真正关心的协议语义
                    match decode_message(message)? {
                    DecodeResult::Message(inbound) => {
                        // reader 永远只做“解码 + 把来源 peer_id 带回 router”，
                        // 不在这里做任何业务级分发。
                        // 参数说明：
                        // - peer_id：标记这条入站业务消息来自哪个 peer
                        // - inbound：已经脱离 websocket frame 的业务消息
                        // - RoutedInbound { peer_id, message: inbound }：把“消息内容”和“来源 peer”
                        //   绑定在一起，供 router 后续按来源回响应
                        // - SessionControl::Inbound(...)：统一走 session 总线回到 router
                        if route_channel_writer
                            .send(SessionControl::Inbound(RoutedInbound {
                                peer_id,
                                message: inbound,
                            }))
                            .is_err()
                        {
                            debug_log("ws", format!("Router control channel closed; WebSocket reader will exit for peer {peer_id}"));
                            break;
                        }
                        debug_log("ws", format!("Inbound message forwarded to router for peer {peer_id}"));
                    }
                    DecodeResult::Ignore => {
                        debug_log("ws", format!("Inbound frame ignored for peer {peer_id}"));
                    }
                    DecodeResult::Close => {
                        debug_log("ws", format!("Inbound close frame received; WebSocket reader will exit for peer {peer_id}"));
                        break;
                    }
                    }
                }
                Some(Err(err)) => {
                    debug_err_log("ws", format!("WebSocket stream error for peer {peer_id}: {err}"));
                    return Err(anyhow::Error::from(err));
                }
                None => {
                    debug_log("ws", format!("WebSocket stream ended by peer {peer_id}"));
                    break;
                }
            }
        }

        debug_log("ws", format!("WebSocket reader task completed for peer {peer_id}"));
        Ok(())
    })
}

// spawn_ws_writer 负责启动“单个 peer 的 websocket 出站写任务”。
//
// writer 的整体策略是：
// 1. 没有待发送数据时，阻塞在共享通知器 wait() 上
// 2. 被唤醒后，优先 drain control 队列
// 3. 再 drain audio 队列
// 4. 两类本地来源都结束后，退出 writer
//
// 这样的设计能同时满足两点：
// - 平时没有数据时不空转
// - 有数据时尽快把 control 小消息优先发出去，不被持续音频流压住
//
// 入参说明：
// - handle：运行 writer async task 的 tokio runtime handle
// - write_half：当前 peer websocket 的只写半边
// - control_ws_reader：控制消息消费端，承载 text / close 这类出站指令
// - audio_ws_reader：音频流消费端，承载录音流二进制数据
// - ws_send_signal：writer 的共享唤醒器；无数据时阻塞等它，有关闭请求时也靠它被拍醒
fn spawn_ws_writer(
    handle: &Handle,
    mut write_half: WsWriteHalf,
    control_ws_reader: mpsc::Receiver<OutboundControl>,
    audio_ws_reader: mpsc::Receiver<Vec<u8>>,
    ws_send_signal: Arc<WriteSignal>,
) -> JoinHandle<Result<(), AppError>> {
    handle.spawn(async move {
        debug_log("ws", "WebSocket writer task started");
        let mut control_queue_closed = false;
        let mut audio_queue_closed = false;
        let mut close_frame_sent = false;
        let mut force_closed = false;

        'writer: loop {
            // writer 没有数据时会睡在共享通知器上。
            // 被任一生产者唤醒后，再按照“先 control、后 audio”的顺序尽量 drain。
            // 参数说明：
            // - tokio::task::block_in_place(...)：当前处在 async task 中，但这里要等待的是
            //   std::sync::Condvar；因此需要切到 block_in_place 包装的阻塞区间
            // - ws_send_signal.wait()：阻塞直到“至少有一侧成功入队”或“外部要求关闭”
            match tokio::task::block_in_place(|| ws_send_signal.wait()) {
                WriteSignalWake::Notified => {}
                WriteSignalWake::Closed => {
                    force_closed = true;
                    debug_log("ws", "Writer force-close signal received");
                    break 'writer;
                }
            }

            loop {
                // progressed 表示“本轮唤醒后是否真的从任一队列取到过数据”。
                //
                // 它的作用不是统计条数，而是帮助 writer 区分两种情况：
                // - true：本轮确实发送过至少一条消息，应该继续尝试下一轮 drain
                // - false：两侧 try_recv() 都已经读空了，可以回到外层 wait() 再次阻塞
                let mut progressed = false;

                loop {
                    if control_queue_closed {
                        break;
                    }
                    if ws_send_signal.is_closed() {
                        force_closed = true;
                        debug_log(
                            "ws",
                            "Writer force-close signal received while draining control queue",
                        );
                        break 'writer;
                    }
                    // 参数说明：
                    // - control_ws_reader.try_recv()：非阻塞地继续 drain control 队列；
                    //   被 wait() 唤醒后，这里会尽量把当前已积压的 control 消息一次性发完
                    match control_ws_reader.try_recv() {
                        Ok(control) => {
                            progressed = true;
                            if let OutboundControl::ClearAudioQueue(ack) = control {
                                let mut cleared = 0usize;
                                loop {
                                    match audio_ws_reader.try_recv() {
                                        Ok(_bytes) => {
                                            cleared += 1;
                                        }
                                        Err(mpsc::TryRecvError::Empty) => break,
                                        Err(mpsc::TryRecvError::Disconnected) => {
                                            audio_queue_closed = true;
                                            break;
                                        }
                                    }
                                }
                                let _ = ack.send(cleared);
                                debug_log(
                                    "ws",
                                    format!("Cleared outbound audio queue: {cleared} frame(s)"),
                                );
                                continue;
                            }
                            // control 通道优先级更高，确保 response / close 这类小消息
                            // 不会被持续音频流长时间压住。
                            let is_close_frame = matches!(control, OutboundControl::Close);
                            // 参数说明：
                            // - control：当前取到的一条控制消息，可能是 Text 或 Close
                            // - encode_outbound(control)：把内部控制语义编码成 websocket frame
                            write_half
                                .send(encode_outbound(control))
                                .await
                                .map_err(|err| {
                                    debug_err_log(
                                        "ws",
                                        format!("Failed to send outbound control message: {err}"),
                                    );
                                    anyhow::Error::from(err)
                                })?;
                            if is_close_frame {
                                close_frame_sent = true;
                                debug_log(
                                    "ws",
                                    "Outbound close frame sent; WebSocket writer will exit",
                                );
                                break 'writer;
                            }
                            debug_log("ws", "Outbound control message sent");
                        }
                        Err(mpsc::TryRecvError::Empty) => break,
                        Err(mpsc::TryRecvError::Disconnected) => {
                            debug_log("ws", "Outbound control channel closed");
                            control_queue_closed = true;
                            break;
                        }
                    }
                }

                loop {
                    if audio_queue_closed {
                        break;
                    }
                    if ws_send_signal.is_closed() {
                        force_closed = true;
                        debug_log(
                            "ws",
                            "Writer force-close signal received while draining audio queue",
                        );
                        break 'writer;
                    }
                    // 参数说明：
                    // - audio_ws_reader.try_recv()：非阻塞地继续 drain audio 队列；
                    //   只有在 control 队列已经暂时读空后，才轮到 audio 继续发送
                    match audio_ws_reader.try_recv() {
                        Ok(bytes) => {
                            progressed = true;
                            // 参数说明：
                            // - bytes：一块已经编码好的录音流负载
                            // - OutboundControl::Binary(bytes)：统一借用 encode_outbound() 走
                            //   websocket 二进制帧编码路径
                            write_half
                                .send(encode_outbound(OutboundControl::Binary(bytes)))
                                .await
                                .map_err(|err| {
                                    debug_err_log(
                                        "ws",
                                        format!(
                                            "Failed to send outbound audio stream frame: {err}"
                                        ),
                                    );
                                    anyhow::Error::from(err)
                                })?;
                            debug_log_limited(
                                "ws",
                                "outbound-audio-stream-frame-sent",
                                Duration::from_secs(60),
                                "Outbound audio stream frame sent",
                            );
                        }
                        Err(mpsc::TryRecvError::Empty) => break,
                        Err(mpsc::TryRecvError::Disconnected) => {
                            debug_log("ws", "Audio stream channel closed");
                            audio_queue_closed = true;
                            break;
                        }
                    }
                }

                if control_queue_closed && audio_queue_closed {
                    // 当两个本地来源都已经断开时，说明这个 peer 后续不再可能有出站数据。
                    debug_log(
                        "ws",
                        "All outbound channels closed; WebSocket writer will exit",
                    );
                    break 'writer;
                }

                if !progressed {
                    break;
                }
            }
        }

        if !force_closed && !close_frame_sent {
            // 参数说明：
            // - write_half.close().await：在“没有被强制打断、也没有主动发送 Close 帧”的情况下，
            //   尝试对 websocket 写半边做一次正常收尾
            let _ = write_half.close().await;
        }
        debug_log("ws", "WebSocket writer task completed");
        Ok(())
    })
}
