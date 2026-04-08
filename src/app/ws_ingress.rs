use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::runtime::Handle;

use super::supervisor::SupervisorEvent;
use crate::base::{AppError, debug_err_log, debug_log};
use crate::transport::{accept_pending_peer, connect_pending_peer};

// 设备侧固定监听地址。
//
// 当前 listen-only / hybrid 两种模式都会在这里接受远端连入。
pub(crate) const LISTEN_ADDR: &str = "0.0.0.0:4399";

// 启动 listener 线程。
//
// 它的职责只有一件事：accept TCP -> 升级为 websocket -> 把 PendingPeer 发回 supervisor。
// 它不直接参与 session 生命周期管理。
//
// 入参说明：
// - runtime_handle：供线程内执行 async bind / accept / websocket 握手的 tokio runtime handle
// - ws_connect_event_writer：把握手成功的 PendingPeer 回传给 supervisor 的发送端
pub(crate) fn spawn_listener_thread(
    runtime_handle: Handle,
    ws_connect_event_writer: mpsc::Sender<SupervisorEvent>,
) {
    thread::Builder::new()
        .name("ws-listener-thread".to_string())
        .spawn(move || {
            loop {
                // listener 放在循环里，是为了 bind 或 accept 链路偶发失败后还能自动重建。
                // 参数说明：
                // - LISTEN_ADDR：固定监听地址 `0.0.0.0:4399`
                // - runtime_handle.block_on(...)：在线程里复用现有 tokio runtime 执行 async bind
                match runtime_handle.block_on(async { TcpListener::bind(LISTEN_ADDR).await }) {
                    Ok(listener) => {
                        debug_log(
                            "supervisor",
                            format!("Listening for WebSocket peers on {LISTEN_ADDR}"),
                        );
                        // 参数说明：
                        // - listener.accept().await：阻塞等待下一条 TCP 连接
                        // - accept_pending_peer(stream).await：把原始 TCP 连接升级成统一 PendingPeer
                        let accept_result = runtime_handle.block_on(async {
                            loop {
                                let (stream, addr) = listener.accept().await?;
                                debug_log(
                                    "supervisor",
                                    format!("Accepted TCP connection from {addr}"),
                                );
                                // 参数说明：
                                // - stream：刚 accept 到的原始 TCP 连接，后续在这里补 ws 握手
                                match accept_pending_peer(stream).await {
                                    Ok(pending_peer) => {
                                        // listener 线程不直接 attach peer；
                                        // 它只负责把“已就绪的 PendingPeer”送回 supervisor 总线。
                                        // 参数说明：
                                        // - SupervisorEvent::PendingPeer(pending_peer)：
                                        //   把当前已完成握手的 listener 对端上报给 supervisor
                                        if ws_connect_event_writer
                                            .send(SupervisorEvent::PendingPeer(pending_peer))
                                            .is_err()
                                        {
                                            return Ok::<(), AppError>(());
                                        }
                                    }
                                    Err(err) => {
                                        debug_err_log(
                                            "supervisor",
                                            format!(
                                                "Failed to upgrade inbound peer to WebSocket: {err}"
                                            ),
                                        );
                                    }
                                }
                            }
                        });
                        if let Err(err) = accept_result {
                            debug_err_log(
                                "supervisor",
                                format!("Listener loop failed on {LISTEN_ADDR}: {err}"),
                            );
                        }
                    }
                    Err(err) => {
                        debug_err_log(
                            "supervisor",
                            format!("Failed to bind listener on {LISTEN_ADDR}: {err}"),
                        );
                    }
                }

                thread::sleep(Duration::from_secs(1));
            }
        })
        .expect("spawn ws listener thread");
}

// 启动主动外连线程。
//
// 它会在后台持续尝试连接配置里的 websocket_server_url，
// 并通过 ConnectServerStatus 保证同一时刻最多保留一个活跃 outbound peer。
//
// 入参说明：
// - runtime_handle：供线程内执行 async websocket connect 的 tokio runtime handle
// - url：当前进程级主动连接目标
// - ws_connect_event_writer：把 connect 成功的 PendingPeer 回传给 supervisor 的发送端
// - connect_server_status：主动外连闸门，确保同时最多只有一个活跃 outbound peer
pub(crate) fn spawn_connector_thread(
    runtime_handle: Handle,
    url: String,
    ws_connect_event_writer: mpsc::Sender<SupervisorEvent>,
    connect_server_status: Arc<ConnectServerStatus>,
) {
    thread::Builder::new()
        .name("ws-connector-thread".to_string())
        .spawn(move || {
            loop {
                // connect_server_status 确保同一时间最多只有一个“已连接的 outbound peer”。
                // 只有当旧的 outbound peer 明确离开后，这里才会继续下一轮 connect。
                // 参数说明：
                // - wait_until_inactive()：如果当前已有活跃 outbound peer，就在这里等待
                connect_server_status.wait_until_inactive();
                // 参数说明：
                // - &runtime_handle：用 supervisor 的 tokio runtime 执行 async connect
                // - &url：当前进程级配置指定的主动连接目标
                match connect_pending_peer(&runtime_handle, &url) {
                    Ok(pending_peer) => {
                        connect_server_status.mark_connected();
                        // 和 listener 一样，connector 线程本身不触碰 session，只上报 PendingPeer。
                        // 参数说明：
                        // - SupervisorEvent::PendingPeer(pending_peer)：
                        //   把当前主动 connect 成功的对端上报给 supervisor
                        if ws_connect_event_writer
                            .send(SupervisorEvent::PendingPeer(pending_peer))
                            .is_err()
                        {
                            connect_server_status.mark_disconnected();
                            break;
                        }
                    }
                    Err(err) => {
                        debug_err_log(
                            "supervisor",
                            format!("Outbound WebSocket connection failed for {url}: {err}"),
                        );
                        thread::sleep(Duration::from_secs(1));
                    }
                }
            }
        })
        .expect("spawn ws connector thread");
}

// ConnectServerStatus 是主动外连方向的一个简单闸门。
//
// 它只关心 outbound connect 这一条链路，不管理 listen 进来的 peer，
// 用来防止 connector 并发建出多个活跃 outbound 连接。
pub(crate) struct ConnectServerStatus {
    // true 表示当前已经存在一个激活的 outbound peer，connector 线程应当等待。
    state: Mutex<bool>,
    condvar: Condvar,
}

impl ConnectServerStatus {
    // 创建一个新的外连闸门，初始状态为“当前没有活跃 outbound peer”。
    //
    // 入参说明：
    // - 无
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(false),
            condvar: Condvar::new(),
        }
    }

    // 等待直到当前不存在活跃 outbound peer。
    //
    // connector 线程在发起下一轮 connect 前都会先经过这里。
    //
    // 入参说明：
    // - self：当前主动外连闸门
    pub(crate) fn wait_until_inactive(&self) {
        // connector 线程会阻塞在这里，直到 outbound peer 被 supervisor 标记为 disconnected。
        let mut active = self.state.lock().expect("connector gate poisoned");
        while *active {
            active = self
                .condvar
                .wait(active)
                .expect("connector gate poisoned while waiting");
        }
    }

    // 把闸门标记为“当前已经有一个活跃 outbound peer”。
    //
    // 入参说明：
    // - self：当前主动外连闸门
    pub(crate) fn mark_connected(&self) {
        // connect 成功后立即上锁，避免并发再起一条新的主动连接。
        *self.state.lock().expect("connector gate poisoned") = true;
    }

    // 把闸门标记为“当前没有活跃 outbound peer”，并唤醒等待中的 connector。
    //
    // 入参说明：
    // - self：当前主动外连闸门
    pub(crate) fn mark_disconnected(&self) {
        // 只有从 active -> inactive 的边沿变化才需要唤醒等待中的 connector 线程。
        let mut active = self.state.lock().expect("connector gate poisoned");
        if *active {
            *active = false;
            self.condvar.notify_all();
        }
    }
}
