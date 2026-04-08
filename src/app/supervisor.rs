use std::sync::{Arc, mpsc};

use tokio::runtime::{Builder, Runtime};

use crate::app::RunConfig;
use crate::app::ws_ingress::{ConnectServerStatus, spawn_connector_thread, spawn_listener_thread};
use crate::base::{AppError, debug_err_log, debug_log, is_debug_enabled};
use crate::transport::{PeerId, PeerSource, PendingPeer};

use super::session_peer::{PeerTaskExit, RouterExit, SessionRuntime};

// SupervisorEvent 是 AppSupervisor 主循环消费的统一事件入口。
//
// listener / connector / peer waiter / router waiter
// 都只负责把变化投递到这条总线，不直接操纵 session 生命周期。
pub(crate) enum SupervisorEvent {
    // 某个 listener / connector 已经完成 websocket 握手，等待 supervisor 决定是否挂入当前 session。
    PendingPeer(PendingPeer),
    // 某个 peer 的 reader 或 writer 任务已经退出。
    PeerTaskExited(PeerTaskExit),
    // 当前 session 的 router 已经退出，通常意味着整轮 session 需要结束。
    RouterExited(RouterExit),
}

// AppSupervisor 是整个进程的“长生命周期编排器”。
//
// 它和旧版“单连接 + 断线重连”的最大差异是：
// - 现在 session 不是一个 websocket，而是一组 peer 的集合
// - listen 线程、connector 线程可以常驻于 session 之外
// - session 只在“至少有一个 peer 存在”时被创建
//
// 可以把它理解成两层循环：
// 1. 进程级：永远监听 supervisor event，等待新 peer / 退出事件
// 2. session 级：有 peer 时创建资源，无 peer 时销毁资源
pub struct AppSupervisor {
    run_config: RunConfig,
    // ws_runtime 专门服务所有 websocket accept/connect/reader/writer 任务。
    ws_runtime: Runtime,
    next_session_id: u64,
    next_peer_id: PeerId,
}

impl AppSupervisor {
    // 创建进程级 supervisor。
    //
    // 它只初始化长期存在的状态：
    // - 命令行解析出来的 RunConfig
    // - 统一承载 websocket 相关 async 任务的 tokio runtime
    // - session_id / peer_id 的本地递增编号器
    //
    // 入参说明：
    // - run_config：启动阶段解析好的进程级运行配置
    pub fn new(run_config: RunConfig) -> Self {
        // 即使当前大部分业务还是同步线程模型，ws 层依然统一放到 tokio runtime 里，
        // 这样 listener、connect、reader、writer 的实现会更自然。
        let ws_runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build ws runtime");
        Self {
            run_config,
            ws_runtime,
            next_session_id: 1,
            next_peer_id: 1,
        }
    }

    // 运行 supervisor 的主循环。
    //
    // 这是整个客户端的长期阻塞入口，负责：
    // - 启动 listener / connector 线程
    // - 接收 SupervisorEvent 总线事件
    // - 在第一个 peer 到来时创建 session
    // - 在 peer 或 router 退出时决定是只清单个 peer，还是清掉整轮 session
    //
    // 入参说明：
    // - self：当前进程级 supervisor
    pub fn run_forever(&mut self) -> Result<(), AppError> {
        println!("client started");
        debug_log(
            "supervisor",
            format!(
                "Client main loop started; debug_enabled={}, listen_enabled={}, server_url={:?}",
                is_debug_enabled(),
                self.run_config.listen_enabled,
                self.run_config.server_url
            ),
        );

        // ws_connect_event_writer / ws_connect_event_reader 是 supervisor 的“总线”：
        // - listener 连上一个 peer，就发 PendingPeer
        // - connector 连上一个 peer，也发 PendingPeer
        // - 某个 peer 的 reader/writer 退出，会发 PeerTaskExited
        // - router 退出，会发 RouterExited
        //
        // supervisor 自己只阻塞在 ws_connect_event_reader.recv() 上，由它统一推进所有生命周期动作。
        let (ws_connect_event_writer, ws_connect_event_reader) = mpsc::channel::<SupervisorEvent>();
        let connect_server_status = Arc::new(ConnectServerStatus::new());

        if self.run_config.listen_enabled {
            // 参数说明：
            // - self.ws_runtime.handle().clone()：让 listener 线程复用 supervisor 的 tokio runtime
            // - ws_connect_event_writer.clone()：把握手成功的 PendingPeer 回传给 supervisor 主循环
            spawn_listener_thread(
                self.ws_runtime.handle().clone(),
                ws_connect_event_writer.clone(),
            );
        }
        if let Some(url) = self.run_config.server_url.clone() {
            // 参数说明：
            // - self.ws_runtime.handle().clone()：供 connector 在线程内执行 async connect
            // - url：当前进程级配置指定的主动连接目标
            // - ws_connect_event_writer.clone()：把 connect 成功的 PendingPeer 送回 supervisor
            // - connect_server_status.clone()：保证同一时刻最多只有一个活跃 outbound peer
            spawn_connector_thread(
                self.ws_runtime.handle().clone(),
                url,
                ws_connect_event_writer.clone(),
                connect_server_status.clone(),
            );
        }

        let mut session = None::<SessionRuntime>;

        loop {
            // 参数说明：
            // - ws_connect_event_reader.recv()：阻塞等待下一条生命周期事件
            let event = ws_connect_event_reader.recv().map_err(|err| {
                anyhow::anyhow!("supervisor event channel closed unexpectedly: {err}")
            })?;

            match event {
                SupervisorEvent::PendingPeer(pending_peer) => {
                    // 第一个 peer 到来时，才真正分配整轮 session 的共享资源。
                    if session.is_none() {
                        // 参数说明：
                        // - ws_connect_event_writer.clone()：交给 session 内部 waiter，
                        //   让 peer/router 的退出事件继续回流到 supervisor 主总线
                        session = Some(self.start_session(ws_connect_event_writer.clone())?);
                    }

                    // peer_id 由 supervisor 统一发号，避免 listen / connect / router 各自维护编号。
                    let peer_id = self.allocate_peer_id();
                    let source = pending_peer.source.clone();
                    if let Some(active_session) = session.as_mut() {
                        // 参数说明：
                        // - peer_id：当前 peer 在本轮 session 内的唯一编号
                        // - pending_peer：已经完成 websocket 握手、但尚未真正挂入 session 的对端
                        // - self.ws_runtime.handle()：reader / writer task 运行所需的 runtime handle
                        // - ws_connect_event_writer.clone()：供本 peer 的退出 waiter 回传退出事件
                        if let Err(err) = active_session.attach_peer(
                            peer_id,
                            pending_peer,
                            self.ws_runtime.handle(),
                            ws_connect_event_writer.clone(),
                        ) {
                            debug_err_log(
                                "supervisor",
                                format!("Failed to attach peer {peer_id}: {err}"),
                            );
                            if matches!(source, PeerSource::OutboundConnect { .. }) {
                                connect_server_status.mark_disconnected();
                            }
                        }
                    }
                }
                SupervisorEvent::PeerTaskExited(exit) => {
                    self.log_peer_task_exit(&exit);

                    let session_matches = session
                        .as_ref()
                        .map(|active_session| active_session.session_id)
                        == Some(exit.session_id);
                    if !session_matches {
                        if matches!(exit.source, PeerSource::OutboundConnect { .. }) {
                            connect_server_status.mark_disconnected();
                        }
                        continue;
                    }

                    // should_shutdown 表示“当前 peer 退出后，是否已经没有任何活跃 peer 了”。
                    let mut should_shutdown = false;
                    if let Some(active_session) = session.as_mut() {
                        if let Some(removal) = active_session.handle_peer_departure(exit.peer_id)? {
                            if matches!(removal.source, PeerSource::OutboundConnect { .. }) {
                                connect_server_status.mark_disconnected();
                            }
                            should_shutdown = removal.remaining_peers == 0;
                        }
                    }

                    if should_shutdown {
                        // 最后一个 peer 离开时，整轮 session 的共享资源都需要被统一回收。
                        let summary = session.take().expect("session exists").shutdown();
                        if summary.closed_outbound_peers > 0 {
                            connect_server_status.mark_disconnected();
                        }
                        println!("connection closed");
                    }
                }
                SupervisorEvent::RouterExited(exit) => {
                    self.log_router_exit(&exit);

                    let session_matches = session
                        .as_ref()
                        .map(|active_session| active_session.session_id)
                        == Some(exit.session_id);
                    if !session_matches {
                        continue;
                    }

                    // router 退出说明这轮业务总线已经不可用，因此直接结束整轮 session。
                    let summary = session.take().expect("session exists").shutdown();
                    if summary.closed_outbound_peers > 0 {
                        connect_server_status.mark_disconnected();
                    }
                    println!("connection closed");
                }
            }
        }
    }

    // 创建一轮新的 SessionRuntime。
    //
    // supervisor 只在“当前没有活跃 session 且收到了首个 PendingPeer”时调用它。
    //
    // 入参说明：
    // - self：当前进程级 supervisor
    // - ws_connect_event_writer：交给新 session 内部各 waiter 使用的事件回传发送端
    fn start_session(
        &mut self,
        ws_connect_event_writer: mpsc::Sender<SupervisorEvent>,
    ) -> Result<SessionRuntime, AppError> {
        let session_id = self.allocate_session_id();
        debug_log(
            "supervisor",
            format!("Session lifecycle event: starting session {session_id}"),
        );
        // 参数说明：
        // - session_id：本地递增的 session 标签，用于日志和退出事件关联
        // - ws_connect_event_writer：交给 session 内部，用于上报 peer/router 退出事件
        SessionRuntime::new(session_id, ws_connect_event_writer)
    }

    // 分配下一轮 session 的本地编号。
    //
    // 这个编号只用于日志、排障和把退出事件和具体 session 对上。
    //
    // 入参说明：
    // - self：当前进程级 supervisor
    fn allocate_session_id(&mut self) -> u64 {
        let id = self.next_session_id;
        self.next_session_id = self.next_session_id.saturating_add(1);
        id
    }

    // 分配当前进程内下一个 peer_id。
    //
    // 编号权统一放在 supervisor，可以避免 attach 流程中出现多个层级各自发号。
    //
    // 入参说明：
    // - self：当前进程级 supervisor
    fn allocate_peer_id(&mut self) -> PeerId {
        let id = self.next_peer_id;
        self.next_peer_id = self.next_peer_id.saturating_add(1);
        id
    }

    // 统一记录某个 peer 任务退出的日志。
    //
    // 正常退出和异常退出都在这里收敛，避免主循环里混杂过多日志分支。
    //
    // 入参说明：
    // - self：当前进程级 supervisor
    // - exit：某个 peer task 的退出摘要
    fn log_peer_task_exit(&self, exit: &PeerTaskExit) {
        match &exit.result {
            Ok(()) => {
                debug_log(
                    "supervisor",
                    format!(
                        "Peer task exited: session={}, peer={}, task={}",
                        exit.session_id, exit.peer_id, exit.task_name
                    ),
                );
            }
            Err(err) => {
                debug_err_log(
                    "supervisor",
                    format!(
                        "Peer task failed: session={}, peer={}, task={}, err={err}",
                        exit.session_id, exit.peer_id, exit.task_name
                    ),
                );
                eprintln!("session error: {err}");
            }
        }
    }

    // 统一记录 router 退出日志。
    //
    // router 退出往往意味着整轮 session 结束，因此这里会把异常信息明确打印出来。
    //
    // 入参说明：
    // - self：当前进程级 supervisor
    // - exit：某轮 session router 的退出摘要
    fn log_router_exit(&self, exit: &RouterExit) {
        match &exit.result {
            Ok(()) => {
                debug_log(
                    "supervisor",
                    format!("Router exited for session {}", exit.session_id),
                );
            }
            Err(err) => {
                debug_err_log(
                    "supervisor",
                    format!("Router failed for session {}: {err}", exit.session_id),
                );
                eprintln!("session error: {err}");
            }
        }
    }
}
