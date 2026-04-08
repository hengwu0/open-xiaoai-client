use std::collections::HashMap;
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::task::JoinHandle as TokioJoinHandle;

use super::commands::register_session_commands;
use super::fanout::{MediaDevices, MonitorHandles, spawn_record_fanout_thread};
use super::supervisor::SupervisorEvent;
use super::ws_peer_hub::WsPeerHub;
use crate::audio::RecordOutputSender;
use crate::base::{AppError, debug_log};
use crate::protocol::registry::CommandRegistry;
use crate::protocol::router::{RouterThread, spawn_router};
use crate::transport::{
    NotifyingSender, OutboundControl, PeerId, PeerSource, PendingPeer, SessionControl, WriteSignal,
    spawn_peer_tasks,
};

// PeerTaskExit 表示“某个 peer 下面的某个具体任务已经退出”。
//
// supervisor 收到它后，并不会区分 reader / writer 谁先退出，
// 而是统一把这个 peer 当作需要收尾的对象处理。
pub(crate) struct PeerTaskExit {
    pub(crate) session_id: u64,
    pub(crate) peer_id: PeerId,
    pub(crate) source: PeerSource,
    pub(crate) task_name: &'static str,
    pub(crate) result: Result<(), AppError>,
}

// RouterExit 表示“当前 session 对应的 router 线程已经退出”。
//
// 这类事件的语义比单个 peer 退出更强，
// 因为 router 一旦没了，整轮 session 的业务总线也就不可用了。
pub(crate) struct RouterExit {
    pub(crate) session_id: u64,
    pub(crate) result: Result<(), AppError>,
}

// SessionRuntime 表示“一轮活跃 session”的完整资源集合。
//
// 和 AppSupervisor 的关系是：
// - supervisor 负责决定何时创建 / 销毁 session
// - SessionRuntime 负责 session 内部资源的创建、peer 挂载、peer 收尾与统一清理
pub(crate) struct SessionRuntime {
    pub(crate) session_id: u64,
    route_channel_writer: mpsc::SyncSender<SessionControl>,
    peer_hub: Arc<WsPeerHub>,
    devices: MediaDevices,
    peer_tasks: HashMap<PeerId, SessionPeer>,
    monitor_handles: MonitorHandles,
    record_output_writer: RecordOutputSender,
    fanout_handle: JoinHandle<()>,
}

impl SessionRuntime {
    // 创建一轮新的 SessionRuntime，并初始化它内部的共享资源。
    //
    // 这里会一次性拉起：
    // - session 级 route_channel
    // - WsPeerHub
    // - AudioPlayer / AudioRecorder
    // - record-fanout-thread
    // - router-thread
    // - instruction / playing / kws 三个 monitor
    //
    // 入参说明：
    // - session_id：当前新 session 的本地编号
    // - ws_connect_event_writer：session 内部各 waiter 用它把退出事件回传给 supervisor
    pub(crate) fn new(
        session_id: u64,
        ws_connect_event_writer: mpsc::Sender<SupervisorEvent>,
    ) -> Result<Self, AppError> {
        let (route_channel_writer, route_channel_reader) =
            mpsc::sync_channel::<SessionControl>(256);
        let peer_hub = Arc::new(WsPeerHub::new());
        let devices = MediaDevices::new();

        let (record_output_writer_raw, record_output_reader) = mpsc::sync_channel::<Vec<u8>>(128);
        let record_output_writer = RecordOutputSender::new(record_output_writer_raw);
        // 参数说明：
        // - peer_hub.clone()：fanout 线程通过它查询当前录音订阅者，并把音频分发出去
        // - record_output_reader：接收 recorder 输出的 session 级单路录音数据
        let fanout_handle = spawn_record_fanout_thread(peer_hub.clone(), record_output_reader);

        let registry = Arc::new(CommandRegistry::new());
        // 参数说明：
        // - &registry：本轮 session 专属的命令注册表
        // - devices.player.clone()：共享播放器，供命令处理器调用
        // - devices.recorder.clone()：共享录音器，供命令处理器调用
        // - peer_hub.clone()：命令处理器通过它维护录音订阅集合
        // - record_output_writer：录音输出先进入 session 级 fanout 队列
        register_session_commands(
            &registry,
            devices.player.clone(),
            devices.recorder.clone(),
            peer_hub.clone(),
            record_output_writer.clone(),
        );

        // 参数说明：
        // - registry：本轮 session 的命令注册表，router 收到 Request 时通过它查 handler
        // - route_channel_reader：session 统一输入队列，收敛 monitor / ws-reader / supervisor 的消息
        // - devices.player.clone()：收到入站播放流时交给共享播放器消费
        // - peer_hub.clone()：router 通过它把 Response / Event / 广播消息继续分发给 peer
        let RouterThread {
            router_thread_handle,
        } = spawn_router(
            registry,
            route_channel_reader,
            devices.player.clone(),
            peer_hub.clone(),
        )?;
        // 参数说明：
        // - session_id：标记这个 router 属于哪一轮 session
        // - router_thread_handle：真正要等待的 router 线程句柄
        // - ws_connect_event_writer：把 router 的退出结果回传到 supervisor 总线
        spawn_router_exit_waiter(session_id, router_thread_handle, ws_connect_event_writer);

        // 参数说明：
        // - route_channel_writer.clone()：monitor 产生的本地事件统一写回 session 总线
        // - 文件类 monitor 通过 close() 关闭内部 fd 退出，playing monitor 通过 kill 子进程退出
        let monitor_handles = MonitorHandles::spawn(route_channel_writer.clone());

        debug_log(
            "supervisor",
            format!("Session resources created for session {session_id}"),
        );

        Ok(Self {
            session_id,
            route_channel_writer,
            peer_hub,
            devices,
            peer_tasks: HashMap::new(),
            monitor_handles,
            record_output_writer,
            fanout_handle,
        })
    }

    // 把一个已经完成 websocket 握手的 PendingPeer 挂入当前 session。
    //
    // 这里会完成三件事：
    // - 为该 peer 建立独立的 control/audio 队列
    // - 把它登记到 WsPeerHub
    // - 启动该 peer 自己的 ws-reader / ws-writer，并为它们挂退出 waiter
    //
    // 入参说明：
    // - self：当前活跃 session 资源集合
    // - peer_id：本次要挂入的 peer 编号
    // - pending_peer：已完成 websocket 握手、但尚未正式接入 session 的对端
    // - runtime_handle：运行 ws reader/writer async task 的 tokio runtime handle
    // - ws_connect_event_writer：把 peer task 退出事件回传给 supervisor 的发送端
    pub(crate) fn attach_peer(
        &mut self,
        peer_id: PeerId,
        pending_peer: PendingPeer,
        runtime_handle: &Handle,
        ws_connect_event_writer: mpsc::Sender<SupervisorEvent>,
    ) -> Result<(), AppError> {
        let source = pending_peer.source.clone();
        let ws_send_signal = Arc::new(WriteSignal::new());
        let (control_queue_writer, control_ws_reader) = mpsc::sync_channel::<OutboundControl>(256);
        let (audio_queue_writer, audio_ws_reader) = mpsc::sync_channel::<Vec<u8>>(128);

        // 参数说明：
        // - control_queue_writer：本 peer 文本控制消息的入队端
        // - ws_send_signal.clone()：有新控制消息时唤醒本 peer 的 writer
        let control_sender = NotifyingSender::new(control_queue_writer, ws_send_signal.clone());
        // 参数说明：
        // - audio_queue_writer：本 peer 二进制音频消息的入队端
        // - ws_send_signal.clone()：有新音频消息时同样唤醒本 peer 的 writer
        let audio_sender = NotifyingSender::new(audio_queue_writer, ws_send_signal.clone());
        // 参数说明：
        // - peer_id：当前 peer 在 session 内的唯一编号
        // - source.clone()：保留该 peer 的来源，便于后续判断是否是 outbound peer
        // - control_sender.clone()：广播/单播控制消息最终通过它发给本 peer
        // - audio_sender.clone()：录音 fanout 最终通过它发给本 peer
        self.peer_hub.add_peer(
            peer_id,
            source.clone(),
            control_sender.clone(),
            audio_sender.clone(),
        );

        // 参数说明：
        // - runtime_handle：让本 peer 的 reader / writer task 跑在 supervisor 管理的 tokio runtime 上
        // - pending_peer：已经完成 websocket 握手、准备挂入 session 的对端
        // - peer_id：当前 peer 在本轮 session 内的唯一编号
        // - self.route_channel_writer.clone()：reader 解码后的入站消息通过它回流给 router
        // - control_ws_reader：writer 从这里消费文本控制消息
        // - audio_ws_reader：writer 从这里消费音频二进制消息
        // - ws_send_signal：本 peer 独享的 writer 唤醒器 / 关闭信号
        match spawn_peer_tasks(
            runtime_handle,
            pending_peer,
            peer_id,
            self.route_channel_writer.clone(),
            control_ws_reader,
            audio_ws_reader,
            ws_send_signal,
        ) {
            Ok(peer_tasks) => {
                // 参数说明：
                // - runtime_handle.clone()：waiter 在线程里用它等待 tokio task 结束
                // - self.session_id：标明退出事件属于哪一轮 session
                // - peer_id：标明退出事件属于哪个 peer
                // - source.clone()：保留来源信息，供 supervisor 判断 outbound 状态
                // - "ws-reader"：退出事件里的任务名标签
                // - peer_tasks.ws_read_thread_handle：真正要等待的 reader task
                // - ws_connect_event_writer.clone()：把退出事件发回 supervisor
                spawn_tokio_peer_exit_waiter(
                    runtime_handle.clone(),
                    self.session_id,
                    peer_id,
                    source.clone(),
                    "ws-reader",
                    peer_tasks.ws_read_thread_handle,
                    ws_connect_event_writer.clone(),
                );
                // 参数说明：
                // - runtime_handle.clone()：waiter 在线程里用它等待 tokio task 结束
                // - self.session_id：标明退出事件属于哪一轮 session
                // - peer_id：标明退出事件属于哪个 peer
                // - source.clone()：保留来源信息，供 supervisor 判断 outbound 状态
                // - "ws-writer"：退出事件里的任务名标签
                // - peer_tasks.ws_write_thread_handle：真正要等待的 writer task
                // - ws_connect_event_writer：把退出事件发回 supervisor
                spawn_tokio_peer_exit_waiter(
                    runtime_handle.clone(),
                    self.session_id,
                    peer_id,
                    source.clone(),
                    "ws-writer",
                    peer_tasks.ws_write_thread_handle,
                    ws_connect_event_writer,
                );
                self.peer_tasks.insert(
                    peer_id,
                    SessionPeer {
                        source,
                        handle: peer_tasks.peer_handle,
                    },
                );
                debug_log(
                    "supervisor",
                    format!(
                        "Peer attached: session={}, peer={}, total_peers={}",
                        self.session_id,
                        peer_id,
                        self.peer_hub.peer_count()
                    ),
                );
                Ok(())
            }
            Err(err) => {
                let _ = self.peer_hub.remove_peer(peer_id);
                Err(err)
            }
        }
    }

    // 处理“单个 peer 已经离开”的收尾逻辑。
    //
    // 注意这里不是整轮 session 关闭，而只是：
    // - 关闭该 peer 的 reader / writer
    // - 从 WsPeerHub 删除它的发送句柄和录音订阅
    // - 如果它刚好是最后一个录音订阅者，则停止 recorder
    //
    // 入参说明：
    // - self：当前活跃 session 资源集合
    // - peer_id：已经离开的 peer 编号
    pub(crate) fn handle_peer_departure(
        &mut self,
        peer_id: PeerId,
    ) -> Result<Option<PeerDeparture>, AppError> {
        let Some(peer) = self.peer_tasks.remove(&peer_id) else {
            return Ok(None);
        };

        // 和旧版单连接客户端一样，单 peer 收尾时先尽量给对端一个正常 close 握手机会。
        if self.peer_hub.try_close_peer(peer_id) {
            std::thread::sleep(Duration::from_millis(200));
        }
        // 参数说明：
        // - peer.handle.close()：如果优雅 close 没能让本地任务完全退出，这里做强制关闭兜底
        peer.handle.close();
        let Some(removal) = self.peer_hub.remove_peer(peer_id) else {
            return Ok(None);
        };
        if removal.stop_recording {
            self.devices.recorder.stop_recording()?;
        }

        debug_log(
            "supervisor",
            format!(
                "Peer detached: session={}, peer={}, remaining_peers={}",
                self.session_id, peer_id, removal.remaining_peers
            ),
        );

        Ok(Some(PeerDeparture {
            source: removal.source,
            remaining_peers: removal.remaining_peers,
        }))
    }

    // 统一关闭整轮 session。
    //
    // 当最后一个 peer 离开，或者 router 退出时，会走到这里。
    // 它负责回收整轮 session 的所有共享资源，确保不会留下悬挂线程。
    //
    // 入参说明：
    // - self：当前要被整体关闭的 session 资源集合
    pub(crate) fn shutdown(mut self) -> SessionShutdownSummary {
        debug_log(
            "supervisor",
            format!(
                "Session lifecycle event: shutting_down session {}",
                self.session_id
            ),
        );

        // 先尽量让所有 peer 正常收到 Close，再做强制关闭兜底。
        self.peer_hub.close_all_peers();
        let closed_outbound_peers = self
            .peer_tasks
            .values()
            .filter(|peer| matches!(peer.source, PeerSource::OutboundConnect { .. }))
            .count();
        for (_, peer) in self.peer_tasks.drain() {
            peer.handle.close();
        }

        // 这里发 Close，是为了把 router 从 route_channel.recv() 的阻塞中唤醒出来。
        // 参数说明：
        // - SessionControl::Close：通知 router 主动结束当前 session 总线
        let _ = self.route_channel_writer.send(SessionControl::Close);
        let _ = self.devices.recorder.stop_recording();
        self.record_output_writer.close();
        let _ = self.devices.player.stop();

        self.monitor_handles.join();
        let _ = self.fanout_handle.join();

        debug_log(
            "supervisor",
            format!("Session cleanup completed for session {}", self.session_id),
        );

        SessionShutdownSummary {
            closed_outbound_peers,
        }
    }
}

// SessionPeer 只保存“当前 session 已挂载 peer 的最小关闭信息”。
//
// 真正的 reader / writer task 句柄在创建后交给 waiter 线程等待，
// 这里保留的是统一强制关闭时需要的 WsPeerHandle。
struct SessionPeer {
    source: PeerSource,
    handle: Arc<crate::transport::WsPeerHandle>,
}

// PeerDeparture 是 session 向 supervisor 回报“单个 peer 收尾结果”的结构。
//
// supervisor 主要关心：
// - 这个 peer 的来源是什么
// - 收尾后 session 里还剩多少 peer
pub(crate) struct PeerDeparture {
    pub(crate) source: PeerSource,
    pub(crate) remaining_peers: usize,
}

// SessionShutdownSummary 是 session 整体 shutdown 后回传给 supervisor 的摘要。
//
// 当前只保留一个统计项：本轮关闭的 outbound peer 数量，
// 用来帮助 supervisor 决定是否放开 connector 的重连闸门。
pub(crate) struct SessionShutdownSummary {
    pub(crate) closed_outbound_peers: usize,
}

// 等待 router 线程退出，并把结果转换成 SupervisorEvent::RouterExited。
//
// router 是 std::thread，所以这里再包一层普通线程 waiter 来做 join 和上报。
//
// 入参说明：
// - session_id：当前 router 所属 session 编号
// - handle：要等待的 router 线程句柄
// - ws_connect_event_writer：把退出结果回传给 supervisor 的发送端
fn spawn_router_exit_waiter(
    session_id: u64,
    handle: JoinHandle<Result<(), AppError>>,
    ws_connect_event_writer: mpsc::Sender<SupervisorEvent>,
) {
    thread::Builder::new()
        .name(format!("router-exit-waiter-{session_id}"))
        .spawn(move || {
            let result = handle
                .join()
                .map_err(|_| anyhow::anyhow!("router thread panicked"))
                .and_then(|result| result);
            // 参数说明：
            // - session_id：标记这是哪一轮 session 的 router 退出
            // - result：router 线程的真实退出结果，保留正常/异常信息
            let _ = ws_connect_event_writer.send(SupervisorEvent::RouterExited(RouterExit {
                session_id,
                result,
            }));
        })
        .expect("spawn router exit waiter");
}

// 等待某个 tokio peer 任务退出，并把结果转换成 SupervisorEvent::PeerTaskExited。
//
// 由于 tokio::task::JoinHandle 不能像 std::thread 那样直接 join，
// 这里需要额外包一层普通线程，通过 runtime_handle.block_on(handle.await) 等待结束。
//
// 入参说明：
// - runtime_handle：用来等待 tokio task 结束的 runtime handle
// - session_id：该 task 所属 session 编号
// - peer_id：该 task 所属 peer 编号
// - source：该 peer 的来源信息
// - task_name：任务名标签，例如 `ws-reader` 或 `ws-writer`
// - handle：要等待的 tokio task 句柄
// - ws_connect_event_writer：把退出结果回传给 supervisor 的发送端
fn spawn_tokio_peer_exit_waiter(
    runtime_handle: Handle,
    session_id: u64,
    peer_id: PeerId,
    source: PeerSource,
    task_name: &'static str,
    handle: TokioJoinHandle<Result<(), AppError>>,
    ws_connect_event_writer: mpsc::Sender<SupervisorEvent>,
) {
    thread::Builder::new()
        .name(format!("{task_name}-exit-waiter-{session_id}-{peer_id}"))
        .spawn(move || {
            // 参数说明：
            // - runtime_handle.block_on(async move { handle.await })：在普通线程里等待 tokio task 结束
            let result = match runtime_handle.block_on(async move { handle.await }) {
                Ok(result) => result,
                Err(err) if err.is_cancelled() => Ok(()),
                Err(err) => Err(anyhow::anyhow!("{task_name} task panicked: {err}")),
            };
            // 参数说明：
            // - session_id：标记退出事件属于哪一轮 session
            // - peer_id：标记退出事件属于哪个 peer
            // - source：保留来源信息，供 supervisor 处理 outbound 状态
            // - task_name：区分是 ws-reader 还是 ws-writer 先退出
            // - result：该任务的最终退出结果
            let _ = ws_connect_event_writer.send(SupervisorEvent::PeerTaskExited(PeerTaskExit {
                session_id,
                peer_id,
                source,
                task_name,
                result,
            }));
        })
        .expect("spawn tokio peer exit waiter");
}
