use std::sync::{Arc, Condvar, Mutex, mpsc};

use crate::protocol::{Event, Request, Response, Stream};

// PeerId 是 supervisor 分配给每个已接入对端的会话内唯一编号。
// 它只在当前进程内有意义，不要求和远端协议里的 request/stream id 对齐。
pub type PeerId = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerSource {
    // 表示这个 peer 是别人主动连到本机 listener 上来的。
    Listener,
    // 表示这个 peer 是当前进程主动 connect 成功后挂进 session 的。
    // 这里保留 url，便于日志与后续清理路径判断“是不是外连 peer”。
    OutboundConnect { url: String },
}

// 这一组枚举定义的是“会话内部的三条消息边界”：
//
// 1. InboundMessage
//    表示从 WebSocket 读出来、并且已经解码成协议对象的入站消息。
//
// 2. OutboundControl
//    表示准备写回 WebSocket 的出站消息。
//
// 3. SessionControl
//    表示 router 线程自己的统一入口。
//    monitor 产出的本地事件、ws 读线程送来的入站消息，都会先进入这个队列。
//
// 这样的好处是：
// - router 只有一个入口，状态更集中
// - ws 写线程只看 OutboundControl，不关心消息来源
// - monitor 不需要知道 WebSocket 的存在，只需要“把本地事件投进会话”
//
// 这三个枚举可以理解成三层边界：
// - InboundMessage：已经脱离网络帧、但还没进业务分发
// - OutboundControl：已经确定“要往外发什么”，但还没编码成 websocket frame
// - SessionControl：router 的统一入口，负责把上面两类消息串起来

#[derive(Debug, Clone)]
pub enum InboundMessage {
    // 远端主动调用本地已注册命令。
    Request(Request),
    // 协议上仍然可能收到 Response，但当前实现不会主动发起远端 RPC。
    // 因此这类消息只保留给 router 做观测和忽略。
    Response(Response),
    // 远端主动推送的文本事件。
    // 为了和旧客户端保持入站解析能力一致，这里继续保留这类消息的解析入口。
    Event(Event),
    // 服务端下发的媒体流，当前实际只消费 `tag=play`。
    Stream(Stream),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutboundTarget {
    // 广播给当前 session 下的所有 peer。
    Broadcast,
    // 只发给一个 peer，常见于 Request 的响应。
    ToPeer(PeerId),
}

#[derive(Debug, Clone)]
pub enum OutboundControl {
    // 文本消息承载 Event / Request / Response 这类 JSON 协议包。
    // 这里的 String 应该始终已经是序列化后的协议文本，而不是任意裸字符串。
    Text(String),
    // 二进制消息承载 Stream，例如录音流 `tag=record`。
    // 这里的 Vec<u8> 也应当是已经按协议编码好的负载。
    Binary(Vec<u8>),
    // 清空当前 peer 尚未发出的音频队列，并通过 ack 返回丢弃条数。
    ClearAudioQueue(mpsc::SyncSender<usize>),
    // 主动尝试走一次 websocket close 握手。
    // 这类消息只应该出现在 session 收尾阶段。
    Close,
}

#[derive(Debug, Clone)]
pub struct RoutedInbound {
    // 标记这条入站消息来自哪个 peer。
    // router 后面做 request -> response 时会依赖这个字段回源。
    pub peer_id: PeerId,
    pub message: InboundMessage,
}

#[derive(Debug, Clone)]
pub struct RoutedOutbound {
    // 指定这条出站消息的目标范围。
    pub target: OutboundTarget,
    pub message: OutboundControl,
}

#[derive(Debug, Clone)]
pub enum SessionControl {
    // 由 WebSocket 读线程送进 router 的入站消息。
    Inbound(RoutedInbound),
    // 由 monitor / router 送往 WebSocket 写线程的出站消息。
    Outbound(RoutedOutbound),
    // 用于主动关闭当前 router 会话。
    // supervisor 在准备回收一轮 session 时会发这个信号。
    Close,
}

// WriteSignal 是 ws-writer 的统一阻塞通知器。
// control 通道和 audio 通道的生产者在成功入队后都会调用 notify，
// writer 在没有待发送数据时则会阻塞在 wait 上。
//
// 这里不用两个独立通知器，是因为 writer 的发送策略本来就是：
// - 被任意一侧唤醒
// - 先 drain control
// - 再 drain audio
// 因此一个共享通知器就足够表达“有新数据可发了”。
pub struct WriteSignal {
    state: Mutex<WriteSignalState>,
    condvar: Condvar,
}

struct WriteSignalState {
    // pending 不是“精确消息数”，而是“有新消息需要 writer 醒来处理”的粗粒度计数。
    pending: usize,
    closed: bool,
}

pub enum WriteSignalWake {
    // 表示有生产者刚刚入队了数据。
    Notified,
    // 表示本轮 writer 应该结束，不必再继续等待。
    Closed,
}

impl WriteSignal {
    // new 创建一个新的 ws-writer 共享通知器。
    //
    // 初始状态下：
    // - 没有待处理通知
    // - 也没有关闭请求
    //
    // 入参说明：
    // - 无
    pub fn new() -> Self {
        Self {
            state: Mutex::new(WriteSignalState {
                pending: 0,
                closed: false,
            }),
            condvar: Condvar::new(),
        }
    }

    // notify 在某个生产者成功入队后唤醒 writer。
    //
    // 入参说明：
    // - self：当前共享通知器
    pub fn notify(&self) {
        let mut state = self.state.lock().expect("write signal poisoned");
        if state.closed {
            return;
        }
        state.pending = state.pending.saturating_add(1);
        self.condvar.notify_one();
    }

    // close 通知 writer 应该结束本轮发送循环。
    //
    // 入参说明：
    // - self：当前共享通知器
    pub fn close(&self) {
        let mut state = self.state.lock().expect("write signal poisoned");
        state.closed = true;
        self.condvar.notify_all();
    }

    // is_closed 返回当前通知器是否已经进入关闭态。
    //
    // 入参说明：
    // - self：当前共享通知器
    pub fn is_closed(&self) -> bool {
        self.state.lock().expect("write signal poisoned").closed
    }

    // wait 阻塞等待“有新数据可发”或“writer 应该退出”这两类事件。
    //
    // 入参说明：
    // - self：当前共享通知器
    pub fn wait(&self) -> WriteSignalWake {
        let mut state = self.state.lock().expect("write signal poisoned");
        while state.pending == 0 && !state.closed {
            state = self
                .condvar
                .wait(state)
                .expect("write signal poisoned while waiting");
        }
        if state.closed {
            return WriteSignalWake::Closed;
        }
        // writer 被唤醒后会把两个发送通道尽量 drain 掉，
        // 因此这里把计数直接清零即可，不需要按消息条数逐个扣减。
        state.pending = 0;
        WriteSignalWake::Notified
    }
}

#[derive(Clone)]
pub struct NotifyingSender<T> {
    // sender 是底层真正做入队的 sync_channel writer。
    sender: mpsc::SyncSender<T>,
    // write_signal 是共享通知器，负责把阻塞中的 ws-writer 唤醒。
    write_signal: Arc<WriteSignal>,
}

impl<T> NotifyingSender<T> {
    // new 创建一个“发送成功后会顺手唤醒 writer”的发送端包装。
    //
    // 入参说明：
    // - sender：底层真正负责入队的 sync_channel 发送端
    // - write_signal：与该队列对应的 writer 共享通知器
    pub fn new(sender: mpsc::SyncSender<T>, write_signal: Arc<WriteSignal>) -> Self {
        Self {
            sender,
            write_signal,
        }
    }

    // send 以阻塞方式发送一条消息，并在成功入队后唤醒 writer。
    //
    // 入参说明：
    // - self：当前发送端包装
    // - value：要发送到队列中的消息值
    pub fn send(&self, value: T) -> Result<(), mpsc::SendError<T>> {
        // 先把数据成功写入队列，再通知 writer 醒来。
        // 这样可以保证“被唤醒时，一定有机会从队列里读到新数据”。
        self.sender.send(value)?;
        self.write_signal.notify();
        Ok(())
    }

    // try_send 以非阻塞方式发送一条消息，并在成功入队后唤醒 writer。
    //
    // 入参说明：
    // - self：当前发送端包装
    // - value：要尝试发送到队列中的消息值
    pub fn try_send(&self, value: T) -> Result<(), mpsc::TrySendError<T>> {
        // try_send 的通知语义和 send 保持一致：
        // 只有入队成功，才会唤醒 writer。
        self.sender.try_send(value)?;
        self.write_signal.notify();
        Ok(())
    }
}

impl<T> Drop for NotifyingSender<T> {
    // drop 在发送端释放时补发一次通知，避免 writer 永久睡死。
    //
    // 入参说明：
    // - self：当前发送端包装
    fn drop(&mut self) {
        // 发送端被释放时也唤醒一次 writer。
        // 这样即使某类消息源是通过“通道断开”而不是“发送 Close”结束的，
        // writer 也不会永久睡死在 condvar 上。
        self.write_signal.notify();
    }
}
