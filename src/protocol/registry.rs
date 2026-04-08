use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::base::AppError;
use crate::transport::PeerId;

use super::{Request, Response};

// 本地命令注册表。
// 当远端发来 Request 时，router 会在这里按 command 名称找到对应处理函数。
//
// 这个结构本质上就是“客户端对外暴露的 RPC 方法表”。
// 它把“协议名字”和“本地实现”解耦开来：
// - supervisor 负责注册
// - router 负责查表和调度
// - handler 只关心业务本身
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandContext {
    // 当前 request 是由哪个 peer 发来的。
    // 加上这个上下文后，handler 就能做：
    // - per-peer 录音订阅
    // - 将来的权限隔离或来源统计
    pub peer_id: PeerId,
}

// handler 除了原始 Request 外，还能拿到 CommandContext。
// 这样“业务动作”可以感知 peer 身份，但仍然不用直接依赖 websocket 层。
pub type CommandHandler =
    Arc<dyn Fn(CommandContext, Request) -> Result<Response, AppError> + Send + Sync>;

#[derive(Default)]
pub struct CommandRegistry {
    handlers: RwLock<HashMap<String, CommandHandler>>,
}

impl CommandRegistry {
    // new 创建一个空的本地命令注册表。
    //
    // supervisor 启动 session 时会先构造它，再把所有支持的命令注册进去。
    //
    // 入参说明：
    // - 无
    pub fn new() -> Self {
        Self::default()
    }

    // register 用于把一个命令名和其本地处理函数绑定到注册表中。
    //
    // 注册完成后，router 收到对应 command 的 Request 时，就能通过 handle() 找到它。
    //
    // 入参说明：
    // - self：当前命令注册表
    // - name：协议层暴露给远端的命令名
    // - handler：该命令对应的本地处理逻辑
    pub fn register<F>(&self, name: &str, handler: F)
    where
        F: Fn(CommandContext, Request) -> Result<Response, AppError> + Send + Sync + 'static,
    {
        // register 时直接覆盖同名 handler。
        // 当前初始化流程里不会重复注册同名命令，因此这样最简单直接。
        self.handlers
            .write()
            .expect("command registry poisoned")
            .insert(name.to_string(), Arc::new(handler));
    }

    // handle 负责根据 Request.command 查找并执行对应本地命令处理器。
    //
    // 它是 router 和具体业务 handler 之间的唯一查表入口。
    //
    // 入参说明：
    // - self：当前命令注册表
    // - context：当前请求附带的上下文，例如来源 peer_id
    // - request：原始协议请求对象
    pub fn handle(&self, context: CommandContext, request: Request) -> Result<Response, AppError> {
        // 这里不直接持有读锁执行 handler，先 clone 出来，避免长时间占锁。
        // 否则如果 handler 内部执行较慢，整个注册表的读锁都会被无谓地持有太久。
        let handler = self
            .handlers
            .read()
            .expect("command registry poisoned")
            .get(&request.command)
            .cloned();

        match handler {
            // 参数说明：
            // - handler(context, request)：把上下文和原始请求一起交给真实业务处理器执行
            Some(handler) => handler(context, request),
            None => Err(anyhow::anyhow!("command not found: {}", request.command)),
        }
    }
}
