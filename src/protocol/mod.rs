// protocol 模块定义客户端与远端之间的统一协议对象，以及本地命令注册和路由分发能力。
pub mod data;
pub mod registry;
pub mod router;

pub use data::{AppMessage, Event, Request, Response, Stream};
