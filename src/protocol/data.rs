use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

// WebSocket 上统一传输的顶层消息类型。
// 当前协议不区分“文本协议”和“二进制协议”的概念层；
// 无论哪种消息，最终都统一落到这几类业务语义上。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AppMessage {
    Request(Request),
    Response(Response),
    Event(Event),
    Stream(Stream),
}

// 二进制流消息，主要用于音频数据。
// 相比 Event / Request / Response，Stream 更偏向“连续媒体数据”的语义。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stream {
    pub id: String,
    pub tag: String,
    pub bytes: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub data: Option<Value>,
}

impl Stream {
    // new 用于构造一条新的 Stream 业务消息。
    //
    // 当前最常见的场景是本地录音流上报，其中：
    // - tag 用来描述流的业务类型，例如 `record`
    // - bytes 承载真正的媒体数据
    // - data 预留给附加元信息
    //
    // 入参说明：
    // - tag：当前流的业务标签
    // - bytes：当前流对应的二进制负载
    // - data：可选的附加 JSON 元数据
    pub fn new(tag: &str, bytes: Vec<u8>, data: Option<Value>) -> Self {
        // 每条 Stream 都生成独立 id，方便后续如果服务端要做跟踪或排查。
        Self {
            id: Uuid::new_v4().to_string(),
            tag: tag.to_string(),
            bytes,
            data,
        }
    }
}

// 文本事件消息，通常用于上报本地状态变化。
// 它强调“通知”语义，而不是“请求-响应”语义。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    pub event: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub data: Option<Value>,
}

impl Event {
    // new 用于构造一条新的 Event 业务消息。
    //
    // Event 强调“本地状态变化通知”语义，常用于把 monitor 观测到的设备状态上报给服务端。
    //
    // 入参说明：
    // - event：事件名，例如 `kws`、`instruction`、`playing`
    // - data：可选事件数据，通常是结构化 JSON
    pub fn new(event: &str, data: Option<Value>) -> Self {
        // 事件也生成独立 id，方便服务端做日志串联。
        Self {
            id: Uuid::new_v4().to_string(),
            event: event.to_string(),
            data,
        }
    }
}

// RPC 请求，由一端发起、另一端处理。
// 当前客户端主要扮演“被动接收 Request 并执行本地命令”的角色。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub id: String,
    pub command: String,
    pub payload: Option<Value>,
}

// RPC 响应，成功和失败都通过它返回。
// 这里保留了 code / msg / data 三种字段组合，兼容“纯结果”和“带错误码”的两种返回风格。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub msg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub data: Option<Value>,
}

impl Response {
    // success 构造一条“空 payload 的标准成功响应”。
    //
    // 它适用于命令执行成功，但没有额外业务数据需要返回的场景。
    //
    // 入参说明：
    // - 无
    pub fn success() -> Self {
        // success 是最常见的“空 payload 成功响应”模板。
        Self {
            id: "0".to_string(),
            code: Some(0),
            msg: Some("success".to_string()),
            data: None,
        }
    }

    pub fn success_msg(msg: &str) -> Self {
        Self {
            id: "0".to_string(),
            code: Some(0),
            msg: Some(msg.to_string()),
            data: None,
        }
    }

    // from_data 构造一条“只返回 data、不显式设置 code/msg”的响应。
    //
    // 它常用于查询类命令：调用方更关心返回内容本身，而不是统一成功文案。
    //
    // 入参说明：
    // - data：要返回给远端的 JSON 业务数据
    pub fn from_data(data: Value) -> Self {
        // from_data 适合“只关心返回数据，不额外设置 code/msg”的场景。
        Self {
            id: "0".to_string(),
            code: None,
            msg: None,
            data: Some(data),
        }
    }

    // from_error 构造一条标准错误响应。
    //
    // 当前约定是所有本地命令失败时都回 `code=-1`，并把错误细节放到 msg 中。
    //
    // 入参说明：
    // - id：要回填的请求 id，保证远端能把错误响应和原请求对应起来
    // - err：当前错误对象或可显示文本
    pub fn from_error(id: &str, err: impl std::fmt::Display) -> Self {
        // 出错时统一返回 -1，错误细节直接放到 msg 中。
        Self {
            id: id.to_string(),
            code: Some(-1),
            msg: Some(err.to_string()),
            data: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{AppMessage, Event, Response, Stream};

    #[test]
    // 验证 Event 顶层包装格式与旧客户端保持一致。
    fn event_text_format_matches_open_xiaoai() {
        let value = serde_json::to_value(AppMessage::Event(Event {
            id: "event-1".to_string(),
            event: "kws".to_string(),
            data: Some(json!({"Keyword":"小爱同学"})),
        }))
        .unwrap();

        assert_eq!(
            value,
            json!({
                "Event": {
                    "id": "event-1",
                    "event": "kws",
                    "data": {"Keyword":"小爱同学"}
                }
            })
        );
    }

    #[test]
    // 验证默认成功响应的 JSON 结构没有偏离旧协议。
    fn response_success_format_matches_open_xiaoai() {
        let value = serde_json::to_value(AppMessage::Response(Response::success())).unwrap();

        assert_eq!(
            value,
            json!({
                "Response": {
                    "id": "0",
                    "code": 0,
                    "msg": "success"
                }
            })
        );
    }

    #[test]
    // 验证二进制 Stream 序列化后的字段形态符合既有协议约定。
    fn binary_stream_payload_shape_matches_open_xiaoai() {
        let bytes = serde_json::to_vec(&Stream {
            id: "stream-1".to_string(),
            tag: "record".to_string(),
            bytes: vec![1, 2, 3, 4],
            data: None,
        })
        .unwrap();

        let value = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap();

        assert_eq!(
            value,
            json!({
                "id": "stream-1",
                "tag": "record",
                "bytes": [1, 2, 3, 4]
            })
        );
    }
}
