use serde::{Deserialize, Serialize};
use serde_json::json;

use super::file::{FileMonitorHandle, spawn_file_monitor};
use crate::base::{debug_err_log, debug_log};
use crate::protocol::{AppMessage, Event};
use crate::transport::{OutboundControl, SessionControl};

const INSTRUCTION_FILE_PATH: &str = "/tmp/mico_aivs_lab/instruction.log";

// 以下这些结构体对应 instruction.log 中常见的 JSON 结构。
// 这里把模型留在客户端侧有两个好处：
// 1. 读日志时可以先做一次本地结构化解析
// 2. 服务端收到 event 后可以直接按字段消费，而不是再猜原始文本格式
//
// 这些结构体本身不参与业务决策，主要承担“结构描述”和“解析兜底”两个职责。

// 旧版 open-xiaoai 向服务端上报 instruction 事件时，发送的是这个枚举的序列化结果。
// 为了保持协议兼容，这里继续沿用相同的事件外形。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileMonitorEvent {
    NewFile,
    NewLine(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    pub dialog_id: String,
    pub id: String,
    pub name: String,
    pub namespace: String,
}

// `results` 数组里每一项的识别结果。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecognizeResult {
    #[serde(default)]
    pub confidence: f64,
    #[serde(default)]
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asr_binary_offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub begin_offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_nlp_request: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_stop: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_text: Option<String>,
}

// instruction 的 payload 形态很多，这里用 untagged enum 做兼容匹配。
// 谁能匹配上，就说明当前日志行属于哪类业务消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Payload {
    RecognizeResultPayload {
        is_final: bool,
        is_vad_begin: bool,
        results: Vec<RecognizeResult>,
    },
    StopCapturePayload {
        stop_time: u64,
    },
    SpeakPayload {
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        emotion: Option<Emotion>,
    },
    PlayPayload {
        audio_items: Vec<AudioItem>,
        audio_type: String,
        loadmore_token: String,
        needs_loadmore: bool,
        origin_id: String,
        play_behavior: String,
    },
    SetPropertyPayload {
        name: String,
        value: String,
    },
    InstructionControlPayload {
        behavior: String,
    },
    EmptyPayload {},
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Emotion {
    pub category: String,
    pub level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioItem {
    pub item_id: ItemId,
    pub log: Log,
    pub stream: Stream,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemId {
    pub audio_id: String,
    pub cp: Cp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cp {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log {
    pub eid: String,
    pub refer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stream {
    pub authentication: bool,
    pub duration_in_ms: u64,
    pub offset_in_ms: u64,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogMessage {
    pub header: Header,
    pub payload: Payload,
}

// 监听 instruction.log 的追加内容，并把每一行作为 event 上报给服务端。
// 这属于“主动出站 event”链路的一部分。
// 它和 kws / playing monitor 一样，都是把设备侧的局部状态变化转成标准事件。
//
// 入参说明：
// - route_channel_writer：把 instruction 事件写回 session 总线的发送端
pub fn spawn_instruction_monitor(
    route_channel_writer: std::sync::mpsc::SyncSender<SessionControl>,
) -> FileMonitorHandle {
    spawn_file_monitor(
        "instruction-monitor-thread",
        "instruction-monitor",
        INSTRUCTION_FILE_PATH,
        {
            let route_channel_writer_for_reset = route_channel_writer.clone();
            move || {
                // 参数说明：
                // - Event::new("instruction", Some(json!(FileMonitorEvent::NewFile)))：
                //   把“文件重新开始”包装成统一 instruction 事件
                let text = serde_json::to_string(&AppMessage::Event(Event::new(
                    "instruction",
                    Some(json!(FileMonitorEvent::NewFile)),
                )))?;
                route_channel_writer_for_reset.send(SessionControl::Outbound(
                    crate::transport::RoutedOutbound {
                        target: crate::transport::OutboundTarget::Broadcast,
                        message: OutboundControl::Text(text),
                    },
                ))?;
                debug_log(
                    "instruction-monitor",
                    "Outbound instruction event queued: NewFile",
                );
                Ok(())
            }
        },
        move |trimmed| {
            // 为了兼容旧服务端，真正上报的 payload 仍然保持 `FileMonitorEvent::NewLine(String)` 格式。
            // 结构化解析只用于本地 debug 观察，不参与实际出站协议。
            // 参数说明：
            // - parse_instruction_data(trimmed)：只做本地结构化观测，不影响真实出站内容
            let _ = parse_instruction_data(trimmed);
            let data = json!(FileMonitorEvent::NewLine(trimmed.to_string()));
            let text =
                serde_json::to_string(&AppMessage::Event(Event::new("instruction", Some(data))))?;
            route_channel_writer.send(SessionControl::Outbound(
                crate::transport::RoutedOutbound {
                    target: crate::transport::OutboundTarget::Broadcast,
                    message: OutboundControl::Text(text),
                },
            ))?;
            debug_log(
                "instruction-monitor",
                format!(
                    "Outbound instruction event queued from log line: {} chars",
                    trimmed.len()
                ),
            );
            Ok(())
        },
    )
}

// parse_instruction_data 尝试把 instruction.log 的一行文本解析成结构化 JSON。
//
// 它只服务于本地观测和调试，不影响真正发给服务端的出站协议格式。
//
// 入参说明：
// - line：instruction.log 中读到的一整行文本
fn parse_instruction_data(line: &str) -> serde_json::Value {
    match serde_json::from_str::<LogMessage>(line) {
        // 解析成功时，直接把强类型结构再转成 Value。
        // 这样一来，上游写模型时能吃到编译期校验，下游发消息时仍保持 JSON 灵活性。
        Ok(message) => {
            debug_log(
                "instruction-monitor",
                format!("Instruction log line parsed as structured JSON: {line}"),
            );
            serde_json::to_value(message).unwrap_or_else(|_| serde_json::json!({ "raw": line }))
        }
        // 解析失败时保留原始文本，并附上错误原因，方便后续排查格式漂移。
        Err(err) => {
            debug_err_log(
                "instruction-monitor",
                format!("Instruction log line could not be parsed structurally: {err}"),
            );
            serde_json::json!({
                "raw": line,
                "parse_error": err.to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::FileMonitorEvent;

    #[test]
    // 验证 instruction monitor 事件外形与旧客户端保持兼容。
    fn instruction_file_monitor_event_shape_matches_open_xiaoai() {
        assert_eq!(
            serde_json::to_value(FileMonitorEvent::NewFile).unwrap(),
            json!("NewFile")
        );
        assert_eq!(
            serde_json::to_value(FileMonitorEvent::NewLine("line".to_string())).unwrap(),
            json!({"NewLine":"line"})
        );
    }
}
