use std::time::Duration;

use anyhow::{Context, Result};
use tokio_tungstenite::tungstenite::Message;

use crate::base::{debug_err_log, debug_log, debug_log_limited};
use crate::protocol::{AppMessage, Stream};
use crate::transport::{InboundMessage, OutboundControl};

// codec 模块是“协议对象 <-> WebSocket 帧”之间的最后一道边界。
// 它的职责很明确：
// - decode：把网络帧还原成当前客户端真正关心的语义
// - encode：把本地内部的发送意图编码成网络帧
//
// 这样 router / ws-writer 都不需要直接碰 serde 细节。

#[derive(Debug)]
pub enum DecodeResult {
    Message(InboundMessage),
    // 表示当前帧是合法的，但业务层无需处理。
    Ignore,
    // 表示当前帧要求上层结束本轮读取循环。
    Close,
}

// decode_message 负责把底层 WebSocket 帧转换成“当前客户端真正关心的输入”。
// 这里会主动体现协议约束：
// - 文本帧里的 Request/Response/Event/Stream 保留
// - 二进制帧按 Stream 解析
//
// 这样做的好处是，router 层可以只面对“业务上有效的入站消息”，
// 不必再知道底层 websocket 还有哪些控制帧类型。
//
// 入参说明：
// - message：一帧刚从 websocket 读取到的原始 tungstenite::Message
pub fn decode_message(message: Message) -> Result<DecodeResult> {
    match message {
        Message::Text(text) => {
            // 文本帧承载完整的 AppMessage JSON。
            debug_log(
                "codec",
                format!(
                    "Decoding inbound text frame: {}",
                    format_text_frame_for_log(text.as_ref())
                ),
            );
            // 参数说明：
            // - &text：文本帧里的 JSON 文本
            // - serde_json::from_str::<AppMessage>(...)：按统一协议对象做反序列化
            let app = match serde_json::from_str::<AppMessage>(&text)
                .context("decode text app message")
            {
                Ok(app) => app,
                Err(err) => {
                    debug_err_log(
                        "codec",
                        format!("Failed to decode inbound text frame: {err}"),
                    );
                    return Err(err);
                }
            };
            let inbound = match app {
                AppMessage::Request(v) => InboundMessage::Request(v),
                AppMessage::Response(v) => InboundMessage::Response(v),
                AppMessage::Event(v) => InboundMessage::Event(v),
                AppMessage::Stream(v) => InboundMessage::Stream(v),
            };
            Ok(DecodeResult::Message(inbound))
        }
        Message::Binary(bytes) => {
            // 二进制帧目前只用于承载 Stream。
            // 对当前程序来说，最常见的情况就是服务端推送 `tag=play` 的音频流。
            debug_log(
                "codec",
                format!("Decoding inbound binary frame: {} bytes", bytes.len()),
            );
            // 参数说明：
            // - &bytes：二进制帧原始字节
            // - serde_json::from_slice::<Stream>(...)：按协议里的 Stream 结构反序列化
            let stream =
                match serde_json::from_slice::<Stream>(&bytes).context("decode binary stream") {
                    Ok(stream) => stream,
                    Err(err) => {
                        debug_err_log(
                            "codec",
                            format!("Failed to decode inbound binary frame: {err}"),
                        );
                        return Err(err);
                    }
                };
            Ok(DecodeResult::Message(InboundMessage::Stream(stream)))
        }
        // Close 表示对端主动结束连接，这里返回 None 让上层中断循环。
        Message::Close(_) => {
            debug_log("codec", "Inbound close frame received");
            Ok(DecodeResult::Close)
        }
        // Ping/Pong/Frame 对业务层没有意义，直接忽略。
        Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
            debug_log("codec", "Ignoring inbound control frame");
            Ok(DecodeResult::Ignore)
        }
    }
}

// encode_outbound 负责把本地内部的“发送意图”转换成真正的 WebSocket 帧。
// 当前出站只需要覆盖四类业务语义：
// - 主动上报 Event
// - 录音流 `Stream(tag=record)`
// - Request 的响应 Response
// - 主动关闭当前 websocket 会话
//
// 在 ws 线程看来，它们最终都会被编码成具体的 WebSocket 帧。
// 因此这里是“协议对象”和“网络帧”之间的最后一道转换点。
//
// 入参说明：
// - outbound：内部出站控制语义，可能是文本、二进制流或 Close
pub fn encode_outbound(outbound: OutboundControl) -> Message {
    // OutboundControl 是内部发送指令，真正发到网络前在这里转换成 tungstenite::Message。
    // 这样 ws-writer 的逻辑能保持成“收什么发什么”，不再关心协议对象的内部结构。
    match outbound {
        OutboundControl::Text(text) => {
            debug_log(
                "codec",
                format!(
                    "Encoding outbound text frame: {}",
                    format_text_frame_for_log(&text)
                ),
            );
            Message::Text(text.into())
        }
        OutboundControl::Binary(bytes) => {
            debug_log_limited(
                "codec",
                "encoding-outbound-binary-frame",
                Duration::from_secs(60),
                format!("Encoding outbound binary frame: {} bytes", bytes.len()),
            );
            Message::Binary(bytes.into())
        }
        OutboundControl::Close => {
            debug_log("codec", "Encoding outbound close frame");
            Message::Close(None)
        }
    }
}

// format_text_frame_for_log 把文本帧内容压缩成更适合写入日志的一行文本。
//
// 它会尽量把标准 AppMessage JSON 重新序列化成稳定单行文本；
// 如果不是合法协议 JSON，再退回原始文本转义后的形式。
//
// 入参说明：
// - text：原始文本帧内容
fn format_text_frame_for_log(text: &str) -> String {
    match serde_json::from_str::<AppMessage>(text) {
        Ok(AppMessage::Request(request)) => serde_json::to_string(&AppMessage::Request(request))
            .unwrap_or_else(|_| sanitize_text_frame_for_log(text)),
        Ok(AppMessage::Response(response)) => {
            serde_json::to_string(&AppMessage::Response(response))
                .unwrap_or_else(|_| sanitize_text_frame_for_log(text))
        }
        Ok(AppMessage::Event(event)) => serde_json::to_string(&AppMessage::Event(event))
            .unwrap_or_else(|_| sanitize_text_frame_for_log(text)),
        Ok(AppMessage::Stream(stream)) => format!("<stream bytes={}>", stream.bytes.len()),
        Err(_) => match serde_json::from_str::<serde_json::Value>(text) {
            Ok(value) => {
                serde_json::to_string(&value).unwrap_or_else(|_| sanitize_text_frame_for_log(text))
            }
            Err(_) => sanitize_text_frame_for_log(text),
        },
    }
}

// sanitize_text_frame_for_log 把原始文本里的换行和回车显式转义，避免日志串行。
//
// 入参说明：
// - text：原始文本帧内容
fn sanitize_text_frame_for_log(text: &str) -> String {
    text.replace('\r', "\\r").replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio_tungstenite::tungstenite::Message;

    use super::{DecodeResult, decode_message, format_text_frame_for_log};
    use crate::protocol::{AppMessage, Event, Request, Stream};
    use crate::transport::InboundMessage;

    #[test]
    // 验证文本 Request 能被正确解码回入站请求对象。
    fn decode_request_text_matches_old_client_behavior() {
        let text = serde_json::to_string(&AppMessage::Request(Request {
            id: "req-1".to_string(),
            command: "get_version".to_string(),
            payload: Some(json!({"k":"v"})),
        }))
        .unwrap();

        match decode_message(Message::Text(text.into())).unwrap() {
            DecodeResult::Message(InboundMessage::Request(request)) => {
                assert_eq!(request.id, "req-1");
                assert_eq!(request.command, "get_version");
                assert_eq!(request.payload, Some(json!({"k":"v"})));
            }
            other => panic!("unexpected decode result: {other:?}"),
        }
    }

    #[test]
    // 验证文本 Event 的解码能力仍然保留。
    fn decode_event_text_stays_supported() {
        let text = serde_json::to_string(&AppMessage::Event(Event {
            id: "evt-1".to_string(),
            event: "playing".to_string(),
            data: Some(json!("Playing")),
        }))
        .unwrap();

        match decode_message(Message::Text(text.into())).unwrap() {
            DecodeResult::Message(InboundMessage::Event(event)) => {
                assert_eq!(event.id, "evt-1");
                assert_eq!(event.event, "playing");
                assert_eq!(event.data, Some(json!("Playing")));
            }
            other => panic!("unexpected decode result: {other:?}"),
        }
    }

    #[test]
    // 验证二进制 Stream 的解码结果与旧客户端一致。
    fn decode_binary_stream_matches_old_client_behavior() {
        let bytes = serde_json::to_vec(&Stream {
            id: "stream-1".to_string(),
            tag: "play".to_string(),
            bytes: vec![1, 2, 3],
            data: Some(json!({"seq": 7})),
        })
        .unwrap();

        match decode_message(Message::Binary(bytes.into())).unwrap() {
            DecodeResult::Message(InboundMessage::Stream(stream)) => {
                assert_eq!(stream.id, "stream-1");
                assert_eq!(stream.tag, "play");
                assert_eq!(stream.bytes, vec![1, 2, 3]);
                assert_eq!(stream.data, Some(json!({"seq": 7})));
            }
            other => panic!("unexpected decode result: {other:?}"),
        }
    }

    #[test]
    // 验证 JSON 文本在日志里会被压成单行。
    fn format_text_frame_for_log_compacts_json_to_single_line() {
        let formatted = format_text_frame_for_log("{\"b\":1,\"a\":{\"c\":2}}");
        assert!(!formatted.contains('\n'));
        assert!(formatted.contains("\"a\""));
        assert!(formatted.contains("\"c\":2"));
    }

    #[test]
    // 验证非法 JSON 文本会退回原始文本的单行转义形式。
    fn format_text_frame_for_log_falls_back_to_single_line_raw_text() {
        let text = "not-json\nline-2";
        assert_eq!(format_text_frame_for_log(text), "not-json\\nline-2");
    }

    #[test]
    // 验证日志格式化时不会把 Stream 的原始 bytes 全量展开到日志里。
    fn format_text_frame_for_log_omits_stream_bytes() {
        let text = serde_json::to_string(&AppMessage::Stream(Stream {
            id: "stream-1".to_string(),
            tag: "play".to_string(),
            bytes: vec![1, 2, 3],
            data: Some(json!({"seq": 7})),
        }))
        .unwrap();

        assert_eq!(format_text_frame_for_log(&text), "<stream bytes=3>");
    }
}
