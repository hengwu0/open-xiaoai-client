use serde::{Deserialize, Serialize};

use super::file::{FileMonitorHandle, spawn_file_monitor};
use crate::base::debug_log;
use crate::protocol::{AppMessage, Event};
use crate::transport::{OutboundControl, SessionControl};

const KWS_FILE_PATH: &str = "/tmp/open-xiaoai/kws.log";

// 监听唤醒词日志文件。
// 日志格式约定为 `timestamp@keyword`，其中 `__STARTED__` 表示服务刚启动。
#[derive(Debug, Serialize, Deserialize)]
pub enum KwsMonitorEvent {
    Started,
    Keyword(String),
}

// spawn_kws_monitor 启动对 kws.log 的追加监听，并把唤醒词事件写回 session 总线。
//
// 入参说明：
// - route_channel_writer：把 kws 事件写回 session 总线的发送端
pub fn spawn_kws_monitor(
    route_channel_writer: std::sync::mpsc::SyncSender<SessionControl>,
) -> FileMonitorHandle {
    let mut last_ts = 0_u64;
    spawn_file_monitor(
        "kws-monitor-thread",
        "kws-monitor",
        KWS_FILE_PATH,
        || Ok(()),
        move |trimmed| {
            // 参数说明：
            // - trimmed：从 kws.log 读到的一行去首尾空白后的文本
            let parts = trimmed.split('@').collect::<Vec<_>>();
            if parts.len() >= 2 {
                let ts = parts[0].parse::<u64>().unwrap_or(0);
                // 用时间戳去重，避免重复读取同一条唤醒记录。
                if ts != last_ts {
                    last_ts = ts;
                    let event = if parts[1] == "__STARTED__" {
                        KwsMonitorEvent::Started
                    } else {
                        KwsMonitorEvent::Keyword(parts[1].to_string())
                    };
                    let text = serde_json::to_string(&AppMessage::Event(Event::new(
                        "kws",
                        Some(serde_json::json!(event)),
                    )))?;
                    route_channel_writer.send(SessionControl::Outbound(
                        crate::transport::RoutedOutbound {
                            target: crate::transport::OutboundTarget::Broadcast,
                            message: OutboundControl::Text(text),
                        },
                    ))?;
                    debug_log(
                        "kws-monitor",
                        format!("Outbound kws event queued: timestamp={ts}"),
                    );
                }
            }
            Ok(())
        },
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::KwsMonitorEvent;

    #[test]
    // 验证 kws monitor 事件外形与旧客户端兼容。
    fn kws_event_shape_matches_open_xiaoai() {
        assert_eq!(
            serde_json::to_value(KwsMonitorEvent::Started).unwrap(),
            json!("Started")
        );
        assert_eq!(
            serde_json::to_value(KwsMonitorEvent::Keyword("小爱同学".to_string())).unwrap(),
            json!({"Keyword":"小爱同学"})
        );
    }
}
