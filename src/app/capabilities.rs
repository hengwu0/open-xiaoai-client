use crate::base::{debug_err_log, debug_log};
use crate::shell::lx06_audio::prepare_lx06_audio_capability;

// AppCapabilities 保存进程启动阶段探测到的本机能力。
//
// 这些能力只在启动阶段准备一次，后续每轮 session 只是读取结果，
// 避免每次 websocket 重连都重复检查设备型号或重复 mount ALSA 配置。
#[derive(Clone, Debug, Default)]
pub(crate) struct AppCapabilities {
    pub(crate) fast_recording_enabled: bool,
}

impl AppCapabilities {
    pub(crate) fn detect_at_startup() -> Self {
        debug_log("supervisor", "Detecting startup capabilities");
        let fast_recording_enabled = match prepare_lx06_audio_capability() {
            Ok(enabled) => enabled,
            Err(err) => {
                debug_err_log(
                    "supervisor",
                    format!("LX06 audio capability setup failed; fast_recording disabled: {err}"),
                );
                false
            }
        };

        debug_log(
            "supervisor",
            format!("Startup capability detection completed: fast_recording_enabled={fast_recording_enabled}"),
        );
        Self {
            fast_recording_enabled,
        }
    }
}
