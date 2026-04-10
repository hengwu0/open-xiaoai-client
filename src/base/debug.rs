use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use chrono::Local;

// 这一层不是通用 logging facade，而是面向当前项目做的极薄封装。
// 设计目标是：
// 1. 避免在设备侧引入更重的日志框架
// 2. 保持输出格式稳定，方便串口/日志文件直接 grep
// 3. 把“普通调试输出”和“错误调试输出”分流到 stdout / stderr
//
// 换句话说，这里解决的是“可观测性最小闭环”，而不是完整日志系统。

// debug 开关默认关闭，启动阶段按配置写入一次，后面都是只读访问。
// 这里用 AtomicBool 保存最小状态，避免为单个布尔开关引入额外初始化封装。
static DEBUG_ENABLED: AtomicBool = AtomicBool::new(false);
static DEBUG_RATE_LIMITS: OnceLock<Mutex<HashMap<String, RateLimitState>>> = OnceLock::new();

#[derive(Default)]
struct RateLimitState {
    last_logged_at: Option<Instant>,
    suppressed: u64,
}

// set_debug_enabled 负责在进程启动阶段初始化全局 debug 开关。
//
// 这个开关后续会被 debug_log / debug_err_log 读取，用来决定是否真正输出调试日志。
// 它是整个程序最顶层的“可观测性总闸门”。
//
// 入参说明：
// - enabled：是否启用调试日志；通常由命令行参数解析结果决定
pub fn set_debug_enabled(enabled: bool) {
    // 当前调用路径里它只会在 main 阶段设置，因此这里直接覆盖即可。
    DEBUG_ENABLED.store(enabled, Ordering::Relaxed);
}

// is_debug_enabled 负责读取当前进程级 debug 开关状态。
//
// 其他模块不会直接接触全局 AtomicBool，而是统一通过这个函数拿到“当前是否应该打印调试日志”。
//
// 入参说明：
// - 无
pub fn is_debug_enabled() -> bool {
    DEBUG_ENABLED.load(Ordering::Relaxed)
}

// debug_log 负责输出普通调试日志到 stdout。
//
// 它会先检查全局 debug 开关；如果当前未开启调试，就完全静默，不产生任何输出。
//
// 入参说明：
// - component：当前日志所属的功能组件名，例如 `ws`、`router`、`supervisor`
// - message：要输出的日志正文；允许传入任何可借用成字符串的对象
pub fn debug_log(component: &str, message: impl AsRef<str>) {
    if is_debug_enabled() {
        // 普通调试日志走 stdout，便于和真正的错误输出分流。
        // 参数说明：
        // - format_log_line(component, message.as_ref())：统一把组件名、时间戳和正文格式化成稳定文本
        println!("{}", format_log_line(component, message.as_ref()));
    }
}

// debug_log_limited 负责对高频调试日志做限频输出。
//
// 它仍然受全局 debug 开关控制，但会额外按 `(component, key)` 做时间窗限频，
// 避免录音、播放、网络收发这类热路径把 stdout 刷满。
//
// 入参说明：
// - component：当前日志所属组件名
// - key：同一类高频日志的稳定键；同一个键会共享限频状态
// - interval：两次允许真实输出之间的最小时间间隔
// - message：当前这次想输出的日志正文
pub fn debug_log_limited(component: &str, key: &str, interval: Duration, message: impl AsRef<str>) {
    if !is_debug_enabled() {
        return;
    }

    let message = message.as_ref();
    let output = match DEBUG_RATE_LIMITS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
    {
        Ok(mut rate_limits) => {
            let state = rate_limits
                .entry(format!("{component}:{key}"))
                .or_insert_with(RateLimitState::default);
            let now = Instant::now();
            let should_log = state
                .last_logged_at
                .map(|last| now.duration_since(last) >= interval)
                .unwrap_or(true);

            if should_log {
                let line = if state.suppressed > 0 {
                    format!(
                        "{} (suppressed {} similar logs in last {} ms)",
                        message,
                        state.suppressed,
                        interval.as_millis()
                    )
                } else {
                    message.to_string()
                };
                state.last_logged_at = Some(now);
                state.suppressed = 0;
                Some(line)
            } else {
                state.suppressed += 1;
                None
            }
        }
        Err(_) => Some(message.to_string()),
    };

    if let Some(line) = output {
        println!("{}", format_log_line(component, &line));
    }
}

// debug_err_log 负责输出异常路径调试日志到 stderr。
//
// 它和 debug_log 使用同一套格式，但会把输出流分到 stderr，便于现场排查时单独收集错误路径。
//
// 入参说明：
// - component：当前日志所属的功能组件名
// - message：要输出的错误日志正文
pub fn debug_err_log(component: &str, message: impl AsRef<str>) {
    if is_debug_enabled() {
        // 错误调试日志走 stderr。
        // 这样在重定向或现场排查时，可以单独收集异常路径。
        // 参数说明：
        // - format_log_line(component, message.as_ref())：统一格式化错误日志文本
        eprintln!("{}", format_log_line(component, message.as_ref()));
    }
}

// format_log_line 负责把组件名和消息正文拼成统一的日志行格式。
//
// 这个函数是整个项目日志格式稳定性的唯一收口点：
// - 时间格式在这里定义
// - 组件标签格式在这里定义
// - 正文拼接顺序也在这里定义
//
// 入参说明：
// - component：日志所属组件名
// - message：已经确定要输出的纯文本日志正文
fn format_log_line(component: &str, message: &str) -> String {
    // 输出格式固定为：
    // [2026-03-05 09:04:59:123] [component] message
    //
    // 这里选择“本地时间 + 毫秒”主要是为了方便和设备侧其他本地日志对齐。
    let now = Local::now();
    format!(
        "[{}:{:03}] [{component}] {}",
        now.format("%Y-%m-%d %H:%M:%S"),
        now.timestamp_subsec_millis(),
        message
    )
}
