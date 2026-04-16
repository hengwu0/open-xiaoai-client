// base 模块收敛整个项目最底层、最通用的基础能力：
// - debug：极薄日志封装
// - error：统一错误别名
// - version：编译期版本号
pub mod debug;
pub mod error;
pub mod version;

// 统一导出最常用的基础能力，避免上层模块重复写深层路径。
pub use debug::{debug_err_log, debug_log, debug_log_limited, is_debug_enabled, set_debug_enabled};
pub use error::AppError;
pub use version::VERSION;
