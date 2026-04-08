// VERSION 暴露当前编译产物对应的 Cargo 包版本号。
//
// 它通常会被 `get_version` 这类命令直接返回给远端，便于排查设备端实际运行的版本。
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
