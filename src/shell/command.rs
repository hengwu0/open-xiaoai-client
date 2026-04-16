use serde::{Deserialize, Serialize};
use std::process::Command;

use crate::base::{AppError, debug_err_log, debug_log};

// shell 执行结果会作为 RPC 返回值传回服务端。
// 这里把 stdout / stderr / exit_code 都带回去，是为了让服务端在调试设备时有足够上下文。
#[derive(Debug, Serialize, Deserialize)]
pub struct CommandResult {
    // stdout：shell 标准输出文本
    pub stdout: String,
    // stderr：shell 标准错误文本
    pub stderr: String,
    // exit_code：shell 进程退出码；如果无法获得则约定为 -1
    pub exit_code: i32,
}

// run_shell 负责同步执行一段 shell 脚本，并把完整执行结果包装成 CommandResult 返回。
//
// 它是所有“本地 shell 能力”对上层暴露的统一入口：
// - speaker 备份方法会走它
// - 将来如果命令注册里有直接执行 shell 的需求，也会走它
//
// 入参说明：
// - script：要交给 `/bin/sh -c` 执行的脚本文本
pub fn run_shell(script: &str) -> Result<CommandResult, AppError> {
    // 统一走 /bin/sh -c，便于兼容已有脚本调用方式。
    // 这意味着服务端传来的脚本可以是完整的 shell 片段，而不仅仅是单个可执行文件名。
    debug_log(
        "shell",
        format!("Executing shell command: {}", script.replace('\n', " ")),
    );
    // 参数说明：
    // - Command::new(\"/bin/sh\")：显式指定 shell 解释器
    // - .arg(\"-c\")：让后续字符串按 shell 脚本执行
    // - .arg(script)：真正要执行的脚本文本
    let output = Command::new("/bin/sh")
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|err| {
            debug_err_log("shell", format!("Failed to spawn shell command: {err}"));
            anyhow::Error::from(err)
        })?;
    let result = CommandResult {
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        exit_code: output.status.code().unwrap_or(-1),
    };
    let completion_message = format!(
        "Shell command completed: exit_code={}, stdout_bytes={}, stderr_bytes={}, stdout={}, stderr={}",
        result.exit_code,
        result.stdout.len(),
        result.stderr.len(),
        format_shell_output_for_log(&result.stdout),
        format_shell_output_for_log(&result.stderr),
    );
    // 非零退出码不算 transport 层错误，命令结果仍会正常返回给上层；
    // 但从设备侧可观测性的角度，它已经属于值得单独收集的异常路径，
    // 因此详细结果直接走 stderr。
    //
    // 不过从 stdout 的执行时间线来看，我们仍然希望保留一条“命令已经结束”的普通日志，
    // 否则只看 stdout 时会看到 `Executing shell command`，却看不到结束节点。
    if result.exit_code != 0 {
        debug_log(
            "shell",
            format!(
                "Shell command finished with non-zero status: exit_code={}, stdout_bytes={}, stderr_bytes={}",
                result.exit_code,
                result.stdout.len(),
                result.stderr.len(),
            ),
        );
        debug_err_log("shell", completion_message);
    } else {
        // 成功路径继续保留在 stdout，便于和真正异常输出分流。
        debug_log("shell", completion_message);
    }
    Ok(result)
}

// format_shell_output_for_log 负责把原始 shell 输出格式化成更适合写入日志的一行文本。
//
// 当前直接使用 Rust 的 debug 字符串格式，这样换行、回车等不可见字符都会被显式转义。
//
// 入参说明：
// - output：原始 stdout 或 stderr 文本
fn format_shell_output_for_log(output: &str) -> String {
    format!("{output:?}")
}
