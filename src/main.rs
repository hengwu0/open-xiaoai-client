mod app;
mod audio;
mod base;
mod config;
mod monitor;
mod protocol;
mod shell;
mod transport;

use app::AppSupervisor;
use base::{debug_log, set_debug_enabled};
use config::{parse_args, usage};

// main 是整个进程的唯一入口。
//
// 它只负责三类顶层动作：
// 1. 解析命令行参数
// 2. 初始化全局 debug 开关
// 3. 启动 supervisor 主循环
//
// 真正复杂的 session / peer / router 生命周期都下沉到其它模块，不在这里展开。
//
// 入参说明：
// - 无
fn main() {
    // main 故意保持成一个很薄的入口层。
    // 这个项目现在已经有三种运行模式和多 peer session 生命周期，
    // 所以后续排查问题时，最好能保持一条稳定阅读路径：
    // main -> parse_args -> AppSupervisor::run_forever -> session / peer / router。
    let (_program, run_config) = match parse_args(std::env::args()) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{err}");
            eprintln!("{}", usage(&program_name_fallback()));
            std::process::exit(1);
        }
    };

    set_debug_enabled(run_config.debug_enabled);
    debug_log(
        "main",
        format!("Debug logging enabled; config={run_config:?}"),
    );

    // AppSupervisor 负责一次完整会话的创建、清理和断线重连。
    // main 本身尽量保持很薄，只做“解析入参 + 打开调试开关 + 启动主循环”三件事。
    //
    // 这样后面排查问题时，阅读路径会固定成：
    // - 命令行和 debug 开关问题，看 main
    // - 会话重连和生命周期问题，看 supervisor
    // - 路由分发问题，看 router
    // - 网络读写问题，看 ws_pump
    // 参数说明：
    // - AppSupervisor::new(run_config)：用最终运行配置创建进程级 supervisor
    // - .run_forever()：进入常驻主循环，持续管理 listener / connector / session 生命周期
    if let Err(err) = AppSupervisor::new(run_config).run_forever() {
        // 这里只有“进程级别”的致命错误才会走到。
        // 普通连接断开、会话异常、线程退出，都会在 supervisor 内部被吸收并重建会话，
        // 不会直接导致整个进程退出。
        eprintln!("fatal error: {err}");
        std::process::exit(1);
    }
}

// program_name_fallback 在参数解析失败时，为 usage 文本提供一个兜底程序名。
//
// 它不参与任何业务逻辑，只是为了让错误提示里的用法说明更友好。
//
// 入参说明：
// - 无
fn program_name_fallback() -> String {
    // 出错时重新取一次 argv[0] 只是为了让 usage 里的程序名更友好；
    // 这里不参与任何业务判断，即使取不到也退回固定名字。
    std::env::args()
        .next()
        .unwrap_or_else(|| "client-rust".to_string())
}
