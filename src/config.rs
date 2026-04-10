use std::fmt;

// RunConfig 是启动阶段产出的“进程级运行意图”。
// 它只描述四件事：
// 1. debug 是否开启
// 2. 是否要监听 4399
// 3. 是否还要并行做一个主动 connect
// 4. 主动 connect 时是否需要附带 ws 鉴权 token
//
// 后面的 supervisor 就只依赖这个结构，不再直接碰原始 argv。
#[derive(Clone, PartialEq, Eq)]
pub struct RunConfig {
    pub debug_enabled: bool,
    pub listen_enabled: bool,
    pub server_url: Option<String>,
    pub ws_token: Option<String>,
}

impl fmt::Debug for RunConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ws_token = self
            .ws_token
            .as_ref()
            .map(|token| "*".repeat(token.chars().count()));
        f.debug_struct("RunConfig")
            .field("debug_enabled", &self.debug_enabled)
            .field("listen_enabled", &self.listen_enabled)
            .field("server_url", &self.server_url)
            .field("ws_token", &ws_token)
            .finish()
    }
}

// parse_args 负责把原始命令行参数解析成程序名和 RunConfig。
//
// 它是 main 和真正运行逻辑之间的边界：从这里往后，程序都只面对结构化配置，不再直接处理 argv。
//
// 入参说明：
// - args：原始命令行参数序列，通常直接来自 `std::env::args()`
pub fn parse_args<I>(args: I) -> Result<(String, RunConfig), String>
where
    I: IntoIterator<Item = String>,
{
    // 这里仍然坚持手写参数解析，而不是引入 clap 一类依赖，
    // 原因和项目早期保持一致：
    // - 参数数量仍然很少
    // - 设备侧程序更看重依赖简单
    // - 当前规则固定后，手写逻辑也更容易完整表达“hybrid 模式”
    let mut argv = args.into_iter();
    let program = argv.next().unwrap_or_else(|| "client-rust".to_string());

    let mut debug_enabled = false;
    let mut listen_enabled = false;
    let mut server_url = None;
    let mut ws_token = None;

    while let Some(arg) = argv.next() {
        match arg.as_str() {
            // debug 开关允许任意位置出现，这样 `-d -l ws://...` 和 `-l debug ws://...`
            // 都能表达同一个意思。
            "debug" | "-d" => debug_enabled = true,
            // `-l` 只负责声明“我要监听”，并不排斥同时给 URL；
            // 如果同时给了 URL，最终会落到 hybrid 模式。
            "-l" => listen_enabled = true,
            // `-t` 只影响主动 connect 方向；
            // 如果当前进程没有 server_url，它会被保留但不会被实际使用。
            "-t" => {
                let token = argv
                    .next()
                    .ok_or_else(|| "missing websocket token after -t".to_string())?;
                if matches!(token.as_str(), "-l" | "-d" | "debug" | "-t") {
                    return Err("missing websocket token after -t".into());
                }
                if token.is_empty() {
                    return Err("websocket token after -t must not be empty".into());
                }
                if ws_token.is_some() {
                    return Err("multiple websocket token values are not supported".into());
                }
                ws_token = Some(token);
            }
            _ if arg.starts_with('-') => {
                return Err(format!("unknown flag: {arg}"));
            }
            _ => {
                // 当前只允许一个 websocket_server_url。
                // 多个 URL 会让 connect 侧的职责不清晰，因此直接在入口拒绝。
                if server_url.is_some() {
                    return Err("multiple websocket_server_url values are not supported".into());
                }
                server_url = Some(arg);
            }
        }
    }

    // 项目当前最小有效配置是：
    // - 监听，或者
    // - 指定一个主动连接目标。
    //
    // 两者都没有时，supervisor 既不能 listen，也不能 connect，程序没有任何可执行职责。
    if !listen_enabled && server_url.is_none() {
        return Err("either -l or websocket_server_url is required".into());
    }

    Ok((
        program,
        RunConfig {
            debug_enabled,
            listen_enabled,
            server_url,
            ws_token,
        },
    ))
}

// usage 生成当前程序的最简命令行帮助文本。
//
// 入参说明：
// - program：要展示在 usage 前缀中的程序名
pub fn usage(program: &str) -> String {
    // usage 保持为最小形式，复杂模式的解释交给 README。
    format!("usage: {program} [debug|-d] [-l] [-t websocket_token] [websocket_server_url]")
}

#[cfg(test)]
mod tests {
    use super::{RunConfig, parse_args, usage};

    #[test]
    // 验证“只给 URL”时会进入 connect-only 模式。
    fn parse_connect_only_mode() {
        let (_, config) = parse_args(vec![
            "client".to_string(),
            "ws://127.0.0.1:9000".to_string(),
        ])
        .unwrap();

        assert_eq!(
            config,
            RunConfig {
                debug_enabled: false,
                listen_enabled: false,
                server_url: Some("ws://127.0.0.1:9000".to_string()),
                ws_token: None,
            }
        );
    }

    #[test]
    // 验证“只给 -l”时会进入 listen-only 模式。
    fn parse_listen_only_mode() {
        let (_, config) = parse_args(vec!["client".to_string(), "-l".to_string()]).unwrap();

        assert_eq!(
            config,
            RunConfig {
                debug_enabled: false,
                listen_enabled: true,
                server_url: None,
                ws_token: None,
            }
        );
    }

    #[test]
    // 验证 hybrid 模式下 debug 标记允许出现在任意位置。
    fn parse_hybrid_mode_with_debug_anywhere() {
        let (_, config) = parse_args(vec![
            "client".to_string(),
            "-l".to_string(),
            "debug".to_string(),
            "ws://server.example".to_string(),
        ])
        .unwrap();

        assert_eq!(
            config,
            RunConfig {
                debug_enabled: true,
                listen_enabled: true,
                server_url: Some("ws://server.example".to_string()),
                ws_token: None,
            }
        );
    }

    #[test]
    // 验证 `-t` 能正确解析 websocket token，且保留 hybrid / connect-only 的兼容行为。
    fn parse_mode_with_ws_token() {
        let (_, config) = parse_args(vec![
            "client".to_string(),
            "-l".to_string(),
            "-t".to_string(),
            "secret-token".to_string(),
            "ws://server.example".to_string(),
        ])
        .unwrap();

        assert_eq!(
            config,
            RunConfig {
                debug_enabled: false,
                listen_enabled: true,
                server_url: Some("ws://server.example".to_string()),
                ws_token: Some("secret-token".to_string())
            }
        );
    }

    #[test]
    // 验证既不监听也不给 URL 时会被拒绝。
    fn reject_empty_mode() {
        let err = parse_args(vec!["client".to_string()]).unwrap_err();
        assert!(err.contains("either -l or websocket_server_url is required"));
    }

    #[test]
    // 验证未知短/长选项会被入口解析直接拒绝。
    fn reject_unknown_flag() {
        let err = parse_args(vec!["client".to_string(), "--bad".to_string()]).unwrap_err();
        assert!(err.contains("unknown flag"));
    }

    #[test]
    // 验证 `-t` 后缺少 token 时，入口会明确拒绝。
    fn reject_missing_ws_token_value() {
        let err = parse_args(vec![
            "client".to_string(),
            "-l".to_string(),
            "-t".to_string(),
        ])
        .unwrap_err();
        assert!(err.contains("missing websocket token after -t"));
    }

    #[test]
    // 验证当前实现只允许一个 websocket token。
    fn reject_multiple_ws_tokens() {
        let err = parse_args(vec![
            "client".to_string(),
            "-t".to_string(),
            "one".to_string(),
            "-t".to_string(),
            "two".to_string(),
            "ws://server.example".to_string(),
        ])
        .unwrap_err();
        assert!(err.contains("multiple websocket token values"));
    }

    #[test]
    // 验证 Debug 输出不会泄露 token 原文，而是打印等长星号。
    fn debug_output_masks_ws_token_with_same_length_asterisks() {
        let config = RunConfig {
            debug_enabled: false,
            listen_enabled: false,
            server_url: Some("ws://server.example".to_string()),
            ws_token: Some("secret-token".to_string()),
        };

        let debug_text = format!("{config:?}");
        assert!(debug_text.contains("************"));
        assert!(!debug_text.contains("secret-token"));
    }

    #[test]
    // 验证当前实现只允许一个主动连接 URL。
    fn reject_multiple_urls() {
        let err = parse_args(vec![
            "client".to_string(),
            "ws://one".to_string(),
            "ws://two".to_string(),
        ])
        .unwrap_err();
        assert!(err.contains("multiple websocket_server_url"));
    }

    #[test]
    // 验证 usage 文本保持稳定，便于脚本和文档引用。
    fn usage_string_matches_new_cli() {
        assert_eq!(
            usage("client"),
            "usage: client [debug|-d] [-l] [-t websocket_token] [websocket_server_url]"
        );
    }
}
