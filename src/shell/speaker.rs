use crate::base::AppError;
use crate::shell::command::{CommandResult, run_shell as execute_shell};

// 这是从 open-xiaoai 迁移过来的设备控制方法备份。
// 当前故意不在 shell/mod.rs 中声明，因此不会接入 client-rust 的编译链。
// 这里保留原有方法注释，后续如果要正式启用，再按需要整理模块边界和命令注册。

pub struct SpeakerManager;

impl SpeakerManager {
    // get_boot 读取设备当前配置的启动分区。
    //
    // 入参说明：
    // - 无
    pub async fn get_boot() -> Result<String, AppError> {
        const COMMAND: &str = r#"
            echo $(fw_env -g boot_part)
        "#;
        // 参数说明：
        // - COMMAND：设备侧读取 boot_part 的 shell 脚本
        // - SpeakerManager::run_shell(COMMAND)：执行脚本并返回完整 stdout/stderr/exit_code
        let res = SpeakerManager::run_shell(COMMAND).await?;
        Ok(res.stdout.trim().to_string())
    }

    // set_boot 设置设备下次启动要使用的分区。
    //
    // 入参说明：
    // - boot_part：目标启动分区名
    pub async fn set_boot(boot_part: &str) -> Result<bool, AppError> {
        const COMMAND: &str = r#"
            fw_env -s boot_part %s >/dev/null 2>&1 && echo $(fw_env -g boot_part)
        "#;
        // 参数说明：
        // - COMMAND.replace(\"%s\", boot_part)：把占位符替换成目标分区名
        let script = COMMAND.replace("%s", boot_part);
        let res = SpeakerManager::run_shell(&script).await?;
        Ok(res.stdout.contains(boot_part))
    }

    // get_device_model 读取设备型号字符串。
    //
    // 入参说明：
    // - 无
    pub async fn get_device_model() -> Result<String, AppError> {
        const COMMAND: &str = r#"
            echo $(micocfg_model)
        "#;
        let res = SpeakerManager::run_shell(COMMAND).await?;
        Ok(res.stdout.trim().to_string())
    }

    // get_device_sn 读取设备序列号。
    //
    // 入参说明：
    // - 无
    pub async fn get_device_sn() -> Result<String, AppError> {
        const COMMAND: &str = r#"
            echo $(micocfg_sn)
        "#;
        let res = SpeakerManager::run_shell(COMMAND).await?;
        Ok(res.stdout.trim().to_string())
    }

    // get_play_status 查询设备当前播放状态，并映射成简化状态字符串。
    //
    // 入参说明：
    // - 无
    pub async fn get_play_status() -> Result<String, AppError> {
        const COMMAND: &str = r#"
            mphelper mute_stat
        "#;
        let res = SpeakerManager::run_shell(COMMAND).await?;
        let status = if res.stdout.contains("1") {
            "playing"
        } else if res.stdout.contains("2") {
            "paused"
        } else {
            "idle"
        };
        Ok(status.to_string())
    }

    // play 触发设备开始播放。
    //
    // 入参说明：
    // - 无
    pub async fn play() -> Result<bool, AppError> {
        const COMMAND: &str = r#"
            mphelper play
        "#;
        let res = SpeakerManager::run_shell(COMMAND).await?;
        Ok(res.stdout.contains("\"code\": 0"))
    }

    // pause 触发设备暂停播放。
    //
    // 入参说明：
    // - 无
    pub async fn pause() -> Result<bool, AppError> {
        const COMMAND: &str = r#"
            mphelper pause
        "#;
        let res = SpeakerManager::run_shell(COMMAND).await?;
        Ok(res.stdout.contains("\"code\": 0"))
    }

    // play_text 调用设备侧 TTS 播放一段文本。
    //
    // 入参说明：
    // - text：要播报的文本
    pub async fn play_text(text: &str) -> Result<bool, AppError> {
        const COMMAND: &str = r#"
            /usr/sbin/tts_play.sh '%s'
        "#;
        // 参数说明：
        // - COMMAND.replace(\"%s\", text)：把要播报的文本插入脚本模板
        let script = COMMAND.replace("%s", text);
        let res = SpeakerManager::run_shell(&script).await?;
        Ok(res.stdout.contains("\"code\": 0"))
    }

    // play_url 让设备播放指定 URL 的音频资源。
    //
    // 入参说明：
    // - url：目标音频资源地址
    pub async fn play_url(url: &str) -> Result<bool, AppError> {
        const COMMAND: &str = r#"
            ubus call mediaplayer player_play_url '{"url":"%s","type": 1}'
        "#;
        let script = COMMAND.replace("%s", url);
        let res = SpeakerManager::run_shell(&script).await?;
        Ok(res.stdout.contains("\"code\": 0"))
    }

    // get_mic_status 查询麦克风当前开关状态。
    //
    // 入参说明：
    // - 无
    pub async fn get_mic_status() -> Result<String, AppError> {
        const COMMAND: &str = r#"
            [ ! -f /tmp/mipns/mute ] && echo on || echo off
        "#;
        let res = SpeakerManager::run_shell(COMMAND).await?;
        let status = if res.stdout.contains("on") {
            "on"
        } else {
            "off"
        };
        Ok(status.to_string())
    }

    // mic_on 打开设备麦克风。
    //
    // 入参说明：
    // - 无
    pub async fn mic_on() -> Result<bool, AppError> {
        const COMMAND: &str = r#"
            ubus -t1 -S call pnshelper event_notify '{"src":3, "event":7}' 2>&1
        "#;
        let res = SpeakerManager::run_shell(COMMAND).await?;
        Ok(res.stdout.contains("\"code\":0"))
    }

    // mic_off 关闭设备麦克风。
    //
    // 入参说明：
    // - 无
    pub async fn mic_off() -> Result<bool, AppError> {
        const COMMAND: &str = r#"
            ubus -t1 -S call pnshelper event_notify '{"src":3, "event":8}' 2>&1
        "#;
        let res = SpeakerManager::run_shell(COMMAND).await?;
        Ok(res.stdout.contains("\"code\":0"))
    }

    // ask_xiaoai 把一段文本作为指令发送给小爱服务。
    //
    // 入参说明：
    // - text：要提交给小爱的文本内容
    pub async fn ask_xiaoai(text: &str) -> Result<bool, AppError> {
        const COMMAND: &str = r#"
            ubus call mibrain ai_service '{"tts":1,"nlp":1,"nlp_text":"%s"}'
        "#;
        let script = COMMAND.replace("%s", text);
        let res = SpeakerManager::run_shell(&script).await?;
        Ok(res.stdout.contains("\"code\": 0"))
    }

    // abort_xiaoai 重启设备侧小爱服务，用于打断当前处理流程。
    //
    // 入参说明：
    // - 无
    pub async fn abort_xiaoai() -> Result<bool, AppError> {
        const COMMAND: &str = r#"
            /etc/init.d/mico_aivs_lab restart >/dev/null 2>&1
        "#;
        let res = SpeakerManager::run_shell(COMMAND).await?;
        Ok(res.exit_code == 0)
    }

    // wake_up 触发一次设备唤醒或模拟一次短暂唤醒流程。
    //
    // 入参说明：
    // - flag：true 表示直接触发唤醒；false 表示执行一次开麦再关麦的唤醒相关流程
    pub async fn wake_up(flag: bool) -> Result<bool, AppError> {
        let command = if flag {
            r#"
                ubus call pnshelper event_notify '{"src":1,"event":0}'
            "#
        } else {
            r#"
                ubus call pnshelper event_notify '{"src":3, "event":7}'
                sleep 0.1
                ubus call pnshelper event_notify '{"src":3, "event":8}'
            "#
        };
        // 参数说明：
        // - command：根据 flag 选择出的实际 shell 脚本
        let res = SpeakerManager::run_shell(command).await?;
        Ok(res.stdout.contains("\"code\": 0"))
    }

    // run_shell 是 SpeakerManager 内部统一执行 shell 的薄包装。
    //
    // 这里单独包一层，主要是为了保留后续在 SpeakerManager 维度补充额外日志、
    // 兼容逻辑或 mock 能力的空间。
    //
    // 入参说明：
    // - script：要执行的 shell 脚本文本
    async fn run_shell(script: &str) -> Result<CommandResult, AppError> {
        execute_shell(script)
    }
}
