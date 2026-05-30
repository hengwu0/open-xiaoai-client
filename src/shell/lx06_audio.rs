use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context};

use crate::base::{debug_err_log, debug_log, AppError};

const LX06_MODEL: &str = "LX06";
const ASOUND_INSERT_ANCHOR: &str = "defaults.pcm.rate_converter \"speexrate_medium\"";
const LX06_AEC_PCM_NAME: &str = "pcm.lx06_aec_2ch";
const LX06_AEC_CONFIG: &str = r#"pcm.lx06_aec_2ch_route {
    type route
    slave {
        pcm "Capture"
        channels 8
    }

    ttable.0.0 1
    ttable.1.6 1
}

pcm.lx06_aec_2ch {
    type plug
    slave {
        pcm "lx06_aec_2ch_route"
        format S32_LE
        rate 48000
        channels 2
    }
}"#;

const ASOUND_TARGETS: &[&str] = &["/etc/asound.conf", "/etc/asound.conf.dts"];

// prepare_lx06_audio_capability 在进程启动阶段执行一次 LX06 录音能力准备。
//
// 返回值语义：
// - Ok(true)：当前设备是 LX06，且 asound.conf / asound.conf.dts 都已经具备 lx06_aec_2ch 配置
// - Ok(false)：当前设备不是 LX06，因此不启用 fast_recording 相关命令
// - Err(err)：当前设备是 LX06，但配置准备失败；调用方应禁用 fast_recording 相关命令并记录错误
pub(crate) fn prepare_lx06_audio_capability() -> Result<bool, AppError> {
    debug_log("lx06-audio", "Starting LX06 audio capability check");

    if !is_lx06_model()? {
        debug_log(
            "lx06-audio",
            "Device model is not LX06; fast_recording capability disabled",
        );
        return Ok(false);
    }

    debug_log(
        "lx06-audio",
        "Device model is LX06; preparing lx06_aec_2ch ALSA configuration",
    );
    let program_dir = current_program_dir()?;
    debug_log(
        "lx06-audio",
        format!("Using program directory for patched ALSA files: {}", program_dir.display()),
    );

    for target in ASOUND_TARGETS {
        ensure_asound_config_mounted(Path::new(target), &program_dir)?;
    }

    debug_log(
        "lx06-audio",
        "LX06 audio capability prepared successfully; fast_recording commands can be registered",
    );
    Ok(true)
}

fn is_lx06_model() -> Result<bool, AppError> {
    debug_log("lx06-audio", "Checking device model with micocfg_model");
    let output = Command::new("micocfg_model").output().map_err(|err| {
        debug_err_log(
            "lx06-audio",
            format!("Failed to execute micocfg_model: {err}"),
        );
        anyhow!("failed to execute micocfg_model: {err}")
    })?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let exit_code = output.status.code().unwrap_or(-1);
    debug_log(
        "lx06-audio",
        format!(
            "micocfg_model completed: exit_code={exit_code}, stdout={stdout:?}, stderr={stderr:?}"
        ),
    );

    if !output.status.success() {
        debug_err_log(
            "lx06-audio",
            format!("micocfg_model returned non-zero exit code: {exit_code}"),
        );
        return Ok(false);
    }

    Ok(stdout == LX06_MODEL)
}

fn current_program_dir() -> Result<PathBuf, AppError> {
    let exe = std::env::current_exe().context("failed to resolve current executable path")?;
    exe.parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("current executable path has no parent directory: {}", exe.display()))
}

fn ensure_asound_config_mounted(system_path: &Path, program_dir: &Path) -> Result<(), AppError> {
    debug_log(
        "lx06-audio",
        format!("Checking ALSA config file: {}", system_path.display()),
    );
    let original = fs::read_to_string(system_path).with_context(|| {
        format!("failed to read ALSA config file: {}", system_path.display())
    })?;

    if contains_lx06_aec_pcm(&original) {
        debug_log(
            "lx06-audio",
            format!(
                "{} already contains {}; skip patch and bind mount",
                system_path.display(),
                LX06_AEC_PCM_NAME
            ),
        );
        return Ok(());
    }

    let file_name = system_path.file_name().ok_or_else(|| {
        anyhow!(
            "ALSA config path has no file name: {}",
            system_path.display()
        )
    })?;
    let patched_path = program_dir.join(file_name);
    if patched_path.exists() {
        debug_log(
            "lx06-audio",
            format!(
                "{} does not contain {}; using existing program-local ALSA config {}",
                system_path.display(),
                LX06_AEC_PCM_NAME,
                patched_path.display()
            ),
        );
        return bind_mount(&patched_path, system_path);
    }

    debug_log(
        "lx06-audio",
        format!(
            "{} does not contain {}; creating patched copy at {}",
            system_path.display(),
            LX06_AEC_PCM_NAME,
            patched_path.display()
        ),
    );

    let patched = insert_lx06_aec_config(&original);
    let original_permissions = fs::metadata(system_path)
        .with_context(|| {
            format!(
                "failed to read ALSA config metadata: {}",
                system_path.display()
            )
        })?
        .permissions();
    fs::write(&patched_path, patched).with_context(|| {
        format!("failed to write patched ALSA config: {}", patched_path.display())
    })?;
    fs::set_permissions(&patched_path, original_permissions).with_context(|| {
        format!(
            "failed to apply original permissions to patched ALSA config: {}",
            patched_path.display()
        )
    })?;

    bind_mount(&patched_path, system_path)
}

fn bind_mount(source: &Path, target: &Path) -> Result<(), AppError> {
    debug_log(
        "lx06-audio",
        format!("Running bind mount: mount --bind {} {}", source.display(), target.display()),
    );
    let output = Command::new("mount")
        .arg("--bind")
        .arg(source)
        .arg(target)
        .output()
        .with_context(|| {
            format!(
                "failed to execute mount --bind {} {}",
                source.display(),
                target.display()
            )
        })?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let exit_code = output.status.code().unwrap_or(-1);
    if !output.status.success() {
        debug_err_log(
            "lx06-audio",
            format!(
                "Bind mount failed: source={}, target={}, exit_code={}, stdout={:?}, stderr={:?}",
                source.display(),
                target.display(),
                exit_code,
                stdout,
                stderr
            ),
        );
        return Err(anyhow!(
            "mount --bind {} {} failed with exit_code={}: {}",
            source.display(),
            target.display(),
            exit_code,
            stderr
        ));
    }

    debug_log(
        "lx06-audio",
        format!(
            "Bind mount succeeded: source={}, target={}, stdout={:?}, stderr={:?}",
            source.display(),
            target.display(),
            stdout,
            stderr
        ),
    );
    Ok(())
}

fn contains_lx06_aec_pcm(contents: &str) -> bool {
    contents.lines().any(|line| {
        let line = line.trim_start();
        let Some(rest) = line.strip_prefix(LX06_AEC_PCM_NAME) else {
            return false;
        };
        let rest = rest.trim_start();
        rest.is_empty() || rest.starts_with('{')
    })
}

fn insert_lx06_aec_config(contents: &str) -> String {
    if let Some(insert_at) = find_anchor_line_offset(contents) {
        let mut out = String::with_capacity(contents.len() + LX06_AEC_CONFIG.len() + 4);
        out.push_str(&contents[..insert_at]);
        if !out.ends_with('\n') {
            out.push('\n');
        }
        out.push('\n');
        out.push_str(LX06_AEC_CONFIG);
        out.push_str("\n\n");
        out.push_str(&contents[insert_at..]);
        return out;
    }

    let mut out = String::with_capacity(contents.len() + LX06_AEC_CONFIG.len() + 2);
    out.push_str(contents);
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.push('\n');
    out.push_str(LX06_AEC_CONFIG);
    out.push('\n');
    out
}

fn find_anchor_line_offset(contents: &str) -> Option<usize> {
    let mut offset = 0;
    for line in contents.split_inclusive('\n') {
        if line.trim() == ASOUND_INSERT_ANCHOR {
            return Some(offset);
        }
        offset += line.len();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{contains_lx06_aec_pcm, insert_lx06_aec_config, ASOUND_INSERT_ANCHOR};

    #[test]
    fn detects_exact_lx06_aec_pcm_but_not_route_only() {
        assert!(!contains_lx06_aec_pcm("pcm.lx06_aec_2ch_route {\n}\n"));
        assert!(contains_lx06_aec_pcm("pcm.lx06_aec_2ch {\n}\n"));
    }

    #[test]
    fn inserts_before_rate_converter_anchor() {
        let input = format!("pcm.Capture {{\n}}\n\n{ASOUND_INSERT_ANCHOR}\n");
        let output = insert_lx06_aec_config(&input);
        let pcm_pos = output.find("pcm.lx06_aec_2ch {").unwrap();
        let anchor_pos = output.find(ASOUND_INSERT_ANCHOR).unwrap();
        assert!(pcm_pos < anchor_pos);
    }

    #[test]
    fn appends_when_anchor_is_missing() {
        let output = insert_lx06_aec_config("pcm.Capture {\n}\n");
        assert!(output.contains("pcm.lx06_aec_2ch {"));
        assert!(output.ends_with("}\n"));
    }
}
