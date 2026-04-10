use std::fs;
use std::path::Path;

use crate::base::AppError;

const SYS_CLASS_NET_PATH: &str = "/sys/class/net";
const INVALID_MAC: &str = "00:00:00:00:00:00";
const LISTEN_CODE_ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
const LISTEN_CODE_LEN: usize = 8;

// get_primary_mac_address 返回当前设备首个可用网卡的 MAC 地址。
//
// 它优先从 `/sys/class/net` 中选择一个：
// - 不是 `lo`
// - 地址格式合法
// - 不是全 0 地址
//
// 入参说明：
// - 无
pub fn get_primary_mac_address() -> Result<String, AppError> {
    let mut interface_names = fs::read_dir(SYS_CLASS_NET_PATH)?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .collect::<Vec<_>>();
    interface_names.sort();

    for interface_name in interface_names {
        if interface_name == "lo" {
            continue;
        }

        let address_path = Path::new(SYS_CLASS_NET_PATH)
            .join(&interface_name)
            .join("address");
        let Ok(address) = fs::read_to_string(&address_path) else {
            continue;
        };
        let address = normalize_mac_address(&address);
        if is_valid_mac_address(&address) && address != INVALID_MAC {
            return Ok(address);
        }
    }

    Err(anyhow::anyhow!(
        "no usable mac address found under {SYS_CLASS_NET_PATH}"
    ))
}

// get_device_listen_code 读取设备 MAC，并映射成固定 8 位 listen code。
//
// 入参说明：
// - 无
pub fn get_device_listen_code() -> Result<String, AppError> {
    let mac = get_primary_mac_address()?;
    Ok(hash_mac_to_eight_char_code(&mac))
}

// hash_mac_to_eight_char_code 把 MAC 地址稳定映射成 8 位数字+英文 code。
//
// 它会先把 MAC 中的字母统一转成小写，再做固定的 FNV-1a 64bit 计算，
// 最后把结果编码成 8 位 base36（`0-9a-z`）。
//
// 入参说明：
// - mac：形如 `aa:bb:cc:dd:ee:ff` 的 MAC 地址字符串
pub fn hash_mac_to_eight_char_code(mac: &str) -> String {
    let normalized_mac = normalize_mac_address(mac);
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in normalized_mac.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    let mut value = hash % 36_u64.pow(LISTEN_CODE_LEN as u32);
    let mut code = ['0'; LISTEN_CODE_LEN];
    for index in (0..LISTEN_CODE_LEN).rev() {
        code[index] = LISTEN_CODE_ALPHABET[(value % 36) as usize] as char;
        value /= 36;
    }
    code.iter().collect()
}

fn normalize_mac_address(mac: &str) -> String {
    mac.trim().to_ascii_lowercase()
}

fn is_valid_mac_address(mac: &str) -> bool {
    let mut parts = mac.split(':');
    for _ in 0..6 {
        let Some(part) = parts.next() else {
            return false;
        };
        if part.len() != 2 || !part.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return false;
        }
    }
    parts.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::{hash_mac_to_eight_char_code, is_valid_mac_address, normalize_mac_address};

    #[test]
    fn hash_mac_to_eight_char_code_is_stable_and_alphanumeric() {
        let code = hash_mac_to_eight_char_code("aa:bb:cc:dd:ee:ff");
        assert_eq!(code.len(), 8);
        assert!(code.chars().all(|ch| ch.is_ascii_alphanumeric()));
        assert_eq!(code, hash_mac_to_eight_char_code("aa:bb:cc:dd:ee:ff"));
    }

    #[test]
    fn hash_mac_to_eight_char_code_normalizes_uppercase_mac_before_hashing() {
        assert_eq!(
            hash_mac_to_eight_char_code("aa:bb:cc:dd:ee:ff"),
            hash_mac_to_eight_char_code("AA:BB:CC:DD:EE:FF")
        );
    }

    #[test]
    fn normalize_mac_address_trims_and_lowercases_letters() {
        assert_eq!(
            normalize_mac_address(" AA:BB:CC:DD:EE:FF \n"),
            "aa:bb:cc:dd:ee:ff"
        );
    }

    #[test]
    fn mac_address_validation_matches_expected_shape() {
        assert!(is_valid_mac_address("aa:bb:cc:dd:ee:ff"));
        assert!(is_valid_mac_address("01:23:45:67:89:ab"));
        assert!(!is_valid_mac_address("aa:bb:cc:dd:ee"));
        assert!(!is_valid_mac_address("aa:bb:cc:dd:ee:gg"));
        assert!(!is_valid_mac_address("aabbccddeeff"));
    }
}
