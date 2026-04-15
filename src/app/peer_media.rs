use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::audio::{AudioPlayer, AudioRecorder};
use crate::transport::PeerId;

// PeerMedia 表示“单个 peer 独享的一组本地音频设备”。
//
// 它和 websocket 连接本身分离：
// - player 负责消费这个 peer 发来的 `tag=play` 播放流
// - recorder 负责把本机录音回送给这个 peer
#[derive(Clone)]
pub(crate) struct PeerMedia {
    pub(crate) player: Arc<AudioPlayer>,
    pub(crate) recorder: Arc<AudioRecorder>,
}

impl PeerMedia {
    fn new() -> Self {
        Self {
            player: Arc::new(AudioPlayer::new()),
            recorder: Arc::new(AudioRecorder::new()),
        }
    }
}

// PeerMediaRegistry 按 peer_id 管理本轮 session 内的本地音频设备。
//
// 这样命令处理器和 router 都可以只拿 peer_id，就找到对应 peer 的 player/recorder，
// 不再共享同一套全局音频设备。
pub(crate) struct PeerMediaRegistry {
    peers: Mutex<HashMap<PeerId, PeerMedia>>,
}

impl PeerMediaRegistry {
    pub(crate) fn new() -> Self {
        Self {
            peers: Mutex::new(HashMap::new()),
        }
    }

    // 为一个新接入的 peer 分配独享的音频设备。
    pub(crate) fn add_peer(&self, peer_id: PeerId) {
        self.peers
            .lock()
            .expect("peer media registry poisoned")
            .insert(peer_id, PeerMedia::new());
    }

    // 返回某个 peer 对应的音频设备句柄。
    pub(crate) fn get(&self, peer_id: PeerId) -> Option<PeerMedia> {
        self.peers
            .lock()
            .expect("peer media registry poisoned")
            .get(&peer_id)
            .cloned()
    }

    // 移除某个 peer，并把它持有的音频设备返回给调用方做收尾。
    pub(crate) fn remove_peer(&self, peer_id: PeerId) -> Option<PeerMedia> {
        self.peers
            .lock()
            .expect("peer media registry poisoned")
            .remove(&peer_id)
    }

    // 在 session 整体关闭时一次性取出所有 peer 的设备句柄，交给上层统一 stop。
    pub(crate) fn drain(&self) -> Vec<PeerMedia> {
        self.peers
            .lock()
            .expect("peer media registry poisoned")
            .drain()
            .map(|(_, media)| media)
            .collect()
    }
}
