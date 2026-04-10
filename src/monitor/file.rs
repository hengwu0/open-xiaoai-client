use std::ffi::CString;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::mem::size_of;
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::thread::{self, JoinHandle};

use crate::base::{AppError, debug_err_log, debug_log};

const INOTIFY_BUFFER_LEN: usize = size_of::<libc::inotify_event>() * 4096;
const UNREGISTERED_FD: RawFd = -1;
const FILE_WATCH_MASK: u32 =
    libc::IN_MODIFY | libc::IN_MOVE_SELF | libc::IN_DELETE_SELF | libc::IN_ATTRIB;
const DIRECTORY_WATCH_MASK: u32 =
    libc::IN_CREATE | libc::IN_MOVED_TO | libc::IN_MOVE_SELF | libc::IN_DELETE_SELF;

// FileMonitorHandle 是文件类 monitor 对外暴露的最小控制面。
// 外部只需要：
// 1. `close()`：请求 monitor 尽快退出
// 2. `join()`：等待 monitor 线程收尾
pub(crate) struct FileMonitorHandle {
    join_handle: JoinHandle<Result<(), AppError>>,
    control: Arc<FileMonitorControl>,
}

impl FileMonitorHandle {
    // close 请求当前文件 monitor 尽快退出。
    //
    // 入参说明：
    // - self：当前文件 monitor 的控制句柄
    pub(crate) fn close(&self) {
        self.control.close();
    }

    // join 取出内部线程句柄，交给上层自行等待收尾。
    //
    // 入参说明：
    // - self：当前文件 monitor 的控制句柄
    pub(crate) fn join(self) -> JoinHandle<Result<(), AppError>> {
        self.join_handle
    }
}

// 这里保留了一个极小的 shutdown 控制对象，而不是完全“只靠 close 当前 fd”。
// 原因是 monitor 线程会在“文件 inotify”和“目录 inotify”之间切换：
//
// 1. 如果 close() 恰好发生在旧 fd 已经关闭、但新 fd 还没安装进共享槽位的窗口里，
//    那么单靠关闭当前 fd 会错过这次 stop，请求无法跨越这个切换窗口。
// 2. 因此需要一个持久化的 shutdown 标记，让线程在每次准备进入下一段阻塞前，
//    都能看见“当前这轮已经要求退出了”。
//
// 也就是说：
// - “关闭当前 inotify fd”负责唤醒已经阻塞住的 read
// - “shutdown_requested”负责覆盖 fd 切换瞬间的竞态窗口
struct FileMonitorControl {
    shutdown_requested: AtomicBool,
    file_inotify_fd: AtomicI32,
    directory_inotify_fd: AtomicI32,
}

impl Default for FileMonitorControl {
    fn default() -> Self {
        Self {
            shutdown_requested: AtomicBool::new(false),
            file_inotify_fd: AtomicI32::new(UNREGISTERED_FD),
            directory_inotify_fd: AtomicI32::new(UNREGISTERED_FD),
        }
    }
}

impl FileMonitorControl {
    // is_shutdown_requested 返回当前 monitor 是否已经收到退出请求。
    //
    // 入参说明：
    // - self：当前 monitor 的 shutdown 控制对象
    fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }

    // close 发起 monitor 关闭流程，并主动关闭当前可能阻塞中的 inotify fd。
    //
    // 入参说明：
    // - self：当前 monitor 的 shutdown 控制对象
    fn close(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        // 先关目录 inotify，再关文件 inotify。
        // 这样如果当前线程正卡在“等待文件重建”的路径上，会第一时间被唤醒。
        self.close_registered_fd(InotifyKind::Directory);
        self.close_registered_fd(InotifyKind::File);
    }

    // register_inotify_fd 把一个新建的 inotify fd 注册到共享控制对象里。
    //
    // 如果这时已经收到 shutdown 请求，就会立即关闭该 fd，并告诉调用方不要继续使用它。
    //
    // 入参说明：
    // - self：当前 monitor 的 shutdown 控制对象
    // - kind：这个 fd 属于文件监听还是目录监听
    // - fd：刚创建好的 inotify 文件描述符
    fn register_inotify_fd(&self, kind: InotifyKind, fd: RawFd) -> Result<bool, AppError> {
        if self.is_shutdown_requested() {
            let _ = unsafe { libc::close(fd) };
            return Ok(false);
        }
        self.slot(kind).store(fd, Ordering::SeqCst);
        if self.is_shutdown_requested() {
            if let Some(fd) = self.take_inotify_fd_if_matches(kind, fd) {
                let _ = unsafe { libc::close(fd) };
            }
            return Ok(false);
        }
        Ok(true)
    }

    // take_inotify_fd_if_matches 只有在槽位里正好还是这个 fd 时，才把它取出来。
    //
    // 这样可以避免新旧 fd 切换时误关掉不属于自己的那一个。
    //
    // 入参说明：
    // - self：当前 monitor 的 shutdown 控制对象
    // - kind：要访问的槽位类型
    // - fd：调用方认为“自己拥有”的 fd
    fn take_inotify_fd_if_matches(&self, kind: InotifyKind, fd: RawFd) -> Option<RawFd> {
        self.slot(kind)
            .compare_exchange(fd, UNREGISTERED_FD, Ordering::SeqCst, Ordering::SeqCst)
            .ok()
    }

    // close_registered_fd 关闭某个类型当前登记在槽位里的 inotify fd。
    //
    // 入参说明：
    // - self：当前 monitor 的 shutdown 控制对象
    // - kind：要关闭的是文件监听 fd 还是目录监听 fd
    fn close_registered_fd(&self, kind: InotifyKind) {
        let fd = self.slot(kind).swap(UNREGISTERED_FD, Ordering::SeqCst);
        if fd != UNREGISTERED_FD {
            let _ = unsafe { libc::close(fd) };
        }
    }

    // slot 返回指定 inotify 类型对应的共享槽位。
    //
    // 入参说明：
    // - self：当前 monitor 的 shutdown 控制对象
    // - kind：要访问的槽位类型
    fn slot(&self, kind: InotifyKind) -> &AtomicI32 {
        match kind {
            InotifyKind::File => &self.file_inotify_fd,
            InotifyKind::Directory => &self.directory_inotify_fd,
        }
    }
}

#[derive(Clone, Copy)]
enum InotifyKind {
    File,
    Directory,
}

// ManagedInotify 负责两件事：
// 1. 创建一个新的 inotify fd，并注册到共享控制对象里，便于 close() 时从别的线程把它关掉
// 2. 在当前作用域结束时，把属于自己的 fd 从控制对象里摘掉并关闭
struct ManagedInotify {
    fd: RawFd,
    kind: InotifyKind,
    control: Arc<FileMonitorControl>,
}

impl ManagedInotify {
    // new 创建并注册一个新的 ManagedInotify。
    //
    // 入参说明：
    // - control：当前 monitor 共享的 shutdown 控制对象
    // - kind：当前要创建的是文件监听 fd 还是目录监听 fd
    fn new(control: Arc<FileMonitorControl>, kind: InotifyKind) -> Result<Option<Self>, AppError> {
        let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if !control.register_inotify_fd(kind, fd)? {
            return Ok(None);
        }
        Ok(Some(Self { fd, kind, control }))
    }

    // fd 返回底层 inotify 文件描述符。
    //
    // 入参说明：
    // - self：当前托管的 inotify 句柄
    fn fd(&self) -> RawFd {
        self.fd
    }

    // is_shutdown_requested 透传当前 monitor 的 shutdown 标记。
    //
    // 入参说明：
    // - self：当前托管的 inotify 句柄
    fn is_shutdown_requested(&self) -> bool {
        self.control.is_shutdown_requested()
    }
}

impl Drop for ManagedInotify {
    // drop 在作用域结束时摘掉并关闭仍归自己拥有的 inotify fd。
    //
    // 入参说明：
    // - self：当前托管的 inotify 句柄
    fn drop(&mut self) {
        if let Some(fd) = self.control.take_inotify_fd_if_matches(self.kind, self.fd) {
            let _ = unsafe { libc::close(fd) };
        }
    }
}

// spawn_file_monitor 启动一个基于 inotify 的通用文件追加监听线程。
//
// 调用方只需要提供：
// - 当文件被重置/重新创建时要执行的 on_reset 回调
// - 当读到一条新行时要执行的 on_line 回调
//
// 入参说明：
// - thread_name：要创建的 monitor 线程名
// - component：日志里使用的组件标签
// - path：要监听的目标文件路径
// - on_reset：文件重置或重新出现时的回调
// - on_line：读取到新增行时的回调
pub(crate) fn spawn_file_monitor<FR, FL>(
    thread_name: &str,
    component: &'static str,
    path: &'static str,
    on_reset: FR,
    on_line: FL,
) -> FileMonitorHandle
where
    FR: FnMut() -> Result<(), AppError> + Send + 'static,
    FL: FnMut(&str) -> Result<(), AppError> + Send + 'static,
{
    let control = Arc::new(FileMonitorControl::default());
    let control_for_thread = control.clone();
    let join_handle = thread::Builder::new()
        .name(thread_name.to_string())
        .spawn(move || monitor_file(component, path, control_for_thread, on_reset, on_line))
        .expect("spawn file monitor");

    FileMonitorHandle {
        join_handle,
        control,
    }
}

// monitor_file 是文件 monitor 线程的主状态机。
//
// 它在“文件存在”和“文件不存在”两种稳定状态之间切换：
// - 文件存在时监听文件自身事件
// - 文件不存在时监听父目录，等待目标文件重新出现
//
// 入参说明：
// - component：日志组件名
// - path：目标文件路径字符串
// - control：当前 monitor 的 shutdown 控制对象
// - on_reset：文件被重置时的回调
// - on_line：读到新增行时的回调
fn monitor_file<FR, FL>(
    component: &'static str,
    path: &'static str,
    control: Arc<FileMonitorControl>,
    mut on_reset: FR,
    mut on_line: FL,
) -> Result<(), AppError>
where
    FR: FnMut() -> Result<(), AppError>,
    FL: FnMut(&str) -> Result<(), AppError>,
{
    let file_path = Path::new(path);
    let Some(parent_dir) = file_path.parent() else {
        debug_err_log(
            component,
            format!("Monitored file has no parent directory; monitor will exit: {path}"),
        );
        return Ok(());
    };
    let Some(file_name) = file_path.file_name() else {
        debug_err_log(
            component,
            format!("Monitored file has no file name; monitor will exit: {path}"),
        );
        return Ok(());
    };

    // monitor 的大状态机只有两种稳定状态：
    // 1. 文件存在：监听文件本身的 inotify 事件
    // 2. 文件不存在：监听父目录，等目标文件重新出现
    //
    // 两种状态都在同一个线程里切换，不起临时线程，也不做 100ms reopen 轮询。
    while !control.is_shutdown_requested() {
        match try_open_monitored_file(component, file_path)? {
            Some(mut reader) => match watch_file_events(
                component,
                file_path,
                control.clone(),
                &mut reader,
                &mut on_reset,
                &mut on_line,
            )? {
                MonitorLoopResult::Reopen => continue,
                MonitorLoopResult::Closed | MonitorLoopResult::Stop => break,
            },
            None => match wait_for_file_recreation(
                component,
                file_path,
                parent_dir,
                file_name,
                control.clone(),
            )? {
                MonitorLoopResult::Reopen => continue,
                MonitorLoopResult::Closed | MonitorLoopResult::Stop => break,
            },
        }
    }

    Ok(())
}

// try_open_monitored_file 尝试以“从文件尾开始追踪新增内容”的方式打开目标文件。
//
// 入参说明：
// - component：日志组件名
// - file_path：要打开的目标文件路径
fn try_open_monitored_file(
    component: &str,
    file_path: &Path,
) -> Result<Option<BufReader<File>>, AppError> {
    if !file_path.exists() {
        return Ok(None);
    }

    match OpenOptions::new().read(true).open(file_path) {
        Ok(mut file) => {
            // 和旧项目保持一致：从文件尾部开始，只关心“新增内容”，不回放历史行。
            file.seek(SeekFrom::End(0))?;
            debug_log(
                component,
                format!("Monitored file opened: {}", file_path.display()),
            );
            Ok(Some(BufReader::new(file)))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => {
            debug_err_log(
                component,
                format!(
                    "Failed to open monitored file {}: {err}",
                    file_path.display()
                ),
            );
            Err(err.into())
        }
    }
}

// wait_for_file_recreation 在目标文件暂时不存在时，改为监听父目录并等待文件重新出现。
//
// 入参说明：
// - component：日志组件名
// - file_path：目标文件完整路径
// - parent_dir：目标文件所在目录
// - file_name：目标文件名
// - control：当前 monitor 的 shutdown 控制对象
fn wait_for_file_recreation(
    component: &'static str,
    file_path: &Path,
    parent_dir: &Path,
    file_name: &OsStr,
    control: Arc<FileMonitorControl>,
) -> Result<MonitorLoopResult, AppError> {
    debug_log(
        component,
        format!(
            "Waiting for monitored file to appear: {}",
            file_path.display()
        ),
    );

    if !parent_dir.exists() {
        debug_err_log(
            component,
            format!(
                "Monitored directory does not exist; monitor will exit: {}",
                parent_dir.display()
            ),
        );
        return Ok(MonitorLoopResult::Stop);
    }

    let Some(inotify) = ManagedInotify::new(control.clone(), InotifyKind::Directory)? else {
        return Ok(MonitorLoopResult::Closed);
    };
    let watch_descriptor = match add_watch(inotify.fd(), parent_dir, DIRECTORY_WATCH_MASK) {
        Ok(watch_descriptor) => watch_descriptor,
        Err(err)
            if control.is_shutdown_requested()
                && err
                    .downcast_ref::<std::io::Error>()
                    .and_then(std::io::Error::raw_os_error)
                    .is_some_and(|code| matches!(code, libc::EBADF | libc::EINVAL)) =>
        {
            return Ok(MonitorLoopResult::Closed);
        }
        Err(err) => return Err(err),
    };
    debug_log(
        component,
        format!(
            "Directory watch registered: dir={}, target_file={}",
            parent_dir.display(),
            file_path.display()
        ),
    );

    let wait_result = watch_directory_events(component, file_path, parent_dir, file_name, &inotify);
    remove_watch(component, inotify.fd(), watch_descriptor);
    wait_result
}

// add_watch 在给定 inotify fd 上为某个路径注册一条 watch。
//
// 入参说明：
// - fd：目标 inotify 文件描述符
// - path：要监听的文件或目录路径
// - watch_mask：inotify 事件掩码
fn add_watch(fd: RawFd, path: &Path, watch_mask: u32) -> Result<i32, AppError> {
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|err| anyhow::anyhow!("invalid monitored path: {err}"))?;
    let watch_descriptor = unsafe { libc::inotify_add_watch(fd, path.as_ptr(), watch_mask) };
    if watch_descriptor < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(watch_descriptor)
}

// remove_watch 尝试从 inotify fd 上移除一条 watch，并在异常时记录日志。
//
// 入参说明：
// - component：日志组件名
// - fd：目标 inotify 文件描述符
// - watch_descriptor：要移除的 watch descriptor
fn remove_watch(component: &str, fd: RawFd, watch_descriptor: i32) {
    let result = unsafe { libc::inotify_rm_watch(fd, watch_descriptor) };
    if result < 0 {
        let err = std::io::Error::last_os_error();
        if !matches!(err.raw_os_error(), Some(libc::EINVAL) | Some(libc::EBADF)) {
            debug_err_log(
                component,
                format!("Failed to remove inotify watch {watch_descriptor}: {err}"),
            );
        }
    }
}

enum MonitorLoopResult {
    Reopen,
    Closed,
    Stop,
}

enum InotifyReadResult {
    Events(usize),
    Closed,
}

// watch_file_events 负责在“文件存在”状态下建立文件监听并进入文件事件循环。
//
// 入参说明：
// - component：日志组件名
// - file_path：当前正在监听的目标文件路径
// - control：当前 monitor 的 shutdown 控制对象
// - reader：与目标文件绑定的 BufReader
// - on_reset：文件被重置时的回调
// - on_line：读到新增行时的回调
fn watch_file_events<FR, FL>(
    component: &'static str,
    file_path: &Path,
    control: Arc<FileMonitorControl>,
    reader: &mut BufReader<File>,
    on_reset: &mut FR,
    on_line: &mut FL,
) -> Result<MonitorLoopResult, AppError>
where
    FR: FnMut() -> Result<(), AppError>,
    FL: FnMut(&str) -> Result<(), AppError>,
{
    let Some(inotify) = ManagedInotify::new(control.clone(), InotifyKind::File)? else {
        return Ok(MonitorLoopResult::Closed);
    };
    let watch_descriptor = match add_watch(inotify.fd(), file_path, FILE_WATCH_MASK) {
        Ok(watch_descriptor) => watch_descriptor,
        Err(err)
            if control.is_shutdown_requested()
                && err
                    .downcast_ref::<std::io::Error>()
                    .and_then(std::io::Error::raw_os_error)
                    .is_some_and(|code| matches!(code, libc::EBADF | libc::EINVAL)) =>
        {
            return Ok(MonitorLoopResult::Closed);
        }
        Err(err) => return Err(err),
    };
    debug_log(
        component,
        format!(
            "Inotify watch registered: path={}, wd={watch_descriptor}",
            file_path.display()
        ),
    );

    let watch_result =
        watch_file_event_loop(component, file_path, &inotify, reader, on_reset, on_line);
    remove_watch(component, inotify.fd(), watch_descriptor);
    watch_result
}

// watch_file_event_loop 处理目标文件上的 inotify 事件。
//
// 入参说明：
// - component：日志组件名
// - file_path：当前正在监听的目标文件路径
// - inotify：当前文件监听使用的托管 inotify 句柄
// - reader：与目标文件绑定的 BufReader
// - on_reset：文件被重置时的回调
// - on_line：读到新增行时的回调
fn watch_file_event_loop<FR, FL>(
    component: &'static str,
    file_path: &Path,
    inotify: &ManagedInotify,
    reader: &mut BufReader<File>,
    on_reset: &mut FR,
    on_line: &mut FL,
) -> Result<MonitorLoopResult, AppError>
where
    FR: FnMut() -> Result<(), AppError>,
    FL: FnMut(&str) -> Result<(), AppError>,
{
    let mut buffer = vec![0u8; INOTIFY_BUFFER_LEN];

    loop {
        let read_len = match read_inotify_events(inotify, &mut buffer)? {
            InotifyReadResult::Events(read_len) => read_len,
            InotifyReadResult::Closed => return Ok(MonitorLoopResult::Closed),
        };

        let mut offset = 0usize;
        while offset + size_of::<libc::inotify_event>() <= read_len {
            let event_start = offset;
            let event =
                unsafe { &*(buffer.as_ptr().add(event_start) as *const libc::inotify_event) };
            let event_len = size_of::<libc::inotify_event>() + event.len as usize;
            let event_end = event_start + event_len;
            if event_end > read_len {
                break;
            }
            offset = event_end;

            let mask = event.mask;

            if mask & libc::IN_Q_OVERFLOW != 0 {
                debug_err_log(
                    component,
                    "Inotify queue overflowed; some file events may have been lost",
                );
            }

            if mask & libc::IN_IGNORED != 0 {
                debug_log(
                    component,
                    format!(
                        "Inotify watch ignored for {}; reopening",
                        file_path.display()
                    ),
                );
                return Ok(MonitorLoopResult::Reopen);
            }

            if mask & (libc::IN_MOVE_SELF | libc::IN_DELETE_SELF) != 0 {
                debug_log(
                    component,
                    format!(
                        "Monitored file moved or deleted; waiting for recreation: {}",
                        file_path.display()
                    ),
                );
                return Ok(MonitorLoopResult::Reopen);
            }

            if mask & libc::IN_ATTRIB != 0 {
                if !file_path.exists() {
                    debug_log(
                        component,
                        format!(
                            "Monitored file attributes changed and path disappeared; waiting for recreation: {}",
                            file_path.display()
                        ),
                    );
                    return Ok(MonitorLoopResult::Reopen);
                }
                handle_file_reset_if_needed(component, reader, on_reset)?;
            }

            if mask & libc::IN_MODIFY != 0 {
                handle_file_reset_if_needed(component, reader, on_reset)?;
                drain_new_lines(reader, on_line)?;
            }
        }
    }
}

// watch_directory_events 在目标文件缺失期间监听父目录事件，等待文件重新出现。
//
// 入参说明：
// - component：日志组件名
// - file_path：目标文件完整路径
// - parent_dir：目标文件所在目录
// - file_name：目标文件名
// - inotify：当前目录监听使用的托管 inotify 句柄
fn watch_directory_events(
    component: &'static str,
    file_path: &Path,
    parent_dir: &Path,
    file_name: &OsStr,
    inotify: &ManagedInotify,
) -> Result<MonitorLoopResult, AppError> {
    let target_file_name = file_name.as_bytes();
    let mut buffer = vec![0u8; INOTIFY_BUFFER_LEN];

    loop {
        let read_len = match read_inotify_events(inotify, &mut buffer)? {
            InotifyReadResult::Events(read_len) => read_len,
            InotifyReadResult::Closed => return Ok(MonitorLoopResult::Closed),
        };

        let mut offset = 0usize;
        while offset + size_of::<libc::inotify_event>() <= read_len {
            let event_start = offset;
            let event =
                unsafe { &*(buffer.as_ptr().add(event_start) as *const libc::inotify_event) };
            let event_len = size_of::<libc::inotify_event>() + event.len as usize;
            let event_end = event_start + event_len;
            if event_end > read_len {
                break;
            }
            let event_bytes = &buffer[event_start..event_end];
            offset = event_end;

            let mask = event.mask;

            if mask & libc::IN_Q_OVERFLOW != 0 {
                debug_err_log(
                    component,
                    "Directory inotify queue overflowed while waiting for file recreation",
                );
            }

            if mask & libc::IN_IGNORED != 0 {
                if inotify.is_shutdown_requested() {
                    return Ok(MonitorLoopResult::Closed);
                }
                debug_err_log(
                    component,
                    format!(
                        "Directory watch ignored while waiting for monitored file: {}",
                        parent_dir.display()
                    ),
                );
                return Ok(MonitorLoopResult::Stop);
            }

            if mask & (libc::IN_MOVE_SELF | libc::IN_DELETE_SELF) != 0 {
                debug_err_log(
                    component,
                    format!(
                        "Monitored directory disappeared while waiting for file recreation: {}",
                        parent_dir.display()
                    ),
                );
                return Ok(MonitorLoopResult::Stop);
            }

            if mask & (libc::IN_CREATE | libc::IN_MOVED_TO) != 0
                && event_name_matches(event_bytes, target_file_name)
            {
                debug_log(
                    component,
                    format!("Monitored file reappeared: {}", file_path.display()),
                );
                return Ok(MonitorLoopResult::Reopen);
            }
        }
    }
}

// read_inotify_events 从 inotify fd 上阻塞读取一批事件。
//
// 它会自动处理 EINTR，并在 shutdown 场景下把特定 EBADF/EINVAL 转成 Closed 语义。
//
// 入参说明：
// - inotify：当前托管的 inotify 句柄
// - buffer：用于承接事件批量读取结果的缓冲区
fn read_inotify_events(
    inotify: &ManagedInotify,
    buffer: &mut [u8],
) -> Result<InotifyReadResult, AppError> {
    loop {
        let read_len = unsafe {
            libc::read(
                inotify.fd(),
                buffer.as_mut_ptr().cast::<libc::c_void>(),
                buffer.len(),
            )
        };

        if read_len < 0 {
            let err = std::io::Error::last_os_error();
            if matches!(err.raw_os_error(), Some(libc::EINTR)) {
                continue;
            }
            if inotify.is_shutdown_requested()
                && matches!(err.raw_os_error(), Some(libc::EBADF) | Some(libc::EINVAL))
            {
                return Ok(InotifyReadResult::Closed);
            }
            return Err(err.into());
        }

        if read_len == 0 {
            if inotify.is_shutdown_requested() {
                return Ok(InotifyReadResult::Closed);
            }
            continue;
        }

        return Ok(InotifyReadResult::Events(read_len as usize));
    }
}

// event_name_matches 判断某条目录事件是否正好对应我们关心的目标文件名。
//
// 入参说明：
// - event_bytes：一整条 inotify 目录事件的原始字节
// - target_file_name：目标文件名对应的字节串
fn event_name_matches(event_bytes: &[u8], target_file_name: &[u8]) -> bool {
    if event_bytes.len() <= size_of::<libc::inotify_event>() {
        return false;
    }
    let name_bytes = &event_bytes[size_of::<libc::inotify_event>()..];
    let name_end = name_bytes
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(name_bytes.len());
    &name_bytes[..name_end] == target_file_name
}

// handle_file_reset_if_needed 检测目标文件是否被 truncate，并在需要时重置 reader。
//
// 入参说明：
// - component：日志组件名
// - reader：当前目标文件的读取器
// - on_reset：文件被视作“重置”时的回调
fn handle_file_reset_if_needed<FR>(
    component: &str,
    reader: &mut BufReader<File>,
    on_reset: &mut FR,
) -> Result<(), AppError>
where
    FR: FnMut() -> Result<(), AppError>,
{
    // 文件被 truncate 后，当前 reader 的光标可能已经越过文件末尾。
    // 这时必须把 reader 重新拉回 0，并通知调用方做一次“文件重置”语义处理。
    let position = reader.stream_position()?;
    let size = reader.get_ref().metadata()?.len();
    if size < position {
        debug_log(
            component,
            format!("Monitored file truncated; resetting reader from {position} to 0"),
        );
        reader.seek(SeekFrom::Start(0))?;
        on_reset()?;
    }
    Ok(())
}

// drain_new_lines 把当前 reader 能读到的新增完整行全部消费掉。
//
// 入参说明：
// - reader：当前目标文件的读取器
// - on_line：每读到一条非空新行时的回调
fn drain_new_lines<FL>(reader: &mut BufReader<File>, on_line: &mut FL) -> Result<(), AppError>
where
    FL: FnMut(&str) -> Result<(), AppError>,
{
    // 一次 inotify modify 可能对应多行追加，因此这里始终把当前可读的新行全部 drain 掉。
    let mut line = String::new();
    while reader.read_line(&mut line)? > 0 {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            on_line(trimmed)?;
        }
        line.clear();
    }
    Ok(())
}
