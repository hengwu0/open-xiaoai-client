use std::ffi::CString;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::mem::size_of;
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use crate::base::{AppError, debug_err_log, debug_log};

const INOTIFY_BUFFER_LEN: usize = size_of::<libc::inotify_event>() * 4096;
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

// FileMonitorControl 同时承担两层职责：
// 1. shutdown_requested：覆盖 monitor 在线程内部切换“文件监听 / 目录监听”时的竞态窗口
// 2. shutdown_event_fd：专门负责把阻塞在 poll/read 上的线程稳定唤醒
//
// 这里不再依赖“另一个线程直接 close 当前 inotify fd”来取消阻塞 I/O。
// 在 Linux 上，这种跨线程 close 并不能可靠地打断另外一个线程里已经进入内核的 read；
// 更稳妥的做法是给 monitor 一条独立的停止唤醒通道，让它自己在同一线程里收尾并关闭 inotify。
struct FileMonitorControl {
    shutdown_requested: AtomicBool,
    shutdown_event_fd: RawFd,
}

impl FileMonitorControl {
    fn new() -> Result<Self, AppError> {
        let shutdown_event_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if shutdown_event_fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(Self {
            shutdown_requested: AtomicBool::new(false),
            shutdown_event_fd,
        })
    }

    // is_shutdown_requested 返回当前 monitor 是否已经收到退出请求。
    //
    // 入参说明：
    // - self：当前 monitor 的 shutdown 控制对象
    fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }

    // close 发起 monitor 关闭流程，并通过 shutdown eventfd 唤醒可能阻塞中的 poll/read。
    //
    // 入参说明：
    // - self：当前 monitor 的 shutdown 控制对象
    fn close(&self) {
        if self.shutdown_requested.swap(true, Ordering::SeqCst) {
            return;
        }

        let value: u64 = 1;
        loop {
            let write_result = unsafe {
                libc::write(
                    self.shutdown_event_fd,
                    (&value as *const u64).cast::<libc::c_void>(),
                    size_of::<u64>(),
                )
            };
            if write_result >= 0 {
                break;
            }

            let err = std::io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EINTR) => continue,
                Some(libc::EAGAIN) => break,
                _ => {
                    debug_err_log(
                        "file-monitor",
                        format!("Failed to signal file monitor shutdown eventfd: {err}"),
                    );
                    break;
                }
            }
        }
    }

    fn shutdown_event_fd(&self) -> RawFd {
        self.shutdown_event_fd
    }
}

impl Drop for FileMonitorControl {
    fn drop(&mut self) {
        let _ = unsafe { libc::close(self.shutdown_event_fd) };
    }
}

// ManagedInotify 负责两件事：
// 1. 创建一个新的 inotify fd，供当前 monitor 线程在某一阶段独占使用
// 2. 在当前作用域结束时，把这个 fd 关闭
struct ManagedInotify {
    fd: RawFd,
    control: Arc<FileMonitorControl>,
}

impl ManagedInotify {
    // new 创建并注册一个新的 ManagedInotify。
    //
    // 入参说明：
    // - control：当前 monitor 共享的 shutdown 控制对象
    fn new(control: Arc<FileMonitorControl>) -> Result<Option<Self>, AppError> {
        if control.is_shutdown_requested() {
            return Ok(None);
        }

        let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if control.is_shutdown_requested() {
            let _ = unsafe { libc::close(fd) };
            return Ok(None);
        }

        Ok(Some(Self { fd, control }))
    }

    // fd 返回底层 inotify 文件描述符。
    //
    // 入参说明：
    // - self：当前托管的 inotify 句柄
    fn fd(&self) -> RawFd {
        self.fd
    }

    fn shutdown_event_fd(&self) -> RawFd {
        self.control.shutdown_event_fd()
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
    // drop 在作用域结束时关闭当前 inotify fd。
    //
    // 入参说明：
    // - self：当前托管的 inotify 句柄
    fn drop(&mut self) {
        let _ = unsafe { libc::close(self.fd) };
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
    let control = Arc::new(FileMonitorControl::new().expect("create file monitor control"));
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

    debug_log(
        component,
        format!("File monitor thread exiting: {}", file_path.display()),
    );

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

    let Some(inotify) = ManagedInotify::new(control.clone())? else {
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
    let Some(inotify) = ManagedInotify::new(control.clone())? else {
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

// read_inotify_events 先等待“inotify 就绪”或“shutdown 事件到来”，然后读取一批事件。
//
// 这里使用 `poll(inotify_fd, shutdown_event_fd)` 而不是依赖“另一个线程 close 当前 inotify fd”
// 来中断 read。这样可以稳定地把阻塞中的 monitor 唤醒出来。
//
// 入参说明：
// - inotify：当前托管的 inotify 句柄
// - buffer：用于承接事件批量读取结果的缓冲区
fn read_inotify_events(
    inotify: &ManagedInotify,
    buffer: &mut [u8],
) -> Result<InotifyReadResult, AppError> {
    loop {
        if inotify.is_shutdown_requested() {
            return Ok(InotifyReadResult::Closed);
        }

        let mut poll_fds = [
            libc::pollfd {
                fd: inotify.fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: inotify.shutdown_event_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let poll_result =
            unsafe { libc::poll(poll_fds.as_mut_ptr(), poll_fds.len() as libc::nfds_t, -1) };
        if poll_result < 0 {
            let err = std::io::Error::last_os_error();
            if matches!(err.raw_os_error(), Some(libc::EINTR)) {
                continue;
            }
            return Err(err.into());
        }

        if poll_fds[1].revents & libc::POLLIN != 0 {
            return Ok(InotifyReadResult::Closed);
        }

        let file_revents = poll_fds[0].revents;
        if file_revents == 0 {
            continue;
        }
        if file_revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
            if inotify.is_shutdown_requested() {
                return Ok(InotifyReadResult::Closed);
            }
            return Err(anyhow::anyhow!(
                "inotify poll returned unexpected revents: {file_revents}"
            ));
        }
        if file_revents & libc::POLLIN == 0 {
            continue;
        }

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
            if matches!(err.raw_os_error(), Some(libc::EAGAIN)) {
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

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::path::{Path, PathBuf};
    use std::sync::mpsc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::spawn_file_monitor;

    fn unique_test_path(file_name: &str) -> PathBuf {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_millis();
        std::env::temp_dir().join(format!(
            "client-rust-file-monitor-test-{}-{millis}-{file_name}",
            std::process::id()
        ))
    }

    fn leaked_path_string(path: &Path) -> &'static str {
        Box::leak(path.display().to_string().into_boxed_str())
    }

    fn join_with_timeout(
        handle: super::FileMonitorHandle,
        timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = handle.join().join();
            let _ = done_tx.send(result);
        });

        match done_rx.recv_timeout(timeout) {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(err))) => Err(err.into()),
            Ok(Err(_)) => Err("file monitor join thread panicked".into()),
            Err(err) => Err(err.into()),
        }
    }

    #[test]
    fn close_wakes_monitor_while_waiting_on_file_inotify() {
        let file_path = unique_test_path("existing.log");
        fs::create_dir_all(file_path.parent().expect("temp path parent")).unwrap();
        File::create(&file_path).unwrap();

        let handle = spawn_file_monitor(
            "test-existing-file-monitor-thread",
            "test-file-monitor",
            leaked_path_string(&file_path),
            || Ok(()),
            |_| Ok(()),
        );

        std::thread::sleep(Duration::from_millis(100));
        handle.close();
        join_with_timeout(handle, Duration::from_secs(2)).unwrap();

        let _ = fs::remove_file(&file_path);
    }

    #[test]
    fn close_wakes_monitor_while_waiting_on_directory_inotify() {
        let dir_path = unique_test_path("missing-parent");
        fs::create_dir_all(&dir_path).unwrap();
        let file_path = dir_path.join("missing.log");

        let handle = spawn_file_monitor(
            "test-missing-file-monitor-thread",
            "test-file-monitor",
            leaked_path_string(&file_path),
            || Ok(()),
            |_| Ok(()),
        );

        std::thread::sleep(Duration::from_millis(100));
        handle.close();
        join_with_timeout(handle, Duration::from_secs(2)).unwrap();

        let _ = fs::remove_dir_all(&dir_path);
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
