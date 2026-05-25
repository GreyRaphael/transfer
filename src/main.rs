use clap::{Parser, Subcommand};
use notify::{RecursiveMode, Watcher};
use shared_memory::ShmemConf;
use std::collections::HashSet;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::time::Duration;
use walkdir::WalkDir;

// --- 常量配置 ---
// 更新了 SHM_ID 防止和上次运行的死锁内存冲突
const SHM_ID: &str = "transfer_cli_sync_v6";
const DATA_SIZE: usize = 8 * 1024 * 1024;
const STATE_SPACE_READY: u32 = 0;
const STATE_DATA_READY: u32 = 1;
const PROTO_MAGIC: [u8; 4] = *b"TRF1";

// --- 共享内存布局 ---
#[repr(C)]
struct ShmBlock {
    state: AtomicU32,
    session_id: AtomicU32,
    is_eof: AtomicBool,
    reader_aborted: AtomicBool, // 新增：用于防死锁的异常中止信号
    reader_ready: AtomicBool,   // Reader 已连接并进入主循环
    length: AtomicUsize,
    data: [u8; DATA_SIZE],
}

#[derive(Clone)]
struct ShmContext {
    ptr: *mut ShmBlock,
}
unsafe impl Send for ShmContext {}

impl ShmContext {
    // SAFETY: ptr 指向共享内存，通过原子操作保证并发安全
    #[allow(clippy::mut_from_ref)]
    fn get(&self) -> &mut ShmBlock {
        unsafe { &mut *self.ptr }
    }
}

fn collect_snapshot(root: &Path) -> io::Result<HashSet<PathBuf>> {
    let mut out = HashSet::new();

    for entry in WalkDir::new(root).min_depth(1).follow_links(false) {
        let entry = entry.map_err(io::Error::other)?;

        let rel = entry.path().strip_prefix(root).map_err(io::Error::other)?.to_path_buf();

        out.insert(rel);
    }

    Ok(out)
}

fn encode_deletions(paths: &[PathBuf]) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.extend_from_slice(&PROTO_MAGIC);
    buf.extend_from_slice(&(paths.len() as u32).to_le_bytes());

    for p in paths {
        let s = p.to_string_lossy().replace('\\', "/");
        let bytes = s.as_bytes();

        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(bytes);
    }

    buf
}

fn remove_path(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

struct ShmWriterSession {
    ctx: ShmContext,
    header: Vec<u8>,
    header_sent: bool,
}

impl ShmWriterSession {
    fn send_bytes(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let block = self.ctx.get();

        let mut spins = 0;
        // 等待 Reader 腾出空间，如果发现 Reader 已经崩溃/放弃，则直接停止发送 EOF
        while block.state.load(Ordering::Acquire) != STATE_SPACE_READY {
            if block.reader_aborted.load(Ordering::Acquire) {
                return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "Reader aborted"));
            }

            if spins < 1000 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
            spins += 1;
        }

        let write_len = bytes.len().min(DATA_SIZE);

        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), block.data.as_mut_ptr(), write_len);
        }

        block.length.store(write_len, Ordering::Release);
        block.state.store(STATE_DATA_READY, Ordering::Release);

        Ok(write_len)
    }

    fn finish(self) {
        let block = self.ctx.get();

        let mut spins = 0;
        while block.state.load(Ordering::Acquire) != STATE_SPACE_READY {
            // 防死锁核心：如果 Reader 解包出错主动退出，Writer 立刻感知并抛出 Error 打断 Tar
            if block.reader_aborted.load(Ordering::Acquire) {
                return;
            }

            if spins < 1000 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
            spins += 1;
        }

        block.is_eof.store(true, Ordering::Release);
        block.state.store(STATE_DATA_READY, Ordering::Release);
    }
}

impl Write for ShmWriterSession {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.header_sent {
            let header = self.header.clone();
            self.send_bytes(&header)?;
            self.header_sent = true;
        }

        self.send_bytes(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct ShmReaderSession {
    ctx: ShmContext,
    session_id: u32,
    current_offset: usize,
    current_len: usize,
    first_read: bool,
    eof_reached: bool, // 记录是否正常读取完毕
}

impl Read for ShmReaderSession {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let block = self.ctx.get();

        if self.current_offset == self.current_len {
            if !self.first_read {
                block.state.store(STATE_SPACE_READY, Ordering::Release);
            }

            self.first_read = false;

            let mut spins = 0;
            while block.state.load(Ordering::Acquire) != STATE_DATA_READY {
                // 如果 Writer 发生了重启开启了新局，Reader 直接抛错截断当前解压
                if block.session_id.load(Ordering::Acquire) != self.session_id {
                    return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "Writer restarted mid-stream"));
                }

                if spins < 1000 {
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                }
                spins += 1;
            }

            if block.is_eof.load(Ordering::Acquire) {
                self.eof_reached = true;
                return Ok(0);
            }

            self.current_len = block.length.load(Ordering::Acquire);
            self.current_offset = 0;
        }

        let available = self.current_len - self.current_offset;
        let read_len = buf.len().min(available);

        unsafe {
            std::ptr::copy_nonoverlapping(block.data.as_ptr().add(self.current_offset), buf.as_mut_ptr(), read_len);
        }

        self.current_offset += read_len;
        Ok(read_len)
    }
}

// Rust 魔法：如果 Tar 异常中止，对象销毁时自动通知 Writer 打断死锁
impl Drop for ShmReaderSession {
    fn drop(&mut self) {
        if !self.eof_reached {
            let block = self.ctx.get();
            block.reader_aborted.store(true, Ordering::Release);
        }
    }
}

struct SyncReaderSession {
    inner: ShmReaderSession,
}

impl SyncReaderSession {
    fn new(mut inner: ShmReaderSession) -> io::Result<Self> {
        let mut magic = [0u8; 4];
        inner.read_exact(&mut magic)?;

        if magic != PROTO_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid protocol magic"));
        }

        let mut count_bytes = [0u8; 4];
        inner.read_exact(&mut count_bytes)?;

        let delete_count = u32::from_le_bytes(count_bytes);
        let mut deletions = Vec::new();

        for _ in 0..delete_count {
            let mut len_bytes = [0u8; 4];
            inner.read_exact(&mut len_bytes)?;

            let len = u32::from_le_bytes(len_bytes) as usize;

            let mut path_bytes = vec![0u8; len];
            inner.read_exact(&mut path_bytes)?;

            let rel = String::from_utf8(path_bytes).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "utf8 error"))?;

            deletions.push(PathBuf::from(rel));
        }

        deletions.sort_by_key(|p| std::cmp::Reverse(p.components().count()));

        for rel in deletions {
            let target = Path::new(".").join(rel);

            if let Err(e) = remove_path(&target) {
                eprintln!("删除失败 {:?}: {}", target, e);
            }
        }

        Ok(Self { inner })
    }
}

impl Read for SyncReaderSession {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

#[derive(Parser)]
#[command(name = "transfer",author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    SyncW {
        #[arg(short, long)]
        input: String,
    },
    SyncR,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::SyncW { input } => {
            let path = Path::new(&input);

            let shmem = match ShmemConf::new().size(std::mem::size_of::<ShmBlock>()).os_id(SHM_ID).create() {
                Ok(shm) => shm,
                Err(shared_memory::ShmemError::MappingIdExists) => ShmemConf::new().os_id(SHM_ID).open().unwrap(),
                Err(e) => panic!("Init failed: {}", e),
            };

            let ctx = ShmContext {
                ptr: shmem.as_ptr() as *mut ShmBlock,
            };
            let block = ctx.get();
            let mut current_session = block.session_id.load(Ordering::SeqCst) + 1;

            let mut last_snapshot = collect_snapshot(path).unwrap_or_default();

            let (tx, rx) = channel();
            let mut watcher = notify::recommended_watcher(tx).unwrap();
            watcher.watch(path, RecursiveMode::Recursive).unwrap();

            println!("👀 正在监控目录: {:?}", path);

            let perform_sync = |session: u32, last_snapshot: &mut HashSet<PathBuf>| {
                if !block.reader_ready.load(Ordering::Acquire) {
                    return;
                }

                let current_snapshot = match collect_snapshot(path) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("扫描目录失败: {}", e);
                        return;
                    }
                };

                let mut deleted: Vec<PathBuf> = last_snapshot.difference(&current_snapshot).cloned().collect();

                deleted.sort_by_key(|p| std::cmp::Reverse(p.components().count()));

                let delete_header = encode_deletions(&deleted);

                block.session_id.store(session, Ordering::SeqCst);
                block.reader_aborted.store(false, Ordering::SeqCst);
                block.state.store(STATE_SPACE_READY, Ordering::SeqCst);
                block.is_eof.store(false, Ordering::SeqCst);

                let writer = ShmWriterSession {
                    ctx: ctx.clone(),
                    header: delete_header,
                    header_sent: false,
                };

                let mut builder = tar::Builder::new(writer);

                if let Err(e) = builder.append_dir_all(".", path) {
                    eprintln!("⚠️ 打包被中断: {}", e);
                }

                builder.into_inner().unwrap().finish();

                println!("✅ 同步流发送完毕 (Session {})", session);

                *last_snapshot = current_snapshot;
            };

            perform_sync(current_session, &mut last_snapshot);

            loop {
                match rx.recv() {
                    Ok(Ok(_)) => {
                        std::thread::sleep(Duration::from_millis(300));

                        while rx.try_recv().is_ok() {}

                        // 等待 Reader 就绪
                        while !block.reader_ready.load(Ordering::Acquire) {
                            std::thread::sleep(Duration::from_millis(100));
                        }

                        current_session += 1;

                        println!("📝 检测到文件修改，启动自动推送 (Session {})...", current_session);

                        perform_sync(current_session, &mut last_snapshot);
                    }
                    Ok(Err(e)) => {
                        eprintln!("Watcher 错误: {:?}", e)
                    }
                    Err(_) => break,
                }
            }
        }

        Commands::SyncR => {
            println!("⏳ 等待 Writer 建立共享内存...");
            let shmem = loop {
                if let Ok(shm) = ShmemConf::new().os_id(SHM_ID).open() {
                    break shm;
                }
                std::thread::sleep(Duration::from_millis(100));
            };

            let ctx = ShmContext {
                ptr: shmem.as_ptr() as *mut ShmBlock,
            };
            println!("🔗 已连接! 开启后台守护流同步...");
            ctx.get().reader_ready.store(true, Ordering::Release);

            let mut last_processed_session = 0;

            loop {
                let block = ctx.get();
                let current_session = block.session_id.load(Ordering::Acquire);

                if current_session > last_processed_session {
                    println!("📥 开始接收新版本 (Session {})...", current_session);

                    let raw_reader = ShmReaderSession {
                        ctx: ctx.clone(),
                        session_id: current_session,
                        current_offset: 0,
                        current_len: 0,
                        first_read: true,
                        eof_reached: false,
                    };

                    let reader = match SyncReaderSession::new(raw_reader) {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!("⚠️ 删除协议读取失败: {}", e);
                            last_processed_session = current_session;
                            continue;
                        }
                    };

                    let mut archive = tar::Archive::new(reader);
                    match archive.unpack(".") {
                        Ok(_) => println!("✨ 目录更新完毕! (Session {})", current_session),
                        Err(e) => eprintln!("⚠️ 解包发生错误: {}", e),
                    }
                    // 修复死锁：不管成功失败，绝不回退重试当前卡死的 Session
                    last_processed_session = current_session;
                } else {
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }
}
