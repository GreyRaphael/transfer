use clap::{Parser, Subcommand};
use notify::{RecursiveMode, Watcher};
use shared_memory::ShmemConf;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::time::Duration;

// --- 常量配置 ---
// 更新了 SHM_ID 防止和上次运行的死锁内存冲突
const SHM_ID: &str = "transfer_cli_sync_v3"; 
const DATA_SIZE: usize = 8 * 1024 * 1024;
const STATE_SPACE_READY: u32 = 0;
const STATE_DATA_READY: u32 = 1;

// --- 共享内存布局 ---
#[repr(C)]
struct ShmBlock {
    state: AtomicU32,
    session_id: AtomicU32,
    is_eof: AtomicBool,
    reader_aborted: AtomicBool, // 新增：用于防死锁的异常中止信号
    length: AtomicUsize,
    data: [u8; DATA_SIZE],
}

#[derive(Clone)]
struct ShmContext { ptr: *mut ShmBlock }
unsafe impl Send for ShmContext {}

impl ShmContext {
    fn get(&self) -> &mut ShmBlock { unsafe { &mut *self.ptr } }
}

// ==========================================
// 连续写入会话 (Writer Session)
// ==========================================
struct ShmWriterSession {
    ctx: ShmContext,
}

impl ShmWriterSession {
    fn finish(self) {
        let block = self.ctx.get();
        let mut spins = 0;
        // 等待 Reader 腾出空间，如果发现 Reader 已经崩溃/放弃，则直接停止发送 EOF
        while block.state.load(Ordering::Acquire) != STATE_SPACE_READY {
            if block.reader_aborted.load(Ordering::Acquire) { return; }
            if spins < 1000 { std::hint::spin_loop(); } else { std::thread::yield_now(); }
            spins += 1;
        }
        block.is_eof.store(true, Ordering::Release);
        block.state.store(STATE_DATA_READY, Ordering::Release);
    }
}

impl Write for ShmWriterSession {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let block = self.ctx.get();
        
        let mut spins = 0;
        while block.state.load(Ordering::Acquire) != STATE_SPACE_READY {
            // 防死锁核心：如果 Reader 解包出错主动退出，Writer 立刻感知并抛出 Error 打断 Tar
            if block.reader_aborted.load(Ordering::Acquire) {
                return Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "Reader aborted"));
            }
            if spins < 1000 { std::hint::spin_loop(); } else { std::thread::yield_now(); }
            spins += 1;
        }

        let write_len = std::cmp::min(buf.len(), DATA_SIZE);
        unsafe {
            std::ptr::copy_nonoverlapping(buf.as_ptr(), block.data.as_mut_ptr(), write_len);
        }

        block.length.store(write_len, Ordering::Release);
        block.state.store(STATE_DATA_READY, Ordering::Release);
        Ok(write_len)
    }

    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

// ==========================================
// 连续读取会话 (Reader Session)
// ==========================================
struct ShmReaderSession {
    ctx: ShmContext,
    session_id: u32,
    current_offset: usize,
    current_len: usize,
    first_read: bool,
    eof_reached: bool, // 记录是否正常读取完毕
}

impl Read for ShmReaderSession {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
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
                    return Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "Writer restarted mid-stream"));
                }
                if spins < 1000 { std::hint::spin_loop(); } else { std::thread::yield_now(); }
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
        let read_len = std::cmp::min(buf.len(), available);
        unsafe {
            std::ptr::copy_nonoverlapping(
                block.data.as_ptr().add(self.current_offset),
                buf.as_mut_ptr(),
                read_len,
            );
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

// ==========================================
// 命令行界面 (CLI)
// ==========================================
#[derive(Parser)]
#[command(name = "transfer")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    SyncW { #[arg(short, long)] input: String },
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

            let ctx = ShmContext { ptr: shmem.as_ptr() as *mut ShmBlock };
            let block = ctx.get();
            let mut current_session = block.session_id.load(Ordering::SeqCst) + 1;
            
            let (tx, rx) = channel();
            let mut watcher = notify::recommended_watcher(tx).unwrap();
            watcher.watch(path, RecursiveMode::Recursive).unwrap();

            println!("👀 正在监控目录: {:?}", path);
            
            let perform_sync = |session: u32| {
                // 新一轮同步开始时，重置所有状态
                block.session_id.store(session, Ordering::SeqCst);
                block.reader_aborted.store(false, Ordering::SeqCst);
                block.state.store(STATE_SPACE_READY, Ordering::SeqCst);
                block.is_eof.store(false, Ordering::SeqCst);

                let writer = ShmWriterSession { ctx: ctx.clone() };
                let mut builder = tar::Builder::new(writer);
                if let Err(e) = builder.append_dir_all(".", path) {
                    eprintln!("⚠️ 打包被中断: {}", e); // 可能是文件被锁或 Reader 异常
                }
                builder.into_inner().unwrap().finish();
                println!("✅ 同步流发送完毕 (Session {})", session);
            };

            perform_sync(current_session);

            loop {
                match rx.recv() {
                    Ok(Ok(_)) => {
                        std::thread::sleep(Duration::from_millis(300));
                        while let Ok(_) = rx.try_recv() {} // 抽干防抖

                        current_session += 1;
                        println!("📝 检测到文件修改，启动自动推送 (Session {})...", current_session);
                        perform_sync(current_session);
                    }
                    Ok(Err(e)) => eprintln!("Watcher 错误: {:?}", e),
                    Err(_) => break, // 通道关闭
                }
            }
        }

        Commands::SyncR => {
            println!("⏳ 等待 Writer 建立共享内存...");
            let shmem = loop {
                if let Ok(shm) = ShmemConf::new().os_id(SHM_ID).open() { break shm; }
                std::thread::sleep(Duration::from_millis(100));
            };
            
            let ctx = ShmContext { ptr: shmem.as_ptr() as *mut ShmBlock };
            println!("🔗 已连接! 开启后台守护流同步...");

            let mut last_processed_session = 0;

            loop {
                let block = ctx.get();
                let current_session = block.session_id.load(Ordering::Acquire);
                
                if current_session > last_processed_session {
                    println!("📥 开始接收新版本 (Session {})...", current_session);
                    
                    let reader = ShmReaderSession { 
                        ctx: ctx.clone(), 
                        session_id: current_session,
                        current_offset: 0, 
                        current_len: 0, 
                        first_read: true,
                        eof_reached: false,
                    };
                    
                    let mut archive = tar::Archive::new(reader);
                    match archive.unpack(".") {
                        Ok(_) => println!("✨ 目录更新完毕! (Session {})", current_session),
                        Err(e) => eprintln!("⚠️ 解包发生错误: {}。丢弃当前版本，等待下次文件变动...", e),
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