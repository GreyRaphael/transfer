use clap::{Parser, Subcommand};
use notify::{RecursiveMode, Watcher};
use shared_memory::ShmemConf;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::time::Duration;

// --- 常量配置 ---
const SHM_ID: &str = "transfer_cli_sync_v2"; // 换个 ID，避免和之前的冲突
const DATA_SIZE: usize = 8 * 1024 * 1024;
const STATE_SPACE_READY: u32 = 0;
const STATE_DATA_READY: u32 = 1;

// --- 共享内存布局 ---
#[repr(C)]
struct ShmBlock {
    state: AtomicU32,
    session_id: AtomicU32, // 新增：版本号/会话ID。用于连续同步
    is_eof: AtomicBool,
    length: AtomicUsize,
    data: [u8; DATA_SIZE],
}

// 提取一个上下文包装器，方便生成 Reader/Writer 实例
#[derive(Clone)]
struct ShmContext {
    ptr: *mut ShmBlock,
}
unsafe impl Send for ShmContext {}

impl ShmContext {
    fn get(&self) -> &mut ShmBlock {
        unsafe { &mut *self.ptr }
    }
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
        wait_for_state(&block.state, STATE_SPACE_READY, None);
        block.is_eof.store(true, Ordering::Release);
        block.state.store(STATE_DATA_READY, Ordering::Release);
    }
}

impl Write for ShmWriterSession {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let block = self.ctx.get();
        wait_for_state(&block.state, STATE_SPACE_READY, None);

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
}

impl Read for ShmReaderSession {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let block = self.ctx.get();

        if self.current_offset == self.current_len {
            if !self.first_read {
                block.state.store(STATE_SPACE_READY, Ordering::Release);
            }
            self.first_read = false;

            // 监听数据，同时监控 session_id
            // 如果在读取中途，Writer被杀掉并重启(session变大)，直接报错打断当前 Tar 解压
            if !wait_for_state(&block.state, STATE_DATA_READY, Some(self.session_id)) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "Writer restarted mid-stream",
                ));
            }

            if block.is_eof.load(Ordering::Acquire) {
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

// 辅助函数：带中断机制的自旋。返回 false 表示发现 session 更新（发生了中断）
fn wait_for_state(state: &AtomicU32, expected: u32, check_session: Option<u32>) -> bool {
    let mut spins = 0;
    while state.load(Ordering::Acquire) != expected {
        if let Some(expected_session) = check_session {
            // 这是一段极其精妙的逻辑：防止 Writer 崩溃重启时 Reader 卡死
            // 如果在等待期间，发现会话 ID 变了，说明 Writer 重启开启了新的一轮
            let block_ptr = (state as *const AtomicU32 as usize - std::mem::offset_of!(ShmBlock, state)) as *mut ShmBlock;
            let current_session = unsafe { (*block_ptr).session_id.load(Ordering::Acquire) };
            if current_session != expected_session {
                return false; 
            }
        }

        if spins < 1000 {
            std::hint::spin_loop();
        } else {
            std::thread::yield_now();
        }
        spins += 1;
    }
    true
}

// ==========================================
// 命令行界面 (CLI)
// ==========================================
#[derive(Parser)]
#[command(name = "transfer", about = "Continuous Directory Sync via Shared Memory")]
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
            if !path.is_dir() {
                eprintln!("Error: Path '{}' is not a directory.", input);
                return;
            }

            // 初始化/恢复共享内存
            let shmem = match ShmemConf::new().size(std::mem::size_of::<ShmBlock>()).os_id(SHM_ID).create() {
                Ok(shm) => shm,
                Err(shared_memory::ShmemError::MappingIdExists) => {
                    println!("⚠️ 复用已存在的共享内存通道...");
                    ShmemConf::new().os_id(SHM_ID).open().unwrap()
                }
                Err(e) => panic!("Init failed: {}", e),
            };

            let ctx = ShmContext { ptr: shmem.as_ptr() as *mut ShmBlock };
            let block = ctx.get();
            
            // Writer 每次启动，获取旧的 session_id 并 +1，通知 Reader 这是全新的一局
            let mut current_session = block.session_id.load(Ordering::SeqCst) + 1;
            
            // 初始化/清理内存状态
            block.session_id.store(current_session, Ordering::SeqCst);
            block.state.store(STATE_SPACE_READY, Ordering::SeqCst);
            block.is_eof.store(false, Ordering::SeqCst);

            // 设置监听器
            let (tx, rx) = channel();
            let mut watcher = notify::recommended_watcher(tx).unwrap();
            watcher.watch(path, RecursiveMode::Recursive).unwrap();

            println!("👀 正在监控目录: {:?}", path);
            println!("🔄 执行首次全量同步...");
            
            // 定义一个执行打包并发送的闭包
            let perform_sync = |session: u32| {
                block.session_id.store(session, Ordering::SeqCst);
                block.state.store(STATE_SPACE_READY, Ordering::SeqCst);
                block.is_eof.store(false, Ordering::SeqCst);

                let writer = ShmWriterSession { ctx: ctx.clone() };
                let mut builder = tar::Builder::new(writer);
                if let Err(e) = builder.append_dir_all(".", path) {
                    eprintln!("打包错误: {}", e);
                }
                builder.into_inner().unwrap().finish();
                println!("✅ 同步完成 (Session {})", session);
            };

            perform_sync(current_session);

            // 主循环：处理文件系统事件
            loop {
                match rx.recv() {
                    Ok(Ok(_)) => {
                        // 【防抖机制 Debounce】: 
                        // 发生改变后，等待 300ms，并把这期间积压的其他事件全部吞掉
                        // 防止保存一个文件触发 5 次同步
                        std::thread::sleep(Duration::from_millis(300));
                        while let Ok(_) = rx.try_recv() {} 

                        current_session += 1;
                        println!("📝 检测到文件修改，启动同步 (Session {})...", current_session);
                        perform_sync(current_session);
                    }
                    _ => {}
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
            
            let ctx = ShmContext { ptr: shmem.as_ptr() as *mut ShmBlock };
            println!("🔗 已连接! 等待接收同步流...");

            let mut last_processed_session = 0;

            loop {
                let block = ctx.get();
                let current_session = block.session_id.load(Ordering::Acquire);
                
                // 轮询等待新的 session 开启
                if current_session > last_processed_session {
                    println!("📥 开始接收新版本 (Session {})...", current_session);
                    
                    let reader = ShmReaderSession { 
                        ctx: ctx.clone(), 
                        session_id: current_session,
                        current_offset: 0, 
                        current_len: 0, 
                        first_read: true 
                    };
                    
                    let mut archive = tar::Archive::new(reader);
                    
                    match archive.unpack(".") {
                        Ok(_) => {
                            println!("✨ 目录更新完毕! (Session {})", current_session);
                            last_processed_session = current_session;
                        }
                        Err(e) => {
                            // 如果是中途 Writer 退出导致的异常，会走到这里并重试
                            eprintln!("⚠️ 解包被中断或发生错误: {}。等待下一次同步...", e);
                            last_processed_session = current_session - 1; // 标记失败，强制重试
                        }
                    }
                } else {
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }
}