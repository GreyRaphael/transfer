use clap::{Parser, Subcommand};
use shared_memory::{Shmem, ShmemConf};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

// --- 常量配置 ---
const SHM_ID: &str = "transfer_cli_shm_v1";
const DATA_SIZE: usize = 16 * 1024 * 1024; // 16MB 的传输块大小
const STATE_SPACE_READY: u32 = 0; // 内存块空闲，可写
const STATE_DATA_READY: u32 = 1;  // 内存块有数据，可读

// --- 共享内存布局 ---
// 通过跨进程原子变量实现自旋锁机制
#[repr(C)]
struct ShmBlock {
    state: AtomicU32,
    is_eof: AtomicBool,
    length: AtomicUsize,
    data: [u8; DATA_SIZE],
}

// ==========================================
// Writer (发送端) 的实现
// ==========================================
struct ShmWriter {
    shmem: Shmem,
}

impl ShmWriter {
    fn new() -> Self {
        // 尝试创建共享内存。如果遇到残留，则直接打开复用（自愈机制）
        let shmem = match ShmemConf::new()
            .size(std::mem::size_of::<ShmBlock>())
            .os_id(SHM_ID)
            .create()
        {
            Ok(shm) => shm,
            Err(shared_memory::ShmemError::MappingIdExists) => {
                println!("⚠️  检测到残留的共享内存 (可能是上一次异常退出导致的)。正在强行复用...");
                ShmemConf::new()
                    .os_id(SHM_ID)
                    .open()
                    .expect("Failed to open existing shared memory")
            }
            Err(e) => panic!("Failed to create shared memory: {}", e),
        };

        // 无论新建还是复用，都必须强制初始化/重置状态机
        let block = unsafe { &mut *(shmem.as_ptr() as *mut ShmBlock) };
        block.state.store(STATE_SPACE_READY, Ordering::SeqCst);
        block.is_eof.store(false, Ordering::SeqCst);
        block.length.store(0, Ordering::SeqCst);

        Self { shmem }
    }

    fn finish(self) {
        let block = unsafe { &mut *(self.shmem.as_ptr() as *mut ShmBlock) };
        wait_for_state(&block.state, STATE_SPACE_READY);
        block.is_eof.store(true, Ordering::Release);
        block.state.store(STATE_DATA_READY, Ordering::Release);
        println!("Transfer complete.");
    }
}

impl Write for ShmWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let block = unsafe { &mut *(self.shmem.as_ptr() as *mut ShmBlock) };
        
        wait_for_state(&block.state, STATE_SPACE_READY);

        // 写入数据
        let write_len = std::cmp::min(buf.len(), DATA_SIZE);
        unsafe {
            std::ptr::copy_nonoverlapping(buf.as_ptr(), block.data.as_mut_ptr(), write_len);
        }

        // 更新状态，通知 Reader
        block.length.store(write_len, Ordering::Release);
        block.state.store(STATE_DATA_READY, Ordering::Release);

        Ok(write_len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// ==========================================
// Reader (接收端) 的实现
// ==========================================
struct ShmReader {
    shmem: Shmem,
    current_offset: usize,
    current_len: usize,
    first_read: bool,
}

impl ShmReader {
    fn new() -> Self {
        println!("Waiting for writer to initialize shared memory...");
        // 轮询等待 Writer 创建共享内存
        let shmem = loop {
            if let Ok(shm) = ShmemConf::new().os_id(SHM_ID).open() {
                break shm;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        };
        println!("Connected to shared memory!");
        Self { shmem, current_offset: 0, current_len: 0, first_read: true }
    }
}

impl Read for ShmReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let block = unsafe { &mut *(self.shmem.as_ptr() as *mut ShmBlock) };

        // 如果当前块的数据已经读完，则等待新数据
        if self.current_offset == self.current_len {
            if !self.first_read {
                // 释放空间，通知 Writer 继续写
                block.state.store(STATE_SPACE_READY, Ordering::Release);
            }
            self.first_read = false;

            wait_for_state(&block.state, STATE_DATA_READY);

            if block.is_eof.load(Ordering::Acquire) {
                return Ok(0); // 接收到 EOF
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

// 辅助函数：带退避机制的自旋等待（避免占满 CPU）
fn wait_for_state(state: &AtomicU32, expected: u32) {
    let mut spins = 0;
    while state.load(Ordering::Acquire) != expected {
        if spins < 1000 {
            std::hint::spin_loop();
        } else {
            std::thread::yield_now();
        }
        spins += 1;
    }
}


// ==========================================
// 命令行界面 (CLI)
// ==========================================
#[derive(Parser)]
#[command(name = "transfer", about = "Transfer files via Shared Memory")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Writer mode
    W {
        #[arg(short, long)]
        input: String,
    },
    /// Reader mode
    R,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::W { input } => {
            let path = Path::new(&input);
            if !path.exists() {
                eprintln!("Error: Path '{}' does not exist.", input);
                return;
            }

            println!("Initializing transfer for: {:?}", path);
            let shm_writer = ShmWriter::new();
            let mut builder = tar::Builder::new(shm_writer);

            if path.is_dir() {
                // 动态打包目录并写入共享内存
                builder.append_dir_all(path.file_name().unwrap(), path).unwrap();
            } else {
                // 动态打包单文件并写入共享内存
                let mut file = std::fs::File::open(path).unwrap();
                builder.append_file(path.file_name().unwrap(), &mut file).unwrap();
            }

            // 完成 Tar 构建并获取回内部的 writer 来发送 EOF
            let shm_writer = builder.into_inner().unwrap();
            shm_writer.finish();
        }
        Commands::R => {
            let shm_reader = ShmReader::new();
            let mut archive = tar::Archive::new(shm_reader);
            
            println!("Unpacking data...");
            // 将接受到的 Tar 流解压到当前目录 "."
            if let Err(e) = archive.unpack(".") {
                eprintln!("Failed to unpack: {}", e);
            } else {
                println!("Data received and unpacked successfully!");
            }
        }
    }
}