extern crate itertools;
extern crate libc;
extern crate rand;
extern crate twox_hash;

use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::io;
use std::mem;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use catfs;
use catfs::flags::DiskSpace;
use catfs::error;
use catfs::rlibc;

pub mod dir_walker;
use self::dir_walker::DirWalker;
use self::itertools::Itertools;
use self::twox_hash::XxHash;

#[cfg(not(target_os = "macos"))]
use self::libc::statvfs64;
#[cfg(target_os = "macos")]
use self::libc::{statvfs as statvfs64};

pub struct Evicter {
    dir: RawFd,
    high_watermark: DiskSpace,
    low_watermark: DiskSpace,
    scan_freq: Duration,
    hot_percent: usize, // 25 to keep most recently used 25%
    request_weight: u32,
    statvfs: fn(RawFd) -> io::Result<statvfs64>,
    cv: Arc<Condvar>,
    shutting_down: Arc<Mutex<bool>>,
    t: Option<JoinHandle<()>>,
}

struct EvictItem {
    hash: u64,
    atime: SystemTime,
    size: usize,
}

impl EvictItem {
    fn new(dir: RawFd, path: &dyn AsRef<Path>) -> error::Result<EvictItem> {
        let st = rlibc::fstatat(dir, path)?;

        Ok(EvictItem {
            hash: EvictItem::hash_of(path),
            size: (st.st_blocks * 512) as usize,
            atime: UNIX_EPOCH + Duration::new(st.st_atime as u64, st.st_atime_nsec as u32),
        })
    }

    fn new_for_lookup(path: &dyn AsRef<Path>) -> EvictItem {
        EvictItem {
            hash: EvictItem::hash_of(path),
            size: Default::default(),
            atime: UNIX_EPOCH,
        }
    }

    fn hash_of(path: &dyn AsRef<Path>) -> u64 {
        let mut h = XxHash::with_seed(0);
        path.as_ref().hash(&mut h);
        h.finish()
    }
}

impl Hash for EvictItem {
    fn hash<H: Hasher>(&self, h: &mut H) {
        h.write_u64(self.hash);
    }
}

impl PartialEq for EvictItem {
    fn eq(&self, other: &EvictItem) -> bool {
        return self.hash == other.hash;
    }
}

impl Eq for EvictItem {}

#[derive(Default)]
struct IdentU64Hasher(u64);

impl Hasher for IdentU64Hasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _b: &[u8]) {
        panic!("use write_u64 instead");
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
}

// in blocks
fn to_evict(spec: &DiskSpace, st: &statvfs64) -> u64 {
    let desired = match *spec {
        DiskSpace::Percent(p) => ((st.f_blocks as u64 * st.f_frsize as u64) as f64 * p / 100.0) as u64,
        DiskSpace::Bytes(b) => b,
    } as i64;

    let x = desired - (st.f_bfree as u64 * st.f_frsize as u64) as i64;
    return if x > 0 { x as u64 } else { 0 };
}

impl Evicter {
    fn should_evict(&self, st: &statvfs64) -> u64 {
        return to_evict(&self.high_watermark, st);
    }

    fn to_evict(&self, st: &statvfs64) -> u64 {
        return to_evict(&self.low_watermark, st);
    }

    pub fn loop_once(&self) -> error::Result<()> {
        let st = (self.statvfs)(self.dir)?;

        let to_evict_bytes = self.should_evict(&st);
        debug!(
            "total: {} free: {} to_evict: {}",
            st.f_blocks,
            st.f_bfree,
            to_evict_bytes
        );

        if to_evict_bytes > 0 {
            let to_evict_bytes = self.to_evict(&st);
            let mut evicted_bytes = 0;

            let mut items = DirWalker::new(self.dir)?
                .map(|x| EvictItem::new(self.dir, &x))
                .map_results(Box::new)
                .fold_results(Box::new(Vec::new()), |mut v, x| {
                    v.push(x);
                    v
                })?;

            if items.is_empty() {
                return Ok(());
            }

            items.sort_by_key(|x| x.atime);

            let mut total_size = 0u64;
            for i in 0..items.len() {
                total_size += items[i].size as u64;

                if total_size >= to_evict_bytes &&
                    i >= items.len() * (100 - self.hot_percent) / 100
                {
                    items.truncate(i + 1);
                    break;
                }
            }

            let now = SystemTime::now();
            let oldest = now.duration_since(items[0].atime).unwrap().as_secs();

            // now I have items that have not been accessed recently,
            // weight them according to size and age
            items.sort_by_key(|x| {
                let cost = x.size as u64 + self.request_weight as u64;
                let age = now.duration_since(x.atime).unwrap().as_secs();
                if oldest == 0 {
                    cost
                } else {
                    cost * age / oldest
                }
            });

            let mut candidates_to_evict = 0u64;

            type EvictItemSet = HashSet<Box<EvictItem>, BuildHasherDefault<IdentU64Hasher>>;
            let mut item_set = EvictItemSet::default();

            for i in items.into_iter().rev() {
                candidates_to_evict += i.size as u64;
                item_set.insert(i);

                if candidates_to_evict >= to_evict_bytes {
                    break;
                }
            }

            DirWalker::new(self.dir)?
                .map(|p| (Box::new(EvictItem::new_for_lookup(&p)), p))
                .foreach(|i| if let Some(item) = item_set.get(&i.0) {
                    evicted_bytes += item.size;
                    if let Err(e) = rlibc::unlinkat(self.dir, &i.1, 0) {
                        debug!("wanted to evict {:?}={} but got {}", i.1, item.size, e);
                    } else {
                        debug!("evicting {:?}={}", i.1, item.size);
                    }
                });
        }

        return Ok(());
    }

    pub fn new(dir: RawFd, free: &DiskSpace) -> Evicter {
        Evicter::new_internal(dir, free, Duration::from_secs(60), rlibc::fstatvfs)
    }

    pub fn run(&mut self) {
        if self.scan_freq != Default::default() && self.high_watermark != Default::default() {
            let evicter = catfs::make_self(self);
            let builder = thread::Builder::new().name(String::from("evicter"));

            self.t = Some(
                builder
                    .spawn(move || loop {
                        if let Err(e) = evicter.loop_once() {
                            error!("evicter error: {}", e);
                        }

                        let guard = evicter.shutting_down.lock().unwrap();
                        let res = evicter.cv.wait_timeout(guard, evicter.scan_freq).unwrap();
                        if *res.0 {
                            debug!("shutting down");
                            break;
                        }
                    })
                    .unwrap(),
            );
        }
    }

    fn new_internal(
        dir: RawFd,
        free: &DiskSpace,
        scan_freq: Duration,
        statvfs: fn(RawFd) -> io::Result<statvfs64>,
    ) -> Evicter {
        let mut ev = Evicter {
            dir: dir,
            high_watermark: free.clone(),
            low_watermark: Default::default(),
            scan_freq: scan_freq,
            hot_percent: 25,
            // modeling by the google nearline operation cost:
            // $0.01/10000 requests and $0.01/GB = 0.000001/r and
            // $.00000000000931322574/byte = 107374/r and 1/byte
            request_weight: 107374,
            statvfs: statvfs,
            cv: Arc::new(Condvar::new()),
            shutting_down: Arc::new(Mutex::new(false)),
            t: Default::default(),
        };

        if ev.high_watermark != DiskSpace::Bytes(0) {
            if ev.low_watermark == DiskSpace::Bytes(0) {
                ev.low_watermark = match ev.high_watermark {
                    DiskSpace::Percent(p) => DiskSpace::Percent((p * 1.1).min(100.0)),
                    DiskSpace::Bytes(b) => DiskSpace::Bytes((b as f64 * 1.1) as u64),
                };
            }

        }

        return ev;
    }
}

impl Drop for Evicter {
    fn drop(&mut self) {
        {
            let mut b = self.shutting_down.lock().unwrap();
            *b = true;
            debug!("requesting to shutdown");
            self.cv.notify_one();
        }

        let mut t: Option<JoinHandle<()>> = None;

        mem::swap(&mut self.t, &mut t);

        if let Some(t) = t {
            t.join().expect("evictor panic");
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate env_logger;
    use std::fs;
    use std::path::PathBuf;
    use catfs::rlibc;
    use super::*;
    use self::dir_walker::DirWalker;

    fn count_cache_size(dir: RawFd) -> error::Result<u64> {
        fn get_file_size(dir: RawFd, p: PathBuf) -> io::Result<u64> {
            Ok(512 * rlibc::fstatat(dir, &p)?.st_blocks as u64)
        }

        return Ok(DirWalker::new(dir)?
            .map(|p| get_file_size(dir, p))
            .fold_results(0u64, |mut t, s| {
                t += s as u64;
                t
            })?);
    }

    #[test]
    fn count_cache() {
        let _ = env_logger::init();
        let prefix = catfs::tests::copy_resources();
        let fd = rlibc::open(&prefix, rlibc::O_RDONLY, 0).unwrap();

        // each file takes 4K (8 blocks) minimum
        assert_eq!(count_cache_size(fd).unwrap(), 5 * 4096);
        fs::remove_dir_all(&prefix).unwrap();
    }

    #[test]
    fn to_evict_bytes() {
        let mut st: statvfs64 = unsafe { mem::zeroed() };
        st.f_bsize = 4096;
        st.f_frsize = 4096;
        st.f_blocks = 100;
        st.f_bfree = 16;

        assert_eq!(to_evict(&DiskSpace::Bytes(1), &st), 0);
        assert_eq!(to_evict(&DiskSpace::Bytes(512), &st), 0);
        assert_eq!(to_evict(&DiskSpace::Bytes(17 * 4096), &st), 4096);
        assert_eq!(
            to_evict(&DiskSpace::Bytes(50 * 4096), &st),
            (50 - 16) * 4096
        );
        assert_eq!(to_evict(&DiskSpace::Percent(1.0), &st), 0);
        assert_eq!(to_evict(&DiskSpace::Percent(10.0), &st), 0);
        assert_eq!(to_evict(&DiskSpace::Percent(30.0), &st), (30 - 16) * 4096);
    }

    #[test]
    fn evict_none() {
        let _ = env_logger::init();
        let prefix = catfs::tests::copy_resources();
        let fd = rlibc::open(&prefix, rlibc::O_RDONLY, 0).unwrap();

        fn fake_statvfs(_dir: RawFd) -> io::Result<statvfs64> {
            let mut st: statvfs64 = unsafe { mem::zeroed() };
            st.f_bsize = 4096;
            st.f_frsize = 4096;
            st.f_blocks = 10;
            st.f_bfree = 1;
            return Ok(st);
        }

        let ev = Evicter::new_internal(fd, &DiskSpace::Bytes(1), Default::default(), fake_statvfs);
        let used = count_cache_size(fd).unwrap();
        ev.loop_once().unwrap();
        assert_eq!(count_cache_size(fd).unwrap(), used);
        fs::remove_dir_all(&prefix).unwrap();
    }

    #[test]
    fn evict_one() {
        let _ = env_logger::init();
        let prefix = catfs::tests::copy_resources();
        let fd = rlibc::open(&prefix, rlibc::O_RDONLY, 0).unwrap();

        fn fake_statvfs(dir: RawFd) -> io::Result<statvfs64> {
            let cache_size = count_cache_size(dir).unwrap();

            let mut st: statvfs64 = unsafe { mem::zeroed() };
            st.f_bsize = 4096;
            st.f_frsize = 4096;
            st.f_blocks = 100;
            // want 1 free block at beginning. cache_size is 5 * 4K blocks so pretend
            // 94 blocks are used by other things
            st.f_bfree = (st.f_blocks as u64 - cache_size / (st.f_frsize as u64) - 94);
            return Ok(st);
        }

        let ev = Evicter::new_internal(
            fd,
            &DiskSpace::Bytes(4096 + 2048),
            Default::default(),
            fake_statvfs,
        );

        let st = fake_statvfs(fd).unwrap();
        assert_eq!(st.f_bfree, 1);
        assert_eq!(ev.should_evict(&st), 2048);
        let used = count_cache_size(fd).unwrap();
        ev.loop_once().unwrap();
        // evicted one file
        assert_eq!(used - count_cache_size(fd).unwrap(), 4096);
        fs::remove_dir_all(&prefix).unwrap();
    }

    #[test]
    fn evict_all() {
        let _ = env_logger::init();
        let prefix = catfs::tests::copy_resources();
        let fd = rlibc::open(&prefix, rlibc::O_RDONLY, 0).unwrap();

        fn fake_statvfs(dir: RawFd) -> io::Result<statvfs64> {
            let cache_size = count_cache_size(dir).unwrap();

            let mut st: statvfs64 = unsafe { mem::zeroed() };
            st.f_bsize = 4096;
            st.f_frsize = 4096;
            st.f_blocks = 100;
            // want 1 free block at beginning. cache_size is 5 * 4K blocks so pretend
            // 94 blocks are used by other things
            st.f_bfree = (st.f_blocks as u64 - cache_size / (st.f_frsize as u64) - 94);
            return Ok(st);
        }


        let ev = Evicter::new_internal(
            fd,
            &DiskSpace::Percent(100.0),
            Default::default(),
            fake_statvfs,
        );

        let st = fake_statvfs(fd).unwrap();
        assert_eq!(st.f_bfree, 1);
        assert_eq!(ev.low_watermark, DiskSpace::Percent(100.0));
        assert_eq!(ev.should_evict(&st), 99 * 4096);
        ev.loop_once().unwrap();
        // evicted one file
        assert_eq!(count_cache_size(fd).unwrap(), 0);
        fs::remove_dir_all(&prefix).unwrap();
    }
}
