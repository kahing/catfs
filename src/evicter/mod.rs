extern crate itertools;
extern crate libc;
extern crate rand;
extern crate twox_hash;

use std::cmp;
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

pub struct Evicter {
    dir: RawFd,
    high_watermark: DiskSpace,
    low_watermark: DiskSpace,
    scan_freq: Duration,
    hot_percent: usize, // 25 to keep most recently used 25%
    size_weight: u32,
    statvfs: fn(RawFd) -> io::Result<libc::statvfs>,
    cv: Arc<Condvar>,
    shutting_down: Arc<Mutex<bool>>,
    t: Option<JoinHandle<()>>,
}

struct EvictItem {
    hash: u64,
    atime: SystemTime,
    size: u32, // in blocks, truncates to u32_max if it's over (20GB)
}

impl EvictItem {
    fn new(dir: RawFd, path: &AsRef<Path>) -> error::Result<EvictItem> {
        let st = rlibc::fstatat(dir, path)?;
        let size: u32;
        if st.st_blocks > ::std::u32::MAX as i64 {
            size = ::std::u32::MAX;
        } else {
            size = st.st_blocks as u32;
        }

        Ok(EvictItem {
            hash: EvictItem::hash_of(path),
            size: size,
            atime: UNIX_EPOCH + Duration::new(st.st_atime as u64, st.st_atime_nsec as u32),
        })
    }

    fn new_for_lookup(path: &AsRef<Path>) -> EvictItem {
        EvictItem {
            hash: EvictItem::hash_of(path),
            size: Default::default(),
            atime: UNIX_EPOCH,
        }
    }

    fn hash_of(path: &AsRef<Path>) -> u64 {
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
fn to_evict(spec: &DiskSpace, st: &libc::statvfs) -> u64 {
    let desired_blocks = match *spec {
        DiskSpace::Percent(p) => (st.f_blocks as f64 * p / 100.0) as u64,
        DiskSpace::Bytes(b) => b / st.f_bsize,
    } as i64;

    let x = desired_blocks - st.f_bfree as i64;
    // a file usually has at least 8 blocks (4KB)
    return if x > 0 { cmp::max(x as u64, 8) } else { 0 };
}

impl Evicter {
    fn should_evict(&self, st: &libc::statvfs) -> u64 {
        return to_evict(&self.high_watermark, st);
    }

    fn to_evict(&self, st: &libc::statvfs) -> u64 {
        return to_evict(&self.low_watermark, st);
    }

    fn loop_once(&self) -> error::Result<()> {
        let st = (self.statvfs)(self.dir)?;

        debug!("total: {} free: {}", st.f_blocks, st.f_bfree);

        let to_evict_blks = self.should_evict(&st);
        if to_evict_blks > 0 {
            let to_evict_blks = self.to_evict(&st);
            let mut evicted_blks = 0;

            let mut items = DirWalker::new(self.dir)?
                .map(|x| EvictItem::new(self.dir, &x))
                .map_results(|x| Box::new(x))
                .fold_results(Box::new(Vec::new()), |mut v, x| {
                    v.push(x);
                    v
                })?;

            items.sort_by_key(|x| x.atime);

            let mut total_size = 0u64;
            for i in 0..items.len() {
                total_size += items[i].size as u64;

                if total_size >= to_evict_blks &&
                    i >= items.len() * (100 - self.hot_percent) / 100
                {
                    items.truncate(i);
                    break;
                }
            }

            // now I have items that have not been accessed recently,
            // weight them according to size
            items.sort_by_key(|x| x.size * self.size_weight + 1);

            let mut candidates_to_evict = 0u64;

            type EvictItemSet = HashSet<Box<EvictItem>, BuildHasherDefault<IdentU64Hasher>>;
            let mut item_set = EvictItemSet::default();

            for i in items.into_iter().rev() {
                candidates_to_evict += i.size as u64;
                item_set.insert(i);

                if candidates_to_evict >= to_evict_blks {
                    break;
                }
            }

            DirWalker::new(self.dir)?
                .map(|p| (Box::new(EvictItem::new_for_lookup(&p)), p))
                .filter(|i| item_set.contains(&i.0))
                .foreach(|i| {
                    evicted_blks += i.0.size;
                    if let Err(e) = rlibc::unlinkat(self.dir, &i.1, 0) {
                        debug!("wanted to evict {:?}={} but got {}", i.1, i.0.size, e);
                    } else {
                        debug!("evicting {:?}={}", i.1, i.0.size);
                    }
                });
        }

        return Ok(());
    }

    pub fn new(dir: RawFd, free: &DiskSpace) -> Evicter {
        Evicter::new_internal(dir, free, Duration::from_secs(60), rlibc::fstatvfs)
    }

    pub fn run(&mut self) {
        if self.scan_freq != Default::default() {
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
        statvfs: fn(RawFd) -> io::Result<libc::statvfs>,
    ) -> Evicter {
        let mut ev = Evicter {
            dir: dir,
            high_watermark: free.clone(),
            low_watermark: Default::default(),
            scan_freq: scan_freq,
            hot_percent: 25,
            // modeling by the google nearline operation cost:
            // $0.01/10000 requests and $0.01/GB = 0.000001/r and
            // .0000048828/blk = 1/r and 4.88/blk (round the latter to
            // 5)
            size_weight: 5,
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

    fn count_cache_blks(dir: RawFd) -> error::Result<u64> {
        /*
        let mut d = DirWalker::new(dir)?;
        let mut d = d.map(|p: PathBuf| Ok(rlibc::fstatat(dir, &p)?.st_blocks as u64));
        let d: error::Result<u64> = d.fold_results(0u64, |mut t, s| {
            t += s as u64;
            t
        });
        let d: u64 = d?;
        return Ok(d);
        */
        fn get_file_size(dir: RawFd, p: PathBuf) -> io::Result<u64> {
            Ok(rlibc::fstatat(dir, &p)?.st_blocks as u64)
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
        assert_eq!(count_cache_blks(fd).unwrap(), 5 * 8);
        fs::remove_dir_all(&prefix).unwrap();
    }

    #[test]
    fn to_evict_blks() {
        let mut st: libc::statvfs = unsafe { mem::zeroed() };
        st.f_bsize = 512;
        st.f_blocks = 100;
        st.f_bfree = 16;

        assert_eq!(to_evict(&DiskSpace::Bytes(1), &st), 0);
        assert_eq!(to_evict(&DiskSpace::Bytes(512), &st), 0);
        assert_eq!(to_evict(&DiskSpace::Bytes(17 * 512), &st), 8);
        assert_eq!(to_evict(&DiskSpace::Bytes(50 * 512), &st), 34);
        assert_eq!(to_evict(&DiskSpace::Percent(1.0), &st), 0);
        assert_eq!(to_evict(&DiskSpace::Percent(10.0), &st), 0);
        assert_eq!(to_evict(&DiskSpace::Percent(30.0), &st), 14);
    }

    #[test]
    fn evict_none() {
        let _ = env_logger::init();
        let prefix = catfs::tests::copy_resources();
        let fd = rlibc::open(&prefix, rlibc::O_RDONLY, 0).unwrap();

        fn fake_statvfs(_dir: RawFd) -> io::Result<libc::statvfs> {
            let mut st: libc::statvfs = unsafe { mem::zeroed() };
            st.f_bsize = 512;
            st.f_blocks = 10;
            st.f_bfree = 1;
            return Ok(st);
        }

        let ev = Evicter::new_internal(fd, &DiskSpace::Bytes(1), Default::default(), fake_statvfs);
        let used = count_cache_blks(fd).unwrap();
        ev.loop_once().unwrap();
        assert_eq!(count_cache_blks(fd).unwrap(), used);
        fs::remove_dir_all(&prefix).unwrap();
    }

    #[test]
    fn evict_one() {
        let _ = env_logger::init();
        let prefix = catfs::tests::copy_resources();
        let fd = rlibc::open(&prefix, rlibc::O_RDONLY, 0).unwrap();

        fn fake_statvfs(dir: RawFd) -> io::Result<libc::statvfs> {
            let cache_size = count_cache_blks(dir).unwrap();

            let mut st: libc::statvfs = unsafe { mem::zeroed() };
            st.f_bsize = 512;
            st.f_blocks = 100;
            // want 1 free block at beginning. cache_size is 5 * 8 = 40 so pretend
            // 59 blocks are used by other things
            st.f_bfree = st.f_blocks - cache_size - 59;
            return Ok(st);
        }

        let ev = Evicter::new_internal(
            fd,
            &DiskSpace::Bytes(2 * 512),
            Default::default(),
            fake_statvfs,
        );

        let st = fake_statvfs(fd).unwrap();
        assert_eq!(st.f_bfree, 1);
        assert_eq!(ev.should_evict(&st), 8);
        assert_eq!(ev.to_evict(&st), 8);
        let used = count_cache_blks(fd).unwrap();
        ev.loop_once().unwrap();
        // evicted one file
        assert_eq!(used - count_cache_blks(fd).unwrap(), 8);
        fs::remove_dir_all(&prefix).unwrap();
    }

    #[test]
    fn evict_all() {
        let _ = env_logger::init();
        let prefix = catfs::tests::copy_resources();
        let fd = rlibc::open(&prefix, rlibc::O_RDONLY, 0).unwrap();

        fn fake_statvfs(dir: RawFd) -> io::Result<libc::statvfs> {
            let cache_size = count_cache_blks(dir).unwrap();

            let mut st: libc::statvfs = unsafe { mem::zeroed() };
            st.f_bsize = 512;
            st.f_blocks = 100;
            // want 1 free block at beginning. cache_size is 5 * 8 = 40 so pretend
            // 59 blocks are used by other things
            st.f_bfree = st.f_blocks - cache_size - 59;
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
        assert_eq!(ev.should_evict(&st), 99);
        assert_eq!(ev.to_evict(&st), 99);
        ev.loop_once().unwrap();
        // evicted one file
        assert_eq!(count_cache_blks(fd).unwrap(), 0);
        fs::remove_dir_all(&prefix).unwrap();
    }
}
