extern crate itertools;
extern crate libc;
extern crate rand;

use std::cmp;
use std::io;
use std::mem;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use catfs;
use catfs::flags::DiskSpace;
use catfs::error;
use catfs::rlibc;

pub mod dir_walker;
use self::dir_walker::DirWalker;
use self::itertools::Itertools;
use self::rand::{thread_rng, Rng};

pub struct Evicter {
    dir: RawFd,
    high_watermark: DiskSpace,
    low_watermark: DiskSpace,
    scan_freq: Duration,
    statvfs: fn(RawFd) -> io::Result<libc::statvfs>,
    cv: Arc<Condvar>,
    shutting_down: Arc<Mutex<bool>>,
    t: Option<JoinHandle<()>>,
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

    fn get_file_blks(&self, p: &AsRef<Path>) -> error::Result<u64> {
        return Ok(rlibc::fstatat(self.dir, &p)?.st_blocks as u64);
    }

    fn count_cache_blks(&self) -> error::Result<u64> {
        return Ok(DirWalker::new(self.dir)?
            .map(|p| self.get_file_blks(&p))
            .fold_results(0u64, |mut t, s| {
                t += s as u64;
                t
            })?);
    }

    fn loop_once(&self) -> error::Result<()> {
        let st = (self.statvfs)(self.dir)?;

        debug!("total: {} free: {}", st.f_blocks, st.f_bfree);

        let to_evict_blks = self.should_evict(&st);
        if to_evict_blks > 0 {
            let mut cache_blks = self.count_cache_blks()?;
            if cache_blks == 0 {
                return Ok(());
            }

            let mut to_evict_blks = self.to_evict(&st) as f64;
            let mut r = thread_rng();
            let mut walker = DirWalker::new(self.dir)?;
            let mut walked_files = 0;
            let mut evicted_blks = 0;
            let mut evict_pct = to_evict_blks / (cache_blks as f64);

            debug!(
                "cache={} to_evict={} pct={}",
                cache_blks,
                to_evict_blks as u64,
                evict_pct
            );

            loop {
                if let Some(p) = walker.next() {
                    walked_files += 1;

                    // pick files to evict
                    let size = self.get_file_blks(&p)?;
                    let dice = r.next_f64() * size as f64 / to_evict_blks;
                    if dice < evict_pct {
                        debug!("evicting {:?}={} dice: {} < {}", p, size, dice, evict_pct);
                        evicted_blks += size;
                        rlibc::unlinkat(self.dir, &p, 0)?;
                    }

                    // re-examine how much free space we have every
                    // 100 files and every 10% to the goal, since
                    // other evictions maybe going on (ex: if there
                    // are 2 catfs running)
                    if walked_files % 100 == 0 || evicted_blks as f64 / to_evict_blks > 0.1 {
                        let st = (self.statvfs)(self.dir)?;
                        to_evict_blks = self.to_evict(&st) as f64;
                        if to_evict_blks <= 0f64 {
                            break;
                        }
                        if cache_blks > evicted_blks {
                            cache_blks -= evicted_blks;
                            evict_pct = to_evict_blks / (cache_blks as f64);
                            evicted_blks = 0;
                        } else {
                            // if file size changed it's possible for
                            // cache_blks < evicted_blks
                            break;
                        }
                    }
                } else if walked_files == 0 {
                    break;
                } else {
                    walker = DirWalker::new(self.dir)?;
                }
            }

            debug!(
                "<-- loop_once terminated after walking {} files",
                walked_files
            );
        }

        return Ok(());
    }

    pub fn new(dir: RawFd, free: &DiskSpace) -> Evicter {
        Evicter::new_internal(dir, free, Duration::from_secs(6), rlibc::fstatvfs)
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
                        } else {
                            debug!("not shutting down");
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

        debug!("joined");
    }
}

#[cfg(test)]
mod tests {
    extern crate env_logger;
    use std::fs;
    use catfs::rlibc;
    use super::*;

    #[test]
    fn count_cache() {
        let _ = env_logger::init();
        let prefix = catfs::tests::copy_resources();
        let fd = rlibc::open(&prefix, rlibc::O_RDONLY, 0).unwrap();

        let ev = Evicter::new(fd, &DiskSpace::Bytes(0));
        // each file takes 4K (8 blocks) minimum
        assert_eq!(ev.count_cache_blks().unwrap(), 5 * 8);
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
        let used = ev.count_cache_blks().unwrap();
        ev.loop_once().unwrap();
        assert_eq!(ev.count_cache_blks().unwrap(), used);
        fs::remove_dir_all(&prefix).unwrap();
    }

    #[test]
    fn evict_one() {
        let _ = env_logger::init();
        let prefix = catfs::tests::copy_resources();
        let fd = rlibc::open(&prefix, rlibc::O_RDONLY, 0).unwrap();

        fn fake_statvfs(dir: RawFd) -> io::Result<libc::statvfs> {
            let ev = Evicter::new(dir, &DiskSpace::Bytes(0));
            let cache_size = ev.count_cache_blks().unwrap();

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
        let used = ev.count_cache_blks().unwrap();
        ev.loop_once().unwrap();
        // evicted one file
        assert_eq!(used - ev.count_cache_blks().unwrap(), 8);
        fs::remove_dir_all(&prefix).unwrap();
    }

    #[test]
    fn evict_all() {
        let _ = env_logger::init();
        let prefix = catfs::tests::copy_resources();
        let fd = rlibc::open(&prefix, rlibc::O_RDONLY, 0).unwrap();

        fn fake_statvfs(dir: RawFd) -> io::Result<libc::statvfs> {
            let ev = Evicter::new(dir, &DiskSpace::Bytes(0));
            let cache_size = ev.count_cache_blks().unwrap();

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
        assert_eq!(ev.count_cache_blks().unwrap(), 0);
        fs::remove_dir_all(&prefix).unwrap();
    }
}
