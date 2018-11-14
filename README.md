# jthreadpool
A simple threadpool implemented in Rust with fixed number of threads , supports elegant exit. 
Here "**fixed number** of threads" means the threadpool will detect running threads, discard the one meets panic and replenish a new one.
It is guranteed that there are specified thread_num threads before invoke `shutdown()` method.

Besides, this threadpool supports elegant exit. All submited jobs are guranteed to be executed before the pool is released(drop).

## Example

### Create a thread with 2 threads, simulate Barrier

```rust
use jthreadpool::ThreadPool;
use std::sync::atomic::{AtomicUsize,Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// create a pool with 2 threads
let pool=ThreadPool::new(2);
let counter:Arc<AtomicUsize>=Arc::new(AtomicUsize::new(0));
let c1=counter.clone();
let c2=counter.clone();
let c3=counter.clone();

pool.submit(move||{
thread::sleep(Duration::from_millis(10));
c1.fetch_add(1, Ordering::SeqCst);
}).unwrap();

pool.submit(move||{
thread::sleep(Duration::from_millis(10));
c2.fetch_add(2, Ordering::SeqCst);
}).unwrap();

// thread panic
pool.submit(||{
thread::sleep(Duration::from_millis(10));
panic!();
}).unwrap();

pool.submit(move||{
thread::sleep(Duration::from_millis(10));
c3.fetch_add(3, Ordering::SeqCst);
}).unwrap();

pool.join();
assert_eq!(counter.load(Ordering::SeqCst),6);
```
