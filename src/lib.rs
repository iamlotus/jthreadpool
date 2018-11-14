//! A simple thread pool with fix number of threads. Support
//! * auto replenishment of thread when panic happens
//! * elegant exit

use std::thread;
use std::sync::mpsc;
use std::sync::mpsc::{Sender,Receiver};
use std::sync::{Arc,Mutex,Condvar,Weak};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::fmt;
use std::collections::HashMap;
use std::thread::ThreadId;

// enable log by specify
macro_rules! log {
    ()=>({
        #[cfg(feature="enable_log")]
        eprint!("\n")
    });


   ($($arg:tt)*) => ({
        #[cfg(feature="enable_log")]
        eprint!("{}\n", format_args!($($arg)*))
    });
}


// to borrow closure
trait FnBox{
    fn call_box(self:Box<Self>);
}

impl <F:FnOnce()> FnBox for F{
    fn call_box(self:Box<F>){
        (*self)()
    }
}

type Job=Box<FnBox+Send+'static>;

enum Message{
    NewJob(Job),
    Terminate
}

#[derive(Debug,Copy,Clone,PartialEq)]
enum WorkerStatus{
    // no data has not been attached, no thread
    NotReady=0,
    // data has been attached, thread loop starts up
    Init=1,
    // idle
    Idle=2,
    // do job
    InProcess=3,
    // receive terminate message, exit loop, will be discarded from ThreadPool
    Terminated=4
}

impl From<usize> for WorkerStatus{
    fn from (val:usize)->WorkerStatus{
        use WorkerStatus::*;
        match val {
            0=>NotReady,
            1=>Init,
            2=>Idle,
            3=>InProcess,
            4=>Terminated,
            _=>unreachable!()
        }
    }
}

#[derive(Clone)]
struct AtomicWorkerStatus(Arc<AtomicUsize>);

impl AtomicWorkerStatus{
    fn new(status:WorkerStatus)->AtomicWorkerStatus{
        AtomicWorkerStatus(Arc::new(AtomicUsize::new(status as usize)))
    }

    fn set_state(&self,new_status:WorkerStatus){
        self.0.store(new_status as usize,Ordering::SeqCst)
    }

    fn get_state(&self)->WorkerStatus{
        self.0.load(Ordering::SeqCst).into()
    }
}

struct Worker{
    thread:Option<thread::JoinHandle<()>>,
    status: Arc<AtomicWorkerStatus>,
    data:Weak<ThreadPoolSharedData>
}

impl fmt::Display for Worker{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result{
        fmt::Debug::fmt(self,f)
    }
}

impl fmt::Debug for Worker{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result{
        write!(f,"Worker[{},{:?},{:?}]",self.name().unwrap_or_default(),self.id(),self.status())
    }
}

impl Worker{
    // Init worker has not data
    fn new(data:Weak<ThreadPoolSharedData>)->Worker{
        let status=Arc::new(AtomicWorkerStatus::new(WorkerStatus::Init));
        let status_clone=status.clone();
        let mut worker=Worker{
            thread:None,
            status,
            data: data.clone()
        };
        if let Some(data)=worker.data.upgrade() {
            let mut builder=thread::Builder::new();

            if let Some(ref prefix)=data.name_prefix{
                  let id=data.worker_counter.fetch_add(1, Ordering::SeqCst);
                  let name=format!("{}-{}",prefix.to_string(),id);
                  builder=builder.name(name);
            };

            if let Some(stack_size)=data.stack_size {
                builder=builder.stack_size(stack_size.to_owned());
            };

            let thread = builder.spawn(move || {
                let mut sentinel = Sentinel::new(data.clone());
                loop {
                    let message = data.receiver.lock().unwrap().recv().unwrap();
                    match message {
                        Message::NewJob(job) => {
                            status_clone.set_state(WorkerStatus::InProcess);
                            data.executing_count.fetch_add(1, Ordering::SeqCst);
                            data.queued_count.fetch_sub(1, Ordering::SeqCst);
                            log!("[Worker {:?}] start new job",thread::current().id());
                            job.call_box();
                            log!("[Worker {:?}] finish new job",thread::current().id());
                            data.success_count.fetch_add(1, Ordering::SeqCst);
                            data.executing_count.fetch_sub(1, Ordering::SeqCst);
                            status_clone.set_state(WorkerStatus::Idle);
                            data.notify_all_empty();
                        },
                        Message::Terminate => {
                            log!("[Worker {:?}] start terminate",thread::current().id());
                            status_clone.set_state(WorkerStatus::Terminated);
                            data.executing_count.fetch_add(1, Ordering::SeqCst);
                            data.queued_count.fetch_sub(1, Ordering::SeqCst);
                            let &(ref lock, ref shutdown_cvar)=&data.shutdown_message_consumed;
                            let mut consumed=lock.lock().unwrap();
                            *consumed+=1;
                            shutdown_cvar.notify_all();
                            log!("[Worker {:?}] finish terminate",thread::current().id());
                            data.executing_count.fetch_sub(1, Ordering::SeqCst);
                            data.notify_all_empty();
                            break;
                        }
                    }

                }
                sentinel.cancel();
            }).unwrap();
            worker.thread=Some(thread);
        }
        worker
    }

    pub fn status(&self)->WorkerStatus{
        self.status.get_state()
    }

    pub fn id(&self)->Option<ThreadId>{
        match self.thread {
            Some(ref join_handler)=>Some(join_handler.thread().id()),
            None=>None
        }
    }

    pub fn name(&self)->Option<String>{
        match self.thread {
            Some(ref join_handler)=>
                match join_handler.thread().name() {
                    Some (s)=>Some(s.to_string()),
                    None=>None
                },
            None=>None
        }
    }
}

#[derive(Debug)]
pub struct ThreadPoolError {
    err: &'static str
}

impl ThreadPoolError{
    pub fn new(err:&'static str)-> ThreadPoolError {
        ThreadPoolError {
            err
        }
    }
}

struct Sentinel {
    cancel:bool,
    data:Arc<ThreadPoolSharedData>
}

impl Sentinel {
    fn new(data:Arc<ThreadPoolSharedData>)-> Sentinel {
        Sentinel {
            data,
            cancel:false
        }
    }

    fn cancel(&mut self){
        self.cancel=true
    }
}

impl Drop for Sentinel{
    fn drop(&mut self){
        let id=thread::current().id();
        if !self.cancel {
            log!("[Worker {:?}] panic",thread::current().id());

            // remove panic thread
            let mut workers = self.data.workers.lock().unwrap();
            workers.remove(&id).unwrap();
            log!("[Worker {:?}] drop itself",thread::current().id());

            // add new worker
            let new_worker = Worker::new(Arc::downgrade(&self.data));
            workers.insert(new_worker.id().unwrap(), new_worker);
            log!("[Worker {:?}] create replacement for itself",thread::current().id());

            // reset counter
            self.data.panic_count.fetch_add(1, Ordering::SeqCst);
            self.data.executing_count.fetch_sub(1, Ordering::SeqCst);
            self.data.notify_all_empty();
        }
    }
}

// Some fields in a thread pool that can be accessed in thread-safety way.
struct ThreadPoolSharedData{
    name_prefix: Option<String>,
    stack_size:Option<usize>,
    receiver:Mutex<Receiver<Message>>,
    is_shutdown:AtomicBool,
    success_count:AtomicUsize,
    panic_count:AtomicUsize,
    executing_count:AtomicUsize,
    queued_count:AtomicUsize,
    worker_counter:AtomicUsize,
    workers:Mutex<HashMap<ThreadId,Worker>>,
    empty:(Mutex<()>,Condvar),
    shutdown_message_consumed:(Mutex<usize>,Condvar),
}

impl ThreadPoolSharedData{
    fn is_empty(&self)->bool{
        self.queued_count.load(Ordering::SeqCst)==0 && self.executing_count.load(Ordering::SeqCst)==0
    }

    fn notify_all_empty(&self){
        if self.is_empty(){
            let &(ref empty_lock, ref empty_cvar)=&self.empty;
            let _lock=empty_lock.lock().expect("unable to notify all empty threads");
            empty_cvar.notify_all();
        }
    }
}

/// [`ThreadPool`] Factory, used to config properties of [`ThreadPool`]
/// Some configuration option by now:
/// * `name_prefix`: name_prefix of thread in the built [`ThreadPool`]. By specified `name_prefix`, name of thread
/// will be `$name_prefix_0`,`$name_prefix_1`, etc.
///
/// * `num_threads`: number of thread in the built [`ThreadPool`], noted if a thread panics, a replenishment will
/// be generated, it is guaranteed there are `num_threads` threads in the  `ThreadPool`
///
/// * stack_size: size of stack of generated thread (in byte).
///
/// # Examples
/// Generate a threadpool with fixed 4 threads and 8M stack. Thread name like "daemon-0","daemon-1",etc:
///
/// ```
/// let pool=jthreadpool::Builder::new()
///         .num_threads(4)
///         .name_prefix(String::from("daemon"))
///         .stack_size(8_000_000)
///         .build();
/// ```
///
/// [`ThreadPool`]: ThreadPool.html

pub struct Builder {
    name_prefix: Option<String>,
    num_threads:Option<usize>,
    stack_size:Option<usize>,
}

impl Builder {
    pub fn new()-> Builder {
        Builder {
            name_prefix:None,
            num_threads:None,
            stack_size:None
        }
    }

    pub fn name_prefix(mut self,name_prefix:String)-> Builder {
        self.name_prefix=Some(name_prefix);
        self
    }

    pub fn num_threads(mut self,num_threads:usize)-> Builder {
        assert!(num_threads>0,"num_threads must be greater than 0");
        self.num_threads=Some(num_threads);
        self
    }

    #[allow(dead_code)]
    pub fn stack_size(mut self, stack_size:usize) -> Builder {
        self.stack_size =Some(stack_size);
        self
    }

    pub fn build(self) ->ThreadPool{
        let num_threads=match self.num_threads {
            Some(size)=>size,
            None=> panic!("num_threads must be set")
        };

        let (sender,receiver)=mpsc::channel();
        let receiver=Mutex::new(receiver);
        let workers= HashMap::with_capacity(num_threads);
        let workers=Mutex::new(workers );

        let shared_data=Arc::new(ThreadPoolSharedData{
            name_prefix:self.name_prefix,
            stack_size: self.stack_size,
            receiver,
            is_shutdown: AtomicBool::new(false),
            success_count: AtomicUsize::new(0),
            panic_count: AtomicUsize::new(0),
            queued_count: AtomicUsize::new(0),
            executing_count: AtomicUsize::new(0),
            worker_counter:AtomicUsize::new(0),
            workers,
            empty:(Mutex::new(()),Condvar::new()),
            shutdown_message_consumed:(Mutex::new(0),Condvar::new()),
        });

        let shared_data_clone=shared_data.clone();
        for _ in 0..num_threads {
            let worker = Worker::new( Arc::downgrade(&shared_data_clone));
            let s=shared_data_clone.clone();
            s.workers.lock().unwrap().insert(worker.id().unwrap(), worker);
        }

        log!("[Pool] constructed in {:?}",thread::current().id());
        ThreadPool{
            sender,
            shared_data:shared_data.clone()
        }
    }


}

pub struct ThreadPool{
    sender:Sender<Message>,
    shared_data:Arc<ThreadPoolSharedData>
}

impl ThreadPool {
    /// Create a new Thread Pool, to config more properties of `ThreadPool`, use [`Builder`]
    /// #argument
    ///
    /// num_threads - the number of threads in the pool
    ///
    /// #Panics
    ///
    /// The `new` function will panic if size is 0
    /// # Examples
    /// Create a thread with 2 threads, simulate Barrier.
    ///
    /// ```
    /// use jthreadpool::ThreadPool;
    /// use std::sync::atomic::{AtomicUsize,Ordering};
    /// use std::sync::Arc;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// // a pool
    /// let pool=ThreadPool::new(2);
    /// let counter:Arc<AtomicUsize>=Arc::new(AtomicUsize::new(0));
    /// let c1=counter.clone();
    /// let c2=counter.clone();
    /// let c3=counter.clone();
    ///
    /// pool.submit(move||{
    /// thread::sleep(Duration::from_millis(10));
    /// c1.fetch_add(1, Ordering::SeqCst);
    /// }).unwrap();
    ///
    /// pool.submit(move||{
    /// thread::sleep(Duration::from_millis(10));
    /// c2.fetch_add(2, Ordering::SeqCst);
    /// }).unwrap();
    ///
    /// // thread panic
    /// pool.submit(||{
    /// thread::sleep(Duration::from_millis(10));
    /// panic!();
    /// }).unwrap();
    ///
    /// pool.submit(move||{
    /// thread::sleep(Duration::from_millis(10));
    /// c3.fetch_add(3, Ordering::SeqCst);
    /// }).unwrap();
    ///
    /// pool.join();
    /// assert_eq!(counter.load(Ordering::SeqCst),6);
    /// ```
    ///
    /// [`Builder`]: Builder.html
    pub fn new(num_threads:usize)->ThreadPool{
        Builder::new().num_threads(num_threads).build()
    }

    /// Get the number of workers in the ThreadPool
    /// # Examples
    ///
    /// ```
    /// use jthreadpool::ThreadPool;
    /// let pool=ThreadPool::new(4);
    /// assert_eq!(pool.size(),4);
    /// ```
    ///
    /// [`Builder`]: Builder.html
    pub fn size(&self)->usize{
        self.shared_data.workers.lock().unwrap().len()
    }

    fn submit_inner(&self, message:Message) {
        self.sender.send(message).expect("can not submit message to thread pool");
        self.shared_data.queued_count.fetch_add(1, Ordering::SeqCst);
    }


    /// Submit task to `ThreadPool`
    /// # Examples
    /// create a thread pool and calculate parallel
    ///
    /// ```
    /// use jthreadpool::ThreadPool;
    ///
    /// let pool=ThreadPool::new(4);
    ///
    pub fn submit<F>(&self, job:F)->Result<(), ThreadPoolError>
        where F:FnOnce()+Send+'static{

        if self.is_shutdown(){
            Err(ThreadPoolError::new("can not submit task to shutdown thread pool"))
        }else {
            self.submit_inner(Message::NewJob(Box::new(job)));
            Ok(())
        }
    }

    pub fn panic_count(&self)->usize{
        self.shared_data.panic_count.load(Ordering::SeqCst)
    }

    pub fn success_count(&self)->usize{
        self.shared_data.success_count.load(Ordering::SeqCst)
    }

    pub fn queued_count(&self)->usize{
        self.shared_data.queued_count.load(Ordering::SeqCst)
    }

    pub fn executing_count(&self)->usize{
        self.shared_data.executing_count.load(Ordering::SeqCst)
    }

    fn is_shutdown(&self) ->bool{
        self.shared_data.is_shutdown.load(Ordering::SeqCst)
    }


    #[allow(dead_code)]
    fn worker_status(&self)->HashMap<ThreadId,WorkerStatus>{
        let workers=self.shared_data.workers.lock().unwrap();
        workers.iter().map(|(k,v)|(*k,v.status.get_state())).collect()
    }

    #[allow(dead_code)]
    fn worker_names(&self)->HashMap<ThreadId,Option<String>>{
        let workers=self.shared_data.workers.lock().unwrap();
        workers.iter().map(|(k,v)|(*k,v.name())).collect()
    }


    pub fn join(&self){
        //fast path without lock
        if self.shared_data.is_empty(){
            return;
        }
        let &(ref empty_lock, ref empty_cvar)=&self.shared_data.empty;
        let mut lock=empty_lock.lock().unwrap();
        while !self.shared_data.is_empty(){
            lock=empty_cvar.wait(lock).unwrap();
        }
    }

    /// Shutdown thread pool, no more task can be submitted, but all submitted tasks will be executed as far as possible.
    /// Note: when
    /// Invoke `shutdown` more than once has no effect.
    /// The return value means if this is the first time call shutdown();
    pub fn shutdown(&self)->bool{
        let first_time=!self.shared_data.is_shutdown.compare_and_swap(false, true, Ordering::SeqCst);
        if  first_time{
            // send `Terminate` Message to every worker
            let workers=self.shared_data.workers.lock().unwrap();
            let workers_num=workers.len();

            log!("[Pool] shutdown in {:?}",thread::current().id());
            for _ in 0..workers_num {
                self.submit_inner(Message::Terminate);
            }
            log!("[Pool] sent {} Terminate Message in {:?}",workers_num, thread::current().id());
        }
        first_time
    }

    /// Wait until all tasks are executed, this function will block.
    /// # panic
    /// panic if `shutdown` has not been invoked
    pub fn await(&mut self){
        if !self.is_shutdown(){
            panic!("must call await after shutdown");
        }

        log!("[Pool] start drop  in {:?}, wait for all workers consume Terminate Message", thread::current().id());

        // workers_num will not change after shutdown, no need to lock work and invoke len() every time
        let workers_num:usize;
        {
            // send `Terminate` Message to every worker
            let workers = self.shared_data.workers.lock().unwrap();
            workers_num=workers.len();
        }

        // Wait until all workers have consumed TERMINATE message
        {
            let &(ref lock, ref cvar) = &self.shared_data.shutdown_message_consumed;
            let mut consumed = lock.lock().unwrap();
            while *consumed < workers_num {
                consumed = cvar.wait(consumed).unwrap();
            }
        }

        log!("[Pool] go on drop  in {:?}, wait for all worker to join", thread::current().id());
        // Since all TERMINATED message are consumed, we can ensure there is no new worker will be spawned.
        // So these is absolutely no reentrant requirement of workers from `Sentine::drop`
        let mut workers = self.shared_data.workers.lock().unwrap();
        for (_id,worker) in &mut workers.iter_mut(){
            if let Some(thread)=worker.thread.take() {
               thread.join().unwrap();
            }
        }
        log!("[Pool] finish drop  in {:?}", thread::current().id());
    }
}

impl Drop for ThreadPool{
    fn drop(&mut self){
        self.shutdown();
        self.await();
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use std::thread;


    #[test]
    pub fn count() {
        let mut pool = ThreadPool::new(2);
        assert_eq!(0, pool.panic_count());
        assert_eq!(0, pool.success_count());

        pool.submit(|| {
            thread::sleep(Duration::from_millis(1));
        }).unwrap();

        pool.shutdown();
        pool.await();
        assert_eq!(1, pool.success_count());
        assert_eq!(0, pool.panic_count());
    }

    #[test]
    pub fn worker_status() {
        let mut pool = ThreadPool::new(1);

        for (_id, status) in pool.worker_status() {
            assert_eq!(WorkerStatus::Init, status);
        }

        let job = || {
            thread::sleep(Duration::from_millis(200));
        };

        pool.submit(job).unwrap();

        thread::sleep(Duration::from_millis(20));
        for (_id, status) in pool.worker_status() {
            assert_eq!(WorkerStatus::InProcess, status);
        }

        thread::sleep(Duration::from_millis(200));
        for (_id, status) in pool.worker_status() {
            assert_eq!(WorkerStatus::Idle, status);
        }

        pool.shutdown();
        thread::sleep(Duration::from_millis(20));

        for (_id, status) in pool.worker_status() {
            assert_eq!(WorkerStatus::Terminated, status);
        }

        pool.await();
        for (_id, status) in pool.worker_status() {
            assert_eq!(WorkerStatus::Terminated, status);
        }
    }

    #[test]
    pub fn success_and_panic_count() {
        let mut pool = ThreadPool::new(2);
        assert_eq!(0, pool.panic_count());
        assert_eq!(0, pool.success_count());

        pool.submit(|| {
            thread::sleep(Duration::from_millis(1));
            // panic after one millis
            panic!()
        }).unwrap();

        pool.shutdown();
        pool.await();
        assert_eq!(0, pool.success_count());
        assert_eq!(1, pool.panic_count());
    }

    #[test]
    pub fn queued_and_executing_count() {
        let pool = ThreadPool::new(1);
        assert_eq!(0, pool.queued_count());
        assert_eq!(0, pool.executing_count());

        let job = || {
            thread::sleep(Duration::from_millis(200));
        };
        pool.submit(job).unwrap();
        //wait for schedule
        thread::sleep(Duration::from_millis(20));
        assert_eq!(0, pool.queued_count());
        assert_eq!(1, pool.executing_count());

        pool.submit(job).unwrap();
        //wait for schedule
        thread::sleep(Duration::from_millis(20));
        assert_eq!(1, pool.queued_count());
        assert_eq!(1, pool.executing_count());

        //wait until the first job finish
        thread::sleep(Duration::from_millis(200));
        assert_eq!(0, pool.queued_count());
        assert_eq!(1, pool.executing_count());

        //wait until all jobs finish
        thread::sleep(Duration::from_millis(200));
        assert_eq!(0, pool.queued_count());
        assert_eq!(0, pool.executing_count());
    }

    #[test]
    pub fn join(){
        let pool=ThreadPool::new(1);
        assert_eq!(pool.success_count(),0);
        pool.submit(||{
            thread::sleep(Duration::from_millis(20));
        }).unwrap();
        pool.join();
        assert_eq!(pool.success_count(),1);
        assert_eq!(pool.panic_count(),0);
        pool.submit(||{
            thread::sleep(Duration::from_millis(20));
            panic!();
        }).unwrap();
        pool.join();
        assert_eq!(pool.success_count(),1);
        assert_eq!(pool.panic_count(),1);

        pool.shutdown();
        pool.join();
        assert_eq!(pool.success_count(),1);
        assert_eq!(pool.panic_count(),1);
    }

    #[test]
    pub fn await() {
        let mut pool = ThreadPool::new(2);
        pool.shutdown();
        pool.await();
        pool.await();
        assert_eq!(2, pool.size());
    }

    #[test]
    #[should_panic]
    pub fn init_panic() {
        ThreadPool::new(0);
    }

    #[test]
    pub fn init_with_name_prefix() {
        let pool=Builder::new().num_threads(2).name_prefix(String::from("***")).build();
        let names = pool.worker_names();
        assert_eq!(names.len(), 2);
        for (_id, name) in names {
            assert!(name.unwrap().starts_with("***"));
        }
    }

    #[test]
    pub fn work() {
        let mut pool =  ThreadPool::new(2);
        let counter = Arc::new(Mutex::new(0));
        let c1 = counter.clone();
        let c2 = counter.clone();
        pool.submit(move || {
            let lock = c1.lock();
            *lock.unwrap() += 1;
        }).unwrap();

        pool.submit(move || {
            let lock = c2.lock();
            *lock.unwrap() += 2;
        }).unwrap();

        pool.shutdown();
        pool.await();

        let c = counter.clone();
        assert_eq!(3, *c.lock().unwrap());
    }

    #[test]
    pub fn shutdown_and_terminated() {
        let pool =  ThreadPool::new(2);
        assert_eq!(false, pool.is_shutdown());
        pool.shutdown();
        assert_eq!(true, pool.is_shutdown());

        // shutdown can be invoked more than once
        pool.shutdown();
        assert_eq!(true, pool.is_shutdown());
    }

    #[test]
    pub fn panic_and_recovery() {
        let mut pool =  ThreadPool::new(1);
        let c = Arc::new(AtomicUsize::new(0));
        let c2 = c.clone();
        // the first thread panics
        pool.submit(|| {
            panic!()
        }).unwrap();

        // a new thread will be spawn to do inc operation
        pool.submit(move || {
            c2.fetch_add(1, Ordering::SeqCst);
        }).unwrap();

        // wait until new thread is fork
        thread::sleep(Duration::from_millis(100));
        pool.shutdown();
        pool.await();
        assert_eq!(1, c.load(Ordering::SeqCst));
    }

    #[test]
    pub fn panic_and_recovery_1() {
        let mut pool = ThreadPool::new(1);
        let c = Arc::new(AtomicUsize::new(0));
        let c2 = c.clone();
        // the first thread panics
        pool.submit(|| {
            // wait 100 ms that panic occurs when pool has been shutdown
            thread::sleep(Duration::from_millis(100));
            panic!()
        }).unwrap();

        // although this job is executed after shutdown and the only worker panics before,
        // a new worker need to be generated to make sure all job are done before await return
        pool.submit(move || {
            c2.fetch_add(1, Ordering::SeqCst);
        }).unwrap();

        pool.shutdown();
        pool.await();
        assert_eq!(1, c.load(Ordering::SeqCst));
    }
}
