use std::process;
use std::thread;
use std::ops::{Deref, Drop};
use redis::{ErrorKind, RedisResult, Value, Commands};
use serde::Serialize;
use serde::de::DeserializeOwned;


/// Job objects that can be reconstructed from the data stored in Redis
///
/// Implemented for all `Deserialize` objects by default by relying on JSON encoding.
pub trait JobDecodable
where
    Self: Sized,
{
    /// Decode the given Redis value into a job
    ///
    /// This should decode the string value into a proper job.
    /// The string value is encoded as JSON.
    fn decode_job(value: &Value) -> RedisResult<Self>;
}

/// Job objects that can be encoded to a string to be stored in Redis
///
/// Implemented for all `Serialize` objects by default by encoding with Msgpack.
pub trait JobEncodable {
    /// Encode the value into a Blob to insert into Redis
    ///
    /// It should encode the value into a string.
    fn encode_job(&self) -> Vec<u8>;
}

impl<T: DeserializeOwned> JobDecodable for T {
    fn decode_job(value: &Value) -> RedisResult<T> {
        match *value {
            Value::Data(ref v) => rmp_serde::decode::from_slice(v)
                .map_err(|_| From::from((ErrorKind::TypeError, "Msgpack decode failed"))),
            _ => Err((ErrorKind::TypeError, "Can only decode from a string"))?,
        }
    }
}

impl<T: Serialize> JobEncodable for T {
    fn encode_job(&self) -> Vec<u8> {
        rmp_serde::encode::to_vec(self).unwrap()
    }
}

pub struct JobGuard<'a, T: 'a> {
    payload: T,
    queue: &'a mut Queue<'a>,
    failed: bool,
}

impl<'a, T> JobGuard<'a, T> {
    /// Fail the current job, in order to keep it in the backup queue.
    pub fn fail(&mut self) {
        self.failed = true;
    }

    /// Get access to the underlying job.
    ///
    /// This should only be needed in very few cases, as this guard derefs automatically.
    pub fn inner(&self) -> &T {
        &self.payload
    }

    /// Get access to the wrapper queue.
    pub fn queue(&self) -> &Queue {
        self.queue
    }
}

impl<'a, T> Deref for JobGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.payload
    }
}

impl<'a, T> Drop for JobGuard<'a, T> {
    fn drop(&mut self) {
        if !self.failed {
            // Pop job from backup queue
            let backup = &self.queue.backup_queue_name[..];
            self.queue
                .client
                .lpop::<_, ()>(backup)
                .expect("LPOP from backup queue failed");
        }
    }
}


pub struct Queue<'a> {
    queue_name: String,
    backup_queue_name: String,
    stopped: bool,
    client: &'a mut redis::Connection,
}


impl<'a> Queue<'a> {
    pub fn new(name: String, client: &'a mut redis::Connection) -> Queue {
        let qname = format!("charon:{}", name);
        let backup_queue = format!(
            "{}:{}:{}",
            qname,
            process::id(),
            thread::current().name().unwrap_or("default")
        );

        Queue {
            queue_name: qname,
            backup_queue_name: backup_queue,
            client: client,
            stopped: false,
        }
    }

    /// Stop processing the queue
    /// The next `Queue::next()` call will return `None`
    pub fn stop(&mut self) {
        self.stopped = true;
    }

    /// Check if queue processing is stopped
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    /// Get the full queue name
    pub fn queue(&self) -> &str {
        &self.queue_name
    }

    /// Get the full backup queue name
    pub fn backup_queue(&self) -> &str {
        &self.backup_queue_name
    }

    /// Get the number of remaining jobs in the queue
    pub fn size(&self) -> u64 {
        self.client.llen(self.queue_name.as_str()).unwrap_or(0)
    }

    /// Push a new job to the queue
    pub fn push<T: JobEncodable>(&mut self, job: T) -> RedisResult<()> {
        self.client.lpush(self.queue_name.as_str(), job.encode_job())
    }

    /// Grab the next job from the queue
    ///
    /// This method blocks and waits until a new job is available.
    pub fn next<T: JobDecodable>(&'a mut self) -> Option<RedisResult<JobGuard<T>>> {
        if self.stopped {
            return None;
        }

        let v;
        {
            let qname = &self.queue_name[..];
            let backup = &self.backup_queue_name[..];

            v = match self.client.brpoplpush(qname, backup, 0) {
                Ok(v) => v,
                Err(_) => {
                    return Some(Err(From::from((ErrorKind::TypeError, "next failed"))));
                }
            };
        }

        let v = match v {
            v @ Value::Data(_) => v,
            _ => {
                return Some(Err(From::from((
                    ErrorKind::TypeError,
                    "unknown result type",
                ))));
            }
        };

        match T::decode_job(&v) {
            Err(e) => Some(Err(e)),
            Ok(payload) => Some(Ok(JobGuard {
                payload: payload,
                queue: self,
                failed: false,
            })),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Queue, JobGuard};
    use serde::{Deserialize, Serialize};
    use rmp_serde::Serializer;
    use redis::Commands;

    #[derive(Deserialize, Serialize)]
    struct Job {
        id: u64,
    }

    fn sample_job_payload(id: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        let job = Job { id };
        job.serialize(&mut Serializer::new(&mut buf)).unwrap();
        buf
    }

    #[test]
    fn decodes_job() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let mut con2 = client.get_connection().unwrap();
        let mut worker = Queue::new("default".into(), &mut con2);

        let _: () = con.rpush(worker.queue(), sample_job_payload(42)).unwrap();

        let j = worker.next::<Job>().unwrap().unwrap();
        assert_eq!(42, j.id);
    }

    #[test]
    fn releases_job() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let mut con2 = client.get_connection().unwrap();
        let mut worker = Queue::new("default".into(), &mut con2);
        let bqueue = worker.backup_queue();

        let _: () = con.del(bqueue).unwrap();
        let _: () = con.lpush(worker.queue(), sample_job_payload(42)).unwrap();

        {
            let j = worker.next::<Job>().unwrap().unwrap();
            assert_eq!(42, j.id);
            let in_backup: Vec<Vec<u8>> = con.lrange(bqueue, 0, -1).unwrap();
            assert_eq!(1, in_backup.len());
            assert_eq!(sample_job_payload(42), in_backup[0]);
        }

        let in_backup: u32 = con.llen(bqueue).unwrap();
        assert_eq!(0, in_backup);
    }

    #[test]
    fn can_be_stopped() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let mut con2 = client.get_connection().unwrap();
        let mut worker = Queue::new("stopper".into(), &mut con2);

        let _: () = con.del(worker.queue()).unwrap();
        let _: () = con.lpush(worker.queue(), sample_job_payload(1)).unwrap();
        let _: () = con.lpush(worker.queue(), sample_job_payload(2)).unwrap();
        let _: () = con.lpush(worker.queue(), sample_job_payload(3)).unwrap();

        assert_eq!(3, worker.size());

        while let Some(task) = worker.next::<Job>() {
            let _task = task.unwrap();
            worker.stop();
        }

        assert_eq!(2, worker.size());
    }

    #[test]
    fn can_enqueue() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let mut con2 = client.get_connection().unwrap();

        let mut worker = Queue::new("enqueue".into(), &mut con2);
        let _: () = con.del(worker.queue()).unwrap();

        assert_eq!(0, worker.size());

        worker.push(Job { id: 53 }).unwrap();

        assert_eq!(1, worker.size());

        let j = worker.next::<Job>().unwrap().unwrap();
        assert_eq!(53, j.id);
    }

    #[test]
    fn does_not_drop_failed() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let mut con2 = client.get_connection().unwrap();
        let mut worker = Queue::new("failure".into(), &mut con2);

        let _: () = con.del(worker.queue()).unwrap();
        let _: () = con.del(worker.backup_queue()).unwrap();
        let _: () = con.lpush(worker.queue(), sample_job_payload(1)).unwrap();

        {
            let mut task: JobGuard<Job> = worker.next().unwrap().unwrap();
            task.fail();
        }

        let len: u32 = con.llen(worker.backup_queue()).unwrap();
        assert_eq!(1, len);
    }
}
