use redis::{from_redis_value, Commands, ErrorKind, RedisResult, Value};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::cell::{Cell, RefCell};
use std::ops::{Deref, Drop};

/// Message objects that can be reconstructed from the data stored in Redis
///
/// Implemented for all `Deserialize` objects by default by relying on JSON encoding.
pub trait MessageDecodable
where
    Self: Sized,
{
    /// Decode the given Redis value into a job
    ///
    /// This should decode the string value into a proper job.
    /// The string value is encoded as JSON.
    fn decode_job(value: &Value) -> RedisResult<Self>;
}

/// Message objects that can be encoded to a string to be stored in Redis
///
/// Implemented for all `Serialize` objects by default by encoding with Msgpack.
pub trait MessageEncodable {
    /// Encode the value into a Blob to insert into Redis
    ///
    /// It should encode the value into a string.
    fn encode_job(&self) -> Vec<u8>;
}

impl<T: DeserializeOwned> MessageDecodable for T {
    fn decode_job(value: &Value) -> RedisResult<T> {
        match *value {
            Value::Data(ref v) => rmp_serde::decode::from_slice(v)
                .map_err(|_| From::from((ErrorKind::TypeError, "Msgpack decode failed"))),
            _ => Err((ErrorKind::TypeError, "Can only decode from a string"))?,
        }
    }
}

impl<T: Serialize> MessageEncodable for T {
    fn encode_job(&self) -> Vec<u8> {
        rmp_serde::encode::to_vec(self).unwrap()
    }
}

pub struct MessageGuard<'a, T: 'a> {
    message: T,
    payload: Vec<u8>,
    consumer: &'a Consumer,
    acked: bool,
    rejected: bool,
}

impl<'a, T> MessageGuard<'a, T> {
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn message(&self) -> &T {
        &self.message
    }

    pub fn ack(&mut self) -> RedisResult<Value> {
        self.acked = true;
        self.consumer.client.borrow_mut().lrem(
            self.consumer.processing_queue_name.as_str(),
            1,
            self.payload.clone(),
        )
    }

    /// Reject the current job, in order to move it to the unacked queue
    pub fn reject(&mut self) -> RedisResult<Value> {
        self.rejected = true;
        redis::pipe()
            .atomic()
            .cmd("LPUSH")
            .arg(self.consumer.unacked_queue_name.as_str())
            .arg(self.payload.clone())
            .ignore()
            .cmd("LREM")
            .arg(self.consumer.processing_queue_name.as_str())
            .arg(1)
            .arg(self.payload.clone())
            .ignore()
            .query(&mut *self.consumer.client.borrow_mut())
    }

    /// Get access to the wrapper queue.
    pub fn consumer(&self) -> &Consumer {
        self.consumer
    }
}

impl<'a, T> Deref for MessageGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.message
    }
}

impl<'a, T> Drop for MessageGuard<'a, T> {
    fn drop(&mut self) {
        if !self.rejected && !self.acked {
            let _ = self.reject();
        }
    }
}

pub struct Consumer {
    name: String,
    source_queue_name: String,
    processing_queue_name: String,
    unacked_queue_name: String,
    stopped: Cell<bool>,
    client: RefCell<redis::Connection>,
}

impl Consumer {
    pub fn new(name: String, source_queue_name: String, client: redis::Connection) -> Consumer {
        let processing_queue_name = format!("orizuru:consumers:{}:processing", name,);
        let unacked_queue_name = format!("orizuru:consumers:{}:unacked", name,);

        Consumer {
            name: name,
            source_queue_name: source_queue_name,
            processing_queue_name: processing_queue_name,
            unacked_queue_name: unacked_queue_name,
            client: RefCell::new(client),
            stopped: Cell::new(false),
        }
    }

    /// Stop processing the queue
    /// The next `Consumer::next()` call will return `None`
    pub fn stop(&self) {
        self.stopped.set(true);
    }

    /// Check if queue processing is stopped
    pub fn is_stopped(&self) -> bool {
        self.stopped.get()
    }

    /// Get the source queue name
    pub fn source_queue(&self) -> &str {
        &self.source_queue_name
    }

    /// Get the processing queue name
    pub fn processing_queue(&self) -> &str {
        &self.processing_queue_name
    }

    /// Get the unacked queue name
    pub fn unacked_queue(&self) -> &str {
        &self.unacked_queue_name
    }

    /// Get the number of remaining jobs in the queue
    pub fn size(&self) -> u64 {
        self.client
            .borrow_mut()
            .llen(self.source_queue_name.as_str())
            .unwrap_or(0)
    }

    /// Push a new job to the queue
    pub fn push<T: MessageEncodable>(&self, job: T) -> RedisResult<()> {
        self.client
            .borrow_mut()
            .lpush(self.source_queue_name.as_str(), job.encode_job())
    }

    /// Grab the next job from the queue
    ///
    /// This method blocks and waits until a new job is available.
    pub fn next<T: MessageDecodable>(&self) -> Option<RedisResult<MessageGuard<T>>> {
        if self.is_stopped() {
            return None;
        }

        let v;
        {
            let source = &self.source_queue_name[..];
            let processing = &self.processing_queue_name[..];

            v = match self.client.borrow_mut().brpoplpush(source, processing, 0) {
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
            Ok(message) => Some(Ok(MessageGuard {
                payload: from_redis_value(&v).unwrap(),
                message: message,
                consumer: &self,
                acked: false,
                rejected: false,
            })),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{Consumer, MessageGuard};
    use redis::Commands;
    use rmp_serde::Serializer;
    use serde::{Deserialize, Serialize};

    #[derive(Deserialize, Serialize)]
    struct Message {
        id: u64,
    }

    fn sample_job_payload(id: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        let job = Message { id };
        job.serialize(&mut Serializer::new(&mut buf)).unwrap();
        buf
    }

    #[test]
    fn decodes_job() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();
        let worker = Consumer::new("consumer-1".into(), "default".into(), con2);

        let _: () = con
            .rpush(worker.source_queue(), sample_job_payload(42))
            .unwrap();

        let j = worker.next::<Message>().unwrap().unwrap();
        assert_eq!(42, j.id);
    }

    #[test]
    fn releases_job_unacked() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();
        let worker = Consumer::new("consumer-2".into(), "default".into(), con2);
        let bqueue = worker.processing_queue();

        let _: () = con.del(bqueue).unwrap();
        let _: () = con
            .lpush(worker.source_queue(), sample_job_payload(42))
            .unwrap();

        {
            let j = worker.next::<Message>().unwrap().unwrap();
            assert_eq!(42, j.id);
            let in_backup: Vec<Vec<u8>> = con.lrange(bqueue, 0, -1).unwrap();
            assert_eq!(1, in_backup.len());
            assert_eq!(sample_job_payload(42), in_backup[0]);
        }

        let in_backup: u32 = con.llen(bqueue).unwrap();
        assert_eq!(0, in_backup);
    }

    #[test]
    fn releases_job_acked() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();
        let worker = Consumer::new("consumer-3".into(), "default".into(), con2);
        let bqueue = worker.processing_queue();

        let _: () = con.del(bqueue).unwrap();
        let _: () = con
            .lpush(worker.source_queue(), sample_job_payload(42))
            .unwrap();

        let mut m = worker.next::<Message>().unwrap().unwrap();
        m.ack();
        let in_backup: u32 = con.llen(bqueue).unwrap();
        assert_eq!(0, in_backup);
    }

    #[test]
    fn can_be_stopped() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();
        let worker = Consumer::new("consumer-4".into(), "stopper".into(), con2);

        let _: () = con.del(worker.source_queue()).unwrap();
        let _: () = con
            .lpush(worker.source_queue(), sample_job_payload(1))
            .unwrap();
        let _: () = con
            .lpush(worker.source_queue(), sample_job_payload(2))
            .unwrap();
        let _: () = con
            .lpush(worker.source_queue(), sample_job_payload(3))
            .unwrap();

        assert_eq!(3, worker.size());

        while let Some(task) = worker.next::<Message>() {
            let _task = task.unwrap();
            worker.stop();
        }

        assert_eq!(2, worker.size());
    }

    #[test]
    fn can_enqueue() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();

        let worker = Consumer::new("consumer-5".into(), "enqueue".into(), con2);
        let _: () = con.del(worker.source_queue()).unwrap();

        assert_eq!(0, worker.size());

        worker.push(Message { id: 53 }).unwrap();

        assert_eq!(1, worker.size());

        let j = worker.next::<Message>().unwrap().unwrap();
        assert_eq!(53, j.id);
    }

    #[test]
    fn does_not_drop_unacked() {
        let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        let mut con = client.get_connection().unwrap();
        let con2 = client.get_connection().unwrap();
        let worker = Consumer::new("consumer-6".into(), "failure".into(), con2);

        let _: () = con.del(worker.source_queue()).unwrap();
        let _: () = con.del(worker.processing_queue()).unwrap();
        let _: () = con.del(worker.unacked_queue()).unwrap();
        let _: () = con
            .lpush(worker.source_queue(), sample_job_payload(1))
            .unwrap();

        {
            let mut m: MessageGuard<Message> = worker.next().unwrap().unwrap();
            m.reject();
        }

        assert_eq!(0, con.llen(worker.processing_queue()).unwrap());
        assert_eq!(1, con.llen(worker.unacked_queue()).unwrap());
    }
}
