use crate::message;
use redis::{from_redis_value, Commands, RedisResult, Value};
use std::cell::{Cell, RefCell};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const CONSUMERS_KEY: &str = "orizuru:consumers";
pub const HEARTBEAT_KEY: &str = "orizuru:consumers:{consumer}:heartbeat";
pub const HEARTBEATS_KEY: &str = "orizuru:heartbeats";
pub const PROCESSING_QUEUE_KEY: &str = "orizuru:consumers:{consumer}:processing";
pub const UNACKED_QUEUE_KEY: &str = "orizuru:consumers:{consumer}:unacked";

pub struct Consumer {
    name: String,
    source_queue_name: String,
    processing_queue_name: String,
    unacked_queue_name: String,
    heartbeat_key: String,
    heartbeats_key: String,
    stopped: Cell<bool>,
    client: RefCell<redis::Connection>,
}

impl Consumer {
    pub fn new(
        name: String,
        source_queue_name: String,
        client: redis::Connection,
    ) -> Consumer {
        let processing_queue_name =
            PROCESSING_QUEUE_KEY.replace("{consumer}", name.as_str());
        let unacked_queue_name =
            UNACKED_QUEUE_KEY.replace("{consumer}", name.as_str());
        let heartbeat_key = HEARTBEAT_KEY.replace("{consumer}", name.as_str());

        Consumer {
            name,
            source_queue_name,
            processing_queue_name,
            unacked_queue_name,
            heartbeat_key,
            heartbeats_key: HEARTBEATS_KEY.into(),
            client: RefCell::new(client),
            stopped: Cell::new(false),
        }
    }

    /// Register this consumer to enable automatic discovery by the garbage
    /// collector.
    pub fn register(&self) -> RedisResult<Value> {
        self.client
            .borrow_mut()
            .sadd(CONSUMERS_KEY, self.name.as_str())
    }

    /// Stop processing the queue.
    /// The consumer will be deregistered, and the next `Consumer::next()` call
    /// will return `None`.
    pub fn stop(&self) -> RedisResult<Value> {
        self.stopped.set(true);
        self.client
            .borrow_mut()
            .srem(CONSUMERS_KEY, self.name.as_str())
    }

    /// Check if queue processing is stopped.
    pub fn is_stopped(&self) -> bool {
        self.stopped.get()
    }

    /// Get the name of the consumer.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the source queue name.
    pub fn heartbeat_key(&self) -> &str {
        &self.heartbeat_key
    }

    /// Get the source queue name.
    pub fn heartbeats_key(&self) -> &str {
        &self.heartbeats_key
    }

    /// Get the source queue name.
    pub fn source_queue(&self) -> &str {
        &self.source_queue_name
    }

    /// Get the processing queue name.
    pub fn processing_queue(&self) -> &str {
        &self.processing_queue_name
    }

    /// Get the unacked queue name.
    pub fn unacked_queue(&self) -> &str {
        &self.unacked_queue_name
    }

    /// Get the number of remaining jobs in the queue.
    pub fn size(&self) -> u64 {
        self.client
            .borrow_mut()
            .llen(self.source_queue_name.as_str())
            .unwrap_or(0)
    }

    pub fn heartbeat(&self, ttl: Duration) -> u128 {
        let now = SystemTime::now();
        let ts = match now.duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_millis(),
            Err(_) => 0,
        };
        let _: RedisResult<()> = redis::pipe()
            .cmd("HSET")
            .arg(self.heartbeats_key.as_str())
            .arg(self.name.as_str())
            .arg(ts.to_string())
            .ignore()
            .cmd("SET")
            .arg(self.heartbeat_key.as_str())
            .arg(ts.to_string())
            .arg("PX")
            .arg(ttl.as_millis().to_string())
            .ignore()
            .query(&mut *self.client.borrow_mut());

        ts
    }

    /// Grab the next job from the queue.
    ///
    /// This method blocks and waits until a new job is available. It returns
    /// None if the consumer has been stopped (with the stop() method).
    /// Otherwise it returns a RedisResult value that may wrap the message.
    pub fn next<T: message::MessageDecodable>(
        &self,
    ) -> Option<Result<message::MessageGuard<T>, &'static str>> {
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
                    return Some(Err(
                        "failed to fetch next message with brpoplpush",
                    ));
                }
            };
        }

        let v = match v {
            v @ Value::Data(_) => v,
            _ => {
                return Some(Err("unknown result type for next message"));
            }
        };

        match T::decode_message(&v) {
            Err(e) => Some(Err(e)),
            Ok(message) => Some(Ok(message::MessageGuard::new(
                message,
                from_redis_value(&v).unwrap(),
                &self.client,
                self.processing_queue_name.clone(),
                self.unacked_queue_name.clone(),
            ))),
        }
    }
}
