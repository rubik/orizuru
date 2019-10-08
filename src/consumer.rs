use redis::{from_redis_value, Commands, ErrorKind, RedisResult, Value};
use std::cell::{Cell, RefCell};
use crate::message;

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

    /// Stop processing the queue.
    /// The next `Consumer::next()` call will return `None`.
    pub fn stop(&self) {
        self.stopped.set(true);
    }

    /// Check if queue processing is stopped.
    pub fn is_stopped(&self) -> bool {
        self.stopped.get()
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

    /// Push a new job to the source queue.
    pub fn push<T: message::MessageEncodable>(&self, job: T) -> RedisResult<()> {
        self.client
            .borrow_mut()
            .lpush(self.source_queue_name.as_str(), job.encode_job())
    }

    /// Grab the next job from the queue.
    ///
    /// This method blocks and waits until a new job is available. It returns
    /// None if the consumer has been stopped (with the stop() method).
    /// Otherwise it returns a RedisResult value that may wrap the message.
    pub fn next<T: message::MessageDecodable>(&self) -> Option<RedisResult<message::MessageGuard<T>>> {
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
            Ok(message) => Some(Ok(message::MessageGuard::new(
                message,
                from_redis_value(&v).unwrap(),
                &self.client,
                self.processing_queue_name.clone(),
                self.unacked_queue_name.clone(),
                false,
                false,
            ))),
        }
    }
}
