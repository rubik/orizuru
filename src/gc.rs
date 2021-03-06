use crate::consumer;
use redis::{Commands, RedisResult, Value};
use std::cell::RefCell;

pub struct GC {
    client: RefCell<redis::Connection>,
}

impl GC {
    pub fn new(client: redis::Connection) -> GC {
        GC {
            client: RefCell::new(client),
        }
    }

    pub fn collect_one(&self, consumer_name: &str) -> RedisResult<u64> {
        let n: u64 = self.client.borrow_mut().llen(
            consumer::UNACKED_QUEUE_KEY.replace("{consumer}", consumer_name),
        )?;

        if n == 0 {
            return Ok(0);
        }

        // It's doesn't matter if more elements are added after the call to LLEN
        // returns, as this method is designed to be called periodically (and
        // should be). It also does not matter if some elements are removed,
        // because we are using RPOPLPUSH here, which is not blocking.
        let mut total: u64 = 0;
        for _ in 0..n {
            let res: RedisResult<Value> = self.client.borrow_mut().rpoplpush(
                consumer::UNACKED_QUEUE_KEY.replace("{consumer}", consumer_name),
                consumer::PROCESSING_QUEUE_KEY
                    .replace("{consumer}", consumer_name),
            );
            match res {
                Err(e) => return Err(e),
                Ok(Value::Nil) => return Ok(total),
                Ok(_) => (),
            }
            total += 1;
        }
        Ok(total)
    }

    pub fn collect(&self) -> RedisResult<u64> {
        let vals: Vec<String> =
            self.client.borrow_mut().smembers(consumer::CONSUMERS_KEY)?;
        let mut total: u64 = 0;
        for name in vals {
            total += self.collect_one(name.as_str()).unwrap_or(0);
        }
        Ok(total)
    }
}
