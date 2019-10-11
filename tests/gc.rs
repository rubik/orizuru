use orizuru::{Consumer, GC};
use redis::Commands;
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[macro_use]
mod test_utils;

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
fn collect_one_runs_with_no_jobs() {
    redis_fixture!(client, con, consumer, "g", gc, {
        assert_eq!(gc.collect_one(consumer.name()), Ok(0));
    });
}

#[test]
fn collect_one_runs_with_some_jobs() {
    redis_fixture!(client, con, consumer, "g", gc, {
        for i in 0..3 {
            let _: () = con
                .lpush(consumer.unacked_queue(), sample_job_payload(i))
                .unwrap();
        }
        assert_eq!(gc.collect_one(consumer.name()), Ok(3));
    });
}
