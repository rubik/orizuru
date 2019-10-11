use orizuru::Consumer;
use redis::{Commands, Value};
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use std::thread;
use std::time;
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
fn decodes_job() {
    redis_fixture!(client, con, consumer, {
        let _: () = con
            .rpush(consumer.source_queue(), sample_job_payload(42))
            .unwrap();

        let j = consumer.next::<Message>().unwrap().unwrap();
        assert_eq!(42, j.id);
    });
}

#[test]
fn unacked_to_unack_queue() {
    redis_fixture!(client, con, consumer, {
        let pqueue = consumer.processing_queue();
        let uqueue = consumer.unacked_queue();

        let _: () = con
            .lpush(consumer.source_queue(), sample_job_payload(42))
            .unwrap();

        {
            let j = consumer.next::<Message>().unwrap().unwrap();
            assert_eq!(42, j.id);
            let in_proc: Vec<Vec<u8>> = con.lrange(pqueue, 0, -1).unwrap();
            assert_eq!(1, in_proc.len());
            assert_eq!(sample_job_payload(42), in_proc[0]);
        }

        assert_eq!(0, con.llen(pqueue).unwrap());
        assert_eq!(1, con.llen(uqueue).unwrap());
    });
}

#[test]
fn rejected_to_unack_queue() {
    redis_fixture!(client, con, consumer, {
        let pqueue = consumer.processing_queue();
        let uqueue = consumer.unacked_queue();

        let _: () = con
            .lpush(consumer.source_queue(), sample_job_payload(42))
            .unwrap();

        {
            let mut j = consumer.next::<Message>().unwrap().unwrap();
            assert_eq!(42, j.id);
            let in_proc: Vec<Vec<u8>> = con.lrange(pqueue, 0, -1).unwrap();
            assert_eq!(1, in_proc.len());
            assert_eq!(sample_job_payload(42), in_proc[0]);
            let _ = j.reject();
        }

        let in_proc_size: u32 = con.llen(pqueue).unwrap();
        assert_eq!(0, in_proc_size);
        assert_eq!(1, con.llen(uqueue).unwrap());
    });
}

#[test]
fn acked_are_released() {
    redis_fixture!(client, con, consumer, {
        let pqueue = consumer.processing_queue();

        let _: () = con
            .lpush(consumer.source_queue(), sample_job_payload(42))
            .unwrap();

        let mut m = consumer.next::<Message>().unwrap().unwrap();
        let _ = m.ack();
        let in_proc: u32 = con.llen(pqueue).unwrap();
        assert_eq!(0, in_proc);
    });
}

#[test]
fn can_be_stopped() {
    redis_fixture!(client, con, consumer, {
        let _: () = con
            .lpush(consumer.source_queue(), sample_job_payload(1))
            .unwrap();
        let _: () = con
            .lpush(consumer.source_queue(), sample_job_payload(2))
            .unwrap();
        let _: () = con
            .lpush(consumer.source_queue(), sample_job_payload(3))
            .unwrap();

        assert_eq!(3, consumer.size());

        while let Some(m) = consumer.next::<Message>() {
            let _m = m.unwrap();
            let _ = consumer.stop();
        }

        assert_eq!(2, consumer.size());
    });
}

#[test]
fn no_heartbeat() {
    redis_fixture!(client, con, consumer, {
        let single: Value = con.get(consumer.heartbeat_key()).unwrap();

        assert_eq!(single, Value::Nil);
    });
}

#[test]
fn one_heartbeat() {
    redis_fixture!(client, con, consumer, {
        let _: () = con.del(consumer.heartbeats_key()).unwrap();

        let ts = consumer.heartbeat(time::Duration::from_secs(5));
        let ts = ts.to_string();

        let all: Value = con.hgetall(consumer.heartbeats_key()).unwrap();
        let single: Value = con.get(consumer.heartbeat_key()).unwrap();

        assert_eq!(
            all,
            Value::Bulk(vec![
                Value::Data(consumer.name().as_bytes().to_vec()),
                Value::Data(ts.clone().into_bytes())
            ])
        );
        assert_eq!(single, Value::Data(ts.into_bytes()));

        let _: () = con.del(consumer.heartbeats_key()).unwrap();
    });
}

#[test]
fn multiple_heartbeat() {
    // ugly hack to ensure this test runs after the previous one, because Cargo
    // runs them in parallel
    thread::sleep(time::Duration::from_millis(300));
    redis_fixture!(client, con, consumer, {
        let _: () = con.del(consumer.heartbeats_key()).unwrap();

        let _ = consumer.heartbeat(time::Duration::from_secs(5));
        let _ = consumer.heartbeat(time::Duration::from_secs(5));
        let ts = consumer.heartbeat(time::Duration::from_secs(5));
        let ts = ts.to_string();

        let all: Value = con.hgetall(consumer.heartbeats_key()).unwrap();
        let single: Value = con.get(consumer.heartbeat_key()).unwrap();

        assert_eq!(
            all,
            Value::Bulk(vec![
                Value::Data(consumer.name().as_bytes().to_vec()),
                Value::Data(ts.clone().into_bytes())
            ])
        );
        assert_eq!(single, Value::Data(ts.into_bytes()));

        let _: () = con.del(consumer.heartbeats_key()).unwrap();
    });
}

#[test]
fn register() {
    redis_fixture!(client, con, consumer, {
        let _: () = con.del(consumer.consumers_key()).unwrap();

        let _ = consumer.register();
        let rv: Value = con
            .sismember(consumer.consumers_key(), consumer.name())
            .unwrap();

        assert_eq!(rv, Value::Int(1));

        let _: () = con.del(consumer.consumers_key()).unwrap();
    });
}
