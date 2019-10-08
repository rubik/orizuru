use orizuru::Producer;
use serde::{Deserialize, Serialize};
use std::thread::sleep;
use std::time::Duration;

#[derive(Deserialize, Serialize, Debug)]
struct Job {
    id: u64,
}

fn main() {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_connection().unwrap();
    let q = Producer::new("orizuru-example".into(), con);

    println!("Enqueuing jobs");

    let d = Duration::from_millis(1000);
    for i in 0.. {
        println!("Pushing job {}", i);
        q.push(Job { id: i }).unwrap();
        sleep(d);
    }
}
