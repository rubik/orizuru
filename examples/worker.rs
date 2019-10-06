use charon::Queue;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
struct Job {
    id: u64,
}

fn main() {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_connection().unwrap();
    let worker = Queue::new("default".into(), con);

    println!("Starting worker with queue `default`");

    while let Some(task) = worker.next::<Job>() {
        if task.is_err() {
            continue;
        }

        let task = task.unwrap();

        println!("Task: {:?}", task.inner());
    }
}
