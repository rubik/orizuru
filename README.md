<div align="center">
  <img alt="Orizuru logo" src="https://github.com/rubik/orizuru/raw/master/images/logo.png" height="130" />
</div>

<div align="center">
  <h1>Orizuru (折鶴)</h1>
  <p>A reliable, scalable and flexible Redis message queue for Rust.</p>
  <a href="https://travis-ci.org/rubik/orizuru">
    <img src="https://img.shields.io/travis/rubik/orizuru?style=for-the-badge" alt="Build">
  </a>
  <a href="https://coveralls.io/github/rubik/orizuru">
    <img src="https://img.shields.io/coveralls/github/rubik/orizuru?style=for-the-badge" alt="Code Coverage">
  </a>
  <a href="https://crates.io/crates/orizuru">
   <img src="https://img.shields.io/crates/d/orizuru?style=for-the-badge" alt="Downloads (all time)">
  <a>
  <a href="https://github.com/rubik/orizuru/blob/master/LICENSE">
    <img src="https://img.shields.io/crates/l/orizuru?style=for-the-badge" alt="ISC License">
  </a>
  <br>
  <br>
</div>


# Design
Queues are backed by Redis lists and as such they support multiple producers
and consumers:

* To publish a message to a **source** queue, a producer runs an
  [`LPUSH`](https://redis.io/commands/lpush) command;
* Periodically, a consumer fetches messages from the *source* queue and pushes
  them to its own **processing** queue. Then, it may choose to acknowledge them
  or reject them. Acknowledged messages are removed from the *processing*
  queue, while rejected messages are moved to the **unack** queue.

Each queue can have an unlimited number of concurrent consumers. Consumers
fetch messages with the [`BRPOPLPUSH`](https://redis.io/commands/brpoplpush)
command, that blocks until at least one message is available on the source
queue, and then pushes it to its processing queue. This operation is atomic,
which means that only one consumer can receive the message even if there are
multiple consumers fetching messages from the *source* queue.

Optionally, unacknowledged messages can be collected by the garbage collector
that periodically returns them to the source queue. If the garbage collector
does not run, *unack* queues are essentially dead-letter queues and could grow
without bound.

<p align="center">
  <img alt="Orizuru architecture" src="https://github.com/rubik/orizuru/raw/master/images/architecture.png" height="470" />
</p>

## Simplicity
In line with Rust's philosophy of not not picking a specific implementation
strategy, Orizuru is designed with simplicity in mind. This informs both the
public API, which is minimal, and the implementation. All the functionality is
implemented as a library, so that the user is free to integrate it in their
architecture however they wish.

## API

`Producer::push<T: MessageEncodable>(message: T) -> Option<RedisResult<i32>>`<br/>
    Push a message onto a *source* queue.

`Consumer::next<T: MessageDecodable>() -> Option<RedisResult<MessageGuard<T>>>`<br/>
    Fetch the next message from the queue. This method blocks and waits until a
    new message is available.

`MessageGuard::ack() -> RedisResult<Value>`<br/>
    Acknowledge the message and remove it from the *processing* queue.

`MessageGuard::reject() -> RedisResult<Value>`<br/>
    Reject the message and push it from the *processing* queue to the *unack*
    queue.

`MessageGuard::push(push_queue_name: String) -> RedisResult<Value>`<br/>
    Remove the message from the processing queue and push it to the specified
    queue. It can be used to implement retries.

The traits `MessageEncodable` and `MessageDecodable` ensure that the message
can be serialized and deserialized to/from Redis. They are implemented by
default for all the objects that implements the `Serialize` and `Deserialized`
traits from the serde crate, by using the [Msgpack](https://msgpack.org/)
encoding. This is a binary encoding analogous to JSON. It was chosen because
of the encoding and decoding speed and space efficiency over JSON.

# Usage patterns
Orizuru is a message queue, but it can be specialized into a *job* queue, when
the messages represent job payloads. However, the acknowledgement pattern
differs between the two. In the case of a generic message queue, messages are
acknowledged as soon as they are received, as that is what matters. In the case
of a job queue, on the other hand, messages are acknowledged after the job has
been executed (maybe even successfully). That protects the jobs from random
failures or worker crashes. In both cases the presence of a garbage collector
is essential to ensure that the deliveries/executions are retried.

# License

ISC

<p align="center"><sub>Logo image based on the one made by <a href="https://www.flaticon.com/authors/smashicons" title="Smashicons">Smashicons</a> for <a href="https://www.flaticon.com/" title="Flaticon">www.flaticon.com</a>.</p>
