use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use bitflags::bitflags;
use futures::future::{join_all, OptionFuture};
use futures::{StreamExt};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{
    consumer::{Consumer, ConsumerContext, StreamConsumer},
    message::{Header, Message},
    ClientConfig, ClientContext, Statistics,
};
use snafu::ResultExt;
use snafu::Snafu;
use stream_cancel::Tripwire;
use tokio::spawn;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, Error>;

const NUM_TOPICS: usize = 100;
const NUM_MSGS: usize = 20000;

bitflags! {
    pub struct Flags : u32{
        const NONE = 0x0;
        const CREATE = 0x1;
        const PRODUCE = 0x2;
        const DROP = 0x4;
        const CONSUME = 0x8;
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let bootstrap_servers = std::env::args()
        .nth(1)
        .expect("no bootstraps servers given");
    let flag_arg = std::env::args().nth(2);
    let flags = match flag_arg {
        Some(ref flags) => flags
            .split(",")
            .map(|value| match value.to_lowercase().as_str() {
                "create" => Flags::CREATE,
                "produce" => Flags::PRODUCE,
                "drop" => Flags::DROP,
                "consume" => Flags::CONSUME,
                &_ => Flags::NONE,
            })
            .reduce(|flags: Flags, flag| flags | flag)
            .unwrap(),
        None => Flags::CREATE,
    };

    println!("Using the following flags: {:?}", flags);

    let admin = Arc::new(create_admin(&bootstrap_servers));

    let topics = vec!["topic1".to_string()];

    create_topics(admin.clone(), &topics).await;

    let mut produce_future = None; 

    if flags.contains(Flags::PRODUCE) {
        produce_future = Some(produce_messages(&topics, bootstrap_servers.as_str()));
    }

    let (trigger, tripwire) = Tripwire::new();
    let mut consume_future = None;

    if flags.contains(Flags::CONSUME) {
        consume_future = Some(consume_messages(bootstrap_servers.as_str(), topics[0].as_str(), tripwire.clone()));
    }

    if produce_future.is_some() && consume_future.is_some() {
        futures::future::join(produce_future.unwrap(), consume_future.unwrap()).await;
    } else if produce_future.is_some() {
        produce_future.unwrap().await;
    } else if consume_future.is_some() {
        consume_future.unwrap().await;
    }

}

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("Could not create Kafka consumer: {}", source))]
    KafkaCreateError { source: rdkafka::error::KafkaError },
    #[snafu(display("Could not subscribe to Kafka topics: {}", source))]
    KafkaSubscribeError { source: rdkafka::error::KafkaError },
}

pub(crate) struct KafkaStatisticsContext;

impl ClientContext for KafkaStatisticsContext {
    fn stats(&self, _statistics: Statistics) {
        //println!("Stats {:?}", statistics);
    }
}

impl ConsumerContext for KafkaStatisticsContext {
    fn pre_rebalance<'a>(&self, _rebalance: &rdkafka::consumer::Rebalance<'a>) {
        println!("rebalance");
    }
}

async fn drop_loop(
    bootstrap_servers: String,
    flags: Flags,
    admin: Arc<AdminClient<DefaultClientContext>>,
) {
    let topics: Vec<String> = { 1..NUM_TOPICS + 1 }
        .into_iter()
        .map(|i| format!("test.rdkafka.{}", i))
        .collect();

    if flags.contains(Flags::CREATE) {
        create_topics(admin.clone(), &topics).await;
    }

    for _ in 0..999999999 {
        let mut futures = vec![];
        let (trigger, tripwire) = Tripwire::new();

        if flags.contains(Flags::DROP) {
            create_topics(admin.clone(), &topics).await;
        }

        if flags.contains(Flags::PRODUCE) {
            produce_messages(&topics, bootstrap_servers.as_str()).await;
        }

        for topic in &topics {
            futures.push(consume_messages(
                bootstrap_servers.as_str(),
                topic.as_str(),
                tripwire.clone(),
            ));
            futures.push(consume_messages(
                bootstrap_servers.as_str(),
                topic.as_str(),
                tripwire.clone(),
            ));
            futures.push(consume_messages(
                bootstrap_servers.as_str(),
                topic.as_str(),
                tripwire.clone(),
            ));
            futures.push(consume_messages(
                bootstrap_servers.as_str(),
                topic.as_str(),
                tripwire.clone(),
            ));
        }

        // Give time for consumers to get messages
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        if flags.contains(Flags::DROP) {
            drop_topics(admin.clone(), &topics).await;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        drop(trigger);

        //println!("joining");
        let len = futures.len();
        let _ = join_all(futures).await;
        println!("complete {} {}", chrono::offset::Local::now(), len);
    }
}

async fn produce_messages(topics: &Vec<String>, bootstrap_servers: &str) {
    for topic in topics {
        let producer: &FutureProducer = &ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("producer creation error");

        let producer_futures = (0..NUM_MSGS)
            .map(|i| async move {
                // The send operation on the topic returns a future, which will be
                // completed once the result or failure from Kafka is received.
                let delivery_status = producer
                    .send(
                        FutureRecord::to(topic)
                            .payload(&format!("Message {} {{ 'hello': 'mezmo from vector aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' }}", i))
                            .key(&format!("Key {}", i))
                            .headers(OwnedHeaders::new().insert(Header {
                                key: "k1",
                                value: Some("v1"),
                            })),
                        Duration::from_secs(0),
                    )
                    .await;
                delivery_status
            })
            .collect::<Vec<_>>();

        let _result = join_all(producer_futures).await;
        //println!("wrote messages to topic {}: {:?}", topic, result);
    }
}

async fn create_topics(admin: Arc<AdminClient<DefaultClientContext>>, topics: &Vec<String>) {
    for topic in topics {
        //println!("adding topic {}", topic);
        let topic = [&NewTopic::new(
            topic.as_str(),
            1,
            TopicReplication::Fixed(-1),
        )];
        admin
            .create_topics(topic, &AdminOptions::new())
            .await
            .expect("admin creation failure");
    }
    println!("created topics");
}

async fn drop_topics(admin: Arc<AdminClient<DefaultClientContext>>, topics: &Vec<String>) {
    println!("drop topics");
    for topic in topics {
        admin
            .delete_topics(&[topic], &AdminOptions::new())
            .await
            .expect("couldn't delete topic");
    }
}

fn consume_messages(
    bootstrap_servers: &str,
    topic: &str,
    tripwire: Tripwire,
) -> tokio::task::JoinHandle<()> {
    let topic = topic.to_string();
    let bootstrap_servers = bootstrap_servers.to_string();
    spawn(async move {
        let client_id = &topic;
        let consumer = create_consumer(
            &bootstrap_servers,
            client_id,
            std::slice::from_ref(&topic.as_str()),
        )
        .expect("consumer creation failure");
        let mut stream = consumer.stream();
        let mut msg_count = 0;
        let mut err_count = 0;
        let mut err_store_offset_count = 0;

        loop {
            tokio::select! {
                _ = tripwire.clone() => {
                    break;
                },
                message = stream.next() => match message {
                    None => {panic!("shouldn't happen!"); },  // WHY?
                    Some(Err(_error)) => { err_count += 1; println!("got error: {:?}", _error) },
                    Some(Ok(msg)) => {
                        msg_count += 1;
                        let _result = msg.payload();
                        //tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        if let Err(_err)  = consumer.store_offset(msg.topic(), msg.partition(), msg.offset()) {
                            err_store_offset_count += 1;
                        }
                        println!("got message {}: {:?}", msg_count, msg);
                    }
                },
            }
        }
    })
}

fn create_consumer(
    bootstrap_servers: &str,
    client_id: &str,
    topics: &[&str],
) -> crate::Result<StreamConsumer<KafkaStatisticsContext>> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("group.id", "pipeline-vector-group-v1")
        .set("bootstrap.servers", bootstrap_servers)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .set("socket.timeout.ms", "60000")
        .set("queued.min.messages", "1")
        .set("queued.max.messages.kbytes", "16")
        .set("message.max.bytes", "2097152")
        .set("fetch.message.max.bytes", "10")
        .set("fetch.error.backoff.ms", "1000")
        .set("fetch.wait.max.ms", "1000")
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        .set("enable.auto.offset.store", "false")
        .set("statistics.interval.ms", "1000")
        .set("client.id", client_id);

    let consumer = client_config
        .create_with_context::<_, StreamConsumer<_>>(KafkaStatisticsContext)
        .context(KafkaCreateSnafu)?;
    consumer.subscribe(topics).context(KafkaSubscribeSnafu)?;

    Ok(consumer)
}

fn create_admin(bootstrap_servers: &str) -> AdminClient<DefaultClientContext> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", bootstrap_servers);
    config.create().expect("admin client creation failed")
}