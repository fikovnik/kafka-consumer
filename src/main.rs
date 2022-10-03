use anyhow::anyhow;
use anyhow::Result;
use clap::Parser;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::BaseConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::util::get_rdkafka_version;
use rdkafka::{Offset, TopicPartitionList};

fn consume(
    broker: &String,
    group_id: &String,
    topic: &String,
    partition: i32,
    offset: i64,
) -> Result<Vec<u8>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("group.id", group_id)
        .create()
        .expect("Consumer creation failed");

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Offset(offset))?;
    consumer.assign(&tpl)?;

    if let Some(res) = consumer.poll(None) {
        let res = res?;
        res.payload()
            .map(|x| x.to_vec())
            .ok_or(anyhow!("Empty payload"))
    } else {
        Err(anyhow!("Nothing came from poll"))
    }
}

#[derive(Parser, Debug)] // requires `derive` feature
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, value_parser, default_value = "localhost:9092")]
    broker: String,
    #[clap(short, long, value_parser, default_value = "group")]
    group_id: String,
    #[clap(short, long, value_parser)]
    topic: String,
    #[clap(short, long, value_parser)]
    partition: i32,
    #[clap(short, long, value_parser)]
    offset: i64,
    #[clap(short, long, value_parser)]
    destination: String,
}

fn main() {
    let args = Args::parse();

    let (_, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: {}", version_s);

    let res = consume(
        &args.broker,
        &args.group_id,
        &args.topic,
        args.partition,
        args.offset,
    );

    match res {
        Ok(contents) => {
            std::fs::write(&args.destination, contents)
                .expect(&format!("Unable to write into {}", &args.destination));
        }
        Err(e) => {
            eprintln!("Error: {:?}", e);
        }
    }
}
