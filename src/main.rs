use std::sync::atomic::{AtomicU64, Ordering, AtomicBool};
use std::time::Duration;

use clap::Parser;
use fred::prelude::*;
use fred::types::{BackpressureConfig, PerformanceConfig, RedisConfig, ServerConfig};
use futures::future;
use itertools::Itertools;
use rand::Rng;
use tokio::task::JoinHandle;
use uuid::Uuid;

static LAST_REQUEST_COUNT: AtomicU64 = AtomicU64::new(0);
static REQUEST_COUNT: AtomicU64 = AtomicU64::new(0);
static LAST_ERROR_COUNT: AtomicU64 = AtomicU64::new(0);
static ERROR_COUNT: AtomicU64 = AtomicU64::new(0);
static DONE: AtomicBool = AtomicBool::new(false);

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1")]
    address: String,

    #[arg(short, long, default_value_t = 6379)]
    port: u32,

    #[arg(short, long, default_value_t = 10)]
    workers: u32,

    #[arg(short, long, default_value_t = 1000000)]
    items: u32,

    // Delete all data in Redis when done?
    #[arg(long, default_value_t = false)]
    flushall: bool,

    //
    // Test types
    //

    #[arg(long, default_value_t = false)]
    set: bool,

    #[arg(long, default_value_t = false)]
    mget: bool,

    #[arg(long, default_value_t = false)]
    script: bool,

    //
    // MGET test params
    //

    #[arg(long, default_value_t = 10)]
    mget_keys: u32,
}

#[derive(Debug, Clone)]
struct Item {
    key: String,
    value: String,
}

fn gen_items(count: u32) -> Vec<Item> {
    let mut res = vec![];
    let mut rng = rand::thread_rng();

    for _ in 0..count {
        res.push(Item {
            key: Uuid::new_v4().to_string(),
            value: rng.gen::<u32>().to_string(),
        })
    }
    res
}

async fn print_stats() {
    loop {
        if DONE.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        println!(
            "Made {} RPS got {} errors",
            REQUEST_COUNT.load(Ordering::SeqCst) - LAST_REQUEST_COUNT.load(Ordering::SeqCst),
            ERROR_COUNT.load(Ordering::SeqCst) - LAST_ERROR_COUNT.load(Ordering::SeqCst)
        );

        LAST_REQUEST_COUNT.store(REQUEST_COUNT.load(Ordering::SeqCst), Ordering::SeqCst);
        LAST_ERROR_COUNT.store(ERROR_COUNT.load(Ordering::SeqCst), Ordering::SeqCst);
    }
}

fn chunk_vector<T>(vec: Vec<T>, chunks: usize) -> Vec<Vec<T>> {
    vec.into_iter()
        .chunks((chunks) as usize)
        .into_iter()
        .map(|chunk| chunk.collect())
        .collect()
}

async fn insert_items(client: &RedisClient, items: &Vec<Item>) {
    for item in items {
        let res: Result<(), RedisError> =
            client.set(&item.key, &item.value, None, None, false).await;
        match res {
            Ok(_) => {
                REQUEST_COUNT.fetch_add(1, Ordering::SeqCst);
            }
            Err(e) => {
                println!("{:?}", e);
                ERROR_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

//
// SET benchmark
//

async fn benchmark_set(client: RedisClient, args: &Args) {
    let items = gen_items(args.items);
    let chunks: Vec<Vec<Item>> = chunk_vector(items.clone(), args.workers as usize);

    println!("Starting connection open tests");

    let mut handlers: Vec<JoinHandle<()>> = chunks
        .into_iter()
        .map(|chunk| {
            let client = client.clone();
            tokio::spawn(async move {
                while !DONE.load(Ordering::SeqCst) {
                    insert_items(&client, &chunk).await;
                }
            })
        })
        .collect();

    handlers.push(tokio::spawn(print_stats()));
    future::join_all(handlers).await;

    if args.flushall {
        println!("Cleaning up test data from Redis...");
        let res: AsyncResult<()> = client.flushall(false);
        let _ = res.await;
    }
}

//
// MGET benchmark
//

async fn benchmark_mget(client: RedisClient, args: &Args) {
    // Prepare test data
    let items = gen_items(args.items);
    let chunks: Vec<Vec<Item>> =
        chunk_vector(items.clone(), (args.items / args.workers) as usize);

    println!("Inserting test data");

    let inserters: Vec<JoinHandle<()>> = chunks
        .clone()
        .into_iter()
        .map(|chunk| {
            let client = client.clone();
            tokio::spawn(async move {
                insert_items(&client, &chunk).await;
            })
        })
        .collect();
    future::join_all(inserters).await;

    // Start the actual stress test

    REQUEST_COUNT.store(0, Ordering::SeqCst);
    ERROR_COUNT.store(0, Ordering::SeqCst);

    println!("Starting MGET tests");

    let key_chunks: Vec<Vec<String>> = chunk_vector(items.clone(), args.mget_keys as usize)
        .into_iter()
        .map(|chunk| chunk.into_iter().map(|x| x.key.clone()).collect())
        .collect();

    let mut keys = vec![];
    let mut rng = rand::thread_rng();

    for _ in 0..args.workers {
        keys.push(key_chunks[rng.gen_range(0..key_chunks.len())].clone());
    }

    let mut handlers: Vec<JoinHandle<()>> = keys
        .into_iter()
        .map(|keys| {
            let client = client.clone();

            tokio::spawn(async move {
                while !DONE.load(Ordering::SeqCst) {
                    let res: Result<(), RedisError> = client.mget(keys.clone()).await;
                    match res {
                        Ok(_) => {
                            REQUEST_COUNT.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(e) => {
                            println!("{:?}", e);
                            ERROR_COUNT.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            })
        })
        .collect();

    handlers.push(tokio::spawn(print_stats()));
    future::join_all(handlers).await;

    // Clean up

    if args.flushall {
        println!("Cleaning up test data from Redis");
        let res: AsyncResult<()> = client.flushall(false);
        let _ = res.await;
    }
}

async fn benchmark_script(client: RedisClient, args: &Args) {}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let config = RedisConfig {
        fail_fast: true,
        server: ServerConfig::new_centralized(args.address.clone(), args.port as u16),
        blocking: fred::types::Blocking::Error,
        username: None,
        password: None,
        version: fred::types::RespVersion::RESP2,
        database: None,
        tls: None,
        performance: PerformanceConfig {
            pipeline: true,
            max_command_attempts: 3,
            default_command_timeout_ms: 0,
            cluster_cache_update_delay_ms: 10,
            max_feed_count: 1000,
            backpressure: BackpressureConfig {
                disable_auto_backpressure: false,
                disable_backpressure_scaling: false,
                min_sleep_duration_ms: 100,
                max_in_flight_commands: 5000,
            },
        },
    };

    let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);
    let client = RedisClient::new(config);

    let connection_task = client.connect(Some(policy));
    let _ = client.wait_for_connect().await;

    let interrupt = tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        println!("Ctrl-c called");
        DONE.store(true, Ordering::SeqCst);
    });

    if args.set {
        let _ = future::join(benchmark_set(client, &args), interrupt).await;
    } else if args.mget {
        let _ = future::join(benchmark_mget(client, &args), interrupt).await;
    } else if args.script {
        let _ = future::join(benchmark_script(client, &args), interrupt).await;
    }

    connection_task.abort();
}
