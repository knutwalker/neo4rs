use std::sync::{atomic::AtomicUsize, Arc};

use neo4rs::*;
use tokio::sync::Semaphore;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrency() -> Result<()> {
    let uri = "bolt://127.0.0.1:7687";
    let user = "neo4j";
    let password = "neoneoneo";

    let config = ConfigBuilder::default()
        .uri(uri)
        .user(user)
        .password(password)
        .max_connections(10240)
        .build()
        .unwrap();

    let graph = Arc::new(Graph::connect(config).await.unwrap());

    let semaphore = Arc::new(Semaphore::new(1024));
    let mut acc: usize = 0;
    let connections = Arc::new(AtomicUsize::new(0));
    let successes = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));

    tokio::spawn({
        let c = connections.clone();
        let s = successes.clone();
        let e = errors.clone();
        async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                println!(
                    "live connections: {} successes: {} errors: {}",
                    c.load(std::sync::atomic::Ordering::Relaxed),
                    s.load(std::sync::atomic::Ordering::Relaxed),
                    e.load(std::sync::atomic::Ordering::Relaxed),
                );
            }
        }
    });

    loop {
        let permit = semaphore.clone();
        let _permit = permit.acquire_owned().await.unwrap();

        let connections = connections.clone();
        let successes = successes.clone();
        let errors = errors.clone();
        let cloned_graph = graph.clone();
        let cloned_acc = acc.to_string();

        tokio::spawn(async move {
            connections.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let temp = cloned_graph
                .run(neo4rs::query(&format!(
                    "MERGE (n:Num {{num: '{cloned_acc}'}});"
                )))
                .await;
            match temp {
                Ok(_) => {
                    successes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(err) => {
                    match err {
                        Error::UnexpectedMessage(msg)
                        | Error::UnknownMessage(msg)
                        | Error::AuthenticationError(msg) => {
                            println!("error: {}", msg);
                        }
                        _ => {}
                    };
                    errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            connections.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            std::mem::drop(_permit);
        });
        acc += 1;
    }
}
