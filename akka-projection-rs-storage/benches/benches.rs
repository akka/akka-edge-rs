use std::{
    collections::HashMap, future::Future, path::PathBuf, pin::Pin, sync::Arc, time::Duration,
};

use akka_persistence_rs::{Offset, WithOffset};
use akka_projection_rs::{Handler, HandlerError, SourceProvider};
use async_stream::stream;
use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use streambed::secret_store::{
    AppRoleAuthReply, Error, GetSecretReply, SecretData, SecretStore, UserPassAuthReply,
};
use tokio::sync::{oneshot, Notify};
use tokio_stream::Stream;

const NUM_EVENTS: usize = 10_000;

struct TestEnvelope {
    offset: u64,
}

impl WithOffset for TestEnvelope {
    fn offset(&self) -> Offset {
        Offset::Sequence(self.offset)
    }
}

struct TestSourceProvider;

#[async_trait]
impl SourceProvider for TestSourceProvider {
    type Envelope = TestEnvelope;

    async fn source<F, FR>(
        &mut self,
        offset: F,
    ) -> Pin<Box<dyn Stream<Item = Self::Envelope> + Send + 'async_trait>>
    where
        F: Fn() -> FR + Send + Sync,
        FR: Future<Output = Option<Offset>> + Send,
    {
        let _ = offset().await;
        Box::pin(stream!(for offset in 0..NUM_EVENTS as u64 {
            yield TestEnvelope { offset };
        }))
    }
}

struct TestHandler {
    events_processed: Arc<Notify>,
}

#[async_trait]
impl Handler for TestHandler {
    type Envelope = TestEnvelope;

    async fn process(&mut self, envelope: Self::Envelope) -> Result<(), HandlerError> {
        const LAST_OFFSET: u64 = NUM_EVENTS as u64 - 1;
        if envelope.offset == LAST_OFFSET {
            self.events_processed.notify_one();
        }
        Ok(())
    }
}

#[derive(Clone)]
struct NoopSecretStore;

#[async_trait]
impl SecretStore for NoopSecretStore {
    async fn approle_auth(
        &self,
        _role_id: &str,
        _secret_id: &str,
    ) -> Result<AppRoleAuthReply, Error> {
        panic!("should not be called")
    }

    async fn create_secret(
        &self,
        _secret_path: &str,
        _secret_data: SecretData,
    ) -> Result<(), Error> {
        panic!("should not be called")
    }

    async fn get_secret(&self, _secret_path: &str) -> Result<Option<GetSecretReply>, Error> {
        let mut data = HashMap::new();
        data.insert(
            "value".to_string(),
            "ed31e94c161aea6ff2300c72b17741f71b616463f294dac0542324bbdbf8a2de".to_string(),
        );

        Ok(Some(GetSecretReply {
            lease_duration: 10,
            data: SecretData { data },
        }))
    }

    async fn token_auth(&self, _token: &str) -> Result<(), Error> {
        panic!("should not be called")
    }

    async fn userpass_auth(
        &self,
        _username: &str,
        _password: &str,
    ) -> Result<UserPassAuthReply, Error> {
        panic!("should not be called")
    }

    async fn userpass_create_update_user(
        &self,
        _current_username: &str,
        _username: &str,
        _password: &str,
    ) -> Result<(), Error> {
        panic!("should not be called")
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("project events", move |b| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();

        let events_processed = Arc::new(Notify::new());
        let (_registration_projection_command, registration_projection_command_receiver) =
            oneshot::channel();

        let task_events_processed = events_processed.clone();
        let _ = rt.spawn(async move {
            let storage_path = PathBuf::from("/dev/null");

            akka_projection_rs_storage::run(
                &NoopSecretStore,
                &"some-secret-path",
                &storage_path,
                registration_projection_command_receiver,
                TestSourceProvider,
                TestHandler {
                    events_processed: task_events_processed,
                },
                Duration::from_secs(1), // Not testing out fs performance
            )
            .await
        });

        b.to_async(&rt).iter(|| {
            let task_events_processed = events_processed.clone();
            async move {
                tokio::spawn(async move {
                    task_events_processed.notified().await;
                })
                .await
            }
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
