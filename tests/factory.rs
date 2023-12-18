use async_scoped::TokioScope;
use atomic_try_update::barrier::ShutdownBarrier;
use slog::{info, Logger};
use steelmill::{
    log::{LogConfig, LogDaemons, NodeLog, RootLog},
    Daemon, DaemonField, DaemonRef, DaemonResult, Factory,
};
use tokio::select;

async fn unsafe_scope<'a, F, E: Send + Sync + 'static>(f: F) -> Result<(), E>
where
    F: FnOnce(&mut TokioScope<'a, std::result::Result<(), E>>) -> std::result::Result<(), E>,
{
    let res = unsafe { TokioScope::scope_and_collect(f).await };
    res.0?;
    for res in res.1 {
        match res {
            Ok(res) => res?,
            Err(join_error) => {
                panic!("couldn't join spawned worker: {join_error:?}")
            }
        }
    }
    Ok(())
}

struct LeafDaemon {
    log: Logger,
    shutdown_barrier: ShutdownBarrier,
}

struct DependentDaemon {
    log: Logger,
    _leaf_daemon: DaemonRef<LeafDaemon>,
}

#[derive(Debug)]
struct TestError {
    msg: String,
}

#[async_trait::async_trait]
impl Daemon<TestError> for LeafDaemon {
    fn name(&self) -> &'static str {
        "leaf_daemon"
    }

    async fn prepare(&self) -> DaemonResult<(), TestError> {
        info!(self.log, "starting");
        Ok(())
    }
    async fn run(&self) -> DaemonResult<(), TestError> {
        info!(self.log, "running");
        let mut i = 0u32;
        loop {
            select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_micros(100)) => {
                    i += 1;
                    if i > 1000 {
                        return Err(TestError{ msg: "not shut down".to_string() });
                    }
                    info!(self.log, "tick"; "i"=>i);
                },
                _ = self.shutdown_barrier.wait() => {
                    info!(self.log, "clean shutdown"; "i"=>i);
                    return Ok(())
                }
            }
        }
    }
    async fn stop(&self) -> DaemonResult<(), TestError> {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        info!(self.log, "signaling stop");
        self.shutdown_barrier.done().map_err(|err| TestError {
            msg: format!("shutdown barrier error {err:?}"),
        })?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Daemon<TestError> for DependentDaemon {
    fn name(&self) -> &'static str {
        "dependent_daemon"
    }

    async fn prepare(&self) -> DaemonResult<(), TestError> {
        info!(self.log, "starting");
        Ok(())
    }
    async fn run(&self) -> DaemonResult<(), TestError> {
        info!(self.log, "running");
        Ok(())
    }
    async fn stop(&self) -> DaemonResult<(), TestError> {
        info!(self.log, "stopping");
        Ok(())
    }
}

#[derive(Default, Clone)]
struct TestDaemons {
    log_config: DaemonField<TestLogConfig>,
    root_log: DaemonField<RootLog>,
    node_log: DaemonField<NodeLog>,
    leaf_daemon: DaemonField<LeafDaemon>,
    dependent_daemon: DaemonField<DependentDaemon>,
}

#[derive(Default)]
struct TestLogConfig {
    my_name: String,
}

impl LogConfig for TestLogConfig {
    fn name(&self) -> &str {
        &self.my_name
    }
}

#[async_trait::async_trait]
impl Daemon<TestError> for TestLogConfig {
    fn name(&self) -> &'static str {
        "test log"
    }

    async fn prepare(&self) -> DaemonResult<(), TestError> {
        Ok(())
    }

    async fn run(&self) -> DaemonResult<(), TestError> {
        Ok(())
    }

    async fn stop(&self) -> DaemonResult<(), TestError> {
        Ok(())
    }
}

impl TestLogConfig {
    fn new<E: std::fmt::Debug + Send + Sync + 'static, Daemons: LogDaemons<E>>(
        _factory: &Factory<Daemons, E>,
    ) -> DaemonResult<Self, E> {
        Ok(Self {
            my_name: "Unit test".to_string(),
        })
    }
}

/// This uses a str for its error type; replace it with whatever error
/// type is native for your application.
impl LogDaemons<TestError> for TestDaemons {
    type LogConfig = TestLogConfig;

    fn root_log(
        factory: &Factory<'_, Self, TestError>,
    ) -> DaemonResult<DaemonRef<RootLog>, TestError> {
        factory
            .build(
                |daemons| &daemons.root_log,
                |_f| {
                    Err(TestError {
                        msg: "Root log must be injected directly!".to_string(),
                    })
                },
            )
            .map_err(|err| TestError {
                msg: format!("{err:?}"),
            })
    }

    fn node_log(
        factory: &Factory<'_, Self, TestError>,
    ) -> DaemonResult<DaemonRef<NodeLog>, TestError> {
        factory
            .build(|daemons| &daemons.node_log, NodeLog::new)
            .map_err(|err| TestError {
                msg: format!("{err:?}"),
            })
    }

    fn local_config(
        factory: &Factory<'_, Self, TestError>,
    ) -> DaemonResult<DaemonRef<TestLogConfig>, TestError> {
        factory
            .build(|daemons| &daemons.log_config, TestLogConfig::new)
            .map_err(|err| TestError {
                msg: format!("{err:?}"),
            })
    }
}

impl TestDaemons {
    fn leaf_daemon(
        factory: &Factory<TestDaemons, TestError>,
    ) -> DaemonResult<DaemonRef<LeafDaemon>, TestError> {
        factory
            .build(
                |daemons| &daemons.leaf_daemon,
                |_| {
                    let shutdown_barrier = ShutdownBarrier::default();
                    Ok(LeafDaemon {
                        log: TestDaemons::node_log(factory)?.logger("leaf daemon"),
                        shutdown_barrier,
                    })
                },
            )
            .map_err(|err| TestError {
                msg: format!("{err:?}"),
            })
    }

    fn dependent_daemon(
        factory: &Factory<TestDaemons, TestError>,
    ) -> DaemonResult<DaemonRef<DependentDaemon>, TestError> {
        factory
            .build(
                |daemons| &daemons.dependent_daemon,
                |factory| {
                    Ok(DependentDaemon {
                        log: TestDaemons::node_log(factory)?.logger("leaf daemon"),
                        _leaf_daemon: Self::leaf_daemon(factory)?,
                    })
                },
            )
            .map_err(|err| TestError {
                msg: format!("{err:?}"),
            })
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn factory() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let root_log = RootLog::runtime_instance();
    let mut factory = Factory::<'_, TestDaemons, _>::new((&root_log).into());
    factory.inject(|daemons| &daemons.root_log, RootLog::test_instance())?;
    let _ = TestDaemons::dependent_daemon(&factory).map_err(|err| err.msg)?;
    factory.finalize_daemons();

    // You could use tokio::spawn here, but then you'd need to wrap factory in an Arc.  The runtime
    // overhead would not be a big deal for unit tests.
    let factory = &factory;
    unsafe_scope(|s| {
        s.spawn(factory.prepare());
        Ok(())
    })
    .await?;

    unsafe_scope(|s| {
        factory.run(s, None);
        s.spawn(factory.stop());
        Ok(())
    })
    .await?;

    Ok(())
}
