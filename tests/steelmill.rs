use slog::{info, Logger};
use steelmill::{
    log::{LogConfig, LogDaemons, NodeLog, RootLog},
    unsafe_scope, Daemon, DaemonField, DaemonRef, DaemonResult, Factory, Steelmill,
};

type TestError = Box<dyn std::error::Error + Sync + Send>;

/// Steelmill insists on having a log subsystem for now.  So, we need to add
/// this boilerplate "configuration" for it.  A real system would use this to
/// store per-machine information, such as the name of the node, and other
/// configuration parameters.  Since everything in Steelmill is a Daemon,
/// we need to add a dummy daemon implementation for it too.  In a real system,
/// the daemon implementation could do things like handle dynamic configuration
/// changes (or just remain a stub).
struct TestLogConfig {
    name: String,
}

impl LogConfig for TestLogConfig {
    fn name(&self) -> &str {
        &self.name
    }
}

impl TestLogConfig {
    fn new(_factory: &TestFactory) -> Result<Self, TestError> {
        Ok(Self {
            name: "test_host".to_string(),
        })
    }
}

#[async_trait::async_trait]
impl Daemon<TestError> for TestLogConfig {
    fn name(&self) -> &'static str {
        "test log config"
    }
    async fn prepare(&self) -> Result<(), TestError> {
        Ok(())
    }
    async fn run(&self) -> Result<(), TestError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), TestError> {
        Ok(())
    }
}

struct TestDaemon {
    log: Logger,
}

impl TestDaemon {
    pub fn new(factory: &TestFactory<'_>) -> Result<Self, TestError> {
        Ok(Self {
            log: TestDaemons::node_log(factory)?.logger("test_daemon"),
        })
    }
}

#[async_trait::async_trait]
impl Daemon<TestError> for TestDaemon {
    fn name(&self) -> &'static str {
        "test daemon"
    }
    async fn prepare(&self) -> Result<(), TestError> {
        info!(self.log, "prepare");
        Ok(())
    }
    async fn run(&self) -> Result<(), TestError> {
        info!(self.log, "run");
        Ok(())
    }
    async fn stop(&self) -> Result<(), TestError> {
        info!(self.log, "stop");
        Ok(())
    }
}

#[derive(Clone, Default)]
struct TestDaemons {
    root_log: DaemonField<RootLog>,
    node_log: DaemonField<NodeLog>,
    local_config: DaemonField<TestLogConfig>,
    test_daemon: DaemonField<TestDaemon>,
}

/// It would be nice if we could #derive these accessors.  For now, we manually
/// add a lot of boilerplate in this and the next impl.
impl LogDaemons<TestError> for TestDaemons {
    type LogConfig = TestLogConfig;

    fn root_log(factory: &Factory<Self, TestError>) -> DaemonResult<DaemonRef<RootLog>, TestError> {
        Ok(factory.build(
            |daemons| &daemons.root_log,
            |_f| Err("Root log must be injected directly!".into()),
        )?)
    }

    fn node_log(factory: &Factory<Self, TestError>) -> DaemonResult<DaemonRef<NodeLog>, TestError> {
        Ok(factory.build(|daemons| &daemons.node_log, NodeLog::new)?)
    }

    fn local_config(
        factory: &Factory<Self, TestError>,
    ) -> DaemonResult<DaemonRef<TestLogConfig>, TestError> {
        Ok(factory.build(|daemons| &daemons.local_config, TestLogConfig::new)?)
    }
}

impl TestDaemons {
    pub fn test_daemon(
        factory: &Factory<Self, TestError>,
    ) -> Result<DaemonRef<TestDaemon>, TestError> {
        Ok(factory.build(|daemons| &daemons.test_daemon, TestDaemon::new)?)
    }
}

type TestFactory<'a> = Factory<'a, TestDaemons, TestError>;

#[tokio::test]
async fn hello_world() -> Result<(), Box<dyn std::error::Error>> {
    let root_log = RootLog::test_instance();
    let ambient_factory = TestFactory::new((&root_log).into());

    fn global_setup(factory: &TestFactory<'_>) -> Result<(), TestError> {
        // We currently have to set up root_log twice; this will probably be fixed at some point, but for
        // now, here's an example of how to inject manually constructed things into the factory.
        factory.inject(|daemons| &daemons.root_log, RootLog::test_instance())?;
        Ok(())
    }

    let steelmill = Steelmill::new_test_instance(ambient_factory, global_setup)?;

    unsafe_scope(|scope| {
        steelmill.spawn_node(
            scope,
            "host.1".to_string(),
            |factory| {
                let test_daemon = TestDaemons::test_daemon(factory)?;
                Ok((test_daemon,)) // typically would return more than one thing here.
            },
            |(test_daemon,)| async move {
                info!(test_daemon.log, "Test body goes here");
                Ok(())
            },
        )?;
        steelmill.spawn_node(
            scope,
            "host.2".to_string(),
            |factory| {
                let test_daemon = TestDaemons::test_daemon(factory)?;
                let another_log = TestDaemons::node_log(factory)?.logger("another_logger");
                Ok((test_daemon, another_log))
            },
            |(test_daemon, another_log)| async move {
                info!(test_daemon.log, "Test body #2 goes here");
                info!(another_log, "note the different context info for this line");
                Ok(())
            },
        )?;

        // can run multiple nodes; they share one ambient factory.
        // each can have its own test body (run lambda); the test will exit when they all return
        steelmill.done_spawning()
    })
    .await?;

    Ok(())
}
