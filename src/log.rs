//! A `Daemon`-ized wrapper around `slog` that provides `Factory`- and `Daemon`-scoped structured logging.
//! This is included in this crate because `Factory` emits uses this `Daemon` to provide runtime diagnostic
//! information.

use std::sync::{Arc, Mutex};

use crate::{DaemonRef, DaemonResult, Factory};

use super::Daemon;

use slog::{self, debug, Never, SendSyncRefUnwindSafeDrain};
use slog::{o, Drain, Logger};
use slog_async;
use slog_term;

/// To use this log, you will need to register it with your `DaemonBundle`, by
/// having it `impl LogDaemons`.
pub struct RootLog {
    pub root: Logger,
}

impl RootLog {
    /// Invoked by the Factory to get a logger for itself; generally not called by applications.
    pub fn get_factory_logger(&self, host: &str) -> Logger {
        self.root
            .new(o! {"daemon"=>"factory".to_string(), "host"=>host.to_string()})
    }
    /// Used to register a logger for sub-factories.  Invoke this against the root_log of the parent
    /// (ambient) factory, and pass the name of the sub-factory into `logger()` to obtain the
    /// sub-factory logger.
    ///
    /// TODO: Can this be handled automatically by `from_ambient()`?
    pub fn get_ambient_logger(&self, daemon: &str) -> Logger {
        self.root
            .new(o! {"daemon"=>daemon.to_string(), "host"=>"*".to_string()})
    }

    /// A logger that's appropiate for emitting unit-test diagnstics.
    pub fn get_test_logger(&self, test: &str) -> Logger {
        self.root
            .new(o! {"daemon"=>test.to_string(), "host"=>"*".to_string()})
    }

    // A root_log configured for human-readable output.
    pub fn test_instance() -> Arc<RootLog> {
        Arc::new(Self::build(true))
    }

    // A root_log configured to emit JSON for use by downstream observability infrastructure.
    pub fn runtime_instance() -> Arc<RootLog> {
        Arc::new(Self::build(false))
    }

    fn build(test: bool) -> RootLog {
        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "info")
        }
        let io = slog_term::TestStdoutWriter;
        let root = if test {
            let decorator = slog_term::PlainDecorator::new(io /*std::io::stdout()*/);
            let drain = slog_term::FullFormat::new(decorator).build();
            // Note the use of Mutex in unit test mode.  We have to do this because slog_async breaks cargo test's
            // ability to capture stdout.
            let drain = slog_envlogger::new(Mutex::new(drain)).fuse();
            slog::Logger::root(drain, o!())
        } else {
            let drain = slog_json::Json::default(std::io::stdout()).map(slog::Fuse);
            let drain = slog_async::Async::new(drain).build().fuse();
            let drain = slog_envlogger::new(drain);
            slog::Logger::root(drain, o!())
        };
        Self { root }
    }
}

#[async_trait::async_trait]
impl<E> Daemon<E> for RootLog {
    fn name(&self) -> &'static str {
        "root_log"
    }

    async fn prepare(&self) -> DaemonResult<(), E> {
        Ok(())
    }
    async fn run(&self) -> DaemonResult<(), E> {
        Ok(())
    }
    async fn stop(&self) -> DaemonResult<(), E> {
        Ok(())
    }
}

/// The main entrypoint for the log wrapper.  Each Daemon that needs to emit log entries
/// should invoke `node_log(factory)` and then invoke `logger("daemon_name") on the result
/// to get a properly-configured `slog` logger.
pub struct NodeLog {
    log: Logger<Arc<dyn SendSyncRefUnwindSafeDrain<Ok = (), Err = Never>>>,
}

/// The name the root log should use to refer to the current context.  In order to
/// use this log wrapper, you need to provide a Daemon instance that implements this
/// trait and instantiate factory to use a `DaemonBundle` that impements the
/// `LogDaemons` trait.
pub trait LogConfig {
    /// A human readable name describing the root Factory (e.g., a hostname)
    fn name(&self) -> &str;
}

/// If you want to use the built-in, slog-based logging facilities, then you will
/// need to implement this trait.  Ideally, you would be able to `#derive` this,
/// but, until then, you can copy paste the implementation from the tests, in
/// `tests/factory.rs`.
pub trait LogDaemons<E: std::fmt::Debug + Send + Sync + 'static>: Default {
    type LogConfig: LogConfig;

    /// This returns a reference to the shared log instance that is used to build
    /// factory-specific logs.  You almost certainly want `node_log()` instead.  
    fn root_log(factory: &Factory<Self, E>) -> DaemonResult<DaemonRef<RootLog>, E>;

    /// This returns a reference to a per-factory log.  You should use
    /// `node_log(factory)?.logger("my_daemon")` to create a log instance that
    /// is scoped to your `Daemon`'s instance.
    fn node_log(factory: &Factory<Self, E>) -> DaemonResult<DaemonRef<NodeLog>, E>;
    fn local_config(factory: &Factory<Self, E>) -> DaemonResult<DaemonRef<Self::LogConfig>, E>;
}

impl NodeLog {
    /// A constructor that you can pass into `factory.build` from your `DaemonBundle`'s `node_log` method.
    pub fn new<E: std::fmt::Debug + Send + Sync + 'static, Daemons: LogDaemons<E>>(
        factory: &Factory<Daemons, E>,
    ) -> DaemonResult<Self, E> {
        let config = Daemons::local_config(factory)?;
        let root = Daemons::root_log(factory)?;
        let log = root.root.new(o!("host"=>config.name().to_string()));
        debug!(log, "Initialized log");
        Ok(Self { log })
    }
    /// Create a new `slog` logger that's scoped to the current `Daemon` instance.
    pub fn logger(&self, name: &str) -> Logger {
        self.log.new(o! {"daemon"=>name.to_string()})
    }
}

#[async_trait::async_trait]
impl<E> Daemon<E> for NodeLog {
    fn name(&self) -> &'static str {
        "node_log"
    }

    async fn prepare(&self) -> DaemonResult<(), E> {
        Ok(())
    }
    async fn run(&self) -> DaemonResult<(), E> {
        Ok(())
    }
    async fn stop(&self) -> DaemonResult<(), E> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde::Serialize;
    use slog::info;
    use slog_derive::SerdeValue;

    #[derive(Debug, Default, Clone, Serialize, SerdeValue)]
    struct Test {
        json: serde_json::Value,
    }

    #[test]
    fn log_json() -> DaemonResult<(), String> {
        let the_file = r#"{
            "FirstName": "John",
            "LastName": "Doe",
            "Age": 43,
            "Address": {
                "Street": "Downing Street 10",
                "City": "London",
                "Country": "Great Britain"
            },
            "PhoneNumbers": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;

        let json: serde_json::Value =
            serde_json::from_str(the_file).expect("JSON was not well-formatted");

        let log = RootLog::test_instance().get_factory_logger("test");
        info!(log, "hello test"; "t"=>Test{ json });

        Ok(())
    }
}
