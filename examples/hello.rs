use std::sync::atomic::{AtomicU64, Ordering};

use clap::Parser;
use slog::{info, Logger};
use steelmill::{
    log::{LogConfig, LogDaemons, NodeLog, RootLog},
    Daemon, DaemonField, DaemonRef, DaemonResult, Factory, Steelmill,
};

type ExampleError = Box<dyn std::error::Error + Sync + Send>;

/// Steelmill insists on having a log subsystem for now.  So, we need to add
/// this boilerplate "configuration" for it.  A real system would use this to
/// store per-machine information, such as the name of the node, and other
/// configuration parameters.  Since everything in Steelmill is a Daemon,
/// we need to add a dummy daemon implementation for it too.  In a real system,
/// the daemon implementation could do things like handle dynamic configuration
/// changes (or just remain a stub).
struct ExampleLogConfig {
    name: String,
}

impl LogConfig for ExampleLogConfig {
    fn name(&self) -> &str {
        &self.name
    }
}

impl ExampleLogConfig {
    fn new(_factory: &ExampleFactory) -> Result<Self, ExampleError> {
        Ok(Self {
            name: "example_host".to_string(),
        })
    }
}

#[async_trait::async_trait]
impl Daemon<ExampleError> for ExampleLogConfig {
    fn name(&self) -> &'static str {
        "Example log config"
    }
    async fn prepare(&self) -> Result<(), ExampleError> {
        Ok(())
    }
    async fn run(&self) -> Result<(), ExampleError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ExampleError> {
        Ok(())
    }
}

struct AliceDaemon {
    log: Logger,
    guest_book: DaemonRef<GuestBookDaemon>,
}

impl AliceDaemon {
    pub fn new(factory: &ExampleFactory<'_>) -> Result<Self, ExampleError> {
        Ok(Self {
            log: ExampleDaemons::node_log(factory)?.logger("alice_daemon"),
            guest_book: ExampleDaemons::guest_book_daemon(factory)?,
        })
    }
}

#[async_trait::async_trait]
impl Daemon<ExampleError> for AliceDaemon {
    fn name(&self) -> &'static str {
        "Alice daemon"
    }
    async fn prepare(&self) -> Result<(), ExampleError> {
        info!(self.log, "prepare");
        Ok(())
    }
    async fn run(&self) -> Result<(), ExampleError> {
        info!(self.log, "run");
        self.guest_book.visitor(self.name());
        Ok(())
    }
    async fn stop(&self) -> Result<(), ExampleError> {
        info!(self.log, "stop");
        Ok(())
    }
}

struct BobDaemon {
    log: Logger,
    guest_book: DaemonRef<GuestBookDaemon>,
}

impl BobDaemon {
    pub fn new(factory: &ExampleFactory<'_>) -> Result<Self, ExampleError> {
        Ok(Self {
            log: ExampleDaemons::node_log(factory)?.logger("bob_daemon"),
            guest_book: ExampleDaemons::guest_book_daemon(factory)?,
        })
    }
}

#[async_trait::async_trait]
impl Daemon<ExampleError> for BobDaemon {
    fn name(&self) -> &'static str {
        "Bob daemon"
    }
    async fn prepare(&self) -> Result<(), ExampleError> {
        info!(self.log, "prepare");
        Ok(())
    }
    async fn run(&self) -> Result<(), ExampleError> {
        info!(self.log, "run");
        self.guest_book.visitor(self.name());
        Ok(())
    }
    async fn stop(&self) -> Result<(), ExampleError> {
        info!(self.log, "stop");
        Ok(())
    }
}

struct GuestBookDaemon {
    log: Logger,
    guest_count: AtomicU64,
}

impl GuestBookDaemon {
    pub fn new(factory: &ExampleFactory<'_>) -> Result<Self, ExampleError> {
        Ok(Self {
            log: ExampleDaemons::node_log(factory)?.logger("guest_book_daemon"),
            guest_count: 0.into(),
        })
    }
    pub fn visitor(&self, name: &str) {
        let current_count = self.guest_count.fetch_add(1, Ordering::Relaxed) + 1;
        info!(self.log, "With {} there are {} guests", name, current_count);
    }
}

#[async_trait::async_trait]
impl Daemon<ExampleError> for GuestBookDaemon {
    fn name(&self) -> &'static str {
        "GuestBook daemon"
    }
    async fn prepare(&self) -> Result<(), ExampleError> {
        info!(self.log, "prepare");
        assert_eq!(
            self.guest_count.load(Ordering::Relaxed),
            0,
            "Something ran during prepare phase!"
        );
        Ok(())
    }
    async fn run(&self) -> Result<(), ExampleError> {
        info!(self.log, "run");
        Ok(())
    }
    async fn stop(&self) -> Result<(), ExampleError> {
        info!(self.log, "stop");
        Ok(())
    }
}

#[derive(Clone, Default)]
struct ExampleDaemons {
    root_log: DaemonField<RootLog>,
    node_log: DaemonField<NodeLog>,
    local_config: DaemonField<ExampleLogConfig>,
    alice_daemon: DaemonField<AliceDaemon>,
    bob_daemon: DaemonField<BobDaemon>,
    guest_book_daemon: DaemonField<GuestBookDaemon>,
}

/// It would be nice if we could #derive these accessors.  For now, we manually
/// add a lot of boilerplate in this and the next impl.
impl LogDaemons<ExampleError> for ExampleDaemons {
    type LogConfig = ExampleLogConfig;

    fn root_log(
        factory: &Factory<Self, ExampleError>,
    ) -> DaemonResult<DaemonRef<RootLog>, ExampleError> {
        Ok(factory.build(
            |daemons| &daemons.root_log,
            |_f| Err("Root log must be injected directly!".into()),
        )?)
    }

    fn node_log(
        factory: &Factory<Self, ExampleError>,
    ) -> DaemonResult<DaemonRef<NodeLog>, ExampleError> {
        Ok(factory.build(|daemons| &daemons.node_log, NodeLog::new)?)
    }

    fn local_config(
        factory: &Factory<Self, ExampleError>,
    ) -> DaemonResult<DaemonRef<ExampleLogConfig>, ExampleError> {
        Ok(factory.build(|daemons| &daemons.local_config, ExampleLogConfig::new)?)
    }
}

impl ExampleDaemons {
    pub fn alice_daemon(
        factory: &Factory<Self, ExampleError>,
    ) -> Result<DaemonRef<AliceDaemon>, ExampleError> {
        Ok(factory.build(|daemons| &daemons.alice_daemon, AliceDaemon::new)?)
    }
    pub fn bob_daemon(
        factory: &Factory<Self, ExampleError>,
    ) -> Result<DaemonRef<BobDaemon>, ExampleError> {
        Ok(factory.build(|daemons| &daemons.bob_daemon, BobDaemon::new)?)
    }
    pub fn guest_book_daemon(
        factory: &Factory<Self, ExampleError>,
    ) -> Result<DaemonRef<GuestBookDaemon>, ExampleError> {
        Ok(factory.build(|daemons| &daemons.guest_book_daemon, GuestBookDaemon::new)?)
    }
}

type ExampleFactory<'a> = Factory<'a, ExampleDaemons, ExampleError>;

const ABOUT: &str = r#"
Steelmill hello world example

This binary lets you instantiate sample daemons AliceDaemon and BobDaemon.  They register
their existence with GuestBookDaemon, then exit.  GuestBookDaemon is only instantiated if
it is needed.

Example invocations: 
  cargo run --example hello --
  cargo run --example hello -- --daemon alice --daemon bob
"#;

#[derive(clap::ValueEnum, Clone, Debug, PartialOrd, Ord, Eq, PartialEq)]
enum DaemonNames {
    Alice,
    Bob,
}
#[derive(Parser, Debug)]
#[clap(author, version, about=ABOUT)]
struct Opt {
    /// list of daemons to run
    #[clap(long = "daemon", short = 'd')]
    daemons: Vec<DaemonNames>,
}

#[tokio::main]
async fn main() -> Result<(), ExampleError> {
    let args = Opt::parse();
    println!("args: {args:?}");

    let root_log = RootLog::test_instance();
    let ambient_factory = ExampleFactory::new((&root_log).into());

    fn global_setup(factory: &ExampleFactory<'_>) -> Result<(), ExampleError> {
        // We currently have to set up root_log twice; this will probably be fixed at some point, but for
        // now, here's an example of how to inject manually constructed things into the factory.
        factory.inject(|daemons| &daemons.root_log, RootLog::test_instance())?;
        Ok(())
    }

    // Normally, you'd use a lambda, like this:
    let setup = |factory: &_| {
        let mut alice_daemon = None;
        for daemon in args.daemons {
            match daemon {
                DaemonNames::Alice => alice_daemon = Some(ExampleDaemons::alice_daemon(factory)?),
                DaemonNames::Bob => {
                    // Don't need to keep a reference to a Daemon for it to run as normal.
                    _ = ExampleDaemons::bob_daemon(factory)?;
                }
            }
        }
        Ok((alice_daemon,))
    };

    // This is more-or-less equivalent, and makes the function signature more explicit.
    fn _setup_fn(factory: &ExampleFactory) -> Result<(DaemonRef<AliceDaemon>,), ExampleError> {
        let alice_daemon = ExampleDaemons::alice_daemon(factory)?;
        Ok((alice_daemon,))
    }

    async fn run(daemons: (Option<DaemonRef<AliceDaemon>>,)) -> Result<(), ExampleError> {
        let (alice,) = daemons;

        if let Some(alice) = alice {
            info!(alice.log, "Hello!");
        }
        Ok(())
    }

    let steelmill = Steelmill::new_runtime_instance(ambient_factory, global_setup)?;
    steelmill
        .run_node("taunton".to_string(), setup, run)
        .await?;
    Ok(())
}
