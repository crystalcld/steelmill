use std::{
    io::Write,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{log::LogDaemons, DaemonRef, Factory, FactoryError};
use async_scoped::TokioScope;
use atomic_try_update::barrier::ShutdownBarrier;
use futures_util::Future;
use slog::{debug, error, info, Logger};

/// Do not invoke this function directly.  It is only exposed in the public API for test purposes,
/// and will be deleted later.
pub async fn unsafe_scope<'a, F, E: Send + Sync + 'static>(f: F) -> std::result::Result<(), E>
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

#[allow(clippy::type_complexity)]
struct HookState {
    original_hook: Option<Box<dyn Fn(&std::panic::PanicInfo) + Send>>,
    shutdown_barriers: Vec<(Logger, DaemonRef<ShutdownBarrier>)>,
}

static HOOK_SETUP: Mutex<HookState> = Mutex::new(HookState {
    original_hook: None,
    shutdown_barriers: Vec::new(),
});

fn install_panic_hook<E: std::fmt::Debug>(
    log: Logger,
    shutdown_barrier: DaemonRef<ShutdownBarrier>,
) -> Result<(), FactoryError<E>> {
    let mut hook = HOOK_SETUP.lock().map_err(|e| {
        FactoryError::InternalError(format!("unable to install shutdown hook: {e:?}"))
    })?;
    hook.shutdown_barriers.push((log, shutdown_barrier));
    if hook.original_hook.is_none() {
        hook.original_hook = Some(std::panic::take_hook());
        drop(hook);
        std::panic::set_hook(Box::new(|panic_info| {
            let mut stdout = std::io::stdout().lock();

            _ = stdout.write_all("\nNot sure which test panicked\n".as_bytes());
            _ = stdout.write_all(format!("\n{}\n", panic_info).as_bytes());

            _ = stdout
                .write_all("\nIf the following doesn't work, try: RUST_BACKTRACE=1 cargo test -- --nocapture\n".as_bytes());

            _ = stdout
                .write_all("\nSleeping for 15 seconds to let running tests finish\n".as_bytes());
            drop(stdout);

            std::thread::sleep(Duration::from_secs(15));

            let mut stdout = std::io::stdout().lock();

            match HOOK_SETUP.lock() {
                Ok(hook) => {
                    _ = stdout.write_all(
                        "\nSignaling any remaining factories to stop.  If the hung test handles\n"
                            .as_bytes(),
                    );
                    _ = stdout
                        .write_all("cancellation errors properly then this should make it exit with failure.\n".as_bytes());
                    _ = stdout.write_all(
                        "\nNote: As a side effect, this will cause any still-running non-faulty\n"
                            .as_bytes(),
                    );
                    _ = stdout
                        .write_all("      tests to fail!  So, it is possible that the test that panicked has\n".as_bytes());
                    _ = stdout
                        .write_all("      bad error handling, and will 'pass', but some unrelated tests will\n".as_bytes());
                    _ = stdout
                        .write_all("      'fail'.  Be sure to look for the word 'panic' (case-insensitve) in\n".as_bytes());
                    _ = stdout.write_all("      the failed tests' logs!\n\n".as_bytes());

                    for (_log, shutdown_barrier) in &hook.shutdown_barriers {
                        _ = shutdown_barrier.cancel();
                    }

                    hook.original_hook.as_deref().unwrap()(panic_info);
                }
                Err(e) => {
                    _ = stdout.write_all(
                        format!("error in panic handler getting hook state: {e:?}\n").as_bytes(),
                    );
                }
            }
        }));
    } else {
        drop(hook);
    }
    Ok(())
}

pub struct Steelmill<'a, D, E>
where
    D: Default,
    E: Send + Sync,
{
    ambient_factory: Factory<'a, D, E>,
    shutdown_barrier: DaemonRef<ShutdownBarrier>,
}

impl<'a, DaemonBundle, Err> Steelmill<'a, DaemonBundle, Err>
where
    DaemonBundle: LogDaemons<Err> + Clone + Default + Sync + Send,
    Err: 'static + std::fmt::Debug + Send + Sync,
{
    /// Instantiate an Arena appropriate for normal operation.
    pub fn new_runtime_instance(
        ambient_factory: Factory<'a, DaemonBundle, Err>,
        global_setup: impl FnOnce(&Factory<'a, DaemonBundle, Err>) -> Result<(), Err>,
    ) -> Result<Self, FactoryError<Err>> {
        Self::instance(false, ambient_factory, global_setup)
    }

    /// Instantiate an Arena appropriate for testing.  This installs a
    /// panic hook that helps debug test failures.
    pub fn new_test_instance(
        ambient_factory: Factory<'a, DaemonBundle, Err>,
        global_setup: impl FnOnce(&Factory<'a, DaemonBundle, Err>) -> Result<(), Err>,
    ) -> Result<Self, FactoryError<Err>> {
        Self::instance(true, ambient_factory, global_setup)
    }

    fn instance(
        panic_hook: bool,
        mut ambient_factory: Factory<'a, DaemonBundle, Err>,
        global_setup: impl FnOnce(&Factory<'a, DaemonBundle, Err>) -> Result<(), Err>,
    ) -> Result<Self, FactoryError<Err>> {
        global_setup(&ambient_factory)
            .map_err(|err| FactoryError::DaemonSetupFailed("global setup failed", err))?;
        let shutdown_barrier: Arc<ShutdownBarrier> = Default::default();
        if panic_hook {
            let log = DaemonBundle::root_log(&ambient_factory)
                .map_err(|err| {
                    FactoryError::DaemonSetupFailed("could not get ambient logger", err)
                })?
                .get_ambient_logger("steelmill_test");
            install_panic_hook::<Err>(log, (&shutdown_barrier).into())?;
        }
        ambient_factory.finalize_daemons();

        Ok(Self {
            ambient_factory,
            shutdown_barrier: (&shutdown_barrier).into(),
        })
    }

    /// Either call spawn_node once per node, followed by done_spawning to run the factory
    /// in the background, or call run_node as you would any other async function.
    pub fn spawn_node<
        'b,
        Daemons: 'b + Send + Sync,
        Fut: 'b + Send + Future<Output = Result<(), Err>>,
    >(
        &'b self,
        scope: &mut async_scoped::Scope<'b, Result<(), FactoryError<Err>>, async_scoped::Tokio>,
        host: String,
        setup: impl FnOnce(&Factory<'a, DaemonBundle, Err>) -> Result<Daemons, Err> + Send + Sync + 'b,
        run: impl FnOnce(Daemons) -> Fut + Send + Sync + 'b,
    ) -> Result<(), FactoryError<Err>> {
        self.shutdown_barrier.spawn().map_err(|err| {
            FactoryError::InternalError(format!(
                "spawn_node(): error invoking shutdown_barrier.spawn(): {err:?}"
            ))
        })?;
        scope.spawn(self.run_node(host, setup, run));
        Ok(())
    }
    pub fn done_spawning(&self) -> Result<(), FactoryError<Err>> {
        self.shutdown_barrier.done().map_err(|err| {
            FactoryError::InternalError(format!(
                "done_spawning(): error invoking shutdown_barrier.done(): {err:?}"
            ))
        })?;

        Ok(())
    }

    /// Either call spawn_node once per node, followed by done_spawning to run the factory
    /// in the background, or call run_node as you would any other async function.
    pub async fn run_node<Daemons: Send + Sync, Fut: Send + Future<Output = Result<(), Err>>>(
        &self,
        host: String,
        setup: impl FnOnce(&Factory<'a, DaemonBundle, Err>) -> Result<Daemons, Err>,
        run: impl FnOnce(Daemons) -> Fut + Send + Sync,
    ) -> Result<(), FactoryError<Err>> {
        let mut factory = Factory::from_ambient(&self.ambient_factory, host);

        let daemons = match setup(&factory) {
            Ok(daemons) => daemons,
            Err(e) => {
                _ = self.shutdown_barrier.cancel();
                return Err(FactoryError::DaemonSetupFailed("setup returned error", e));
            }
        };

        // Grab the log after our caller instantiated their stuff in setup().  It's possible
        // they need to set up their log context (hostname, etc)
        let log = DaemonBundle::node_log(&factory)
            .map_err(|err| FactoryError::DaemonSetupFailed("could not get node log", err))?
            .logger("runtime");
        debug!(log, "Set up factory");

        factory.finalize_daemons();

        let factory = &factory;
        debug!(log, "Preparing factory");
        unsafe_scope(move |s| {
            s.spawn(async move { factory.prepare().await });
            Ok(())
        })
        .await?;
        info!(log, "Running daemons and test");
        let log = &log;
        unsafe_scope(move |s| {
            factory.run(s, Some(&self.shutdown_barrier));
            s.spawn(async move {
                match run(daemons).await {
                    Err(e) => {
                        error!(log, "run() lambda failed"; "e"=>#?e);
                        _ = self.shutdown_barrier.cancel();
                        Err(FactoryError::DaemonRunFailed("error from top-level run", e))?
                    }
                    Ok(()) => {
                        self.shutdown_barrier.done().map_err(|err| {
                            FactoryError::InternalError(format!(
                                "run_node(): shutdown barrier.done() failed: {err:?}"
                            ))
                        })?;
                        Ok(())
                    }
                }
            });
            s.spawn(async move {
                self.shutdown_barrier.wait().await.map_err(|err| {
                    FactoryError::InternalError(format!(
                        "run_node(): shutdown_barrier.wait() failed: {err:?}"
                    ))
                })?;
                debug!(log, "Stopping daemons");
                factory.stop().await
            });
            Ok(())
        })
        .await?;
        info!(log, "Clean shutdown completed");
        Ok(())
    }
}
