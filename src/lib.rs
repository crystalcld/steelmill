use atomic_try_update::barrier::ShutdownBarrier;
use atomic_try_update::once::OnceLockFree;
use log::RootLog;
use slog::{debug, error, o, warn, Logger};

use atomic_try_update::stack::Stack;
use std::any::type_name;
use std::borrow::Borrow;
use std::fmt::Display;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub mod log;
mod steelmill;

pub use steelmill::Steelmill;

/// XXX get rid of this pub!
pub use steelmill::unsafe_scope;

/// Errors returned by `Factory`, including a `Cancellation` error that means
/// we are in the process of shutting down.
#[derive(Debug)]
pub enum FactoryError<E: std::fmt::Debug> {
    Cancellation,
    CyclicDependency(&'static str),
    CouldNotConstruct(&'static str, E),
    FieldAlreadySet(&'static str),
    DaemonSetupFailed(&'static str, E),
    DaemonPrepareFailed(&'static str, E),
    DaemonRunFailed(&'static str, E),
    DaemonStopFailed(&'static str, E),
    InternalError(String),
}

impl<E: std::fmt::Debug> Display for FactoryError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl<E: std::fmt::Debug> std::error::Error for FactoryError<E> {}
pub type FactoryResult<T, E> = std::result::Result<T, FactoryError<E>>;

pub type DaemonResult<T, E> = std::result::Result<T, E>;

/// A `DaemonRef`` is like an `std::sync::Arc`, except:
///  - It does not generate any cross-processor
///    synchronization traffic except at construction
///    and drop.
///  - It cannot be cloned
pub struct DaemonRef<T: ?Sized> {
    inner: std::sync::Arc<T>,
}

impl<T: ?Sized> From<&std::sync::Arc<T>> for DaemonRef<T> {
    fn from(val: &std::sync::Arc<T>) -> Self {
        DaemonRef { inner: val.clone() }
    }
}
impl<T: ?Sized> std::ops::Deref for DaemonRef<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T: ?Sized> Borrow<T> for DaemonRef<T> {
    fn borrow(&self) -> &T {
        &self.inner
    }
}

/// Application-level services that are managed by `Factory`.  A `Daemon` is the unit
/// of depenency injection, and includes methods that allow the factory to manage
/// `Daemon` lifecycles.  Although lifecycle management is handled via this trait
/// (and therefore the `async_trait` crate, which incurs runtime overhead), application
/// specific interfaces are generally provided directly by the `Daemon` without any
/// virtual method or other runtime overhead.
#[async_trait::async_trait]
pub trait Daemon<E>: Send + Sync {
    fn name(&self) -> &'static str;
    /// Daemons are started in the order they were constructed.  Construction
    /// order is a valid topological sort of the daemon dependency graph.
    /// `prepare()` simply prepares the daemon to start working, but does not
    /// perform work.
    ///
    /// After this function returns, calls from upper layers into this daemon
    /// should (eventually) be processed correctly.  Environment startup will
    /// wait for prepare to complete before allowing upper levels to submit
    /// requests to this daemon, so prepare should not attempt to actually
    /// process incoming requests.
    ///
    /// This function should be a no-op for the vast majority of `Daemon`s,
    /// since it cannot make asynchronous requests into its dependencies,
    /// and all synchronous work should be handled by the constructor.
    async fn prepare(&self) -> DaemonResult<(), E>;

    /// The bulk of `Daemon` work happens inside `run`, which is invoked with
    /// no ordering guarantees between daemons.  This call must not return
    /// until this daemon has permanently quiesced, which means:
    ///
    ///  - New incoming requests immediately return a cancellation error.
    ///  - There are no outstanding requests into lower layers.
    async fn run(&self) -> DaemonResult<(), E>;

    /// `Daemon`s are stopped in reverse construction order.  This ensures that
    /// `Daemon`s are stopped before their dependencies, but it does not ensure
    /// that run will return in any particular order.  This is really an
    /// asynchronous "pre-stop" function, except that `stop()` can wait for
    /// calls into this `Daemon`'s dependencies to complete without risking
    /// deadlock.  (Such waiting is generally unnecessary.)
    ///
    /// In a typical daemon implementaion of stop, top-level daemons simply
    /// stop admitting new work, and immediately respond to any new incoming
    /// requests with cancellation errors.
    ///
    /// All daemons (including mid-level daemons) should either gracefully
    /// handle cancellation errors during forward operation (and not retry,
    /// propagate a non-cancellation error, etc), or have stop wait until
    /// cancellation errors can be handled (the latter option is rare).
    ///
    /// For most middle-level daemons, stop() is a no-op.
    ///
    /// Bottom-level (leaf) daemons must make sure any outstanding and future
    /// requests either terminate normally in a timely fashion or immediately
    /// return cancellation errors.
    ///
    /// Once the stop method returns, the external environment will await
    /// completion of the run lambda.  Failure to ensure outstanding requests
    /// return quickly will lead to deadlocks.
    async fn stop(&self) -> DaemonResult<(), E>;
}

/// A helper data structure for `DaemonBundle`'s.  `DaemonField`'s are
/// threadsafe and immutable once set.  Attempts to set them multiple
/// times result in an error.  This allows you to safely register `Daemon`s
/// without having a `mut` reference to your `Factory` or `DaemonBundle`.
pub struct DaemonField<T> {
    svc: OnceLockFree<Arc<T>>,
}

/// The `DaemonGetter` trait is implemented for `DaemonField<T>`, which returns
/// `Arc<T>`.  However, we don't allow code to use `Arc::clone()` during forward
/// operation.  Therefore, the public API of Factory always converts
/// `DaemonField<T>` to `DaemonRef<T>`, or (via this trait, which is only used
/// by `prepare()`, `start()`, and `stop()`) to `&dyn Daemon``
pub trait DaemonGetter<E>: Sync + Send {
    fn get_svc(&self) -> &dyn Daemon<E>;
}

impl<T> Default for DaemonField<T> {
    fn default() -> Self {
        Self {
            svc: Default::default(),
        }
    }
}

impl<T, E> DaemonGetter<E> for DaemonField<T>
where
    T: Daemon<E> + 'static,
{
    fn get_svc(&self) -> &dyn Daemon<E> {
        self.svc.get_or_prepare_to_set().unwrap().unwrap().as_ref()
    }
}

impl<T> Clone for DaemonField<T> {
    fn clone(&self) -> Self {
        let res = Self::default();
        if let Some(val) = self.svc.get_or_seal().unwrap() {
            res.svc.set(val.clone()).unwrap();
        }
        res
    }
}

/// The main entry-point for steelmill.  A `Factory` manages a set of `Daemon`s via
/// the generic `DaemonBundle` type.  Application-provided `Daemon` methods return
/// the application-specific generic error type, `E`.
#[allow(clippy::type_complexity)]
pub struct Factory<'a, DaemonBundle: Default, E: Send + Sync> {
    log: Logger,
    /// `Daemon` lifecycles are stored in strange way.  We have fields with concrete types
    /// `T: Daemon`, but no way to name them (e.g., via integer offsets or via references
    /// whose lifetimes would have to circularly outlive `&self`` and be outlived by
    /// `&self`).  Therefore, we store static `Fn`'s that know how to map from `&self`
    /// to the correct field.
    ///
    /// To simplify types (a bit; we still need a clippy suppression!), we have `DaemonField<T>`
    /// implement a trait, `DaemonGetter`, which returns `&dyn Daemon`.
    daemon_lifecycles_accumulator: Stack<Box<dyn Send + Sync + Fn(&Self) -> &dyn DaemonGetter<E>>>,
    /// `finalize_daemons` atomically moves `daemon_lifecycles_accumulator` to this `Vec`.
    /// Later, `prepare()` and `run()` are invoked in the order the `DaemonGetter`s were
    /// accumulated; `stop()` is invoked in reverse order.
    daemon_lifecycles: Vec<Box<dyn Send + Sync + Fn(&Self) -> &dyn DaemonGetter<E>>>,
    /// A struct containing all the `Daemon`s the application knows about.  This has to
    /// be provided by the application so that it can get non-`dyn` references to the its
    /// `Daemon`s, allowing it to invoke async methods on them without resorting to
    /// `async_trait`.
    daemons: DaemonBundle,
    stopping: AtomicBool,
}

impl<
        'a,
        DaemonBundle: Default + Clone + Sync + Send,
        E: std::fmt::Debug + Send + Sync + 'static,
    > Factory<'a, DaemonBundle, E>
{
    /// Create a new root-level factory.
    pub fn new(root_log: DaemonRef<RootLog>) -> Self {
        Self {
            log: root_log.get_factory_logger("*"),
            daemon_lifecycles_accumulator: Default::default(),
            daemon_lifecycles: Default::default(),
            daemons: Default::default(),
            stopping: false.into(),
        }
    }

    /// Create a factory that inherits `Daemon` instances from `ambient`.  As with
    /// `finalize_daemons`, this must be called after the last invocation of
    /// `build` or `inject`.  However, this can be called after `prepare` and
    /// `run`
    pub fn from_ambient(ambient: &Self, host: String) -> Self {
        Self {
            log: ambient.log.new(o!("host"=>host)),
            daemon_lifecycles: Default::default(),
            daemon_lifecycles_accumulator: Default::default(),
            daemons: ambient.daemons.clone(),
            stopping: false.into(),
        }
    }

    /// Use dependency injection to automatically insantiate a Daemon and all its
    /// dependencies.
    pub fn build<T>(
        &self,
        field_accessor: impl 'static + Sync + Send + Fn(&DaemonBundle) -> &DaemonField<T>,
        constructor: impl Fn(&Self) -> DaemonResult<T, E>,
    ) -> FactoryResult<DaemonRef<T>, E>
    where
        T: 'a + Daemon<E>,
        DaemonField<T>: DaemonGetter<E>,
    {
        let svc = field_accessor(&self.daemons)
            .svc
            .get_or_prepare_to_set()
            .map_err(|_| FactoryError::CyclicDependency(std::any::type_name::<T>()))?;
        match svc {
            Some(ret) => Ok(ret.into()),
            None => {
                let svc = Arc::new(constructor(self).map_err(|err| {
                    FactoryError::CouldNotConstruct(std::any::type_name::<T>(), err)
                })?);
                // Note: daemon_lifecycles_accumulator and OnceLockFree::set use interior mutability
                // to avoid requiring &'mut, which allows this method to avoid requiring &'mut self.
                match field_accessor(&self.daemons).svc.set_prepared(svc) {
                    Ok(ret) => {
                        self.daemon_lifecycles_accumulator
                            .push(Box::new(move |factory| field_accessor(&factory.daemons)));
                        // Convert Arc to DaemonRef so that caller can't accidentally incur refcount costs at runtime
                        Ok(ret.into())
                    }
                    Err(err) => {
                        // The OnceLockFree here should never return error (we prepared it above, and children
                        // should fail to prepare it, not set it).
                        panic!(
                            "Unexpected failure to set field of type {} error: {err:?}",
                            type_name::<T>()
                        );
                    }
                }
            }
        }
    }

    /// Register an already-instantiated Daemon with this factory.  This factory will
    /// manage the `Daemon`'s lifecycle.  If you want to reuse the same `Daemon` instance
    /// in multiple `Factory` objects, then use the `from_ambient` method instead.
    pub fn inject<T>(
        &self,
        field_accessor: impl 'static + Sync + Send + Fn(&DaemonBundle) -> &DaemonField<T>,
        val: Arc<T>,
    ) -> FactoryResult<DaemonRef<T>, E>
    where
        T: 'a + Daemon<E>,
        DaemonField<T>: DaemonGetter<E>,
    {
        // TODO: Ideally, on error, we'd return the field name instead of the field type
        Ok(field_accessor(&self.daemons)
            .svc
            .set(val)
            .map_err(|_| FactoryError::FieldAlreadySet(std::any::type_name::<T>()))?
            .into())
    }

    /// This must be called after the last call to `inject` or `build`, but before `prepare` is invoked.
    pub fn finalize_daemons(&mut self) {
        _ = self.daemons.clone(); // seals all the DaemonBundle fields.
        for svc in self.daemon_lifecycles_accumulator.pop_all() {
            self.daemon_lifecycles.push(svc);
        }
        self.daemon_lifecycles.reverse();
    }

    /// This invokes the prepare() method on each `Daemon` in an order that respects the dependencies
    /// that their constructors encode.
    pub async fn prepare(&self) -> FactoryResult<(), E> {
        for svc in &self.daemon_lifecycles {
            debug!(self.log, "prepare starts"; "target_daemon"=>svc(self).get_svc().name());
            let res = svc(self).get_svc().prepare().await;
            if let Err(err) = res {
                let err = FactoryError::DaemonPrepareFailed(svc(self).get_svc().name(), err);
                return Err(err);
            } else {
                debug!(self.log, "prepare done"; "target_daemon"=>svc(self).get_svc().name(), "res"=>#?res);
            }
        }
        Ok(())
    }

    /// Note the use of `async_scoped` here, which forces the caller to resort to `unsafe` code.  We
    /// do this to work around the fact that all tokio futures are `'static`.  Assuming the caller
    /// obeys the `async_scoped` contract and poll the non-`'static` futures it provides you to
    /// completion, the borrow checker will be able to confirm that you are not holding any
    /// references to the things this `Factory` manages after the `Factory` has been torn down.
    /// We think this is a better correctness tradeoff than making all `Daemon` state `'static`,
    /// and then having application developers reason that there are not any high-level (as opposed to
    /// allocator-level) use-after-free bugs in the application logic.
    pub fn run<'b>(
        &'b self,
        scope: &mut async_scoped::Scope<'b, Result<(), FactoryError<E>>, async_scoped::Tokio>,
        shutdown: Option<&'b ShutdownBarrier>,
    ) where
        'a: 'b,
    {
        for svc in &self.daemon_lifecycles {
            debug!(self.log, "run"; "target_daemon"=>svc(self).get_svc().name());
            scope.spawn(
                async move {
                match svc(self).get_svc().run().await {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        error!(self.log, "daemon run failed"; "name"=>svc(self).get_svc().name(), "err"=>#?e);
                        if let Some(shutdown) = shutdown {
                            warn!(self.log, "daemon run failed; broadcasting shutdown");
                            _ = shutdown.cancel();
                        } else {
                            warn!(self.log, "daemon run failed; not configured for shutdown broadcast");
                        }
                        if let Err(e) = self.stop().await {
                            error!(self.log, "additional error shutting down factory after run error"; "svc"=>svc(self).get_svc().name(), "err"=>#?e);
                        }
                        Err(FactoryError::DaemonRunFailed(svc(self).get_svc().name(), e))
                    }
                }
            });
        }
    }

    /// This signals shutdown to all the `Daemon`s by invoking their `stop()` method.
    /// The futures that `run()` spawned shold complete reasonably quickly after `stop()`
    /// is invoked.
    pub async fn stop(&self) -> FactoryResult<(), E> {
        if self
            .stopping
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            // TODO: Wait for previous shutdown?
            return Ok(());
        }
        let mut res = Ok(());
        for svc_idx in (0..self.daemon_lifecycles.len()).rev() {
            let svc_name = self.daemon_lifecycles[svc_idx](self).get_svc().name();
            debug!(self.log, "stop"; "target_daemon"=>svc_name);
            match self.daemon_lifecycles[svc_idx](self).get_svc().stop().await {
                Ok(()) => (),
                Err(new_err) => {
                    let new_err = FactoryError::DaemonStopFailed(svc_name, new_err);
                    res = match res {
                        Err(old_err) => Err(old_err), // TODO: Push error onto existing CrystalError
                        Ok(()) => Err(new_err),
                    }
                }
            }
        }
        res
    }
}
