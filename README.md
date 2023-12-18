# steelmill

This library makes it easy to manage multiple long-running services (called `Daemon`s) within a single Rust process.  It targets asynchronous, multi-threaded runtimes, and is lock-free.

`Daemons` are managed by a hierarchy of `Factory` objects.  Factories use `Daemon` instances from their parents when appropriate, allowing them to share states.  There are two primary reasons to create children factories:

 - When testing a distributed system, you can use the parent factory to implement a loopback network, and to provide any cluster configuration state that is needed to instantiate the system.  Then, each child `Factory` manages `Daemons` that are logically running on different machines, even though the entire setup is in a single operating system process.
 - When dealing with complicated lifecycles within a process, you can use match the lifetime of a child factory to that of an application-specific concept.  For instance, child factory objects could correspond to leases, and be instantiated / torn down when the lease is acquired / lost, or you could use a child factory to manage a plugin that can be dynamically installed or removed from your program,

 In addition to the base library, this crate contains helpers for unit testing, and an example application that lets you choose which `Daemons` to instantiate by passing in command line arguments.

Zero-cost simulation testing of asynchronous rust code is more challenging than we would like.  On the one hand, we'd like to be able to simulate performance-critical data-path APIs, implying that they should be invoked via some sort of interface.  On the other hand, as of this writing, Rust does not support async traits, and the workaround (the `async_trait` crate) adds a memory allocation to each invocation of an async function!

As a workaround, we currently suggest applications use cargo feature flags to either compile their application for runtime usage, or for simulation testing.  Using conditional compilation to provide mutually-exclusive features goes against cargo's design, so we have to do strange things to trick it into behaving as desired.  The sample application shows how to do it, but we hope to switch to a different approach once async traits have stablized.
