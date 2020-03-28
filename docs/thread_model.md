# TensorPipe's thread model

TensorPipe is spawning multiple threads internally. This is a design
requirement as, for example, a single thread wouldn't manage to drive a
modern network interface card (NIC) at capacity and saturate its
bandwidth, even if it did nothing by write on the socket: multiple
threads writing in parallel to multiple sockets are the only way to
achieve that.

Moreover, the possibility of spawning new threads when needed allows
for a simpler architecture in the implementation of TensorPipe's
modular approach to backends (transports and channels): if one of these
backends needs to perform some heavy operation (a blocking syscall, an
event loop, ...) it can launch a dedicated thread for it rather than
having to schedule it on the user thread or on a shared thread pool,
thus having to "fit" the operation into some framework.

This heavy reliance on multi-threading poses of course challenges in
coordination and robustness. This document aims to outline the patterns
we've ended up adopting to have a structured and principled design
around this.

## Callbacks

TensorPipe uses callbacks to organize the control flow around
asynchronous and deferred execution. While this may be an anti-pattern
leading to so-called "spaghetti code" or "callback hell", we realized
that it was the only approach that would yield the performance we need.
Modern alternatives to callbacks (promises/futures, coroutines, ...) 
would have introduced an unacceptable overhead in some cases.

Nearly all operations in TensorPipe are non-blocking and are performed
asynchronously, in background, with their results notified through
callbacks. This includes the creation of pipes and connections (the
objects may still be performing initialization when they are given to
the user and, although operations can be performed on them, these will
be delayed until setup completes). And it also includes destruction,
which means that internal resources may not be immediately freed when a
user-facing object is deleted. The only synchronization point that
allows the user to wait for such cleanup to finish is the context's
`join` method. Some other methods that may occasionally wait are the
ones that return a value, for example the ones to retrieve addresses.

## Shared pointers

As soon as threads and callbacks enter the mix, race conditions start
to pop up. Among the first ones, there's the problem of ownership:
ideally we want a `unique_ptr`-style semantics, where each object has a
clear owner who controls its lifetime. However, when this owner asks
another thread to perform an operation on that object as part of a
callback, that callback also (temporarily) needs access to the object.
As there may be multiple operations with multiple callbacks at the same
time, transferring ownership isn't an option, and sharing it is the
only way to go. This however requires synchronization among the various
users: if the "real" user had a `unique_ptr` and gave raw pointers to
the callbacks, the real user may delete the object without the
callbacks noticing or having any way to stop/delay it. This would then
cause use-after-free errors. There must thus be a sort of "lock" that
prevents the object from being deleted while someone is working on it,
like a "semaphore" counting the users. It turns out a perfect tool for
the job is `shared_ptr`. Acquiring a lock on the object corresponds to
obtaining a `shared_ptr` instance, which increases the reference count.
The object will only be deleted when its refcount reaches zero, which
means all its users (the "real" ones and the callbacks) have stopped
using the object.

We have however solved a problem by creating an opposite one: a memory
leak. Imagine an object (say, a pipe) that is the "real" owner of
another one (say, a channel) from which it is expecting a callback, and
that callback captures a `shared_ptr` to the first object in its
closure. This is a reference cycle. It means that even if the "real"
owner of the first object relinquishes its `shared_ptr`, the objects
won't be destroyed until the callback fires (if ever). An easy solution
to this is to have callbacks only keep a `shared_ptr` when they are
running, not while they are waiting. Again, the standard library has
the perfect tool for the job: the `weak_ptr`, which will keep the
refcount unchanged but can be "locked" to obtain a real `shared_ptr`
when needed (curious coincidence that the terminology aligns with our).

So, in short: the real owner of an object keeps a `shared_ptr` to it,
it passes `weak_ptr`s to be stored in callbacks, and these are locked
back to `shared_ptr`s just before running the callbacks. (If locking
fails, the callback isn't run).

## Public objects vs private implementations

It turns out that what we said above isn't always true: in some cases
we may want a callback to keep the object alive until it has fired.
This happens because some callbacks are one half of a "contract"
regarding data ownership: throughout the API (at higher and lower
levels), `read`, `write`, `send` and `recv` methods take some data
(source or destination buffers), and by doing so the caller hands over
control of the data to the object. The way for the object to yield
ownership back to the caller is by invoking the callback. We must thus
ensure that these callbacks are always called. However, we must also
avoid calling them when we're not ready yet to give up access to the
data. For a more concrete example, consider the user trying to destroy
a pipe that has a pending write operation, while some other thread is
simultaneously performing a memory copy as part of that write
operation. If we invoke the write operation's callback before aborting
the memory copy we're giving the user the right to deallocate the
buffer, which may lead the other thread to segfault.

Here is what needs to happen: when a user deletes a pipe, all its
pending operations must be interrupted, which in turn also aborts the
lower level operations; the pipe's callbacks, however, must not be
fired and instead kept alive while waiting for the lower level
operations to wrap up, and only then they can be triggered. This shows
that a subset of the pipe, containing at least the callbacks, must
survive the destruction of the whole pipe. In other words, the lifetime
of the inner part must be detacheable from the one of the outer shell.

In order to do so, most public objects are just thin wrappers around a
single member field, which is just a pointer to an instance of a
private "implementation" (abbreviated as impl), which is where
everything happens. The impl is a `shared_ptr` so that its life cycle
can be detached and extended with respect to the one of the public
object. The callbacks that we must wait for in order to regain control
of some resource also capture a `shared_ptr`. This way we can still get
the "signal" from when the public object is deleted (and can start
terminating pending operations) but we're also able to keep the impl
around while wait for the shut down to complete.

## Locking

Objects can be accessed and worked on from many threads, from all
directions, above (user threads, higher up the stack) and below 
(low-level backend threads). To avoid race conditions on the internal
state of these object, we must have mutual exclusion between threads,
using locks. While it may be possible to have separate fine-grained
locks for different parts of some objects, in general it is safer
and easier to have one mutex per object, and use it to lock all
operations.

That's easily said, but it just as easily leads to deadlocks, which in
our experience come in two flavors:

- When an object (holding its own lock) calls a "upward" callback which
  (inline/serially) tries to perform an operation on that same object,
  which tries to acquire the same lock. This is a perfectly legitimate
  behavior, since all of our callbacks are "one-shot", that is, they
  "burn out" after they fire and thus must be immediately rearmed.

- When an object (holding its own lock) performs an operation on a
  lower level object, passing a callback to it, and this callback is
  called immediately (inline/serially) and tries to also acquire the
  lock of the first object. This typically happens when the lower level
  object is in an error state and can thus "shortcut" the operation and
  immediately trigger the callback instead of deferring it to a thread.

Mitigations for these problems are possible but none is universal and
they all have drawbacks. Examples are:

- When calling upward callbacks, extract one from the object onto the
  stack, put the object in a consistent state, release its lock and
  then call the callback. This works but there's a racing risk which
  would cause callbacks to not be called in their intended order.

- Have a dedicated thread from which to invoke callbacks. Therefore
  other threads, instead of triggering callbacks, push them to some
  queue that is consumed by this thread. This resembles the semi-future
  and executor pattern. We used to have such a pattern in place for
  calling the pipe callbacks but it was introducing an unacceptable
  latency overhead.

- The backends already typically have a thread they can defer callbacks
  to, and for the most part they already do. However having such a
  thread isn't necessarily a requirement for a transport, and such
  threads may not be running at all times (e.g., once a backend has
  been joined).

- We could replace regular locks with reentrant locks (also called
  recursive). This is typically considered bad practice, though, and
  when at some point we tried this we indeed hit problems.

The next section presents a more disciplined way of dealing with races.

## Event loops

A classic way of dealing with parallel I/O is event loops: repeatedly
polling a set of file descriptors for readability/writability (blocking
to wait for them to become ready), dealing with them, and repeating.
Syscalls to do this are `select`, `epoll`, and more. The `libuv`
library used by one of TensorPipe's transports is also based on an
event loop. Event loops are typically single-threaded, and they allow
to "simulate" parallelism by multiplexing thread if those threads would
spend most of their time doing blocking I/O.

The simplicity of event loops, their single-threaded safety and their
established effectiveness prompted us to make them a foundation of our
threading model.

If an object already has a thread to which it offloads some operations
(this is the case for most transports and some channels, but not the
pipe) then we defer all operations to it. And we really mean all of
them: all manipulation of the object (scheduling operations, querying
information, running callbacks) must be done from within that event
loop thread. All operations that are attempted on the object, either
from another thread or from within the event loop thread (for example,
by a callback in user code) are deferred, appended to a queue, and
dealt with at a later iteration of the loop. This guarantees that we'll
always have a single thread accessing such objects, thus ensuring
thread safety without even using any locks. Note that such design isn't
a requirement for transports, it's just the pattern that we've adopted
for all our current transports.

If, on the other hand, an object does not have access to a thread to
use as an event loop, we'll "borrow" the caller's thread and
temporarily use it as an event loop. We'll similarly have a queue of
tasks, and the thread will consume them one by one, until none are
left, at which point we'll stop occupying the thread and release it
back to the caller. If any new operation is attempted by another thread
while one of these temporary event loops is running, that operation is
added to the queue and thus deferred to the already-running event loop,
with the new thread immediately able to return to what it was doing.
