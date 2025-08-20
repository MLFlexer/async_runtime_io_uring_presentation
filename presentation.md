---
marp: true
theme: default
class: invert
size: 16:9
style: |
  img {background-color: transparent!important;}
  a:hover, a:active, a:focus {text-decoration: none;}
  header a {color: #ffffff !important; font-size: 30px;}
  footer {color: #148ec8;}
header: '[&#9671;](#1 " ")'
footer: ''

---
## Building an Async Runtime in Rust with `io_uring`

---
## `whoami`

- [Malthe Mølgaard Larsen](https://www.linkedin.com/in/malthe-m-larsen/)
- [MLFlexer](https://github.com/MLFlexer)
- Background
  - BSc @ ITU
  - MSc @ UCPH
  - Starting a PhD @ DIKU developing an open, RISC-V based, EU-produced, storage accelerator, [CHORYS](https://chorys.eu/)
  - Wezterm plugin creator: resurrect, smart workspace switcher, modal keybindings
  - Rust, Nix and Helix enjoyer
  - Love everything systems programming/high performance

---
## Motivation
- Bored after finishing MSc thesis
- Initial idea was to make a zero-copy HTTPS server with `io_uring` + `kTLS`
- Learn `io_uring`

---
## Motivation
Made a single threaded MVP doing the TLS handshake through `io_uring` was able to send HTTPS requests to a client through `io_uring`.
But the code was ugly and multithreading made it even uglier...

---
## Motivation
- Bored after finishing MSc thesis
- ~~Initial idea was to make a zero-copy HTTPS server with `io_uring` + `kTLS`~~
- Build an Async runtime for `io_uring`
- Learn `io_uring`
- Learn Async Rust

---
## TLDR: `io_uring`
- Syscalls can be expensive
  - Userspace -> Kernel
  - Context switching
  - Blocking

![bg right:40% width:90%](img/syscall.svg)

---

## TLDR: `io_uring`
- Ring-buffer pair shared with kernel
- Atomically interact with buffers
  - avoids context switching
- Submit SQEs to submission queue (SQ)
- Submissions complete *asynchronously*
  - avoids blocking
- Get CQEs from completion queue (CQ)
- Entries have 64 bits for user data

![bg right:30% width:90%](img/io_uring_overview.svg)

---
## TLDR: `io_uring`
- Created by Jens Axboe
- Danish BTW
![bg right:50% width:90%](img/denmark_meme.jpeg)

---

## So WTF is async Rust?
- `async`-keyword creates `Futures`
- The `Future` trait can be implemented custom types
- Calling `.await` on a future, *polls* the future
- Polling a future will result in the future being *ready* or *pending*
  - Ready futures will continue execution
  - Pending futures will yield the current execution to the runtime
- Runtime can *wake* a future to *poll* it again

---

## Compiler Magic
- The compiler transforms this:

```rs
async {
    // ... before f
    f().await;
    // ... after f
}
```
![bg right:50% width:90%](img/async_block.svg)

---

## High-level Intermediate Representation (HIR)

```rs
async {
    // ... before f
    f().await;
    // ... after f
}


|mut _task_context: ResumeTy|
{
    // ... before f
    match #[lang = "into_future"](f()) {
        mut __awaitee =>
            loop {
                match unsafe {
                        // Calls poll
                        #[lang = "poll"](#[lang = "new_unchecked"](&mut __awaitee),
                            #[lang = "get_context"](_task_context))
                    } {
                    // Break if ready
                    #[lang = "Ready"] {  0: result } => break result,
                    #[lang = "Pending"] {} => { }
                }
                // Yield if pending
                _task_context = (yield ());
            },
    };
    // ... after f
}
```
![bg right:50% width:90%](img/async_block.svg)

---

## High Level Idea
- Initial poll submits SQEs to `io_uring` and yield execution.
- When submissions complete, then we *wake* and *poll* again.

![bg right:50% width:90%](img/runtime_idea.svg)

---

## What does the runtime do then?
- Recive SQEs from MPSC channel
- Submit SQEs to `io_uring`
- Check for CQEs
- Handle CQEs

---

## What does the runtime do then?
```rs
while let Ok(sqes_w_state) = sqe_rx.try_recv() {
    ring.submission().push(sqes_w_state.get_sqes());
    // Map the id/userdata with the state
    map.insert(sqes_w_state.get_id(), sqes_w_state);
}

ring.submit();

for cqe in ring.completion() {
    // Map the id/userdata with the state
    let state = map.get(cqe.user_data());
    state.handle_cqe(cqe);
    if state.is_complete() {
      state.wake(); // We'll come back to this
    }
}
```
---

## What about multithreading?
- Make a thread pool
- Each thread loops the following:
  1. Recive future from MPMC *task* channel and `poll()` it
  2. If no futures are left in channel, then handle `io_uring` submissions and completions
  3. Repeat
- To spawn a task, we can just send it to the *task* channel
- Waking a future is the same thing, the `wake()` just reenters the future into the task channel

---
## Overview

- Concurrent execution of tasks
- Single `io_uring` behind Mutex
- Channels for send/recv tasks and SQEs
![bg right:60% width:90%](img/multi_runtime.svg)

---
## Write example:
```rs
spawn(async {
    let hello = "Hello from io_uring!\n";
    WriterFuture::new(hello, 0).await;
});
```
---

```rs
pub struct WriterFuture<T: AsRef<[u8]>> {
    shared_state: Arc<AtomicSharedCell<SharedState<T>>>,
    fd: i32,
}

struct SharedState<T: AsRef<[u8]>> {
    buf: T,
    id: OnceCell<u64>,
    sqes: OnceCell<[squeue::Entry; 1]>,
    cqes: OnceCell<cqueue::Entry>,
    waker: AtomicWaker,
}
```
---

```rs
impl<T: AsRef<[u8]> + 'static> Future for WriterFuture<T> {
    type Output = i32;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = self.shared_state.with_inner(|shared_state| {
            match shared_state.cqes.get() {
                None => {
                    shared_state.waker.register(cx.waker());

                    let id = get_id();
                    shared_state.id.set(id);

                    let buf: &[u8] = shared_state.buf.as_ref();
                    let write_e =
                        opcode::Write::new(types::Fd(self.fd), buf.as_ptr(), buf.len() as u32)
                            .build()
                            .user_data(id);
                    shared_state.sqes.set([write_e]);

                    return Poll::Pending;
                }
                Some(cqe) => return Poll::Ready(cqe.result()),
            };
        });

        if res.is_pending() {
            SQE_SENDER.send(self.shared_state.clone());
        }
        return res;
    }
}
```

---
## It Works!
```rs
spawn(async {
    let hello = "Hello from io_uring!\n";
    WriterFuture::new(hello, 0).await;
});
```

```sh
Hello from io_uring!
```

---
## Walkthrough

![bg right:60% width:90%](img/writer/write_1.drawio.svg)

---
## Walkthrough
A thread recives a task.

![bg right:60% width:90%](img/writer/write_2.drawio.svg)

---
## Walkthrough
It executes code by calling `poll()`.

![bg right:60% width:90%](img/writer/write_3.drawio.svg)

---
## Walkthrough
It hits a `.await` and calls `poll()` on the future.

![bg right:60% width:90%](img/writer/write_4.drawio.svg)

---
## Walkthrough
`poll` submits the write SQE with state to SQE channel.

![bg right:60% width:90%](img/writer/write_5.drawio.svg)

---
## Walkthrough
Threads continue to recive tasks - execute code while SQEs can be handled.

![bg right:60% width:90%](img/writer/write_6.drawio.svg)

---
## Walkthrough
Eventually a thread will handle `io_uring` by reciving from SQE channel.

![bg right:60% width:90%](img/writer/write_7.drawio.svg)

---
## Walkthrough
The write SQE is retrived and submitted to submission queue.

![bg right:60% width:90%](img/writer/write_8.drawio.svg)

---
## Walkthrough
It sits in the submission queue until the kernel handles it.
In the mean time other work can be done.

![bg right:60% width:90%](img/writer/write_9.drawio.svg)

---
## Walkthrough
Eventually it ends up in the completion queue with the result.

![bg right:60% width:90%](img/writer/write_11.drawio.svg)

---
## Walkthrough
The CQE is associated with the state and `wake()` sends it to the task channel.

![bg right:60% width:90%](img/writer/write_12.drawio.svg)


---
## HTTP server
- Listen for incomming connections
- Accept connections
- Read request from socket
- Write response to socket
- Shutdown and close connection

---
## `io_uring` Multishot
- Multishot operations can return multiple CQEs on a single SQE
- Multishot accept -> A CQE per connection
- Multishot recive -> A CQE per message

---
## `io_uring` Linking
- SQEs can be linked to complete in a sequence
- If one fails, then the rest are cancelled

---
## HTTP server
- Listen for incomming connections
- *Multishot Accept connections*
- Read request from socket
- Write response to socket
- *Linked shutdown and close connection*

---
## HTTP server
```rs
spawn(async {
    // Listen for incomming connections
    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let listener = TcpListener::bind(addr).unwrap();
    // Accept connections
    let _ = AcceptMultiFuture::new(listener.as_raw_fd(), |fd| async move {
        // Read from socket
        let buf: [u8; 64] = [0u8; 64];
        let _ = ReaderFuture::new(buf, fd).await;
        // Write to socket
        let response = "HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: close\r\n\r\nHello, World!";
        let _ = WriterFuture::new(response, fd).await;
        // Shutdown and close connection
        let _ = ShutdownAndCloseFuture::new(fd).await;
    })
    .await;
});
```

---
## Performance
```
❯ hey -n 1000000 -cpus 5 -c 500 -host localhost http://127.0.0.1:8080

Summary:
  Total:        19.9967 secs
  Slowest:      1.2245 secs
  Fastest:      0.0001 secs
  Average:      0.0087 secs
  Requests/sec: 50008.3323

  Total data:   13000000 bytes
  Size/request: 13 bytes

Response time histogram:
  0.000 [1]     |
  0.123 [996240]        |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.245 [15]    |
  0.367 [0]     |
  0.490 [0]     |
  0.612 [0]     |
  0.735 [0]     |
  0.857 [0]     |
  0.980 [0]     |
  1.102 [3702]  |
  1.224 [42]    |


Latency distribution:
  10% in 0.0014 secs
  25% in 0.0022 secs
  50% in 0.0037 secs
  75% in 0.0067 secs
  90% in 0.0100 secs
  95% in 0.0117 secs
  99% in 0.0158 secs

Details (average, fastest, slowest):
  DNS+dialup:   0.0048 secs, 0.0001 secs, 1.2245 secs
  DNS-lookup:   0.0000 secs, 0.0000 secs, 0.0000 secs
  req write:    0.0013 secs, 0.0000 secs, 0.0202 secs
  resp wait:    0.0013 secs, 0.0000 secs, 0.2109 secs
  resp read:    0.0013 secs, 0.0000 secs, 0.0191 secs

Status code distribution:
  [200] 1000000 responses   
```

---
## Tokio performance
```
❯ hey -n 1000000 -cpus 5 -c 500 -host localhost http://127.0.0.1:8080

Summary:
  Total:        18.0229 secs
  Slowest:      0.0639 secs
  Fastest:      0.0001 secs
  Average:      0.0090 secs
  Requests/sec: 55485.0481

  Total data:   13000000 bytes
  Size/request: 13 bytes

Response time histogram:
  0.000 [1]     |
  0.006 [184497]        |■■■■■■■■■■
  0.013 [749833]        |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.019 [64775] |■■■
  0.026 [394]   |
  0.032 [0]     |
  0.038 [0]     |
  0.045 [113]   |
  0.051 [140]   |
  0.058 [200]   |
  0.064 [47]    |


Latency distribution:
  10% in 0.0055 secs
  25% in 0.0071 secs
  50% in 0.0090 secs
  75% in 0.0108 secs
  90% in 0.0123 secs
  95% in 0.0132 secs
  99% in 0.0153 secs

Details (average, fastest, slowest):
  DNS+dialup:   0.0021 secs, 0.0001 secs, 0.0639 secs
  DNS-lookup:   0.0000 secs, 0.0000 secs, 0.0000 secs
  req write:    0.0024 secs, 0.0000 secs, 0.0403 secs
  resp wait:    0.0020 secs, 0.0000 secs, 0.0408 secs
  resp read:    0.0024 secs, 0.0000 secs, 0.0121 secs

Status code distribution:
  [200] 1000000 responses 
```

---
## With SQE polling
```
❯ hey -n 1000000 -cpus 5 -c 500 -host localhost http://127.0.0.1:8080

Summary:
  Total:        19.9967 secs
  Slowest:      1.2245 secs
  Fastest:      0.0001 secs
  Average:      0.0087 secs
  Requests/sec: 50008.3323

  Total data:   13000000 bytes
  Size/request: 13 bytes

Response time histogram:
  0.000 [1]     |
  0.123 [996240]        |■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.245 [15]    |
  0.367 [0]     |
  0.490 [0]     |
  0.612 [0]     |
  0.735 [0]     |
  0.857 [0]     |
  0.980 [0]     |
  1.102 [3702]  |
  1.224 [42]    |


Latency distribution:
  10% in 0.0014 secs
  25% in 0.0022 secs
  50% in 0.0037 secs
  75% in 0.0067 secs
  90% in 0.0100 secs
  95% in 0.0117 secs
  99% in 0.0158 secs

Details (average, fastest, slowest):
  DNS+dialup:   0.0048 secs, 0.0001 secs, 1.2245 secs
  DNS-lookup:   0.0000 secs, 0.0000 secs, 0.0000 secs
  req write:    0.0013 secs, 0.0000 secs, 0.0202 secs
  resp wait:    0.0013 secs, 0.0000 secs, 0.2109 secs
  resp read:    0.0013 secs, 0.0000 secs, 0.0191 secs

Status code distribution:
  [200] 1000000 responses   
```

---
## Improvements
- Better scheduling
- Improved contention on locks and queues
- Reduced/Improved allocation
- Implement missing opcodes
- Better API

---
## Tokio-uring
Tokio is in the process of implementing a backend for `io_uring` with [tokio-uring](https://github.com/tokio-rs/tokio-uring).

---
# Thanks for comming to my first talk! :smile:

---
# And thanks to the organizers and our host!

---
# Questions?
Code available here: [github.com/CrabRing/presentation](???)
