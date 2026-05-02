# AgentIPC — Design & Rationale

## Why this exists

In late 2025 I had three AI agents running as separate Python processes on a single server.  They needed to collaborate — one researches, one runs experiments, one writes.  They sat in different tmux windows and had no way to talk to each other.

What I tried, in order:

1. **Files in a shared directory, polled by cron.**  Worked, but the minimum latency was 60 seconds.  An agent would ask a question and wait a full minute for the answer.  The polling also added constant CPU overhead.

2. **TCP sockets with a custom wire protocol.**  Now I had to manage ports, auth tokens, reconnection logic, and partial reads.  And I'd opened a port — even on localhost, that bothered me.

3. **Discord bots forwarding messages.**  Three bots, one Discord server, each agent was its own bot user.  It worked surprisingly well for async messages, but it felt wrong.  Why does my local agent need Discord's servers to send a string to the process next door?  Also: rate limits, outages, and the 2000-character message cap.

4. **ZeroMQ PUSH/PULL over IPC.**  No ports.  No serialization overhead.  No external services.  Sub-millisecond delivery.  One `pip install`.  This was the right answer.

## What AgentIPC is and isn't

**It is** a transport layer.  It moves bytes from process A to process B on the same machine.  It doesn't know what the bytes mean.  It doesn't care what LLM you're using.  It doesn't orchestrate workflows.

**It isn't** an orchestration framework.  Tools like CrewAI, AutoGen, and LangGraph handle task allocation, agent roles, and execution flow.  AgentIPC sits underneath those — it's the wire they could use instead of HTTP or in-process function calls.

Think of it as the TCP of agent communication.  TCP doesn't know about HTTP, SMTP, or SSH.  It just delivers bytes reliably.  AgentIPC doesn't know about prompts, tool calls, or reasoning chains.  It just delivers messages between agent processes.

## The PUSH/PULL pattern

ZeroMQ has several socket patterns.  Here's why I chose PUSH/PULL:

- **REQ/REP** — synchronous request-reply.  Too rigid.  An agent might send a question and not need an immediate answer.
- **PUB/SUB** — one-to-many broadcast.  Good for notifications, but no way to target a specific recipient.
- **ROUTER/DEALER** — async request-reply with routing.  Powerful but complex.  Overkill.
- **PUSH/PULL** — one-way pipeline.  Perfect.  Each agent binds a PULL socket.  Senders transiently connect a PUSH, send, disconnect.  The daemon polls the PULL and queues messages.

The key insight: **the sender never binds a socket**.  It just connects, pushes, and disconnects.  Only the daemon binds.  This means there are no persistent connections to manage, no reconnection logic, no half-open sockets.

PUB/SUB is used in parallel for protocol control messages (SYN-ACK, FIN-ACK).  When agent A sends a SYN to agent B, it temporarily subscribes to B's PUB socket to receive the SYN-ACK.  This avoids requiring agent A to also bind a PULL (which would conflict with its own daemon).

## The session protocol

A lot of agent communication tools are fire-and-forget: send a message and hope the other side got it.  I wanted sessions.

The session protocol is a stripped-down TCP handshake:

```
SYN     → "I want to start a session.  Here's the task."
SYN-ACK ← "I got it.  Session established."
DATA    ↔ bidirectional payloads
FIN     → "Session done."
FIN-ACK ← "Acknowledged."
```

This gives you:

- **Confirmation**: you know the other side received your connection request.
- **Task context**: the SYN carries a task description, so the receiver knows what this session is about before the first DATA frame.
- **Clean teardown**: FIN/FIN-ACK means you can clean up state on both sides.
- **Session tracking**: the daemon maintains a count of active sessions and pending message counts per session.  You can query these at any time.

### Why not just put everything in the message body?

You could.  But then every agent would need to parse the body, figure out if it's a new conversation or a continuation, and manage state themselves.  The session protocol moves that boilerplate into the transport layer where it belongs.

### Timeouts and edge cases

- **SYN timeout**: if the target daemon doesn't respond with SYN-ACK within 30 seconds, the session is abandoned.
- **Daemon crash**: if a daemon dies, its sessions are lost.  On restart, it starts fresh.  This is deliberate — AgentIPC is not a durable message queue.  If you need persistence across restarts, layer it on top.
- **Double SYN**: duplicate SYNs with the same session_id are ignored.

## Daemon design

Each agent identity runs one `agentipcd` process, managed by systemd.  Why a daemon?

- The agent process might crash or restart.  Messages sent during downtime are lost, but as soon as the agent comes back, it can query the daemon for pending messages.  (Currently the daemon holds messages in memory.  See "Future work" for persistence.)
- Separating the transport daemon from the agent process means the agent doesn't need to manage ZMQ sockets directly.  It just polls the REP socket.
- Systemd gives us auto-restart, logging to journald, and a standard way to manage lifecycle.

The REP socket is the agent's interface to its daemon:

```
Agent → REP "QUERY"    → daemon returns: active sessions + pending message counts
Agent → REP "ACK:sid" → daemon clears that session's pending queue
Agent → REP "PING"    → daemon returns "pong" + counts
```

## Comparisons

### vs. MCP (Model Context Protocol)

MCP is a client-server protocol for LLMs to call tools.  It uses stdio or HTTP as transport.  AgentIPC is a transport layer for agent-to-agent communication.  You could run MCP over AgentIPC (replace stdio with ZMQ IPC), but they solve different problems.

### vs. Google A2A (Agent-to-Agent)

A2A defines how agents advertise capabilities, negotiate tasks, and return artifacts.  It's an application-layer protocol.  AgentIPC is a transport.  They're complementary — A2A could use AgentIPC as its transport instead of HTTP.

### vs. NATS / Redis Pub/Sub

Both are excellent message brokers.  They also require running a separate server process, binding network ports, and managing authentication.  For same-machine communication between a handful of agents, that's too much infrastructure.  For cross-machine or high-throughput scenarios, they're the right choice.

## Future work

- **Disk-backed message queue.**  Currently messages are held in memory.  If the daemon restarts, pending messages are lost.  A SQLite-backed queue would make it durable.
- **Message compression.**  Agent messages can get long (full reasoning traces).  ZMQ doesn't compress by default.
- **Observability.**  Prometheus metrics for message counts, latency, session duration.
- **Access control.**  Currently any process with access to the socket directory can send messages.  Fine for single-user machines, not for multi-tenant.

## Why not just use `multiprocessing.Queue`?

Because my agents aren't child processes.  They're independent systemd services, started separately, with their own Python interpreters and virtual environments.  `multiprocessing` only works within a single parent process.

## Why ZeroMQ instead of nanomsg / nng?

nanomsg and its successor nng are great libraries.  They're cleaner than ZMQ in many ways.  But ZMQ has:

- Mature Python bindings (`pyzmq` — 3600+ stars, battle-tested)
- Better documentation and community
- PUSH/PULL semantics that map perfectly to my use case

If someone ports AgentIPC to nng, I'd love to see it.  The protocol is simple enough.
