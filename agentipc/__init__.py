"""
AgentIPC — ZeroMQ-based inter-agent communication bus

IPC (Unix domain socket) only. No TCP ports, no external services,
sub-millisecond latency.

Architecture (PUSH/PULL):

  Agent A's daemon binds PULL ← Agent B temporarily PUSH connects
  Each agent daemon runs its own PULL socket on a Unix domain socket.
  Senders fire-and-forget via a transient PUSH connection.

Usage:
  from agentipc import AgentIPCBus, quick_send

  # Daemon mode (persistent listener)
  bus = AgentIPCBus("analyst", agents=["analyst", "executor", "reviewer"])
  bus.start()

  # One-shot send from any process
  quick_send("analyst", "executor", "Need a security audit on PR #42")

Configuration:
  Set AGENTIPC_SOCKET_DIR to change the socket directory.
  Default: /tmp/agentipc/sockets
"""

import json
import os
import time
import uuid
import threading
import logging
from pathlib import Path
from collections import deque

import zmq

logger = logging.getLogger("agentipc")

SOCKET_DIR = Path(os.environ.get("AGENTIPC_SOCKET_DIR", "/tmp/agentipc/sockets"))
MAX_QUEUE = 1000


class AgentIPCBus:
    """ZeroMQ-based inter-agent message bus (daemon mode: binds PULL).

    Each agent process that wants to receive messages runs a daemon
    that binds a PULL socket.  Other processes can send to it via
    quick_send() or by constructing a transient bus.
    """

    def __init__(self, agent_name: str, agents: list[str] | None = None):
        self.name = agent_name
        self.agents = tuple(agents) if agents else ()
        self.ctx: zmq.Context | None = None
        self.pull: zmq.Socket | None = None
        self.pub: zmq.Socket | None = None
        self.sub: zmq.Socket | None = None
        self._recv_queue: deque = deque(maxlen=MAX_QUEUE)
        self._recv_thread: threading.Thread | None = None
        self._running = False
        self._lock = threading.Lock()

    def start(self, daemon_mode: bool = True):
        """Start the ZMQ bus.

        daemon_mode=True:  bind PULL (receive) + PUB (broadcast),
                           connect SUB to peers.
        daemon_mode=False: outbound-only, no binding.
        """
        SOCKET_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.ctx = zmq.Context()

        if daemon_mode:
            # PULL socket — inbound messages
            pull_path = str(SOCKET_DIR / f"{self.name}.pull")
            if os.path.exists(pull_path):
                os.unlink(pull_path)
            self.pull = self.ctx.socket(zmq.PULL)
            self.pull.bind(f"ipc://{pull_path}")
            os.chmod(pull_path, 0o600)
            logger.info("[agentipc:%s] PULL bound %s", self.name, pull_path)

            # PUB socket — broadcasts (SUB peers subscribe)
            pub_path = str(SOCKET_DIR / f"{self.name}.pub")
            if os.path.exists(pub_path):
                os.unlink(pub_path)
            self.pub = self.ctx.socket(zmq.PUB)
            self.pub.bind(f"ipc://{pub_path}")
            os.chmod(pub_path, 0o600)
            logger.info("[agentipc:%s] PUB bound %s", self.name, pub_path)

            # SUB socket — listen to other agents' broadcasts
            self.sub = self.ctx.socket(zmq.SUB)
            for other in self.agents:
                if other == self.name:
                    continue
                sub_path = str(SOCKET_DIR / f"{other}.pub")
                self.sub.connect(f"ipc://{sub_path}")
            self.sub.setsockopt_string(zmq.SUBSCRIBE, self.name)
            self.sub.setsockopt_string(zmq.SUBSCRIBE, "all")
            logger.info("[agentipc:%s] SUB connected to peers", self.name)

            time.sleep(0.3)  # ZMQ slow-joiner

        self._running = True
        if daemon_mode:
            self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
            self._recv_thread.start()

        logger.info("[agentipc:%s] bus started (IPC, portless, daemon=%s)",
                    self.name, daemon_mode)

    def stop(self):
        """Shut down the bus."""
        self._running = False
        if self._recv_thread:
            self._recv_thread.join(timeout=2)
        for sock in [self.sub, self.pub, self.pull]:
            if sock:
                sock.close()
        if self.ctx:
            self.ctx.term()
        logger.info("[agentipc:%s] bus stopped", self.name)

    def _recv_loop(self):
        """Background thread: poll PULL + SUB, enqueue messages."""
        poller = zmq.Poller()
        if self.pull:
            poller.register(self.pull, zmq.POLLIN)
        if self.sub:
            poller.register(self.sub, zmq.POLLIN)

        while self._running:
            try:
                socks = dict(poller.poll(timeout=200))
                for sock in [self.pull, self.sub]:
                    if sock and sock in socks:
                        raw = sock.recv_multipart(zmq.NOBLOCK)
                        body = raw[-1].decode("utf-8")
                        try:
                            msg = json.loads(body)
                            self._recv_queue.append(msg)
                        except json.JSONDecodeError:
                            logger.debug("[agentipc:%s] bad json: %s",
                                         self.name, body[:100])
            except zmq.Again:
                continue
            except zmq.ZMQError as e:
                if self._running:
                    logger.debug("[agentipc:%s] recv error: %s", self.name, e)
            except Exception:
                logger.exception("[agentipc:%s] recv loop error", self.name)

    def send(self, to: str, content: str, msg_type: str = "task",
             reply_to: str | None = None) -> str:
        """Send a message to another agent via its PULL socket."""
        msg = {
            "from": self.name,
            "to": to,
            "type": msg_type,
            "content": content,
            "timestamp": time.time(),
            "msg_id": uuid.uuid4().hex[:12],
        }
        if reply_to:
            msg["reply_to"] = reply_to

        body = json.dumps(msg, ensure_ascii=False)

        if to == "all" and self.agents:
            if self.pub:
                self.pub.send_multipart([b"all", body.encode("utf-8")])
            else:
                for agent in self.agents:
                    if agent != self.name:
                        self._push_to(agent, body)
        else:
            self._push_to(to, body)

        logger.debug("[agentipc:%s] -> %s: %s", self.name, to, content[:80])
        return msg["msg_id"]

    def _push_to(self, target: str, body: str):
        """Connect to target's PULL socket, send, disconnect."""
        pull_path = str(SOCKET_DIR / f"{target}.pull")
        push = self.ctx.socket(zmq.PUSH)
        push.connect(f"ipc://{pull_path}")
        push.send_string(body)
        time.sleep(0.01)
        push.close()

    def receive(self, timeout: float = 0) -> dict | None:
        """Non-blocking pop one message from the receive queue."""
        if timeout > 0:
            deadline = time.time() + timeout
            while time.time() < deadline:
                with self._lock:
                    if self._recv_queue:
                        return self._recv_queue.popleft()
                time.sleep(0.01)
            return None
        else:
            with self._lock:
                if self._recv_queue:
                    return self._recv_queue.popleft()
            return None

    def receive_all(self) -> list[dict]:
        """Drain the receive queue."""
        msgs = []
        with self._lock:
            while self._recv_queue:
                msgs.append(self._recv_queue.popleft())
        return msgs

    def broadcast(self, content: str, msg_type: str = "broadcast") -> str:
        """Broadcast to all agents."""
        return self.send("all", content, msg_type)


def quick_send(from_agent: str, to: str, content: str,
               msg_type: str = "task",
               agents: list[str] | None = None) -> str:
    """One-shot send: connect to target's PULL socket, send, disconnect.

    No daemon required.  Any process can call this.
    """
    if to not in agents and to != "all" and agents is not None:
        raise ValueError(f"Unknown target '{to}'")

    msg = {
        "from": from_agent,
        "to": to,
        "type": msg_type,
        "content": content,
        "timestamp": time.time(),
        "msg_id": uuid.uuid4().hex[:12],
    }
    body = json.dumps(msg, ensure_ascii=False)

    ctx = zmq.Context()
    targets = list(agents) if to == "all" and agents else [to]
    for target in targets:
        if target == from_agent:
            continue
        pull_path = str(SOCKET_DIR / f"{target}.pull")
        push = ctx.socket(zmq.PUSH)
        push.connect(f"ipc://{pull_path}")
        push.send_string(body)
        push.close()
        time.sleep(0.005)

    ctx.term()
    logger.debug("[agentipc:quick] %s -> %s: %s", from_agent, to, content[:80])
    return msg["msg_id"]
