#!/usr/bin/env python3
"""
Minimal two-agent example using AgentIPC.

Run these in two terminals:

  Terminal 1:  python3 examples/two_agents.py analyst
  Terminal 2:  python3 examples/two_agents.py executor

The analyst picks an executor, sends a greeting, and they exchange
a few messages.
"""

import sys
import os
import time
import threading

# So we can import agentipc from the repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agentipc import AgentIPCBus
from agentipc.session import SessionManager

AGENTS = ["analyst", "executor"]


def run_agent(name: str):
    bus = AgentIPCBus(name, agents=AGENTS)
    bus.start(daemon_mode=True)

    mgr = SessionManager(name, bus)
    mgr.on_established(lambda s: print(f"\n{'='*50}\nSESSION OPEN: {s.summary()}\n{'='*50}"))
    mgr.on_data(lambda s, c: print(f"\n<<< FROM {s.initiator}: {c}"))
    mgr.on_closed(lambda s, r: print(f"\nSESSION CLOSED: {s.summary()} ({r})"))
    mgr.start()

    print(f"[{name}] daemon ready.  Waiting for connections...")

    try:
        if name == "analyst":
            # Analyst initiates a session with executor
            time.sleep(2)
            print(f"[{name}] connecting to executor...")
            session = mgr.connect("executor", "greeting + code review task")
            if session:
                time.sleep(0.5)
                mgr.send_data(session, "Hey executor, can you review PR #42?")
                time.sleep(1)
                mgr.send_data(session, "Focus on the auth module, it had issues before.")
                time.sleep(2)
                mgr.close(session, "review complete, thanks")
            else:
                print(f"[{name}] connection failed — is executor running?")

        elif name == "executor":
            # Executor waits for incoming sessions
            while True:
                time.sleep(1)
                sessions = mgr.active_sessions()
                if sessions:
                    s = sessions[0]
                    mgr.send_data(s, "Sure, I'll start reviewing PR #42 now.")
                    time.sleep(2)
                    mgr.send_data(s, "Auth module looks clean.  One nit: hardcoded secret on line 87.")
                    # Let the analyst close it

    except KeyboardInterrupt:
        pass
    finally:
        mgr.stop()
        bus.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in AGENTS:
        print(f"Usage: {sys.argv[0]} <{'|'.join(AGENTS)}>", file=sys.stderr)
        sys.exit(1)
    run_agent(sys.argv[1])
