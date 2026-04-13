#!/usr/bin/env python3
"""
Engine control CLI — sends commands to the running engine's HTTP server.

Usage:
  python scripts/ctl.py smoke_test          # run one-shot BTC smoke test
  python scripts/ctl.py smoke_test_abort    # abort BTC smoke test in progress
  python scripts/ctl.py smoke_test_eth      # run one-shot ETH smoke test
  python scripts/ctl.py smoke_test_eth_abort # abort ETH smoke test in progress
  python scripts/ctl.py delta_check         # run one-shot delta balance check
  python scripts/ctl.py delta_check_abort   # abort delta check in progress
  python scripts/ctl.py status              # check if engine control server is reachable

The engine must be running (make engine) before sending commands.
Host/port are read from config/settings.toml ([control] section).
"""

import http.client
import json
import sys
from pathlib import Path


def _load_control_cfg() -> tuple[str, int]:
    """Read [control] host/port from settings.toml. Falls back to defaults."""
    config_path = Path(__file__).parent.parent / "config" / "settings.toml"
    try:
        import tomllib

        with open(config_path, "rb") as f:
            cfg = tomllib.load(f)
        ctrl = cfg.get("control", {})
        return ctrl.get("host", "127.0.0.1"), int(ctrl.get("port", 8552))
    except Exception:
        return "127.0.0.1", 8552


VALID_ACTIONS = {
    "smoke_test", "smoke_test_abort",
    "smoke_test_eth", "smoke_test_eth_abort",
    "delta_check", "delta_check_abort",
    "status",
}


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in VALID_ACTIONS:
        print("Usage: python scripts/ctl.py <action>")
        print(f"Actions: {', '.join(sorted(VALID_ACTIONS))}")
        sys.exit(1)

    action = sys.argv[1]
    host, port = _load_control_cfg()

    try:
        conn = http.client.HTTPConnection(host, port, timeout=5)

        if action == "status":
            conn.request("GET", "/status")
        else:
            payload = json.dumps({"action": action}).encode()
            conn.request(
                "POST",
                "/control",
                body=payload,
                headers={"Content-Type": "application/json"},
            )

        resp = conn.getresponse()
        body = json.loads(resp.read().decode())

        if resp.status == 200:
            print(f"OK: {body}")
        else:
            print(f"ERROR {resp.status}: {body}", file=sys.stderr)
            sys.exit(1)

    except OSError as e:
        if isinstance(e, TimeoutError):
            print(f"ERROR: Timed out connecting to {host}:{port}", file=sys.stderr)
        else:
            print(
                f"ERROR: Cannot connect to engine control server at {host}:{port}.\n"
                "  Is the engine running? Start it with: make engine",
                file=sys.stderr,
            )
        sys.exit(1)


if __name__ == "__main__":
    main()
