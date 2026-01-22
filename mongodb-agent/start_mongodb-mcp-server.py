#!/usr/bin/env python

import os
import sys
import argparse
import subprocess
import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv, dotenv_values
except Exception:
    print("Missing dependency: python-dotenv. Install with: pip install python-dotenv", file=sys.stderr)
    sys.exit(2)


def build_parser():
    p = argparse.ArgumentParser(
        description="Load a dotenv file into the environment and launch mongodb-mcp-server in the background."
    )
    p.add_argument(
        "dotenv_file",
        metavar="DOTENV",
        help="Path to a dotenv-style file to load (e.g. .env)",
    )
    p.add_argument(
        "--log-path",
        help="Fully qualified filepath of the log file. Default: same directory as dotenv file with name 'mongodb-mcp-server.log'",
        default=None,
    )
    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    dotenv_path = Path(args.dotenv_file).expanduser().resolve()
    if not dotenv_path.is_file():
        print(f"Dotenv file not found: {dotenv_path}", file=sys.stderr)
        sys.exit(1)

    # Read values from the dotenv file (does not modify os.environ)
    values = dotenv_values(dotenv_path=str(dotenv_path))
    # Filter out None values
    dotenv_env = {k: v for k, v in values.items() if v is not None}

    # Optionally update the current process environment so other code can see them
    os.environ.update(dotenv_env)
    print (os.environ.keys())

    # Determine log file path (same dir as dotenv if not specified)
    if args.log_path:
        log_path = Path(args.log_path).expanduser().resolve()
    else:
        timestamp = datetime.datetime.now().isoformat().replace(":", "-")
        log_path = dotenv_path.parent / f"mongodb-mcp-server-{timestamp}.log"

    cmd = ["npx", "-y", "mongodb-mcp-server@latest", "--transport=http", "--readOnly"]

    # Open log file and launch process in background, redirecting stdout+stderr to the file.
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_f = open(str(log_path), "ab")
    except OSError as e:
        print(f"Failed to open log file {log_path}: {e}", file=sys.stderr)
        sys.exit(3)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=log_f,
            stderr=log_f,
            env=os.environ,
            start_new_session=True,  # detach from this process group
            close_fds=True,
        )
    except OSError as e:
        log_f.close()
        print(f"Failed to start process {cmd}: {e}", file=sys.stderr)
        sys.exit(4)

    # Write PID file next to the log for convenience
    try:
        pid_path = log_path.with_suffix(".pid")
        pid_path.write_text(str(proc.pid), encoding="utf-8")
    except Exception:
        # non-fatal
        pass

    print(f"Started mongodb-mcp-server (pid={proc.pid}), logs -> {log_path}")
    # keep the script exit code 0; the server runs detached
    sys.exit(0)


if __name__ == "__main__":
    main()
