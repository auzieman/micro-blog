#!/usr/bin/env python3
import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_APP_SERVICES = ["blog-api", "blog-ui", "blog-projection", "blog-worker"]
DEFAULT_CORE_SERVICES = ["postgres", "rabbitmq", "redis"]


def run_compose(args: list[str]) -> int:
    command = ["docker", "compose", *args]
    print("+", " ".join(command))
    completed = subprocess.run(command, cwd=PROJECT_ROOT)
    return completed.returncode


def services_or_default(values: list[str] | None, default: list[str]) -> list[str]:
    return values if values else list(default)


def cmd_status(_args) -> int:
    return run_compose(["ps"])


def cmd_build(args) -> int:
    services = services_or_default(args.services, DEFAULT_APP_SERVICES)
    cmd = ["build"]
    if args.no_cache:
        cmd.append("--no-cache")
    cmd.extend(services)
    return run_compose(cmd)


def cmd_up(args) -> int:
    services = services_or_default(args.services, DEFAULT_APP_SERVICES)
    cmd = ["up", "-d"]
    if args.force_recreate:
        cmd.append("--force-recreate")
    cmd.extend(services)
    return run_compose(cmd)


def cmd_restart(args) -> int:
    services = services_or_default(args.services, DEFAULT_APP_SERVICES)
    return run_compose(["up", "-d", "--force-recreate", *services])


def cmd_rebuild(args) -> int:
    services = services_or_default(args.services, DEFAULT_APP_SERVICES)
    build_rc = run_compose(["build", "--no-cache", *services])
    if build_rc != 0:
        return build_rc
    return run_compose(["up", "-d", "--force-recreate", *services])


def cmd_down(_args) -> int:
    return run_compose(["down"])


def cmd_core_up(_args) -> int:
    return run_compose(["up", "-d", *DEFAULT_CORE_SERVICES])


def cmd_load_up(args) -> int:
    cmd = ["--profile", "load", "up"]
    if args.detached:
        cmd.append("-d")
    cmd.extend(args.services or ["loadgen"])
    return run_compose(cmd)


def cmd_logs(args) -> int:
    services = services_or_default(args.services, DEFAULT_APP_SERVICES)
    cmd = ["logs", "--tail", str(args.tail)]
    if args.follow:
        cmd.append("-f")
    cmd.extend(services)
    return run_compose(cmd)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local operator helper for the micro-blog Compose stack.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status", help="Show docker compose status")
    status.set_defaults(func=cmd_status)

    build = subparsers.add_parser("build", help="Build app containers")
    build.add_argument("services", nargs="*", help="Services to build")
    build.add_argument("--no-cache", action="store_true", help="Build without cache")
    build.set_defaults(func=cmd_build)

    up = subparsers.add_parser("up", help="Start app services")
    up.add_argument("services", nargs="*", help="Services to start")
    up.add_argument("--force-recreate", action="store_true", help="Force recreate running containers")
    up.set_defaults(func=cmd_up)

    restart = subparsers.add_parser("restart", help="Force recreate app services")
    restart.add_argument("services", nargs="*", help="Services to restart")
    restart.set_defaults(func=cmd_restart)

    rebuild = subparsers.add_parser("rebuild", help="Build without cache and restart app services")
    rebuild.add_argument("services", nargs="*", help="Services to rebuild")
    rebuild.set_defaults(func=cmd_rebuild)

    down = subparsers.add_parser("down", help="Stop the stack")
    down.set_defaults(func=cmd_down)

    core_up = subparsers.add_parser("core-up", help="Start postgres, rabbitmq, and redis")
    core_up.set_defaults(func=cmd_core_up)

    load_up = subparsers.add_parser("load-up", help="Start the load profile")
    load_up.add_argument("services", nargs="*", help="Profile services to start")
    load_up.add_argument("-d", "--detached", action="store_true", help="Run load profile detached")
    load_up.set_defaults(func=cmd_load_up)

    logs = subparsers.add_parser("logs", help="Tail service logs")
    logs.add_argument("services", nargs="*", help="Services to view")
    logs.add_argument("--tail", type=int, default=120, help="Number of lines to show")
    logs.add_argument("-f", "--follow", action="store_true", help="Follow log output")
    logs.set_defaults(func=cmd_logs)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
