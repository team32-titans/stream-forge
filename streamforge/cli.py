#!/usr/bin/env python3
"""
StreamForge CLI — replaces indefinite expansion of main.py
Usage:
  python -m streamforge.cli live --fleet-size 50000 --partitions 32 --workers 20
  python -m streamforge.cli benchmark --events 100000
  python -m streamforge.cli chaos
  python -m streamforge.cli test
  python -m streamforge.cli topics create
  python -m streamforge.cli api --port 8000
"""
from __future__ import annotations

import argparse
import sys


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="streamforge",
        description="StreamForge - Distributed Stateful Event Streaming Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # live
    p_live = sub.add_parser("live", help="Run continuous streaming pipeline (demo/production)")
    p_live.add_argument("--fleet-size", type=int, default=None)
    p_live.add_argument("--partitions", type=int, default=None)
    p_live.add_argument("--workers", type=int, default=None)
    p_live.add_argument("--metrics-port", type=int, default=None)
    p_live.add_argument("--storage-mode", choices=["production", "demo", "test"], default=None)

    # benchmark
    p_bench = sub.add_parser("benchmark", help="Throughput benchmark")
    p_bench.add_argument("--events", type=int, default=100_000)
    p_bench.add_argument("--fleet-size", type=int, default=None)
    p_bench.add_argument("--partitions", type=int, default=None)

    # chaos
    sub.add_parser("chaos", help="Chaos: worker crash + recovery demo")

    # test
    sub.add_parser("test", help="Run test suite")

    # topics
    p_topics = sub.add_parser("topics", help="Kafka topic administration")
    p_topics.add_argument("action", choices=["create", "describe", "delete"], nargs="?", default="describe")

    # api
    p_api = sub.add_parser("api", help="Start FastAPI control plane")
    p_api.add_argument("--host", type=str, default=None)
    p_api.add_argument("--port", type=int, default=None)
    p_api.add_argument("--reload", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Lazy imports to keep --help fast
    if args.command == "live":
        from streamforge.config import get_settings

        s = get_settings()
        fleet = args.fleet_size or s.fleet_size
        parts = args.partitions or s.kafka_partitions
        workers = args.workers or s.target_workers
        port = args.metrics_port or s.metrics_port
        # Update storage mode if requested
        if args.storage_mode:
            import os

            os.environ["STORAGE_MODE"] = args.storage_mode
            from streamforge.config import reload_settings

            reload_settings()
        import main as legacy

        legacy.run_live(fleet_size=fleet, partitions=parts, workers=workers, metrics_port=port)

    elif args.command == "benchmark":
        from streamforge.config import get_settings

        s = get_settings()
        fleet = args.fleet_size or s.fleet_size
        parts = args.partitions or s.kafka_partitions
        import main as legacy

        legacy.run_benchmark(fleet_size=fleet, partitions=parts, total_events=args.events)

    elif args.command == "chaos":
        import main as legacy

        legacy.run_chaos_demo()

    elif args.command == "test":
        import tests.test_stream_engine as t
        import sys

        t_classes = [t.TestRollingAverageMath, t.TestWindowingAndWatermarks, t.TestRocksDBStateAndChaosRecovery]
        passed, failed = 0, 0
        for cls in t_classes:
            inst = cls()
            for m in dir(inst):
                if m.startswith("test_"):
                    try:
                        getattr(inst, m)()
                        print(f"  \u2713 {cls.__name__}.{m}")
                        passed += 1
                    except Exception as err:
                        print(f"  \u2717 {cls.__name__}.{m}: {err}")
                        failed += 1
        print(f"\nTests: {passed} passed, {failed} failed.")
        sys.exit(0 if failed == 0 else 1)

    elif args.command == "topics":
        from streamforge.infra.kafka_admin import main as kafka_main

        kafka_main(args.action)

    elif args.command == "api":
        from streamforge.config import get_settings

        s = get_settings()
        host = args.host or s.api_host
        port = args.port or s.api_port
        import uvicorn

        uvicorn.run("streamforge.api.main:app", host=host, port=port, reload=args.reload)


if __name__ == "__main__":
    main()
