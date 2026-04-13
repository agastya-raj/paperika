from __future__ import annotations

import argparse
import json
import sys

from .config import PaperikaConfig, get_default_config
from .db import Database
from .downloader import Downloader, outcome_to_dict
from .locator import create_locator
from .runtime_check import collect_runtime_report, format_runtime_summary
from .worker import run_worker_once


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="paperika", description="Paper discovery and local Chrome downloader MVP")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Initialize the runtime SQLite database")
    subparsers.add_parser("doctor", help="Report runtime readiness for local Chrome downloads")

    locate = subparsers.add_parser("locate", help="Remote-only paper locator")
    locate.add_argument("query")
    locate.add_argument("--mode", default="auto", choices=["lookup", "discover", "auto"])
    locate.add_argument("--provider", default="mock", choices=["mock", "crossref"])
    locate.add_argument("--shortlist-size", type=int, default=None)

    enqueue = subparsers.add_parser("enqueue-download", help="Enqueue a local Chrome download request")
    enqueue.add_argument("raw_input")
    enqueue.add_argument("--force-redownload", action="store_true")

    process = subparsers.add_parser("process-request", help="Process a single queued request")
    process.add_argument("request_id", type=int)

    subparsers.add_parser("retry-pending", help="Process queued/retrying requests that are due")
    subparsers.add_parser("run-worker-once", help="One-shot cron/Hermes-friendly retry worker")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = PaperikaConfig.from_env() if args.command == "doctor" else get_default_config()
    db = Database.from_config(config)

    if args.command == "init-db":
        db.init()
        print(json.dumps({"status": "ok", "db_path": str(config.db_path)}))
        return 0

    if args.command == "doctor":
        report = collect_runtime_report(config)
        print(json.dumps({**report, "summary": format_runtime_summary(report)}, indent=2))
        return 0 if report["ready"] else 1

    db.init()

    if args.command == "locate":
        service = create_locator(config, args.provider)
        result = service.locate(query=args.query, mode=args.mode, shortlist_size=args.shortlist_size)
        print(json.dumps(result.to_dict(), indent=2))
        return 0

    downloader = Downloader(config, db)
    if args.command == "enqueue-download":
        result = downloader.enqueue(args.raw_input, force_redownload=args.force_redownload)
        print(json.dumps(result, indent=2))
        return 0
    if args.command == "process-request":
        outcome = downloader.process_request(args.request_id)
        print(json.dumps(outcome_to_dict(outcome), indent=2))
        return 0
    if args.command == "retry-pending":
        outcomes = downloader.retry_pending()
        print(json.dumps([outcome_to_dict(outcome) for outcome in outcomes], indent=2))
        return 0
    if args.command == "run-worker-once":
        print(json.dumps(run_worker_once(downloader), indent=2))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
