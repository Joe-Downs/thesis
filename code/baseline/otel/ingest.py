#!/usr/bin/env python3
"""Bulk-index OTLP-JSON trace files (written by otel/tracing.py) into a
local Elasticsearch instance.

Usage:
    python ingest.py <path-to-jsonl-file-or-directory> [--es-url URL]

Each span is indexed into an index named `benchmark-<experiment>`, derived
from the span's `experiment` attribute (falls back to `benchmark-unknown` if
absent, e.g. for spans with no such attribute like benchmark.subprocess).
The Elasticsearch document _id is the span's own span_id, so re-running
ingest on the same file is a safe no-op/overwrite rather than creating
duplicates.
"""
import argparse
import json
import sys
from pathlib import Path

from elasticsearch import Elasticsearch, helpers

DEFAULT_ES_URL = "http://localhost:9200"


def iter_trace_files(path: Path):
    if path.is_dir():
        yield from sorted(path.glob("*.jsonl"))
    else:
        yield path


def span_to_action(span: dict, run_experiment: dict) -> dict:
    trace_id = span["context"]["trace_id"]
    span_id = span["context"]["span_id"]
    # child spans (subprocess/parse/export) don't carry experiment/fabric
    # attributes themselves; inherit them from their trace's root run span,
    # tracked in run_experiment as spans are read in file order.
    experiment = span["attributes"].get("experiment") or run_experiment.get(trace_id, "unknown")
    if span["name"] == "benchmark.run":
        run_experiment[trace_id] = experiment

    doc = {
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_id": span["parent_id"],
        "name": span["name"],
        "start_time": span["start_time"],
        "end_time": span["end_time"],
        "status": span["status"],
        "attributes": span["attributes"],
        "resource": span["resource"],
    }
    return {
        "_index": f"benchmark-{experiment}",
        "_id": span_id,
        "_source": doc,
    }


def load_actions(path: Path):
    run_experiment: dict = {}
    for trace_file in iter_trace_files(path):
        with trace_file.open() as f:
            lines = [json.loads(line) for line in f if line.strip()]
        # process benchmark.run spans first so children can inherit experiment
        lines.sort(key=lambda s: 0 if s["name"] == "benchmark.run" else 1)
        for span in lines:
            yield span_to_action(span, run_experiment)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help="trace .jsonl file or directory of them")
    parser.add_argument("--es-url", default=DEFAULT_ES_URL)
    args = parser.parse_args()

    if not args.path.exists():
        sys.exit(f"path not found: {args.path}")

    es = Elasticsearch(args.es_url)
    actions = list(load_actions(args.path))
    if not actions:
        print("no spans found, nothing to ingest")
        return

    success, errors = helpers.bulk(es, actions, stats_only=False)
    print(f"indexed {success} spans")
    if errors:
        print(f"{len(errors)} errors:", file=sys.stderr)
        for err in errors:
            print(f"  {err}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
