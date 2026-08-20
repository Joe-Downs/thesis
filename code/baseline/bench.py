#!/usr/bin/env python3
"""Raw network baseline benchmark driver: InfiniBand (perftest) vs Ethernet (iperf3).

Usage:
    Server node:
        python bench.py server --fabric infiniband --count 30
        python bench.py server --fabric ethernet

    Client node:
        python bench.py sweep --fabric infiniband --server-node <host> \
            --experiment baseline-ib
        python bench.py sweep --fabric ethernet --server-node <host> \
            --experiment baseline-eth

Each run's timing is recorded as an OpenTelemetry span tree, written to a
local JSONL file under traces/ (see otel/tracing.py). No network export
happens during the run; copy the trace file to wherever Elastic is running
and ingest it there with otel/ingest.py.
"""
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

from opentelemetry import trace
from opentelemetry.trace import StatusCode

from otel.tracing import FileSpanExporter, get_tracer, shutdown_tracing, trace_file_path

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_TRACES_DIR = SCRIPT_DIR / "traces"

# Message sizes in bytes: 4 KiB through 4 MiB.
DEFAULT_SIZES = [2**exp for exp in range(12, 23)]
DEFAULT_REPS = 5

IB_BW_LINE_RE = re.compile(
    r"^\s*(\d+)\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s*$", re.MULTILINE
)


def run_ib_server() -> None:
    subprocess.run(["ib_write_bw"], check=False)


def run_eth_server() -> None:
    subprocess.run(["iperf3", "-s"], check=False)


def cmd_server(args: argparse.Namespace) -> None:
    if args.fabric == "infiniband":
        count = args.count
        if count is None:
            sys.exit("--count is required for --fabric infiniband "
                      "(ib_write_bw serves exactly one client connection per "
                      "invocation, so this must match the client's total "
                      "run count: len(sizes) * reps)")
        for i in range(count):
            print(f"[server] waiting for connection {i + 1}/{count}...")
            run_ib_server()
    elif args.fabric == "ethernet":
        print("[server] starting persistent iperf3 server (Ctrl-C to stop)")
        run_eth_server()
    else:
        sys.exit(f"unknown fabric: {args.fabric}")


def parse_ib_write_bw(output: str) -> float:
    """Return average bandwidth in Mbps, parsed from ib_write_bw stdout."""
    match = None
    for match in IB_BW_LINE_RE.finditer(output):
        pass  # take the last matching data line
    if match is None:
        raise ValueError(f"could not parse ib_write_bw output:\n{output}")
    bw_avg_mb_per_sec = float(match.group(4))
    return bw_avg_mb_per_sec * 8  # MB/sec -> Mbps


def parse_iperf3_json(output: str) -> float:
    """Return average bandwidth in Mbps, parsed from `iperf3 -J` stdout."""
    data = json.loads(output)
    bits_per_second = data["end"]["sum_received"]["bits_per_second"]
    return bits_per_second / 1_000_000  # bps -> Mbps


def run_subprocess(
    tracer: trace.Tracer, exporter: FileSpanExporter, fabric: str, server_node: str, size: int
) -> tuple[str, float]:
    with tracer.start_as_current_span("benchmark.subprocess"):
        if fabric == "infiniband":
            result = subprocess.run(
                ["ib_write_bw", server_node, "-s", str(size)],
                capture_output=True, text=True, check=True,
            )
        else:
            result = subprocess.run(
                ["iperf3", "-c", server_node, "-l", str(size), "-J"],
                capture_output=True, text=True, check=True,
            )
    # span exported synchronously above; exporter.last_duration_s now reflects
    # the write that just happened
    return result.stdout, exporter.last_duration_s


def parse_output(
    tracer: trace.Tracer, exporter: FileSpanExporter, fabric: str, raw: str
) -> tuple[float, float]:
    with tracer.start_as_current_span("benchmark.parse"):
        if fabric == "infiniband":
            bandwidth_mbps = parse_ib_write_bw(raw)
        else:
            bandwidth_mbps = parse_iperf3_json(raw)
    return bandwidth_mbps, exporter.last_duration_s


def run_one(
    tracer: trace.Tracer, exporter: FileSpanExporter, fabric: str, server_node: str, size: int
) -> tuple[float, str, float]:
    raw, subprocess_export_s = run_subprocess(tracer, exporter, fabric, server_node, size)
    bandwidth_mbps, parse_export_s = parse_output(tracer, exporter, fabric, raw)
    # benchmark.export records the write cost of this run's own preceding
    # child spans (see FileSpanExporter docstring for why it can't wrap its
    # own eventual export instead).
    with tracer.start_as_current_span("benchmark.export") as export_span:
        export_span.set_attribute(
            "measured_write_duration_s", subprocess_export_s + parse_export_s
        )
    return bandwidth_mbps, raw, exporter.last_duration_s


def cmd_sweep(args: argparse.Namespace) -> None:
    output_path = trace_file_path(args.traces_dir, args.experiment)
    tracer, exporter = get_tracer("baseline-bench", output_path)

    total = len(args.sizes) * args.reps
    done = 0
    for size in args.sizes:
        for rep in range(1, args.reps + 1):
            done += 1
            print(f"[sweep] ({done}/{total}) size={size} rep={rep}...", end=" ")
            with tracer.start_as_current_span("benchmark.run") as span:
                span.set_attribute("fabric", args.fabric)
                span.set_attribute("experiment", args.experiment)
                span.set_attribute("message_size_b", size)
                span.set_attribute("repetition", rep)
                span.set_attribute("server_node", args.server_node)
                span.set_attribute("client_node", args.client_node)
                try:
                    bandwidth_mbps, raw, _ = run_one(
                        tracer, exporter, args.fabric, args.server_node, size
                    )
                except (subprocess.CalledProcessError, ValueError) as exc:
                    span.record_exception(exc)
                    span.set_status(StatusCode.ERROR)
                    print(f"FAILED: {exc}")
                    continue
                span.set_attribute("bandwidth_mbps", bandwidth_mbps)
                span.set_attribute("raw_output", raw)
                print(f"{bandwidth_mbps:.1f} Mbps")

    shutdown_tracing()
    print(f"[sweep] traces written to {output_path}")


def parse_sizes(raw: str) -> list[int]:
    return [int(s.strip()) for s in raw.split(",") if s.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    server = sub.add_parser("server", help="run the listening side")
    server.add_argument("--fabric", choices=["infiniband", "ethernet"], required=True)
    server.add_argument(
        "--count", type=int, default=None,
        help="number of client connections to serve (required for infiniband; "
             "must equal len(sizes) * reps used on the client)",
    )
    server.set_defaults(func=cmd_server)

    sweep = sub.add_parser("sweep", help="run the sweep from the initiating side")
    sweep.add_argument("--fabric", choices=["infiniband", "ethernet"], required=True)
    sweep.add_argument("--server-node", required=True)
    sweep.add_argument("--client-node", default="localhost")
    sweep.add_argument("--experiment", required=True)
    sweep.add_argument(
        "--sizes", type=parse_sizes, default=DEFAULT_SIZES,
        help="comma-separated message sizes in bytes",
    )
    sweep.add_argument("--reps", type=int, default=DEFAULT_REPS)
    sweep.add_argument("--traces-dir", type=Path, default=DEFAULT_TRACES_DIR)
    sweep.set_defaults(func=cmd_sweep)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
