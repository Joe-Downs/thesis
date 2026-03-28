#!/usr/bin/env python3
"""Run concept binary over a list of input files and record timing results as JSON."""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Patterns matching concept's stdout lines
PATTERNS = {
    "uncompressed_bytes": re.compile(r"Uncompressed:\s+(\d+)"),
    "compressed_bytes":   re.compile(r"Compressed:\s+(\d+)"),
    "compression_s":      re.compile(r"\[run (\d+)\] Compression:\s+([\d.]+)"),
    "send_compressed_s":  re.compile(r"\[run (\d+)\] Send \(C\):\s+([\d.]+)"),
    "send_uncompressed_s":re.compile(r"\[run (\d+)\] Send \(U\):\s+([\d.]+)"),
    "receive_compressed_s":   re.compile(r"\[rank 1\] Receive:\s+([\d.]+)"),
    "decompression_s":        re.compile(r"\[rank 1\] Decompression:\s+([\d.]+)"),
    "receive_uncompressed_s": re.compile(r"\[rank 2\] Receive:\s+([\d.]+)"),
}


def parse_output(stdout: str, input_file: str) -> list[dict]:
    """Parse output with multiple runs; return list of result dicts, one per run."""
    lines = stdout.split('\n')

    # Extract file-level metadata (once per invocation)
    uncompressed_bytes = None
    compressed_bytes = None
    for line in lines:
        m = PATTERNS["uncompressed_bytes"].search(line)
        if m:
            uncompressed_bytes = int(m.group(1))
        m = PATTERNS["compressed_bytes"].search(line)
        if m:
            compressed_bytes = int(m.group(1))

    # Group metrics by run number
    runs = {}
    for line in lines:
        for key, pat in PATTERNS.items():
            if key in ("uncompressed_bytes", "compressed_bytes"):
                continue  # Already handled
            m = pat.search(line)
            if m:
                if key in ("compression_s", "send_compressed_s", "send_uncompressed_s"):
                    # These have [run N] prefix
                    run_num = int(m.group(1))
                    value = float(m.group(2))
                    if run_num not in runs:
                        runs[run_num] = {}
                    runs[run_num][key] = value
                else:
                    # rank 1/2 metrics: match them sequentially to runs
                    # We'll handle this by collecting all and distributing later
                    value = float(m.group(1))
                    if key not in runs.get(0, {}):  # Temporarily collect
                        runs.setdefault(-1, {}).setdefault(key + "_list", []).append(value)

    # Distribute rank 1/2 metrics across runs
    if -1 in runs:
        temp = runs.pop(-1)
        for key, values in temp.items():
            metric_key = key.replace("_list", "")
            for i, val in enumerate(values):
                if i in runs:
                    runs[i][metric_key] = val

    # Build final result list
    results = []
    for run_num in sorted(runs.keys()):
        result = {
            "file": input_file,
            "run": run_num,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if uncompressed_bytes is not None:
            result["uncompressed_bytes"] = uncompressed_bytes
        if compressed_bytes is not None:
            result["compressed_bytes"] = compressed_bytes
        if uncompressed_bytes and compressed_bytes:
            result["compression_ratio"] = round(uncompressed_bytes / compressed_bytes, 4)
        result.update(runs[run_num])
        results.append(result)

    return results if results else [{"file": input_file, "run": 0, "error": "parse failed", "results": results}]


def run_file(binary: str, np: int, input_file: str, num_runs: int, launcher: str = "mpirun",
             verbose: bool = False, log_dir: str = None) -> list[dict]:
    """Run concept binary once with --runs argument; return list of metrics dicts, one per run."""
    import time

    if launcher == "srun":
        cmd = ["srun", "-n", str(np), binary, input_file, "--runs", str(num_runs)]
    else:
        cmd = ["mpirun", "-n", str(np), binary, input_file, "--runs", str(num_runs)]

    if verbose:
        print(f"\n  Command: {' '.join(cmd)}", file=sys.stderr)

    start_time = time.time()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.time() - start_time

    # Save to log file if requested
    log_path = None
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        file_basename = Path(input_file).stem
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = Path(log_dir) / f"{file_basename}_{timestamp}.log"
        with open(log_path, "w") as f:
            f.write(f"Command: {' '.join(cmd)}\n")
            f.write(f"Exit code: {proc.returncode}\n")
            f.write(f"Duration: {elapsed:.2f}s\n")
            f.write(f"\n{'='*60}\nSTDOUT:\n{'='*60}\n")
            f.write(proc.stdout)
            f.write(f"\n{'='*60}\nSTDERR:\n{'='*60}\n")
            f.write(proc.stderr)

    if proc.returncode != 0:
        # Always print detailed error info
        print(f"\n  ERROR: Command failed with exit code {proc.returncode}", file=sys.stderr)
        print(f"  File: {input_file}", file=sys.stderr)
        print(f"  Command: {' '.join(cmd)}", file=sys.stderr)
        if log_path:
            print(f"  Full output saved to: {log_path}", file=sys.stderr)
        print(f"\n  STDERR:\n{proc.stderr}", file=sys.stderr)
        if proc.stdout.strip():
            print(f"\n  STDOUT:\n{proc.stdout}", file=sys.stderr)
        return [{"error": proc.stderr.strip(), "file": input_file, "run": 0, "exit_code": proc.returncode}]

    if verbose:
        print(f"  Duration: {elapsed:.2f}s", file=sys.stderr)
        print(f"  STDOUT:\n{proc.stdout}", file=sys.stderr)
        if proc.stderr.strip():
            print(f"  STDERR:\n{proc.stderr}", file=sys.stderr)

    results = parse_output(proc.stdout, input_file)
    return results


def main():
    parser = argparse.ArgumentParser(description="Batch runner for concept binary")

    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("filelist", nargs="?",
                     help="Text file with one local file path per line")
    src.add_argument("--data-dir",
                     help="Directory containing HDF5 files to test (all files used)")

    parser.add_argument("--binary", default=os.path.join(os.path.dirname(__file__),
                        "../../build/code/concept/concept"),
                        help="Path to concept binary (default: ../../build/code/concept/concept)")
    parser.add_argument("--np", type=int, default=3,
                        help="Number of MPI ranks (default: 3)")
    parser.add_argument("--launcher", choices=["mpirun", "srun"], default="mpirun",
                        help="MPI launcher to use (default: mpirun; use srun inside Slurm jobs)")
    parser.add_argument("--runs", type=int, default=1,
                        help="Number of repeated runs per file (default: 1)")
    parser.add_argument("--output", default=f"results-{datetime.now().isoformat(timespec='seconds')}.json",
                        help="Output JSON file (default: %(default)s)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Print full command output and timing info")
    parser.add_argument("--log-dir",
                        help="Save all run output to log files in this directory")

    args = parser.parse_args()

    if args.data_dir:
        data_dir = Path(args.data_dir)
        if not data_dir.is_dir():
            parser.error(f"--data-dir '{data_dir}' is not a directory")
        files = sorted(str(p) for p in data_dir.iterdir() if p.is_file())
        if not files:
            parser.error(f"--data-dir '{data_dir}' contains no files")
    else:
        with open(args.filelist) as f:
            files = [line.strip() for line in f if line.strip()]

    print(f"Running {len(files)} file(s) × {args.runs} run(s) with {args.np} MPI ranks")
    if args.verbose:
        print(f"Verbose mode enabled", file=sys.stderr)
    if args.log_dir:
        print(f"Logging to: {args.log_dir}", file=sys.stderr)

    results = []
    for path in files:
        label = f"{path}" + (f" ({args.runs} run(s))" if args.runs > 1 else "")
        print(f"  {label} ... ", end="", flush=True)
        run_results = run_file(args.binary, args.np, path, args.runs, args.launcher,
                               verbose=args.verbose, log_dir=args.log_dir)
        results.extend(run_results)

        # Print summary for first run
        if run_results and "error" not in run_results[0]:
            ratio = run_results[0].get("compression_ratio", "?")
            send_c = run_results[0].get("send_compressed_s", "?")
            send_u = run_results[0].get("send_uncompressed_s", "?")
            print(f"ratio={ratio}  send_C={send_c}s  send_U={send_u}s (first run)")
        else:
            print("FAILED (see stderr for details; use --verbose for more info)")

    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nSaved {len(results)} result(s) to {args.output}")


if __name__ == "__main__":
    main()
