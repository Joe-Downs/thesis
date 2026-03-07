#!/usr/bin/env python3
"""Run concept binary over a list of input files and record timing results as JSON."""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

from file_downloader import GlobusDownloader

# Patterns matching concept's stdout lines
PATTERNS = {
    "uncompressed_bytes": re.compile(r"Uncompressed:\s+(\d+)"),
    "compressed_bytes":   re.compile(r"Compressed:\s+(\d+)"),
    "compression_s":      re.compile(r"\[rank 0\] Compression:\s+([\d.]+)"),
    "send_compressed_s":  re.compile(r"\[rank 0\] Send \(C\):\s+([\d.]+)"),
    "send_uncompressed_s":re.compile(r"\[rank 0\] Send \(U\):\s+([\d.]+)"),
    "receive_compressed_s":   re.compile(r"\[rank 1\] Receive:\s+([\d.]+)"),
    "decompression_s":        re.compile(r"\[rank 1\] Decompression:\s+([\d.]+)"),
    "receive_uncompressed_s": re.compile(r"\[rank 2\] Receive:\s+([\d.]+)"),
}


def parse_output(stdout: str) -> dict:
    result = {}
    for key, pat in PATTERNS.items():
        m = pat.search(stdout)
        if m:
            result[key] = int(m.group(1)) if key.endswith("_bytes") else float(m.group(1))
    if "uncompressed_bytes" in result and "compressed_bytes" in result and result["uncompressed_bytes"]:
        result["compression_ratio"] = round(result["uncompressed_bytes"] / result["compressed_bytes"], 4)
    return result


def run_file(binary: str, np: int, input_file: str, launcher: str = "mpirun") -> dict:
    if launcher == "srun":
        cmd = ["srun", "-n", str(np), binary, input_file]
    else:
        cmd = ["mpirun", "-n", str(np), binary, input_file]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(f"  ERROR (exit {proc.returncode}):\n{proc.stderr.strip()}", file=sys.stderr)
        return {"error": proc.stderr.strip()}
    metrics = parse_output(proc.stdout)
    metrics["file"] = input_file
    metrics["timestamp"] = datetime.now(timezone.utc).isoformat()
    return metrics


def main():
    parser = argparse.ArgumentParser(description="Batch runner for concept binary")
    parser.add_argument("filelist", nargs="?",
                        help="Text file with one local (or remote) filename per line "
                             "(required unless --sample is used)")
    parser.add_argument("--binary", default=os.path.join(os.path.dirname(__file__),
                        "../../build/code/concept/concept"),
                        help="Path to concept binary (default: ../../build/code/concept/concept)")
    parser.add_argument("--np", type=int, default=3,
                        help="Number of MPI ranks (default: 3)")
    parser.add_argument("--launcher", choices=["mpirun", "srun"], default="mpirun",
                        help="MPI launcher to use (default: mpirun; use srun inside Slurm jobs)")
    parser.add_argument("--runs", type=int, default=1,
                        help="Number of repeated runs per file (default: 1)")
    parser.add_argument("--seed", type=int, default=None,
                        help="Optional seed to use for repeatability (default: None)")
    parser.add_argument("--output", default="results.json",
                        help="Output JSON file (default: results.json)")

    # Globus options
    globus = parser.add_argument_group("Globus options")
    globus.add_argument("--client-id", default=os.environ.get("GLOBUS_CLIENT_ID"),
                        help="Globus native app client UUID (or $GLOBUS_CLIENT_ID)")
    globus.add_argument("--source-endpoint",
                        help="Source Globus endpoint UUID")
    globus.add_argument("--local-endpoint",
                        help="Local Globus Connect Personal endpoint UUID")
    globus.add_argument("--remote-path",
                        help="Directory on the source endpoint to list/download from")
    globus.add_argument("--local-dir", default="downloads",
                        help="Local directory to download files into (default: downloads)")
    globus.add_argument("--sample", type=int, metavar="N",
                        help="Randomly sample N files from --remote-path instead of using a filelist")

    args = parser.parse_args()

    use_globus = any([args.source_endpoint, args.local_endpoint, args.sample])

    if use_globus:
        for flag in ("client_id", "source_endpoint", "local_endpoint", "remote_path"):
            if not getattr(args, flag):
                parser.error(f"--{flag.replace('_', '-')} is required when using Globus options")
        downloader = GlobusDownloader(
            client_id=args.client_id,
            source_endpoint=args.source_endpoint,
            local_endpoint=args.local_endpoint,
            local_dir=args.local_dir,
        )
        if args.sample:
            print(f"Sampling {args.sample} random file(s) from {args.remote_path} …")
            filenames = downloader.sample_files(args.remote_path, args.sample, seed=args.seed)
        else:
            if not args.filelist:
                parser.error("filelist is required when not using --sample")
            with open(args.filelist) as f:
                filenames = [line.strip() for line in f if line.strip()]
        print(f"Downloading {len(filenames)} file(s) via Globus …")
        files = downloader.download(filenames, args.remote_path)
    else:
        if not args.filelist:
            parser.error("filelist is required when not using Globus options")
        with open(args.filelist) as f:
            files = [line.strip() for line in f if line.strip()]

    print(f"Running {len(files)} file(s) × {args.runs} run(s) with {args.np} MPI ranks")

    results = []
    for path in files:
        for run_idx in range(args.runs):
            label = f"{path}" + (f" (run {run_idx + 1}/{args.runs})" if args.runs > 1 else "")
            print(f"  {label} ... ", end="", flush=True)
            metrics = run_file(args.binary, args.np, path, args.launcher)
            metrics["run"] = run_idx
            results.append(metrics)
            if "error" not in metrics:
                ratio = metrics.get("compression_ratio", "?")
                send_c = metrics.get("send_compressed_s", "?")
                send_u = metrics.get("send_uncompressed_s", "?")
                print(f"ratio={ratio}  send_C={send_c}s  send_U={send_u}s")
            else:
                print("FAILED")

    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nSaved {len(results)} result(s) to {args.output}")


if __name__ == "__main__":
    main()
