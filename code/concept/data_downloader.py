#!/usr/bin/env python3
"""Standalone script to download files from a Globus endpoint.

Run this on a login node (with internet access) before submitting your
batch job.  The downloaded files are placed in --local-dir and can then
be passed to batch_runner.py via --data-dir.

Example
-------
python3 data_downloader.py \\
    --client-id       $GLOBUS_CLIENT_ID \\
    --source-endpoint <source-uuid> \\
    --local-endpoint  <gcp-uuid> \\
    --remote-path     /ncar/rda/ds084.1/2020/ \\
    --sample 15 --seed 42 \\
    --local-dir ./data
"""

import argparse
import os

from file_downloader import GlobusDownloader

def main():
    parser = argparse.ArgumentParser(
        description="Download files from a Globus endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--client-id", default=os.environ.get("GLOBUS_CLIENT_ID"),
                        help="Globus native app client UUID (or $GLOBUS_CLIENT_ID)")
    parser.add_argument("--source-endpoint", required=True,
                        help="Source Globus endpoint UUID")
    parser.add_argument("--local-endpoint", required=True,
                        help="Local Globus Connect Personal endpoint UUID")
    parser.add_argument("--remote-path", required=True,
                        help="Directory on the source endpoint to list/download from")
    parser.add_argument("--local-dir", default="downloads",
                        help="Local directory to download files into (default: downloads)")

    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("filelist", nargs="?",
                     help="Text file with one filename per line to download")
    src.add_argument("--sample", type=int, metavar="N",
                     help="Randomly sample N files from --remote-path")

    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for --sample (for repeatability)")

    args = parser.parse_args()

    if not args.client_id:
        parser.error("--client-id is required (or set $GLOBUS_CLIENT_ID)")

    downloader = GlobusDownloader(
        client_id=args.client_id,
        source_endpoint=args.source_endpoint,
        local_endpoint=args.local_endpoint,
        local_dir=args.local_dir,
    )

    if args.sample:
        print(f"Sampling {args.sample} file(s) from {args.remote_path} …")
        filenames = downloader.sample_files(args.remote_path, args.sample, seed=args.seed)
    else:
        with open(args.filelist) as f:
            filenames = [line.strip() for line in f if line.strip()]

    print(f"Downloading {len(filenames)} file(s) …")
    local_paths = downloader.download(filenames, args.remote_path)
    print(f"Files available in: {args.local_dir}")
    for p in local_paths:
        print(f"  {p}")


if __name__ == "__main__":
    main()
