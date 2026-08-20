# Baseline Network Benchmark

Measures the theoretical-maximum raw transfer speed between two HPC nodes, for
both InfiniBand and Ethernet, as a baseline to compare later application-level
(HDF5/parallel I/O) measurements against.

Each run's timing is recorded as an [OpenTelemetry](https://opentelemetry.io/)
span tree (subprocess launch, output parsing, telemetry write), written to a
local JSONL trace file — no network access needed on the cluster. Traces are
copied to your laptop afterward and ingested into a local
[Elastic](https://www.elastic.co/) (Elasticsearch + Kibana) stack for
dashboarding/analysis. This pattern is meant to be reused by future
benchmarking scripts (compression, transfer, etc.) via `otel/tracing.py`.

## Prerequisites

Cluster nodes need:
- [`perftest`](https://github.com/linux-rdma/perftest) (provides `ib_write_bw`)
  for InfiniBand
- [`iperf3`](https://iperf.fr/) for Ethernet
- Python deps from the repo root: `pip install -r ../../requirements.txt`

Install `perftest`/`iperf3` via the cluster's package manager or
`module load`, as available.

Your laptop needs Docker (for the local Elastic stack) and the same
`requirements.txt` installed (for `otel/ingest.py`'s Elasticsearch client).

## Running a sweep

InfiniBand — `ib_write_bw` serves exactly one client connection per
invocation, so the server must be told how many connections to expect
(`len(sizes) * reps`, matching the client's sweep):

```bash
# server node
python bench.py server --fabric infiniband --count 55   # e.g. 11 sizes x 5 reps

# client node
python bench.py sweep --fabric infiniband --server-node <server-host> \
    --experiment baseline-ib
```

Ethernet — `iperf3 -s` serves any number of connections, so no `--count` is
needed:

```bash
# server node
python bench.py server --fabric ethernet

# client node
python bench.py sweep --fabric ethernet --server-node <server-host> \
    --experiment baseline-eth
```

Defaults: message sizes 4 KiB-4 MiB (powers of two), 5 repetitions each.
Override with `--sizes 4096,16384,...` and `--reps N`. Traces are written to
`traces/<experiment>_<timestamp>.jsonl`.

## Analyzing results in Elastic

```bash
# on your laptop, from the repo root
docker-compose up -d          # starts Elasticsearch (:9200) + Kibana (:5601)

# copy trace files back from the cluster
scp <cluster-host>:path/to/baseline/traces/*.jsonl code/baseline/traces/

# ingest into Elasticsearch (safe to re-run; span_id-keyed, no duplicates)
python code/baseline/otel/ingest.py code/baseline/traces/
```

Each experiment lands in its own index, `benchmark-<experiment>` (e.g.
`benchmark-baseline-ib`). Open Kibana at `http://localhost:5601` to build
dashboards — e.g. `benchmark.run` span duration/`bandwidth_mbps` by
`message_size_b`, or span duration broken down by `name`
(`benchmark.subprocess` / `benchmark.parse` / `benchmark.export`) to see
where time is spent within a run.

`docker-compose down` stops the stack; data persists in a named volume
across restarts. `docker-compose down -v` also deletes the indexed data.
