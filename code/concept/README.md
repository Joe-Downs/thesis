# Batch Compression Benchmarking

Runs the `concept` MPI binary against one or more HDF5 files and records timing
results as JSON.

## Scripts

| Script | Purpose |
|---|---|
| `data_downloader.py` | Download files from a Globus endpoint (run on login node, needs internet) |
| `batch_runner.py` | Run `concept` over a set of local files and save JSON results |

## Quick start

```bash
# Build concept first (from repo root)
cmake -B build && cmake --build build

# Run against a local filelist
python3 batch_runner.py files.txt --binary ./concept --runs 3

# Or point at a directory of files
python3 batch_runner.py --data-dir ./downloads --binary ./concept --runs 3
```

Results are written to `results.json` by default (`--output` to override).

### Downloading files from Globus first

```bash
# Download 15 random files to ./downloads (run this on a machine with internet)
python3 data_downloader.py \
  --client-id       $GLOBUS_CLIENT_ID \
  --source-endpoint <source-endpoint-uuid> \
  --local-endpoint  <your-gcp-endpoint-uuid> \
  --remote-path     /path/on/source/ \
  --sample 15 --seed 42 \
  --local-dir ./downloads

# Then run the benchmark
python3 batch_runner.py --data-dir ./downloads
```


---

## Globus inputs

### `--client-id`

This is the UUID of a "Globus Native App" registration you own.

1. Go to <https://app.globus.org/settings/developers>.
2. Click *Add* > *Register a thick client or script that will be installed and
   run by users on their devices*.
3. Give it any name (e.g. `concept-batch-runner`) and save.
4. Copy the **Client UUID** shown on the app's detail page.
5. Pass it as `--client-id` or export it as `GLOBUS_CLIENT_ID` so you don't have
   to repeat it on every invocation.

### `--source-endpoint`

This is the UUID of the Globus endpoint (collection) that holds the files you
want to download.

- If the dataset is on a published Globus collection, the UUID is in the URL
  when you browse it in the Globus web app:
  `https://app.globus.org/file-manager?origin_id=<uuid>&origin_path=...`
- Alternatively, search for the collection by name at
  <https://app.globus.org/file-manager> and look at the address bar after
  selecting it.

### `--local-endpoint`

This is the UUID of your own machine's Globus Connect Personal endpoint. Globus
transfers data endpoint-to-endpoint, so your laptop/workstation must also be a
Globus endpoint.

1. Install Globus Connect Personal from
   <https://www.globus.org/globus-connect-personal>.
2. Sign in and follow the setup wizard - it will create a personal endpoint tied
   to your account.
3. Open the Globus web app at <https://app.globus.org/file-manager>.
4. In the left panel, click the collection search box and type your machine's
   name. Select your personal endpoint.
5. The UUID appears in the address bar: `?origin_id=<uuid>`.

> **Tip:** You can also run `globus endpoint local-id` (from the `globus-cli`
> package) to print your local endpoint UUID directly.

### `--remote-path`

The directory path on the source endpoint that contains the files. This is the
path as seen by Globus (not a local filesystem path).

- Browse to the collection in the Globus web app and navigate to the directory
  you want. The path is shown in the **Path** field, e.g.
  `/ncar/rda/ds084.1/2020/20200101/`.

### `--local-dir`

A local directory where downloaded files are stored. Defaults to `./downloads`
(resolved to an absolute path automatically). Make sure this path is inside the
accessible paths configured in Globus Connect Personal (by default GCP allows
access to your home directory and any paths you add under *Preferences >
Access*).

---

## All Globus options at a glance

| Flag | Required for Globus | Description |
|---|---|---|
| `--client-id` | Yes | Native App client UUID from app.globus.org |
| `--source-endpoint` | Yes | UUID of the endpoint holding the source files |
| `--local-endpoint` | Yes | UUID of your Globus Connect Personal endpoint |
| `--remote-path` | Yes | Directory path on the source endpoint |
| `--local-dir` | No | Local download destination (default: `downloads`) |
| `--sample N` | No | Randomly pick N files instead of using a filelist |
| `--seed` | No | Integer seed for `--sample` (for repeatability) |

---

## Authentication

On the first run the script will open a browser window for you to log in to
Globus and grant the app transfer permissions. Tokens are cached to
`~/.globus_tokens.json` and refreshed automatically on subsequent runs. You will
not be prompted again unless the tokens expire or the cache is deleted.

---

## Output format

Results are saved as a JSON array, one object per run:

```json
[
  {
    "file": "data-1996-04-16-03-1.h5",
    "run": 1,
    "compress_time_ms": 142.3,
    "send_compressed_ms": 18.7,
    "send_uncompressed_ms": 95.1,
    "compressed_size_bytes": 204800,
    "uncompressed_size_bytes": 1048576
  },
  ...
]
```
