#!/bin/bash
#SBATCH --job-name=concept-batch
#SBATCH --nodes=3
#SBATCH --ntasks=3
#SBATCH --ntasks-per-node=1
#SBATCH --time=02:00:00
#SBATCH --output=slurm-%j.out
#SBATCH --error=slurm-%j.err
# Adjust the partition and account for your cluster:
##SBATCH --partition=compute
##SBATCH --account=your_allocation

# --- Load modules -------------------------------------------------------
# Adjust module names to match your cluster's module system.
module load openmpi        # e.g. openmpi, mpich, cray-mpich
# module load hdf5         # uncomment if HDF5 is a separate module on your cluster

# --- Activate Python venv on shared filesystem --------------------------
# Create this once on the login node (see README for instructions):
#   python3 -m venv $HOME/venvs/concept
#   source $HOME/venvs/concept/bin/activate
#   pip install -r requirements.txt
source "$HOME/venvs/concept/bin/activate"

# --- Paths --------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY="$SCRIPT_DIR/../../build/code/concept/concept"
DATA_DIR="$SCRIPT_DIR/downloads"   # populated by data_downloader.py before job submission

# --- Run ----------------------------------------------------------------
python3 "$SCRIPT_DIR/batch_runner.py" \
    --data-dir "$DATA_DIR" \
    --binary   "$BINARY" \
    --np       3 \
    --runs     3 \
    --launcher srun \
    --output   "results-$SLURM_JOB_ID.json"
