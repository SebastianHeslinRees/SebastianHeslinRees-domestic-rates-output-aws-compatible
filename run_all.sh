#!/bin/bash
set -e  # Exit on error
trap '' PIPE  # Prevent SIGPIPE from halting execution

echo -e "========== 🟢 RUNNING: domestic_rates_outputs.R =========="
Rscript domestic_rates_outputs.R

echo -e "========== 🟢 RUNNING: averaging_rates.R =========="
Rscript averaging_rates.R
