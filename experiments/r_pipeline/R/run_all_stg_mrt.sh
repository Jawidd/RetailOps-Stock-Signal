#!/usr/bin/env bash
set -e


echo "RetailOps: running full R pipeline"


Rscript scripts/01_data_availability_check.R
Rscript scripts/02_data_quality_check.R
Rscript scripts/03_clean_data.R
Rscript scripts/04_test_data.R
Rscript scripts/05_build_marts.R
Rscript scripts/06_analyze_sales.R
Rscript scripts/07_generate_report.R


echo "Pipeline complete"
