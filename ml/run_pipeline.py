"""
ml/run_pipeline.py
==================
ECS entrypoint for the ML pipeline.

Called by the ECS task command:
    python run_pipeline.py [--date YYYY-MM-DD]

Runs the four ML stages in order:
    1. features.py   — feature engineering from Athena mart tables
    2. train.py      — walk-forward CV + Optuna + LightGBM training
    3. evaluate.py   — evaluation + SHAP + forecast generation
    4. reorder_recommendations.py — safety stock + reorder quantities

The --date argument is passed in by Step Functions via ContainerOverrides
on the PIPELINE_DATE environment variable. If not provided, defaults to
today's date (UTC).

Exit codes:
    0 — all stages completed successfully
    1 — one or more stages failed (Step Functions will catch this and route
        to NotifyFailure)
"""

import argparse
import os
import sys
from datetime import date, datetime, timezone


def _resolve_date(arg_date: str | None) -> str:
    """Return YYYY-MM-DD from CLI arg, PIPELINE_DATE env var, or today UTC."""
    if arg_date:
        return arg_date
    env_date = os.environ.get("PIPELINE_DATE", "").strip()
    if env_date:
        return env_date
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def main():
    parser = argparse.ArgumentParser(description="RetailOps ML pipeline entrypoint")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Pipeline date (YYYY-MM-DD). Defaults to PIPELINE_DATE env var or today UTC.",
    )
    args = parser.parse_args()
    pipeline_date = _resolve_date(args.date)

    print("=" * 70)
    print("RETAILOPS ML PIPELINE")
    print(f"Pipeline date : {pipeline_date}")
    print(f"Started at    : {datetime.now(timezone.utc).isoformat()}")
    print("=" * 70)

    # Expose date to all child modules via environment so athena_client and
    # any date-aware logic can pick it up without needing CLI args.
    os.environ["PIPELINE_DATE"] = pipeline_date

    stages = [
        ("Feature Engineering",      "features",                "build_features"),
        ("Model Training",            "train",                   "train"),
        ("Evaluation",               "evaluate",                "evaluate"),
        ("Reorder Recommendations",  "reorder_recommendations", "generate_recommendations"),
    ]

    for stage_name, module_path, fn_name in stages:
        print(f"\n{'─' * 70}")
        print(f"STAGE: {stage_name}")
        print(f"{'─' * 70}")
        try:
            import importlib
            module = importlib.import_module(module_path)
            fn = getattr(module, fn_name)
            fn()
        except Exception as exc:
            print(f"\n[FATAL] Stage '{stage_name}' failed: {exc}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            sys.exit(1)

    print("\n" + "=" * 70)
    print("ML PIPELINE COMPLETE")
    print(f"Finished at : {datetime.now(timezone.utc).isoformat()}")
    print("=" * 70)
    sys.exit(0)


if __name__ == "__main__":
    main()
