#!/usr/bin/env python3
"""
Pipeline orchestrator — runs all post-ingest stages in sequence.

Stages (in order):
  1c  dedup.py      — title-based deduplication
  1b  text_fetch.py — full-text scraping (trafilatura)
  2a  classifier.py — LLM relevance filter
  2b  extractor.py  — LLM structured extraction → incidents table

The ingest stages (gdelt_fetch.py, gdelt_gkg_fetch.py) are handled by cron
and are NOT run here. Run this script after ingest to process new articles
all the way through to incidents.

Usage:
    python3 run_pipeline.py                     # run all stages
    python3 run_pipeline.py --batch 200         # cap each LLM stage at 200 articles
    python3 run_pipeline.py --skip-dedup        # skip dedup (if already run separately)
    python3 run_pipeline.py stats               # show per-stage counts only

Scheduling: add this to cron after the ingest jobs complete, e.g.:
    30 * * * * python3 /home/frieda/Dissertation/pipeline/run_pipeline.py
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR   = Path(__file__).parent
REPO_ROOT    = SCRIPT_DIR.parent
LOG_PATH     = REPO_ROOT / "logs" / "run_pipeline.log"

PYTHON = sys.executable

STAGES = {
    "dedup":      SCRIPT_DIR / "dedup.py",
    "text_fetch": SCRIPT_DIR / "text_fetch.py",
    "classifier": SCRIPT_DIR / "classifier.py",
    "extractor":  SCRIPT_DIR / "extractor.py",
}


def run_stage(script: Path, extra_args: list[str] = ()) -> int:
    cmd = [PYTHON, str(script)] + list(extra_args)
    logging.info("=== running: %s ===", " ".join(str(c) for c in cmd))
    result = subprocess.run(cmd, cwd=str(REPO_ROOT))
    if result.returncode != 0:
        logging.error("stage %s exited with code %d", script.name, result.returncode)
    return result.returncode


def cmd_run(batch: int, skip_dedup: bool) -> None:
    logging.info("=== pipeline start  batch=%d  skip_dedup=%s ===", batch, skip_dedup)
    errors = []

    rc = run_stage(STAGES["text_fetch"], ["fetch", "--batch", str(batch)])
    if rc:
        errors.append("text_fetch")

    if not skip_dedup:
        rc = run_stage(STAGES["dedup"])
        if rc:
            errors.append("dedup")

    rc = run_stage(STAGES["classifier"], ["classify", "--batch", str(batch)])
    if rc:
        errors.append("classifier")

    rc = run_stage(STAGES["extractor"], ["extract", "--batch", str(batch)])
    if rc:
        errors.append("extractor")

    if errors:
        logging.error("=== pipeline done WITH ERRORS in: %s ===", ", ".join(errors))
        sys.exit(1)
    else:
        logging.info("=== pipeline done — all stages succeeded ===")


def cmd_stats() -> None:
    for name, script in STAGES.items():
        print(f"\n{'━'*60}")
        print(f"  {name}")
        print(f"{'━'*60}")
        subprocess.run([PYTHON, str(script), "stats"], cwd=str(REPO_ROOT))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH),
            logging.StreamHandler(),
        ],
    )

    parser = argparse.ArgumentParser(
        description="Pipeline orchestrator — runs dedup → text_fetch → classifier → extractor"
    )
    sub = parser.add_subparsers(dest="cmd")

    p_run = sub.add_parser("run", help="Run all stages (default)")
    p_run.add_argument(
        "--batch", type=int, default=500,
        help="Max articles per LLM stage per run (default 500)"
    )
    p_run.add_argument(
        "--skip-dedup", action="store_true",
        help="Skip dedup (if already run separately)"
    )

    sub.add_parser("stats", help="Show per-stage counts without running anything")

    args = parser.parse_args()

    if args.cmd == "stats":
        cmd_stats()
    else:
        batch      = getattr(args, "batch", 500)
        skip_dedup = getattr(args, "skip_dedup", False)
        cmd_run(batch, skip_dedup)


if __name__ == "__main__":
    main()
