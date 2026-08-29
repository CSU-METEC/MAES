"""
MCVariabilityExample.py — SiteSummary `readings` column

Answers: "For each equipment type at each site, what does the full Monte
Carlo distribution look like — and are any MC runs statistical outliers?"

The SiteSummary dataset stores precomputed statistics (mean, min, max, CI)
but also retains the raw per-MC-run values in the `readings` list column.
This example reads `readings` directly to compute custom percentiles and
flag outlier MC runs that fall more than 3 standard deviations above the
mean — a simple heuristic for identifying unusual simulation behaviour.

Run from the project directory (e.g. MAESForTetra/):
    PYTHONPATH=/home/dugganj/MAES/src python /home/dugganj/MAES/src/Examples/MCVariabilityExample.py \
        -s <study>.xlsx -or ./output
"""

import logging

import numpy as np
import pandas as pd

import AppUtils as au

SPECIES = 'METHANE'
UNITS = 'kg/year'
OUTLIER_SIGMA = 3.0


def main(cMgr):
    logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)
    config = cMgr.asDict()

    summary_path = config['parquetNewSummary']
    logging.info(f"Reading SiteSummary from {summary_path}")

    sites = sorted(
        pd.read_parquet(summary_path, columns=['site'])['site'].unique()
    )
    logging.info(f"Found {len(sites)} site(s): {', '.join(sites)}")

    for site in sites:
        df = pd.read_parquet(summary_path, filters=[('site', '=', site)])

        mask = (
            (df['CICategory'] == 'METype') &
            (df['species'] == SPECIES) &
            (df['includeFugitive'] == True) &
            (df['units'] == UNITS)
        )
        view = df[mask].sort_values('mean', ascending=False)

        print(f"\n=== {site} — {SPECIES} MC distribution by equipment type ({UNITS}) ===")
        if view.empty:
            print("  (no rows matched)")
            continue

        print(
            f"  {'METype':<35}  {'MC runs':>8}  {'P5':>10}  {'P25':>10}  "
            f"{'P50':>10}  {'P75':>10}  {'P95':>10}  {'outliers':>9}"
        )
        print(
            f"  {'-'*35}  {'-'*8}  {'-'*10}  {'-'*10}  "
            f"{'-'*10}  {'-'*10}  {'-'*10}  {'-'*9}"
        )

        for _, row in view.iterrows():
            readings = row['readings']
            if readings is None or len(readings) == 0:
                continue
            arr = np.array(readings, dtype=float)
            n = len(arr)
            p5, p25, p50, p75, p95 = np.percentile(arr, [5, 25, 50, 75, 95])
            threshold = arr.mean() + OUTLIER_SIGMA * arr.std()
            outlier_count = int((arr > threshold).sum())
            print(
                f"  {row['METype']:<35}  {n:>8}  {p5:>10.2f}  {p25:>10.2f}  "
                f"{p50:>10.2f}  {p75:>10.2f}  {p95:>10.2f}  {outlier_count:>9}"
            )


def preMain():
    cMgr, args = au.getConfig()
    if not args.scenarioTimestamp:
        au.findMostRecentScenario(cMgr)
    main(cMgr)


if __name__ == "__main__":
    preMain()
