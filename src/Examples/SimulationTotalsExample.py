"""
SimulationTotalsExample.py — SimSummary dataset

Answers: "What are the simulation-wide total emissions for each species,
and what does the Monte Carlo distribution look like?"

Reads the SimSummary (parquetNewSimSummary) parquet dataset, which rolls up
emissions across all sites in the simulation. This dataset is unpartitioned
(no site column) — one file covers the entire run.

Run from the project directory (e.g. MAESForTetra/):
    PYTHONPATH=/home/dugganj/MAES/src python /home/dugganj/MAES/src/Examples/SimulationTotalsExample.py \
        -s <study>.xlsx -or ./output
"""

import logging

import numpy as np
import pandas as pd

import AppUtils as au

UNITS = 'kg/year'


def main(cMgr):
    logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)
    config = cMgr.asDict()

    sim_path = config['parquetNewSimSummary']
    logging.info(f"Reading SimSummary from {sim_path}")

    # SimSummary is unpartitioned — read the whole dataset.
    df = pd.read_parquet(sim_path)

    # The 'simulation' CICategory row is the single cross-site total for each
    # species / units / includeFugitive combination.
    mask = (
        (df['CICategory'] == 'simulation') &
        (df['units'] == UNITS) &
        (df['includeFugitive'] == True)
    )
    view = df[mask].sort_values('species')

    print(f"\n=== Simulation-wide totals ({UNITS}, includeFugitive=True) ===")
    print(f"  {'species':<12}  {'mean':>12}  {'min':>12}  {'max':>12}  {'lowerCI':>12}  {'upperCI':>12}  {'MC runs':>8}")
    print(f"  {'-'*12}  {'-'*12}  {'-'*12}  {'-'*12}  {'-'*12}  {'-'*12}  {'-'*8}")

    for _, row in view.iterrows():
        readings = row['readings']
        n_runs = len(readings) if readings is not None else 0
        print(
            f"  {row['species']:<12}  {row['mean']:>12.2f}  {row['min']:>12.2f}  "
            f"{row['max']:>12.2f}  {row['lowerCI']:>12.2f}  {row['upperCI']:>12.2f}  {n_runs:>8}"
        )

    # Show MC run percentile distribution for each species.
    print(f"\n=== Monte Carlo percentiles ({UNITS}, includeFugitive=True) ===")
    print(f"  {'species':<12}  {'P10':>12}  {'P25':>12}  {'P50':>12}  {'P75':>12}  {'P90':>12}")
    print(f"  {'-'*12}  {'-'*12}  {'-'*12}  {'-'*12}  {'-'*12}  {'-'*12}")

    for _, row in view.iterrows():
        readings = row['readings']
        if readings is None or len(readings) == 0:
            continue
        p10, p25, p50, p75, p90 = np.percentile(readings, [10, 25, 50, 75, 90])
        print(f"  {row['species']:<12}  {p10:>12.2f}  {p25:>12.2f}  {p50:>12.2f}  {p75:>12.2f}  {p90:>12.2f}")


def preMain():
    cMgr, args = au.getConfig()
    if not args.scenarioTimestamp:
        au.findMostRecentScenario(cMgr)
    main(cMgr)


if __name__ == "__main__":
    preMain()
