"""
AnnualEmissionsExample.py — SiteSummary dataset

Answers: "What are the mean annual CH4 emissions and 95% CI for each
equipment type at each site in the simulation?"

Reads the SiteSummary (parquetNewSummary) parquet dataset, filters to the
METype grouping level, and prints a table per site.

Run from the project directory (e.g. MAESForTetra/):
    PYTHONPATH=/home/dugganj/MAES/src python /home/dugganj/MAES/src/Examples/AnnualEmissionsExample.py \
        -s <study>.xlsx -or ./output
"""

import logging

import pandas as pd

import AppUtils as au

SPECIES = 'METHANE'
UNITS = 'kg/year'


def main(cMgr):
    logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)
    config = cMgr.asDict()

    summary_path = config['parquetNewSummary']
    logging.info(f"Reading SiteSummary from {summary_path}")

    # Enumerate sites without loading the full dataset.
    sites = sorted(
        pd.read_parquet(summary_path, columns=['site'])['site'].unique()
    )
    logging.info(f"Found {len(sites)} site(s): {', '.join(sites)}")

    for site in sites:
        df = pd.read_parquet(summary_path, filters=[('site', '=', site)])

        # Filter to the METype grouping level for the species and unit of interest,
        # including all emission categories (includeFugitive=True).
        mask = (
            (df['CICategory'] == 'METype') &
            (df['species'] == SPECIES) &
            (df['includeFugitive'] == True) &
            (df['units'] == UNITS)
        )
        view = df[mask][['METype', 'mean', 'lowerCI', 'upperCI']].sort_values('mean', ascending=False)

        print(f"\n=== {site} — {SPECIES} annual emissions by equipment type ({UNITS}) ===")
        if view.empty:
            print("  (no rows matched)")
            continue

        print(f"  {'METype':<35}  {'mean':>12}  {'lowerCI':>12}  {'upperCI':>12}")
        print(f"  {'-'*35}  {'-'*12}  {'-'*12}  {'-'*12}")
        for _, row in view.iterrows():
            print(f"  {row['METype']:<35}  {row['mean']:>12.2f}  {row['lowerCI']:>12.2f}  {row['upperCI']:>12.2f}")


def preMain():
    cMgr, args = au.getConfig()
    if not args.scenarioTimestamp:
        au.findMostRecentScenario(cMgr)
    main(cMgr)


if __name__ == "__main__":
    preMain()
