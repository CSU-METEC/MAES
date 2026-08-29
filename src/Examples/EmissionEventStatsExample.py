"""
EmissionEventStatsExample.py — EventSummary dataset

Answers: "Which equipment at each site has the most emission events per
MC run? What are the average event durations and emission rates?"

Reads the EventSummary (parquetNewEventSummary) parquet dataset, which
contains event-level statistics aggregated across all MC runs.

Run from the project directory (e.g. MAESForTetra/):
    PYTHONPATH=/home/dugganj/MAES/src python /home/dugganj/MAES/src/Examples/EmissionEventStatsExample.py \
        -s <study>.xlsx -or ./output
"""

import logging

import pandas as pd

import AppUtils as au

TOP_N = 10
RATE_UNITS = 'kg/h'


def main(cMgr):
    logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)
    config = cMgr.asDict()

    event_path = config['parquetNewEventSummary']
    logging.info(f"Reading EventSummary from {event_path}")

    sites = sorted(
        pd.read_parquet(event_path, columns=['site'])['site'].unique()
    )
    logging.info(f"Found {len(sites)} site(s): {', '.join(sites)}")

    for site in sites:
        df = pd.read_parquet(event_path, filters=[('site', '=', site)])

        # The 'modelReadableName' CICategory gives one row per emitter.
        # Filter to a single rate unit to avoid duplicate rows.
        mask = (
            (df['CICategory'] == 'modelReadableName') &
            (df['emissionRateUnits'] == RATE_UNITS)
        )
        view = (
            df[mask]
            [['modelReadableName', 'unitID', 'eventsPerMCRun', 'meanEventDuration_s', 'meanEmissionRate']]
            .sort_values('eventsPerMCRun', ascending=False)
            .head(TOP_N)
        )

        print(f"\n=== {site} — top {TOP_N} emitters by events/MC run ===")
        if view.empty:
            print("  (no rows matched)")
            continue

        print(
            f"  {'modelReadableName':<35}  {'unitID':<20}  "
            f"{'events/run':>10}  {'mean dur (s)':>12}  {'mean rate (kg/h)':>16}"
        )
        print(f"  {'-'*35}  {'-'*20}  {'-'*10}  {'-'*12}  {'-'*16}")
        for _, row in view.iterrows():
            print(
                f"  {str(row['modelReadableName']):<35}  {str(row['unitID']):<20}  "
                f"{row['eventsPerMCRun']:>10.2f}  {row['meanEventDuration_s']:>12.1f}  "
                f"{row['meanEmissionRate']:>16.4f}"
            )


def preMain():
    cMgr, args = au.getConfig()
    if not args.scenarioTimestamp:
        au.findMostRecentScenario(cMgr)
    main(cMgr)


if __name__ == "__main__":
    preMain()
