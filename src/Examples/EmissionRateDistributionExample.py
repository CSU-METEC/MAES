"""
EmissionRateDistributionExample.py — PDF dataset

Answers: "What fraction of operating time is each site emitting CH4 above
a given threshold rate?"

Reads the PDF (parquetNewPDF) parquet dataset, which stores both the
probability mass function (PDF) and the cumulative distribution function
(CDF) for instantaneous emission rates at each site.

The CDF value at a given emission rate is the fraction of total operating
time spent at or below that rate. The exceedance fraction (fraction of time
above the threshold) is therefore 1 - CDF(threshold).

Run from the project directory (e.g. MAESForTetra/):
    PYTHONPATH=/home/dugganj/MAES/src python /home/dugganj/MAES/src/Examples/EmissionRateDistributionExample.py \
        -s <study>.xlsx -or ./output
"""

import logging

import numpy as np
import pandas as pd

import AppUtils as au

SPECIES = 'METHANE'
THRESHOLD_KG_PER_H = 1.0   # fraction of time above this rate is reported


def _cdf_at_threshold(group_df, threshold):
    """Return the CDF value at `threshold` by linear interpolation on the sorted CDF."""
    rates = group_df['emissionRate_kgPerH'].values
    cdf = group_df['cumulativeProbability'].values

    if threshold <= rates[0]:
        return 0.0
    if threshold >= rates[-1]:
        return cdf[-1]

    # Find the index of the largest rate that is <= threshold.
    idx = np.searchsorted(rates, threshold, side='right') - 1
    return float(cdf[idx])


def main(cMgr):
    logging.basicConfig(level=logging.INFO, format=au.LOG_FORMAT)
    config = cMgr.asDict()

    pdf_path = config['parquetNewPDF']
    logging.info(f"Reading PDF from {pdf_path}")

    sites = sorted(
        pd.read_parquet(pdf_path, columns=['site'])['site'].unique()
    )
    logging.info(f"Found {len(sites)} site(s): {', '.join(sites)}")

    print(
        f"\n=== Fraction of time emitting {SPECIES} above {THRESHOLD_KG_PER_H} kg/h "
        f"(CICategory='site', includeFugitive=True) ==="
    )
    print(f"  {'site':<45}  {'exceedance fraction':>20}")
    print(f"  {'-'*45}  {'-'*20}")

    for site in sites:
        df = pd.read_parquet(pdf_path, filters=[('site', '=', site)])

        # The 'site' CICategory is the site-level aggregate across all equipment.
        mask = (
            (df['CICategory'] == 'site') &
            (df['species'] == SPECIES) &
            (df['includeFugitive'] == True)
        )
        view = df[mask].sort_values('emissionRate_kgPerH')

        if view.empty:
            print(f"  {site:<45}  {'(no data)':>20}")
            continue

        cdf_val = _cdf_at_threshold(view, THRESHOLD_KG_PER_H)
        exceedance = 1.0 - cdf_val
        print(f"  {site:<45}  {exceedance:>20.4f}")


def preMain():
    cMgr, args = au.getConfig()
    if not args.scenarioTimestamp:
        au.findMostRecentScenario(cMgr)
    main(cMgr)


if __name__ == "__main__":
    preMain()
