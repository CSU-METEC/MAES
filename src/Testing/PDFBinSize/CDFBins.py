"""CDFBins.py — PDF/CDF bin size investigation (Issue #70, feat/CDFBins)

Effect A: Recompute per-site PDFs and simulation-level SimPDF from a fixed
PDFCache at varying bin sizes.  Measures accuracy (KS statistic, quantile
error) vs. the 1e-6 kg/h baseline, wall-clock performance, and output row
counts.  Also tests inter-site parallelisation at each bin size.

Inputs
------
--summaryDir   Path to a MAES Summary parquet directory containing PDFCache/
               and PDF/ subdirectories.
--outputDir    Where to write results CSVs (default: <this script's dir>/results/).
--workers      Space-separated worker counts for the parallelisation sweep
               (default: 1 2 4).
--binSizes     Space-separated bin sizes in kg/h to test
               (default: 1e-6 1e-4 1e-3 0.01 0.1 1.0).
--sites        Limit to a subset of sites (space-separated) for quick runs.

Outputs
-------
results_accuracy.csv   Per-site, per-CICategory KS + quantile errors vs. baseline.
results_perf.csv       Wall time and PDF row counts per bin size x worker count.
results_simpdf.csv     SimPDF KS + quantile errors + timing per bin size.

Usage
-----
conda run --no-capture-output -n MAES python3 -u CDFBins.py \\
    --summaryDir /tmp/bundleTest/arrowhead_bundle/MC_20260427_095517/parquet/Summary

Note: both --no-capture-output (conda) and -u (Python) are required for unbuffered
log output. --no-capture-output prevents conda from interposing an internal pipe;
-u prevents Python from buffering its own stdout.
"""

import argparse
import multiprocessing
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import Summaries2 as s2

BIN_SIZES = [1e-6, 1e-4, 1e-3, 0.01, 0.1, 1.0]
QUANTILES = [0.10, 0.25, 0.50, 0.75, 0.90, 0.95]

# Identity columns shared between per-site PDF and baseline (NaN for non-applicable cols)
_PDF_ID_COLS = [
    'site', 'species', 'operator', 'psno',
    'CICategory', 'METype', 'unitID', 'modelReadableName', 'includeFugitive',
]
# Identity columns for simulation-level SimPDF
_SIM_ID_COLS = [
    'CICategory', 'species', 'METype', 'unitID', 'modelReadableName', 'includeFugitive',
]


def _rss_kb() -> int:
    """Current process RSS in KB via /proc/self/status (VmRSS)."""
    with open('/proc/self/status') as _f:
        for _line in _f:
            if _line.startswith('VmRSS:'):
                return int(_line.split()[1])
    return 0


def _fill_na(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Replace None/NaN in object identity columns with '' for consistent groupby keys."""
    df = df.copy()
    for col in cols:
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].fillna('')
    return df


def _cdf_step(x_query: np.ndarray, x_cdf: np.ndarray, y_cdf: np.ndarray) -> np.ndarray:
    """Evaluate a left-continuous step CDF at arbitrary query points."""
    idx = np.searchsorted(x_cdf, x_query, side='right') - 1
    return np.where(idx < 0, 0.0, y_cdf[np.clip(idx, 0, len(y_cdf) - 1)])


def _ks_stat(f1: pd.DataFrame, f2: pd.DataFrame) -> float:
    """KS statistic between two step CDFs (emissionRate_kgPerH, cumulativeProbability)."""
    if f1.empty or f2.empty:
        return np.nan
    x1 = f1['emissionRate_kgPerH'].values
    y1 = f1['cumulativeProbability'].values
    x2 = f2['emissionRate_kgPerH'].values
    y2 = f2['cumulativeProbability'].values
    x_all = np.union1d(x1, x2)
    c1 = _cdf_step(x_all, x1, y1)
    c2 = _cdf_step(x_all, x2, y2)
    return float(np.max(np.abs(c1 - c2)))


def _quantile_values(df: pd.DataFrame) -> list[float]:
    """Emission rate at each QUANTILE by linear interpolation of the stored CDF."""
    grp = df.sort_values('cumulativeProbability')
    p = grp['cumulativeProbability'].values
    x = grp['emissionRate_kgPerH'].values
    return [float(np.interp(q, p, x)) for q in QUANTILES]


def _build_lookup(df: pd.DataFrame, id_cols: list[str]) -> dict:
    """Group df by id_cols (NaN-filled) and return {key_tuple: sorted_group_df}."""
    filled = _fill_na(df, id_cols)
    lookup = {}
    present = [c for c in id_cols if c in filled.columns]
    for key, grp in filled.groupby(present, dropna=False):
        key = key if isinstance(key, tuple) else (key,)
        lookup[key] = grp.sort_values('emissionRate_kgPerH').reset_index(drop=True)
    return lookup


def _compute_sim_pdf(pdfDF: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    """Replicate createSimPDF mixture logic from Summaries2; return (simPDF_df, elapsed_s)."""
    t0 = time.perf_counter()
    rows = []
    for siteCacheLevel, simCacheLevel, simGroupCols in s2.SIM_PDF_LEVEL_MAP:
        levelDF = pdfDF[pdfDF['CICategory'] == siteCacheLevel]
        if levelDF.empty:
            continue
        idCols = [*simGroupCols, 'includeFugitive']
        present = [c for c in idCols if c in levelDF.columns]
        for groupKey, groupDF in levelDF.groupby(present, dropna=False):
            keys = groupKey if isinstance(groupKey, tuple) else (groupKey,)
            identityCols = dict(zip(present, keys))
            nSites = groupDF['site'].nunique()
            if nSites == 0:
                continue
            scaled = groupDF.assign(probability=groupDF['probability'] / nSites)
            mix = (scaled
                   .groupby('emissionRate_kgPerH', as_index=False)['probability']
                   .sum()
                   .sort_values('emissionRate_kgPerH'))
            mix['cumulativeProbability'] = mix['probability'].cumsum()
            n = len(mix)
            rows.append(pd.DataFrame({
                **{col: [val] * n for col, val in identityCols.items()},
                'CICategory': [simCacheLevel] * n,
                'emissionRate_kgPerH': mix['emissionRate_kgPerH'].values,
                'probability': mix['probability'].values,
                'cumulativeProbability': mix['cumulativeProbability'].values,
            }))
    elapsed = time.perf_counter() - t0
    simPDF = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    return simPDF, elapsed


def _compute_site_pdf(args: tuple) -> tuple[str, pd.DataFrame, float, int]:
    """Module-level worker: recompute PDF for one site at the given bin size.

    Must be module-level (not a closure) so multiprocessing can pickle it.
    Returns (site, pdfDF, elapsed_s, rss_delta_kb).
    """
    site, cacheDF, binSize = args
    rss_before = _rss_kb()
    t0 = time.perf_counter()
    fullDF, noFugDF, _ = s2.calculatePDFSummaryFromCache(cacheDF, binSize=binSize)
    elapsed = time.perf_counter() - t0
    rss_delta = max(0, _rss_kb() - rss_before)
    fullDF  = fullDF.assign(site=site,  includeFugitive=True)
    noFugDF = noFugDF.assign(site=site, includeFugitive=False)
    return site, pd.concat([fullDF, noFugDF], ignore_index=True), elapsed, rss_delta


def _accuracy_rows(computedDF: pd.DataFrame, baselineLookup: dict,
                   id_cols: list[str], binSize: float) -> list[dict]:
    """Compare computedDF against baselineLookup; return accuracy metric rows."""
    rows = []
    filled = _fill_na(computedDF, id_cols)
    present = [c for c in id_cols if c in filled.columns]
    for groupKey, grp in filled.groupby(present, dropna=False):
        key = groupKey if isinstance(groupKey, tuple) else (groupKey,)
        id_dict = dict(zip(present, key))
        baseGrp = baselineLookup.get(key)
        grp_sorted = grp.sort_values('emissionRate_kgPerH')
        ks = _ks_stat(baseGrp, grp_sorted) if baseGrp is not None else np.nan
        row = {'binSize': binSize, **id_dict, 'ksStatistic': ks}
        if baseGrp is not None:
            base_q = _quantile_values(baseGrp)
            test_q = _quantile_values(grp_sorted)
            for i, q in enumerate(QUANTILES):
                bq, tq = base_q[i], test_q[i]
                row[f'q{int(q*100)}_base']   = bq
                row[f'q{int(q*100)}_test']   = tq
                row[f'q{int(q*100)}_abserr'] = abs(tq - bq)
                row[f'q{int(q*100)}_relerr'] = (abs(tq - bq) / bq
                                                 if bq and not np.isnan(bq) else np.nan)
        rows.append(row)
    return rows


def _cache_row_count(args: tuple) -> int:
    """Return PDFCache row count for a site args-tuple; used as sort key for dispatch-order comparison."""
    return len(args[1])


def run_sweep(summaryDir: Path, outputDir: Path,
              workerCounts: list[int], binSizes: list[float],
              siteFilter: list[str] | None = None):
    print("Loading per-site PDFCaches...")
    cache_dir = summaryDir / 'PDFCache'
    all_sites = sorted(p.name.split('=', 1)[1] for p in cache_dir.iterdir() if p.is_dir())
    sites = [s for s in all_sites if siteFilter is None or s in siteFilter]
    print(f"  {len(sites)} sites")

    cacheDFs: dict[str, pd.DataFrame] = {}
    for site in sites:
        df = pd.read_parquet(cache_dir / f'site={site}')
        if 'site' not in df.columns:
            df = df.assign(site=site)
        cacheDFs[site] = df

    print("Loading baseline PDF and SimPDF...")
    baselinePDFAll = pd.read_parquet(summaryDir / 'PDF')
    baselinePDF    = baselinePDFAll[baselinePDFAll['site'].astype(str).isin(sites)]
    baselineSimPDF = pd.read_parquet(summaryDir / 'SimPDF')

    baselineLookup    = _build_lookup(baselinePDF,    _PDF_ID_COLS)
    baselineSimLookup = _build_lookup(baselineSimPDF, _SIM_ID_COLS)

    accuracyRows: list[dict] = []
    perfRows:     list[dict] = []
    simPDFRows:   list[dict] = []

    for binSize in binSizes:
        label = f'{binSize:.0e}'
        print(f"\n--- binSize={label} kg/h ---")

        # Accuracy pass: serial for consistent per-site timing
        allSitePDFs = []
        siteTimings = {}
        siteRSS: dict[str, int] = {}
        for site in sites:
            _, pdfDF, elapsed, rss_delta = _compute_site_pdf((site, cacheDFs[site], binSize))
            allSitePDFs.append(pdfDF)
            siteTimings[site] = elapsed
            siteRSS[site] = rss_delta

        allPDFDF = pd.concat(allSitePDFs, ignore_index=True)
        pdfRowCount = len(allPDFDF)
        print(f"  PDF rows: {pdfRowCount:,}")

        # Per-site timing rows (workers=1 serial)
        for site in sites:
            perfRows.append({
                'binSize': binSize, 'workers': 1, 'scope': 'per_site',
                'site': site, 'wallTimeSecs': siteTimings[site],
                'peakRSS_kb': siteRSS[site],
            })

        # Accuracy vs. baseline
        accuracyRows.extend(_accuracy_rows(allPDFDF, baselineLookup, _PDF_ID_COLS, binSize))

        # SimPDF
        simPDF, simElapsed = _compute_sim_pdf(allPDFDF)
        simRowCount = len(simPDF)
        print(f"  SimPDF rows: {simRowCount:,}  time={simElapsed:.3f}s")
        simPDFRows.extend(_accuracy_rows(simPDF, baselineSimLookup, _SIM_ID_COLS, binSize))
        for row in simPDFRows[-len(simPDF):]:  # annotate last batch with timing/size
            row.update({'simPDFRows': simRowCount, 'simWallTimeSecs': simElapsed})

        # Parallelisation sweep: time all sites together at each worker count
        args_list = [(site, cacheDFs[site], binSize) for site in sites]
        for workers in workerCounts:
            print(f"  timing {len(sites)} sites, workers={workers}...", end=' ', flush=True)
            t_start = time.perf_counter()
            if workers == 1:
                timing_results = [_compute_site_pdf(a) for a in args_list]
            else:
                with multiprocessing.Pool(workers) as pool:
                    timing_results = pool.map(_compute_site_pdf, args_list)
            elapsed = time.perf_counter() - t_start
            max_worker_rss = max(r[3] for r in timing_results)
            print(f"{elapsed:.2f}s  maxWorkerRSS={max_worker_rss:,}KB")
            perfRows.append({
                'binSize': binSize, 'workers': workers, 'scope': 'all_sites',
                'site': None, 'wallTimeSecs': elapsed,
                'pdfRows': pdfRowCount, 'simPDFRows': simRowCount,
                'sitesCount': len(sites),
                'maxWorkerRSS_kb': max_worker_rss,
            })

    # Dispatch-order comparison: alphabetical vs heaviest-first at the first bin size only.
    # Measures the load-imbalance effect of the sort fix in SiteMain2.runMultiprocessing.
    defaultBinSize = binSizes[0]
    print(f"\n--- dispatch order comparison (binSize={defaultBinSize:.0e} kg/h) ---")
    sortCompRows: list[dict] = []
    defaultArgs = [(site, cacheDFs[site], defaultBinSize) for site in sites]
    sortedArgs  = sorted(defaultArgs, key=_cache_row_count, reverse=True)
    for workers in workerCounts:
        for label, queue in [('alphabetical', defaultArgs), ('heaviest_first', sortedArgs)]:
            print(f"  workers={workers} order={label}...", end=' ', flush=True)
            t_start = time.perf_counter()
            if workers == 1:
                _ = [_compute_site_pdf(a) for a in queue]
            else:
                with multiprocessing.Pool(workers) as pool:
                    _ = list(pool.map(_compute_site_pdf, queue))
            elapsed = time.perf_counter() - t_start
            print(f"{elapsed:.2f}s")
            sortCompRows.append({
                'binSize': defaultBinSize, 'workers': workers, 'dispatchOrder': label,
                'sitesCount': len(sites), 'wallTimeSecs': elapsed,
            })

    outputDir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(accuracyRows).to_csv(outputDir / 'results_accuracy.csv', index=False)
    pd.DataFrame(perfRows).to_csv(outputDir / 'results_perf.csv', index=False)
    pd.DataFrame(simPDFRows).to_csv(outputDir / 'results_simpdf.csv', index=False)
    pd.DataFrame(sortCompRows).to_csv(outputDir / 'results_sort_comparison.csv', index=False)
    print(f"\nResults written to {outputDir}/")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--summaryDir', required=True,
                        help='Path to MAES Summary parquet directory')
    parser.add_argument('--outputDir',
                        default=str(Path(__file__).parent / 'results'),
                        help='Where to write results CSVs')
    parser.add_argument('--workers', nargs='+', type=int, default=[1, 2, 4, 6, 8],
                        help='Worker counts for parallelisation sweep')
    parser.add_argument('--binSizes', nargs='+', type=float, default=BIN_SIZES,
                        help='Bin sizes in kg/h')
    parser.add_argument('--sites', nargs='+', default=None,
                        help='Limit to these sites (for quick testing)')
    args = parser.parse_args()

    run_sweep(
        summaryDir   = Path(args.summaryDir),
        outputDir    = Path(args.outputDir),
        workerCounts = args.workers,
        binSizes     = args.binSizes,
        siteFilter   = args.sites,
    )


if __name__ == '__main__':
    main()
