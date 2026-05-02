"""PlotCDFBins.py — Visualise results from CDFBins.py (Issue #70)

Reads the three CSVs produced by CDFBins.py and generates a set of figures:

  1. Overlaid CDFs — one representative site, siteTotals CICategory,
     includeFugitive=True, one curve per bin size.
  2. KS statistic vs. bin size — per CICategory, median across sites.
  3. PDF + SimPDF row count vs. bin size.
  4. Wall time vs. bin size, one line per worker count (all-sites scope).
  5. SimPDF KS statistic vs. bin size — per CICategory.
  6. Quantile error heatmap — relative error at each quantile × bin size,
     for siteTotals, includeFugitive=True, median across sites.

Usage
-----
conda run -n MAES python PlotCDFBins.py \\
    --resultsDir results/ \\
    [--summaryDir /tmp/.../parquet/Summary]  # only needed for plot 1 (overlaid CDFs)
    [--site Arrowhead_Compressor_Station]    # site for overlaid CDF plot
    [--outputDir plots/]
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

_FIGSIZE   = (8, 5)
_BIN_LABEL = {1e-6: '1e-6', 1e-4: '1e-4', 1e-3: '1e-3', 0.01: '0.01',
               0.1: '0.1', 1.0: '1.0'}
_COLORS    = plt.cm.viridis(np.linspace(0.1, 0.9, 6))


def _bin_label(b: float) -> str:
    return _BIN_LABEL.get(b, f'{b:.0e}')


def _load(resultsDir: Path):
    acc = pd.read_csv(resultsDir / 'results_accuracy.csv')
    perf = pd.read_csv(resultsDir / 'results_perf.csv')
    sim = pd.read_csv(resultsDir / 'results_simpdf.csv')
    return acc, perf, sim


# ---------------------------------------------------------------------------
# Plot 1: Overlaid CDFs
# ---------------------------------------------------------------------------
def plot_overlaid_cdfs(resultsDir: Path, summaryDir: Path, site: str, outputDir: Path):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    import Summaries2 as s2

    cache_path = summaryDir / 'PDFCache' / f'site={site}'
    if not cache_path.exists():
        print(f"plot_overlaid_cdfs: {cache_path} not found, skipping")
        return

    cacheDF = pd.read_parquet(cache_path)
    if 'site' not in cacheDF.columns:
        cacheDF = cacheDF.assign(site=site)

    bin_sizes = [1e-6, 1e-4, 1e-3, 0.01, 0.1, 1.0]
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    for ax, incFug in zip(axes, [True, False]):
        for i, binSize in enumerate(bin_sizes):
            fullDF, noFugDF, _ = s2.calculatePDFSummaryFromCache(cacheDF, binSize=binSize)
            df = fullDF if incFug else noFugDF
            # siteTotals CICategory
            mask = df['CICategory'] == 'siteTotals'
            grp = df[mask].sort_values('emissionRate_kgPerH')
            if grp.empty:
                continue
            # aggregate species (METHANE + ETHANE) by picking one or summing
            for species, sdf in grp.groupby('species'):
                ax.step(sdf['emissionRate_kgPerH'].values,
                        sdf['cumulativeProbability'].values,
                        where='post',
                        label=f'{_bin_label(binSize)} ({species})',
                        color=_COLORS[i],
                        linestyle='-' if species == 'METHANE' else '--',
                        linewidth=1.2)

        fug_label = 'with fugitive' if incFug else 'without fugitive'
        ax.set_xscale('log')
        ax.set_xlabel('Emission rate (kg/h)')
        ax.set_ylabel('Cumulative probability')
        ax.set_title(f'{site}\nsiteTotals, {fug_label}')
        ax.legend(fontsize=7, ncol=2)
        ax.grid(True, which='both', alpha=0.3)

    fig.tight_layout()
    out = outputDir / 'plot1_overlaid_cdfs.png'
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved {out}")


# ---------------------------------------------------------------------------
# Plot 2: KS statistic vs. bin size (per CICategory, median across sites)
# ---------------------------------------------------------------------------
def plot_ks_vs_binsize(acc: pd.DataFrame, outputDir: Path):
    fig, ax = plt.subplots(figsize=_FIGSIZE)
    categories = acc['CICategory'].dropna().unique()
    for cat in sorted(categories):
        sub = acc[(acc['CICategory'] == cat) & (acc.get('includeFugitive', True) == True)]
        if sub.empty:
            continue
        medians = sub.groupby('binSize')['ksStatistic'].median().reset_index()
        medians = medians.sort_values('binSize')
        ax.plot(medians['binSize'], medians['ksStatistic'],
                marker='o', label=cat, linewidth=1.5)
    ax.set_xscale('log')
    ax.set_xlabel('Bin size (kg/h)')
    ax.set_ylabel('KS statistic (median across sites)')
    ax.set_title('CDF accuracy vs. bin size\n(includeFugitive=True, median per CICategory)')
    ax.legend()
    ax.grid(True, which='both', alpha=0.3)
    fig.tight_layout()
    out = outputDir / 'plot2_ks_vs_binsize.png'
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved {out}")


# ---------------------------------------------------------------------------
# Plot 3: Row counts vs. bin size
# ---------------------------------------------------------------------------
def plot_rowcounts(perf: pd.DataFrame, outputDir: Path):
    all_sites = perf[perf['scope'] == 'all_sites'].copy()
    if all_sites.empty:
        print("plot_rowcounts: no all_sites rows, skipping")
        return
    # one row per (binSize, workers=1) to avoid duplicates
    sub = all_sites[all_sites['workers'] == 1].drop_duplicates('binSize')

    fig, ax = plt.subplots(figsize=_FIGSIZE)
    ax.plot(sub['binSize'], sub['pdfRows'],    marker='o', label='PDF rows',    linewidth=1.5)
    ax.plot(sub['binSize'], sub['simPDFRows'], marker='s', label='SimPDF rows', linewidth=1.5)
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel('Bin size (kg/h)')
    ax.set_ylabel('Row count')
    ax.set_title('PDF and SimPDF row count vs. bin size')
    ax.legend()
    ax.grid(True, which='both', alpha=0.3)
    fig.tight_layout()
    out = outputDir / 'plot3_rowcounts.png'
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved {out}")


# ---------------------------------------------------------------------------
# Plot 4: Wall time vs. bin size (per worker count)
# ---------------------------------------------------------------------------
def plot_walltime(perf: pd.DataFrame, outputDir: Path):
    all_sites = perf[perf['scope'] == 'all_sites'].copy()
    if all_sites.empty:
        print("plot_walltime: no all_sites rows, skipping")
        return

    fig, ax = plt.subplots(figsize=_FIGSIZE)
    for workers, grp in all_sites.groupby('workers'):
        grp = grp.sort_values('binSize')
        ax.plot(grp['binSize'], grp['wallTimeSecs'],
                marker='o', label=f'{workers} worker(s)', linewidth=1.5)
    ax.set_xscale('log')
    ax.set_xlabel('Bin size (kg/h)')
    ax.set_ylabel('Wall time (s) — all sites')
    ax.set_title('PDF computation wall time vs. bin size and worker count')
    ax.legend()
    ax.grid(True, which='both', alpha=0.3)
    fig.tight_layout()
    out = outputDir / 'plot4_walltime.png'
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved {out}")


# ---------------------------------------------------------------------------
# Plot 5: SimPDF KS statistic vs. bin size
# ---------------------------------------------------------------------------
def plot_simpdf_ks(sim: pd.DataFrame, outputDir: Path):
    if sim.empty or 'ksStatistic' not in sim.columns:
        print("plot_simpdf_ks: no SimPDF accuracy data, skipping")
        return
    fig, ax = plt.subplots(figsize=_FIGSIZE)
    for cat, grp in sim.groupby('CICategory'):
        sub = grp.groupby('binSize')['ksStatistic'].median().reset_index()
        sub = sub.sort_values('binSize')
        ax.plot(sub['binSize'], sub['ksStatistic'], marker='o', label=cat, linewidth=1.5)
    ax.set_xscale('log')
    ax.set_xlabel('Bin size (kg/h)')
    ax.set_ylabel('KS statistic (median)')
    ax.set_title('SimPDF accuracy vs. bin size\n(median per CICategory)')
    ax.legend()
    ax.grid(True, which='both', alpha=0.3)
    fig.tight_layout()
    out = outputDir / 'plot5_simpdf_ks.png'
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved {out}")


# ---------------------------------------------------------------------------
# Plot 6: Quantile relative error heatmap
# ---------------------------------------------------------------------------
def plot_quantile_heatmap(acc: pd.DataFrame, outputDir: Path):
    q_cols = [c for c in acc.columns if c.endswith('_relerr')]
    if not q_cols:
        print("plot_quantile_heatmap: no quantile error columns, skipping")
        return

    sub = acc[(acc['CICategory'] == 'siteTotals') & (acc.get('includeFugitive', True) == True)]
    if sub.empty:
        print("plot_quantile_heatmap: no siteTotals rows, skipping")
        return

    bin_sizes = sorted(sub['binSize'].unique())
    quantile_labels = [c.replace('_relerr', '').replace('q', 'p') for c in q_cols]

    matrix = np.zeros((len(bin_sizes), len(q_cols)))
    for i, bs in enumerate(bin_sizes):
        row_data = sub[sub['binSize'] == bs][q_cols].median()
        matrix[i, :] = row_data.values

    fig, ax = plt.subplots(figsize=(10, 4))
    im = ax.imshow(matrix.T, aspect='auto', cmap='YlOrRd',
                   vmin=0, vmax=min(matrix.max(), 1.0))
    ax.set_xticks(range(len(bin_sizes)))
    ax.set_xticklabels([_bin_label(b) for b in bin_sizes])
    ax.set_yticks(range(len(quantile_labels)))
    ax.set_yticklabels(quantile_labels)
    ax.set_xlabel('Bin size (kg/h)')
    ax.set_ylabel('Quantile')
    ax.set_title('Relative quantile error vs. bin size\n(siteTotals, includeFugitive=True, median across sites)')
    plt.colorbar(im, ax=ax, label='Relative error')
    for i in range(len(bin_sizes)):
        for j in range(len(q_cols)):
            val = matrix[i, j]
            ax.text(i, j, f'{val:.2f}', ha='center', va='center', fontsize=8,
                    color='black' if val < 0.5 else 'white')
    fig.tight_layout()
    out = outputDir / 'plot6_quantile_heatmap.png'
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  saved {out}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--resultsDir',
                        default=str(Path(__file__).parent / 'results'),
                        help='Directory containing results_*.csv files')
    parser.add_argument('--summaryDir', default=None,
                        help='MAES Summary parquet directory (required for plot 1)')
    parser.add_argument('--site', default=None,
                        help='Site name for overlaid CDF plot (plot 1)')
    parser.add_argument('--outputDir', default=None,
                        help='Where to save plot PNGs (default: same as resultsDir)')
    args = parser.parse_args()

    resultsDir = Path(args.resultsDir)
    outputDir  = Path(args.outputDir) if args.outputDir else resultsDir
    outputDir.mkdir(parents=True, exist_ok=True)

    acc, perf, sim = _load(resultsDir)

    print("Generating plots...")
    if args.summaryDir and args.site:
        plot_overlaid_cdfs(resultsDir, Path(args.summaryDir), args.site, outputDir)
    else:
        print("  plot 1 (overlaid CDFs): skipped (pass --summaryDir and --site)")

    plot_ks_vs_binsize(acc, outputDir)
    plot_rowcounts(perf, outputDir)
    plot_walltime(perf, outputDir)
    plot_simpdf_ks(sim, outputDir)
    plot_quantile_heatmap(acc, outputDir)

    print(f"Done.  Plots in {outputDir}/")


if __name__ == '__main__':
    main()
