"""
SimRunComparison.py

CLI tool for statistically comparing emission distributions across two MAES
simulation runs using a two-sample Kolmogorov-Smirnov test.  Mirrors the
comparison Jenna performed manually in GitHub issue #39.

Modes
-----
run-and-compare (default)
    Run SiteMain2 twice with the given parameters, extract the configured
    metric from each output, and apply the KS test.

load-and-compare
    Load two existing simulation output directories (--dir-a / --dir-b) and
    apply the KS test.  Useful for cross-branch comparisons: run the simulation
    on each branch separately, then point this tool at both outputs.

Metrics
-------
site-total   Total emissions across all equipment types per MC iteration,
             aggregated from InstEmissions parquet.

metype       Per-MC-run totals for a single METype (e.g. Tank, Wellhead).
             Requires --group-value.  Reads SiteSummary parquet.

unitid       Per-MC-run totals for a single unit ID.
             Requires --group-value.  Reads SiteSummary parquet.

Usage examples
--------------
# Run the simulation twice and compare (10 MC iterations, P1 site)
python src/Testing/SimRunComparison.py \\
    -s C3/C3_Prototypical_Sites/P1_1stage_noflare.xlsx -mc 10 \\
    --assert equivalent

# Compare two existing output directories from different branches
python src/Testing/SimRunComparison.py \\
    --mode load-and-compare \\
    --dir-a /path/to/main/output \\
    --dir-b /path/to/summary_rewrite/output \\
    --assert different
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

VERSION = "1.0.0"

import argparse
import logging
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import scipy.stats

import SiteMain2 as sm

LOG_PREFIX_FMT = "%(asctime)s %(process)d %(thread)d"
LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S%z"

logger = logging.getLogger(__name__)

MAES_ROOT = Path(__file__).parent.parent.parent

GROUP_BY_CHOICES = ['site-total', 'metype', 'unitid']
MODE_CHOICES = ['run-and-compare', 'load-and-compare']
ASSERT_CHOICES = ['equivalent', 'different', 'none']


@dataclass
class KSResult:
    """Result of a two-sample KS test between two simulation run distributions."""
    statistic: float
    pValue: float
    meanA: float
    meanB: float
    stdA: float
    stdB: float
    nA: int
    nB: int
    alpha: float
    verdict: str   # "EQUIVALENT" or "DIFFERENT"


def getParser() -> argparse.ArgumentParser:
    """Build the argument parser, inheriting core SiteMain2 arguments."""
    siteParser = sm.getParser(sm.DEFAULT_CONFIG)
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[siteParser],
        add_help=True,
    )
    parser.add_argument(
        '--mode', choices=MODE_CHOICES, default='run-and-compare',
        help="run-and-compare: run SiteMain2 twice and compare; "
             "load-and-compare: compare two existing output directories",
    )
    parser.add_argument(
        '--dir-a', type=Path,
        help="First simulation output directory (load-and-compare mode only)",
    )
    parser.add_argument(
        '--dir-b', type=Path,
        help="Second simulation output directory (load-and-compare mode only)",
    )
    parser.add_argument(
        '--alpha', type=float, default=0.05,
        help="KS test significance level (default: 0.05)",
    )
    parser.add_argument(
        '--species', default='METHANE',
        help="Species to compare (default: METHANE)",
    )
    parser.add_argument(
        '--group-by', choices=GROUP_BY_CHOICES, default='site-total',
        dest='groupBy',
        help="Metric grouping dimension (default: site-total)",
    )
    parser.add_argument(
        '--group-value', default=None,
        dest='groupValue',
        help="Filter value for --group-by metype or unitid",
    )
    parser.add_argument(
        '--assert', choices=ASSERT_CHOICES, default='none',
        dest='assertMode',
        help="Exit non-zero if assertion fails: equivalent, different, or none (just report)",
    )
    return parser


def buildSiteMain2Command(args: argparse.Namespace, outputRoot: Path, runTag: str) -> list:
    """Construct the SiteMain2 subprocess command for a single run."""
    cmd = [
        sys.executable,
        str(MAES_ROOT / 'src' / 'SiteMain2.py'),
        '-s', args.studyDefinitionFile,
        '-mc', str(args.monteCarloIterations or 10),
        '-or', str(outputRoot),
        '-ts', runTag,
        '-as', 'True',
    ]
    if args.configFile:
        cmd += ['-c', args.configFile]
    if args.testIntervalDays:
        cmd += ['-t', str(args.testIntervalDays)]
    if args.inputRoot:
        cmd += ['-i', args.inputRoot]
    ret = cmd
    return ret


def resolveOutputDir(outputRoot: Path, studyFile: str, runTag: str) -> Path:
    """Derive the simulation output directory from SiteMain2 naming conventions."""
    studyName = Path(studyFile).stem
    ret = outputRoot / studyName / f'MC_{runTag}'
    return ret


def runSimulation(args: argparse.Namespace, outputRoot: Path, runTag: str) -> Path:
    """
    Run SiteMain2 as a subprocess and return the output directory.

    Raises subprocess.CalledProcessError if the simulation fails.
    """
    cmd = buildSiteMain2Command(args, outputRoot, runTag)
    logger.info(f"Running simulation: tag={runTag}")
    subprocess.run(cmd, cwd=MAES_ROOT, check=True, capture_output=False)
    ret = resolveOutputDir(outputRoot, args.studyDefinitionFile, runTag)
    return ret


def readInstEmissions(outputDir: Path, species: str) -> pd.DataFrame:
    """
    Load all InstEmissions parquet files under outputDir, filtered by species.

    Returns a DataFrame with at least mcRun and totalEmission_kg columns.
    """
    files = list(outputDir.glob('parquet/Summary/InstEmissions/**/*.parquet'))
    if not files:
        raise FileNotFoundError(f"No InstEmissions parquet files found under {outputDir}")
    frames = list(map(pd.read_parquet, files))
    df = pd.concat(frames, ignore_index=True)
    ret = df[df['species'] == species]
    return ret


def readSiteSummary(outputDir: Path) -> pd.DataFrame:
    """Load all SiteSummary parquet files under outputDir into a single DataFrame."""
    files = list(outputDir.glob('parquet/Summary/SiteSummary/**/*.parquet'))
    if not files:
        raise FileNotFoundError(f"No SiteSummary parquet files found under {outputDir}")
    frames = list(map(pd.read_parquet, files))
    ret = pd.concat(frames, ignore_index=True)
    return ret


def extractSiteTotal(outputDir: Path, species: str) -> np.ndarray:
    """
    Extract per-MC-run total emissions (kg) across all equipment types.

    Aggregates totalEmission_kg from InstEmissions grouped by mcRun.
    """
    df = readInstEmissions(outputDir, species)
    grouped = df.groupby('mcRun')['totalEmission_kg'].sum()
    ret = grouped.values
    return ret


def extractByMEType(outputDir: Path, species: str, meType: str) -> np.ndarray:
    """
    Extract per-MC-run emission totals for a single METype from SiteSummary.

    Returns the readings array (one value per MC run) for the matching row.
    """
    df = readSiteSummary(outputDir)
    mask = (df['species'] == species) & (df['CICategory'] == 'METype') & (df['METype'] == meType)
    rows = df[mask]
    if rows.empty:
        raise ValueError(f"No SiteSummary row for species={species}, METype={meType}")
    readings = rows['readings'].iloc[0]
    ret = np.array(readings)
    return ret


def extractByUnitID(outputDir: Path, species: str, unitID: str) -> np.ndarray:
    """
    Extract per-MC-run emission totals for a single unit ID from SiteSummary.

    Returns the readings array (one value per MC run) for the matching row.
    """
    df = readSiteSummary(outputDir)
    mask = (df['species'] == species) & (df['CICategory'] == 'unitID') & (df['unitID'] == unitID)
    rows = df[mask]
    if rows.empty:
        raise ValueError(f"No SiteSummary row for species={species}, unitID={unitID}")
    readings = rows['readings'].iloc[0]
    ret = np.array(readings)
    return ret


def extractMetric(outputDir: Path, species: str, groupBy: str, groupValue: str) -> np.ndarray:
    """
    Extract the configured metric distribution from a simulation output directory.

    Returns a 1-D array of per-MC-run values suitable for KS testing.
    """
    if groupBy == 'site-total':
        ret = extractSiteTotal(outputDir, species)
    elif groupBy == 'metype':
        ret = extractByMEType(outputDir, species, groupValue)
    elif groupBy == 'unitid':
        ret = extractByUnitID(outputDir, species, groupValue)
    else:
        raise ValueError(f"Unknown groupBy: {groupBy}")
    return ret


def compareDistributions(readingsA: np.ndarray, readingsB: np.ndarray, alpha: float) -> KSResult:
    """
    Apply a two-sample KS test and return a KSResult.

    Verdict is EQUIVALENT when p >= alpha, DIFFERENT when p < alpha.
    """
    stat, pValue = scipy.stats.ks_2samp(readingsA, readingsB)
    verdict = 'EQUIVALENT' if pValue >= alpha else 'DIFFERENT'
    ret = KSResult(
        statistic=float(stat),
        pValue=float(pValue),
        meanA=float(np.mean(readingsA)),
        meanB=float(np.mean(readingsB)),
        stdA=float(np.std(readingsA)),
        stdB=float(np.std(readingsB)),
        nA=len(readingsA),
        nB=len(readingsB),
        alpha=alpha,
        verdict=verdict,
    )
    return ret


def reportResult(result: KSResult) -> None:
    """Log the KS test result in a human-readable format."""
    logging.info("=" * 60)
    logging.info(f"KS test result (alpha={result.alpha})")
    logging.info(f"  Run A: n={result.nA}  mean={result.meanA:.2f}  std={result.stdA:.2f}")
    logging.info(f"  Run B: n={result.nB}  mean={result.meanB:.2f}  std={result.stdB:.2f}")
    logging.info(f"  KS statistic : {result.statistic:.4f}")
    logging.info(f"  p-value      : {result.pValue:.4f}")
    logging.info(f"  Verdict      : {result.verdict}")
    logging.info("=" * 60)


def main() -> None:
    """Parse arguments, run or load simulations, compare distributions, and report."""
    logging.basicConfig(
        level=logging.INFO,
        format=f"{LOG_PREFIX_FMT} %(levelname)s %(message)s",
        datefmt=LOG_DATEFMT,
    )
    parser = getParser()
    args = parser.parse_args()

    if args.groupBy != 'site-total' and not args.groupValue:
        parser.error(f"--group-value is required when --group-by is {args.groupBy}")

    if args.mode == 'load-and-compare':
        if not args.dir_a or not args.dir_b:
            parser.error("--dir-a and --dir-b are required for load-and-compare mode")
        dirA = args.dir_a
        dirB = args.dir_b
    else:
        with tempfile.TemporaryDirectory() as tmpDir:
            tmpPath = Path(tmpDir)
            dirA = runSimulation(args, tmpPath / 'run_a', 'runA')
            dirB = runSimulation(args, tmpPath / 'run_b', 'runB')
            readingsA = extractMetric(dirA, args.species, args.groupBy, args.groupValue)
            readingsB = extractMetric(dirB, args.species, args.groupBy, args.groupValue)
            result = compareDistributions(readingsA, readingsB, args.alpha)
            reportResult(result)
            _applyAssertion(result, args.assertMode)
            return

    readingsA = extractMetric(dirA, args.species, args.groupBy, args.groupValue)
    readingsB = extractMetric(dirB, args.species, args.groupBy, args.groupValue)
    result = compareDistributions(readingsA, readingsB, args.alpha)
    reportResult(result)
    _applyAssertion(result, args.assertMode)


def _applyAssertion(result: KSResult, assertMode: str) -> None:
    """
    Exit non-zero if the assertion fails.

    equivalent: fail if verdict is DIFFERENT.
    different:  fail if verdict is EQUIVALENT.
    none:       always succeed.
    """
    if assertMode == 'equivalent' and result.verdict == 'DIFFERENT':
        logging.error("ASSERTION FAILED: distributions are DIFFERENT (expected EQUIVALENT)")
        sys.exit(1)
    if assertMode == 'different' and result.verdict == 'EQUIVALENT':
        logging.error("ASSERTION FAILED: distributions are EQUIVALENT (expected DIFFERENT)")
        sys.exit(1)


if __name__ == '__main__':
    main()
