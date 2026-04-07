import sys
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

VERSION = "0.1.0"

LOG_PREFIX_FMT = "%(asctime)s %(process)d %(thread)d"
LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S%z"

SCRIPT_DIR = Path(__file__).resolve().parent


def loadReports(reportsDir: Path) -> list[dict]:
    """Load all JSON report files from reportsDir.

    Returns an empty list if no JSON files are found.
    """
    reportFiles = sorted(reportsDir.glob("*.json"))
    ret = list(map(lambda p: json.loads(p.read_text()), reportFiles))
    return ret


def extractRows(reports: list[dict]) -> pd.DataFrame:
    """Extract one flat row per finding from all reports, before aggregation.

    Columns: issueType, equipmentType, parameter, fileName.
    Pass B errors with no modelParameter (unknown model definition) use the
    modelId as the parameter value and a distinct issueType.
    """
    rows = []
    for report in reports:
        fileName = Path(report['siteFile']).name

        for finding in report.get('passBErrors', []):
            if 'modelParameter' in finding:
                rows.append({
                    'issueType': 'Missing required column',
                    'equipmentType': finding['tab'],
                    'parameter': finding['modelParameter'],
                    'fileName': fileName
                })
            else:
                rows.append({
                    'issueType': 'Unknown model definition',
                    'equipmentType': finding['tab'],
                    'parameter': finding.get('modelId', ''),
                    'fileName': fileName
                })

        for finding in report.get('passBWarnings', []):
            rows.append({
                'issueType': 'Unrecognized column',
                'equipmentType': finding['tab'],
                'parameter': finding['column'],
                'fileName': fileName
            })

        for finding in report.get('passCErrors', []):
            rows.append({
                'issueType': 'Missing required value',
                'equipmentType': finding['tab'],
                'parameter': finding['modelParameter'],
                'fileName': fileName
            })

    ret = pd.DataFrame(rows, columns=['issueType', 'equipmentType', 'parameter', 'fileName'])
    return ret


def aggregateFindings(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate findings by (issueType, equipmentType, parameter).

    Produces one row per unique finding with affected file names joined by '; '.
    Sorted by issueType, then equipmentType, then parameter.
    """
    grouped = df.groupby(['issueType', 'equipmentType', 'parameter'], sort=False)
    aggregated = grouped['fileName'].apply(lambda s: '; '.join(sorted(s.unique()))).reset_index()
    aggregated = aggregated.rename(columns={'fileName': 'affectedFiles'})
    aggregated = aggregated.sort_values(
        ['issueType', 'equipmentType', 'parameter']
    ).reset_index(drop=True)
    ret = aggregated
    return ret


def writeSummary(df: pd.DataFrame, outputDir: Path) -> Path:
    """Write the aggregated summary to a timestamped CSV in outputDir.

    Returns the path of the written file.
    """
    outputDir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    outputPath = outputDir / f"ValidationSummary_{timestamp}.csv"
    df.to_csv(outputPath, index=False)
    logging.info(f"Summary: {len(df)} finding(s) → {outputPath}")
    ret = outputPath
    return ret


def main() -> int:
    """Summarize JSON validation reports from ValidateSiteDir into a single CSV.

    Aggregates Pass B and Pass C findings across all report files, grouping by
    issue type, equipment tab, and parameter. Returns 0 always — this tool
    reports findings but does not itself signal pass/fail.
    """
    logging.basicConfig(
        level=logging.INFO,
        format=f"{LOG_PREFIX_FMT} %(levelname)s %(message)s",
        datefmt=LOG_DATEFMT
    )

    parser = argparse.ArgumentParser(description=f"MAES validation report summarizer v{VERSION}")
    parser.add_argument(
        "reportsDir", type=Path,
        help="Directory containing JSON report files produced by ValidateSiteDir"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=None,
        help="Directory for summary CSV (default: parent of reportsDir)"
    )
    args = parser.parse_args()

    if args.output_dir is not None:
        outputDir = args.output_dir
    else:
        outputDir = args.reportsDir.parent

    logging.info(f"SummarizeReports v{VERSION} — {args.reportsDir}")

    reports = loadReports(args.reportsDir)
    if not reports:
        logging.warning(f"No JSON report files found in {args.reportsDir}")
        ret = 0
        return ret

    logging.info(f"Loaded {len(reports)} report(s)")

    rowDf = extractRows(reports)
    if rowDf.empty:
        logging.info("No findings across all reports.")
        ret = 0
        return ret

    summaryDf = aggregateFindings(rowDf)
    outputPath = writeSummary(summaryDf, outputDir)

    print(f"\n{len(summaryDf)} unique finding(s) across {len(reports)} report(s) → {outputPath}")

    ret = 0
    return ret


if __name__ == "__main__":
    sys.exit(main())
