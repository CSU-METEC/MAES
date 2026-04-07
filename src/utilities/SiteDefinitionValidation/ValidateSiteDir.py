import sys
import logging
import argparse
from pathlib import Path

import ValidateSite as vs

VERSION = "0.1.0"

LOG_PREFIX_FMT = "%(asctime)s %(process)d %(thread)d"
LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S%z"

SCRIPT_DIR = Path(__file__).resolve().parent


def validateDirectory(xlsxDir: Path, outputDir: Path, tableDir: Path) -> tuple[int, int, int]:
    """Validate all site definition xlsx files in xlsxDir and write JSON reports to outputDir.

    Iterates over all *.xlsx files in xlsxDir. Files that lack a 'Master Equipment' sheet
    are logged and counted as skipped. Files that fail to load for other reasons are counted
    as failures. Returns (passed, failed, skipped).
    """
    outputDir.mkdir(parents=True, exist_ok=True)
    modelDefDf = vs.loadStaticTables(tableDir)
    buildMeta = vs.loadBuildMetadata(tableDir)

    xlsxFiles = sorted(xlsxDir.glob("*.xlsx"))
    passed = 0
    failed = 0
    skipped = 0

    for xlsxPath in xlsxFiles:
        logging.info(f"Processing: {xlsxPath.name}")
        try:
            siteData = vs.loadSiteXlsx(xlsxPath)
        except ValueError as e:
            logging.warning(f"  Skipping {xlsxPath.name}: {e}")
            skipped += 1
            continue
        except Exception as e:
            logging.error(f"  Failed to load {xlsxPath.name}: {e}")
            failed += 1
            continue

        passMWarnings = vs.runPassM(siteData['globalSimParams'], buildMeta)
        passBErrors, passBWarnings = vs.runPassB(siteData, modelDefDf)
        passCErrors = vs.runPassC(siteData, modelDefDf)

        reportPath = outputDir / f"{xlsxPath.stem}.json"
        vs.writeJsonReport(reportPath, xlsxPath, buildMeta, passMWarnings, passBErrors, passBWarnings, passCErrors)

        errorCount = len(passBErrors) + len(passCErrors)
        warnCount = len(passMWarnings) + len(passBWarnings)
        if errorCount > 0:
            logging.info(f"  {xlsxPath.name}: FAILED ({errorCount} error(s), {warnCount} warning(s))")
            failed += 1
        else:
            logging.info(f"  {xlsxPath.name}: PASSED ({warnCount} warning(s))")
            passed += 1

    ret = passed, failed, skipped
    return ret


def main() -> int:
    """Validate all site definition xlsx files in one or more directories, writing per-file JSON reports.

    Returns 1 if any files failed validation, 0 otherwise.
    """
    logging.basicConfig(
        level=logging.INFO,
        format=f"{LOG_PREFIX_FMT} %(levelname)s %(message)s",
        datefmt=LOG_DATEFMT
    )

    parser = argparse.ArgumentParser(description=f"MAES site directory validator v{VERSION}")
    parser.add_argument("xlsxDirs", type=Path, nargs='+', help="One or more directories containing site definition xlsx files")
    parser.add_argument(
        "--table-dir", type=Path, default=SCRIPT_DIR,
        help="Directory containing reference CSVs from BuildKwargTable.py (default: script directory)"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("SiteValidationReports"),
        help="Directory for JSON reports (default: SiteValidationReports/)"
    )
    args = parser.parse_args()

    totalPassed = 0
    totalFailed = 0
    totalSkipped = 0

    for xlsxDir in args.xlsxDirs:
        logging.info(f"ValidateSiteDir v{VERSION} — {xlsxDir}")
        passed, failed, skipped = validateDirectory(xlsxDir, args.output_dir, args.table_dir)
        totalPassed += passed
        totalFailed += failed
        totalSkipped += skipped

    total = totalPassed + totalFailed + totalSkipped
    print(f"\nValidated {total} file(s): {totalPassed} passed, {totalFailed} failed, {totalSkipped} skipped")

    if totalFailed > 0:
        ret = 1
    else:
        ret = 0
    return ret


if __name__ == "__main__":
    sys.exit(main())
