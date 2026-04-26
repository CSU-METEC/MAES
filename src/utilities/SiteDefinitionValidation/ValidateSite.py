import sys
import json
import logging
import argparse
from pathlib import Path

import pandas as pd

VERSION = "0.1.0"

LOG_PREFIX_FMT = "%(asctime)s %(process)d %(thread)d"
LOG_DATEFMT = "%Y-%m-%dT%H:%M:%S%z"

SCRIPT_DIR = Path(__file__).resolve().parent
MAES_ROOT = SCRIPT_DIR.parents[2]
MAES_SRC = MAES_ROOT / "src"

# Columns present in every equipment tab that are handled implicitly by the
# loader and do not appear as Sheet parameters in model definition JSONs.
IMPLICIT_COLUMNS = {'model_id'}

sys.path.insert(0, str(MAES_SRC))

from ParamUtils import toParamKey


def loadStaticTables(tableDir: Path = SCRIPT_DIR) -> pd.DataFrame:
    """Load ModelDefinitionMap.csv produced by BuildKwargTable.py.

    Raises FileNotFoundError if the table is missing — the caller should
    prompt the user to run BuildKwargTable.py first.
    """
    modelDefMapPath = tableDir / "ModelDefinitionMap.csv"
    if not modelDefMapPath.exists():
        raise FileNotFoundError(f"Static table not found: {modelDefMapPath} — run BuildKwargTable.py first")
    ret = pd.read_csv(modelDefMapPath)
    return ret


def loadBuildMetadata(tableDir: Path = SCRIPT_DIR) -> dict:
    """Load BuildMetadata.csv produced by BuildKwargTable.py.

    Returns an empty dict if the file is missing rather than raising — build
    metadata is used for version checking only and its absence is non-fatal.
    """
    buildMetadataPath = tableDir / "BuildMetadata.csv"
    if not buildMetadataPath.exists():
        logging.warning(f"BuildMetadata.csv not found at {buildMetadataPath} — version checks skipped")
        ret = {}
        return ret
    df = pd.read_csv(buildMetadataPath)
    if df.empty:
        ret = {}
        return ret
    ret = df.iloc[0].to_dict()
    return ret


def loadSiteXlsx(xlsxPath: Path) -> dict:
    """Load a site definition xlsx and return a dict with master equipment list, tab DataFrames, and global sim params.

    Keys: 'masterEquipment' (list of dicts from Master Equipment tab),
    'tabs' (dict of tab name → DataFrame),
    'globalSimParams' (dict of key → value from Global Simulation Parameters tab; empty dict if tab absent).
    """
    with pd.ExcelFile(xlsxPath) as xlsFile:
        masterEqDf = pd.read_excel(xlsFile, sheet_name="Master Equipment")
        ret = {'masterEquipment': masterEqDf.to_dict('records'), 'tabs': {}, 'globalSimParams': {}}
        if 'Global Simulation Parameters' in xlsFile.sheet_names:
            gspDf = pd.read_excel(xlsFile, sheet_name='Global Simulation Parameters', header=None)
            ret['globalSimParams'] = dict(zip(gspDf[0].astype(str), gspDf[1].astype(str)))
        for row in ret['masterEquipment']:
            tabName = row['Tab']
            ret['tabs'][tabName] = pd.read_excel(xlsFile, sheet_name=tabName)
    return ret


def colKeys(df: pd.DataFrame) -> dict[str, str]:
    """Build a valKey → column name mapping for all columns in a DataFrame.

    Columns whose names cannot be normalized by toParamKey are silently skipped.
    """
    result = {}
    for col in df.columns:
        try:
            result[toParamKey(col)['valKey']] = col
        except Exception:
            pass
    ret = result
    return ret


def sheetParamsForModel(modelDefDf: pd.DataFrame, modelId: str) -> pd.DataFrame:
    """Return the subset of modelDefDf for Sheet-type parameters of a given Model ID.

    Excludes rows with a blank modelParameter (Constant-type entries that were
    included in ModelDefinitionMap but have no xlsx column name).
    Deduplicates by modelParameter so each xlsx column is checked only once.
    """
    mask = (
        (modelDefDf['jsonFile'] == modelId) &
        (modelDefDf['parameterType'] == 'Sheet') &
        (modelDefDf['modelParameter'].notna()) &
        (modelDefDf['modelParameter'] != '')
    )
    ret = modelDefDf[mask].drop_duplicates(subset=['modelParameter'])
    return ret


def runPassM(globalSimParams: dict, buildMeta: dict) -> list[dict]:
    """Run Pass M: check MAES version metadata in the Global Simulation Parameters tab.

    Missing metadata fields produce a single warning. Present fields that differ
    from BuildMetadata.csv produce one warning per mismatched field.
    All findings are warnings — version mismatch is never a validation error.
    """
    warnings = []
    siteVersion = globalSimParams.get('maesVersion', None)
    siteDescribe = globalSimParams.get('gitDescribe', None)

    if siteVersion is None and siteDescribe is None:
        warnings.append({
            'pass': 'M',
            'message': "Missing version tag — add 'maesVersion' or 'gitDescribe' to Global Simulation Parameters"
        })
        ret = warnings
        return ret

    if buildMeta:
        if siteVersion and siteVersion != str(buildMeta.get('maesVersion', '')):
            warnings.append({
                'pass': 'M',
                'field': 'maesVersion',
                'siteValue': siteVersion,
                'currentValue': str(buildMeta.get('maesVersion', 'unknown')),
                'message': f"maesVersion mismatch: site file tagged '{siteVersion}', current tables built from '{buildMeta.get('maesVersion', 'unknown')}'"
            })
        if siteDescribe and siteDescribe != str(buildMeta.get('gitDescribe', '')):
            warnings.append({
                'pass': 'M',
                'field': 'gitDescribe',
                'siteValue': siteDescribe,
                'currentValue': str(buildMeta.get('gitDescribe', 'unknown')),
                'message': f"gitDescribe mismatch: site file tagged '{siteDescribe}', current tables built from '{buildMeta.get('gitDescribe', 'unknown')}'"
            })

    ret = warnings
    return ret


def runPassB(siteData: dict, modelDefDf: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    """Run Pass B: validate that site xlsx columns match model definition parameters.

    For each equipment tab and each unique Model ID in that tab:
    - Required Sheet parameters with no matching xlsx column → error
    - Xlsx columns not matching any model definition parameter → warning

    Column matching uses the same toParamKey() fuzzy logic as the MAES loader.
    Model IDs not present in ModelDefinitionMap are reported as errors.
    """
    errors = []
    warnings = []
    knownJsonFiles = set(modelDefDf['jsonFile'].unique())

    for tabRow in siteData['masterEquipment']:
        tabName = tabRow['Tab']
        tabDf = siteData['tabs'][tabName]
        tabColKeys = colKeys(tabDf)

        modelIds = tabDf['Model ID'].dropna().unique()
        allParamKeys = set(IMPLICIT_COLUMNS)

        for modelId in modelIds:
            modelId = str(modelId)
            if modelId not in knownJsonFiles:
                errors.append({
                    'pass': 'B',
                    'tab': tabName,
                    'modelId': modelId,
                    'message': f"Tab '{tabName}': model definition '{modelId}' not found in ModelDefinitionMap"
                })
                continue

            params = sheetParamsForModel(modelDefDf, modelId)

            for _, paramRow in params.iterrows():
                try:
                    allParamKeys.add(toParamKey(paramRow['modelParameter'])['valKey'])
                except Exception:
                    pass

            requiredParams = params[params['optional'] == False]
            for _, paramRow in requiredParams.iterrows():
                try:
                    paramKey = toParamKey(paramRow['modelParameter'])['valKey']
                except Exception:
                    continue
                if paramKey not in tabColKeys:
                    errors.append({
                        'pass': 'B',
                        'tab': tabName,
                        'modelId': modelId,
                        'modelParameter': paramRow['modelParameter'],
                        'message': f"Tab '{tabName}' (model '{modelId}'): required parameter '{paramRow['modelParameter']}' has no matching column"
                    })

        for colKey, colName in tabColKeys.items():
            if colKey not in allParamKeys:
                warnings.append({
                    'pass': 'B',
                    'tab': tabName,
                    'column': colName,
                    'message': f"Tab '{tabName}': column '{colName}' does not match any model definition parameter"
                })

    ret = errors, warnings
    return ret


def runPassC(siteData: dict, modelDefDf: pd.DataFrame) -> list[dict]:
    """Run Pass C: check that required Sheet parameter values are non-blank for every row.

    For each row in each equipment tab, queries ModelDefinitionMap for the row's
    model definition and checks that every required Sheet parameter has a non-NaN value.
    """
    errors = []

    for tabRow in siteData['masterEquipment']:
        tabName = tabRow['Tab']
        tabDf = siteData['tabs'][tabName]
        tabColKeys = colKeys(tabDf)

        for rowIdx, row in tabDf.iterrows():
            modelId = row.get('Model ID', None)
            if modelId is None or pd.isna(modelId):
                continue
            modelId = str(modelId)

            requiredParams = sheetParamsForModel(modelDefDf, modelId)
            requiredParams = requiredParams[requiredParams['optional'] == False]

            for _, paramRow in requiredParams.iterrows():
                try:
                    paramKey = toParamKey(paramRow['modelParameter'])['valKey']
                except Exception:
                    continue
                matchedCol = tabColKeys.get(paramKey, None)
                if matchedCol is None:
                    continue
                value = row.get(matchedCol, None)
                if value is None or pd.isna(value):
                    errors.append({
                        'pass': 'C',
                        'tab': tabName,
                        'row': rowIdx + 2,
                        'modelParameter': paramRow['modelParameter'],
                        'message': f"Tab '{tabName}', row {rowIdx + 2}: required parameter '{paramRow['modelParameter']}' is blank"
                    })

    ret = errors
    return ret


def printReport(
    passMWarnings: list[dict],
    passBErrors: list[dict],
    passBWarnings: list[dict],
    passCErrors: list[dict],
    xlsxName: str
) -> bool:
    """Print the site validation report to stdout. Returns True if there are any errors."""
    print()
    print(f"PASS M — Version metadata  ({xlsxName})")
    if passMWarnings:
        for finding in passMWarnings:
            print(f"  [WARN]  {finding['message']}")
    else:
        print("  OK — version metadata present and current")

    print()
    print(f"PASS B — Site xlsx vs. Model Definitions  ({xlsxName})")
    if passBErrors or passBWarnings:
        for finding in passBErrors:
            print(f"  [ERROR] {finding['message']}")
        for finding in passBWarnings:
            print(f"  [WARN]  {finding['message']}")
    else:
        print("  OK — all columns accounted for")

    print()
    print("PASS C — Required value completeness")
    if passCErrors:
        for finding in passCErrors:
            print(f"  [ERROR] {finding['message']}")
    else:
        print("  OK — all required values present")

    errorCount = len(passBErrors) + len(passCErrors)
    warnCount = len(passMWarnings) + len(passBWarnings)
    hasErrors = errorCount > 0

    print()
    if hasErrors:
        print(f"Summary: {errorCount} error(s), {warnCount} warning(s) — FAILED")
    else:
        print(f"Summary: 0 errors, {warnCount} warning(s) — PASSED")

    ret = hasErrors
    return ret


def writeJsonReport(
    reportPath: Path,
    xlsxPath: Path,
    buildMeta: dict,
    passMWarnings: list[dict],
    passBErrors: list[dict],
    passBWarnings: list[dict],
    passCErrors: list[dict]
) -> None:
    """Write a structured JSON site validation report to reportPath.

    buildMeta is included verbatim so the report is self-contained and
    traceable to the exact MAES build used for validation.
    """
    from datetime import datetime
    report = {
        'siteFile': str(xlsxPath.resolve()),
        'validatedAt': datetime.now().astimezone().isoformat(),
        'buildMeta': buildMeta,
        'passMWarnings': passMWarnings,
        'passBErrors': passBErrors,
        'passBWarnings': passBWarnings,
        'passCErrors': passCErrors,
        'errorCount': len(passBErrors) + len(passCErrors),
        'warningCount': len(passMWarnings) + len(passBWarnings)
    }
    with open(reportPath, 'w') as f:
        json.dump(report, f, indent=2)
    logging.info(f"JSON report written to {reportPath}")


def main() -> int:
    """Validate a MAES site definition xlsx against model definitions and the kwarg table.

    Returns 1 if validation errors were found, 0 otherwise.
    """
    logging.basicConfig(
        level=logging.INFO,
        format=f"{LOG_PREFIX_FMT} %(levelname)s %(message)s",
        datefmt=LOG_DATEFMT
    )

    parser = argparse.ArgumentParser(description=f"MAES site definition validator v{VERSION}")
    parser.add_argument("xlsxPath", type=Path, help="Path to site definition xlsx file")
    parser.add_argument(
        "--table-dir", type=Path, default=SCRIPT_DIR,
        help="Directory containing reference CSVs from BuildKwargTable.py (default: script directory)"
    )
    parser.add_argument("--json-report", type=Path, default=None, help="Write JSON report to this path")
    args = parser.parse_args()

    logging.info(f"ValidateSite v{VERSION} — {args.xlsxPath.name}")

    modelDefDf = loadStaticTables(args.table_dir)
    buildMeta = loadBuildMetadata(args.table_dir)

    logging.info("Loading site xlsx...")
    siteData = loadSiteXlsx(args.xlsxPath)

    logging.info("Running Pass M...")
    passMWarnings = runPassM(siteData['globalSimParams'], buildMeta)

    logging.info("Running Pass B...")
    passBErrors, passBWarnings = runPassB(siteData, modelDefDf)

    logging.info("Running Pass C...")
    passCErrors = runPassC(siteData, modelDefDf)

    hasErrors = printReport(passMWarnings, passBErrors, passBWarnings, passCErrors, args.xlsxPath.name)

    if args.json_report:
        args.json_report.parent.mkdir(parents=True, exist_ok=True)
        writeJsonReport(args.json_report, args.xlsxPath, buildMeta, passMWarnings, passBErrors, passBWarnings, passCErrors)

    if hasErrors:
        ret = 1
    else:
        ret = 0
    return ret


if __name__ == "__main__":
    sys.exit(main())
