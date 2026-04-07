# SiteDefinitionValidation

Tools for validating MAES site definition xlsx files against model definitions
and Python class kwarg tables.

## Scripts

| Script | Purpose |
|---|---|
| `BuildKwargTable.py` | Generates static reference tables by inspecting MAES Python classes and model definition JSONs |
| `ValidateSite.py` | Validates a single site definition xlsx file |
| `ValidateSiteDir.py` | Validates all xlsx files in a directory, writing per-file JSON reports |

## Workflow

### Step 1 — Build reference tables

Run once after installing or updating MAES (from the repo root):

```
python src/utilities/SiteDefinitionValidation/BuildKwargTable.py [--table-dir <dir>]
```

Writes four CSV files to `--table-dir` (default: the script's own directory):

| File | Contents |
|---|---|
| `KwargTable.csv` | One row per (class, kwarg) across all MAES equipment classes |
| `ModelDefinitionMap.csv` | One row per model definition parameter, with `inKwargTable` flag |
| `UnmappedKwargs.csv` | Required kwargs with no model definition entry (pre-computed for Pass A2) |
| `BuildMetadata.csv` | MAES version and git state at build time |

These files are gitignored — they are derived artifacts. Regenerate them:

- After updating MAES Python classes → regenerates all four files
- After editing model definition JSONs in `input/ModelFormulation/` → regenerates
  `ModelDefinitionMap.csv` and `UnmappedKwargs.csv` only

### Step 2 — Validate a site file

```
python src/utilities/SiteDefinitionValidation/ValidateSite.py <site.xlsx> \
    [--table-dir <dir>] [--json-report <file>]
```

`--table-dir` specifies where to read the reference CSVs from (default: script directory).

### Step 3 — Validate a directory of site files

```
python src/utilities/SiteDefinitionValidation/ValidateSiteDir.py <dir> \
    [--table-dir <dir>] [--output-dir <dir>]
```

`--table-dir` specifies where the reference CSVs are read from (default: script directory).
`--output-dir` specifies where JSON reports are written (default: `SiteValidationReports/`).

## Validation passes

| Pass | Description |
|---|---|
| A1 | Model definition parameters whose Python kwarg is absent from the kwarg table → error |
| A2 | Required Python kwargs with no model definition entry → warning |
| B | Site xlsx columns vs. model definition parameters: missing required column → error; unrecognized column → warning |
| C | Required parameter values are non-blank for every row → error on blank |
| M | MAES version metadata in Global Simulation Parameters tab → warning on mismatch |

## Design notes

- The JSON model definition files (`input/ModelFormulation/*.json`) are authoritative
  for required/optional status. A parameter is required if `"Optional"` is absent or `"False"`.
- Column matching uses the same `toParamKey()` fuzzy normalization as the MAES loader
  (strip units in brackets, lowercase, strip spaces).
- `BuildKwargTable.py` uses `import` + `inspect.signature()` to extract kwargs — no AST parsing.
- The kwarg table flattens the full inheritance hierarchy via MRO, so inherited kwargs are included.
