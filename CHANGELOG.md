# CHANGELOG


## v0.3.0 — Pre Summary Rewrite (2026-04-27)

### Features

- **Post-processing suite** — added `postprocessing/` with emission plot generation
  at site, METype, unitID, and modelReadableName levels; PDF/CDF plots; abnormal
  emission threshold calculation; and aggregate summaries by emission category
  (vented, fugitive, combustion), METype, and modelReadableName
- **Aggregate summaries** — simulation-level and annual aggregate summaries;
  added `MCRuns_emission_list` column in AnnualEmissions and
  AggregatedSimulationEmissions outputs
- **Pneumatics summary** — dedicated summary output for intermittent pneumatic emissions
- **State and timeseries plots** — per-MCRun and mean-emission overlays
- **Runtime statistics** — wall-time stats saved to `.csv` at end of run
- **Docker support** — added Dockerfile for containerized deployment
- **Compressor large-emitter model** — overload rerouting via probes; crankcase
  emissions correctly suppressed for electric drivers
- **Dehydrator glycol pump** — glycol pump emissions added to MEETDehydrator

### Bug Fixes

- Fixed PDF/CDF generation to correctly include zero-emission runs
  (CDF estimates now match MAES output)
- Fixed aggregate simulation-level summary generated more than once per simulation
- Fixed empty parquet generation
- Fixed `np.random.random(1)` in MEETComponentLeaks (should be scalar, not array)
- Fixed pneumatic emissions summary bug; fixed C2/C1 plot bug
- Fixed `-pt True` not plotting aggregate summaries when annual summary files present
- Fixed annualEmissions CI summation bug; removed scalar divide warning
- Fixed intermittent pneumatic `nextState` not reading vapor flows for state duration

### Data

- Added missing `Allen_RM_FLARE_CONT_EF.csv`
- Added field-measured destruction efficiency files (`VariableDE4SLB.csv`,
  `VariableDE4SRB.csv`); deleted incorrect `VariableDE.csv`
- Renamed Rotary Screw Compressor EF files

### License

- Added `LICENSE.pdf`


## v0.2.0 (2025-01-12)

### Features

- Build input study sheet artifacts & version
  ([`d380921`](https://github.com/CSU-METEC/MAES/commit/d3809214d3d0e54a09eaaf7031961fe6fc665dc6))


## v0.1.0 (2025-01-12)

### Features

- Build input study sheet artifacts & version
  ([`a34fee8`](https://github.com/CSU-METEC/MAES/commit/a34fee81894cab451f74db17b2d9303057f2bed0))


## v0.0.4 (2025-01-12)

### Bug Fixes

- Declare packages in pyproject.toml
  ([`1af5f16`](https://github.com/CSU-METEC/MAES/commit/1af5f16cb87cb9332fdd03a348eb1067ed80fcf0))


## v0.0.3 (2025-01-12)

### Bug Fixes

- Remove non-package mode
  ([`dda6ccf`](https://github.com/CSU-METEC/MAES/commit/dda6ccf7aac43069a66f259bc563984fb1724bfc))


## v0.0.2 (2025-01-12)

### Bug Fixes

- Automated input study sheet artifact storage
  ([`5be8536`](https://github.com/CSU-METEC/MAES/commit/5be85363628e801d36d9d4831d50be2c27ac11e2))


## v0.0.1 (2025-01-12)

### Bug Fixes

- Changelog file path
  ([`41931b3`](https://github.com/CSU-METEC/MAES/commit/41931b33730cb54143562fd62ca06324ed892f84))

- **env**: Added requirements file and initial README.md guide
  ([`d3dee5a`](https://github.com/CSU-METEC/MAES/commit/d3dee5a8b0372b4601ef6127ffc61cf5038961a8))

- **git**: Git ignored .idea files/folders
  ([`e3a2746`](https://github.com/CSU-METEC/MAES/commit/e3a274632f8e393a5b559436a440d306d054a4bd))

### Chores

- **release**: 1.0.0
  ([`feabd54`](https://github.com/CSU-METEC/MAES/commit/feabd54678fc73923f0aef514385c7693229db6d))

- **release**: 1.0.1
  ([`0c952a9`](https://github.com/CSU-METEC/MAES/commit/0c952a9ac8fd0978b7edfaaf38ec521b29428926))

### Continuous Integration

- Packaging and automated release with github actions
  ([`c1df4c0`](https://github.com/CSU-METEC/MAES/commit/c1df4c00cfc378e771ed78520e261bd12e8a9399))

- Poetry lock file for depency management
  ([`215102d`](https://github.com/CSU-METEC/MAES/commit/215102d1ca8f843ddd3502b1ecea5a3531b91b20))

- Poetry lock file for depency management
  ([`8867a58`](https://github.com/CSU-METEC/MAES/commit/8867a58c39397b5e1680cbdbaf4050508012980e))

### Documentation

- **changelog**: Added Initial release fork msg
  ([`086daf5`](https://github.com/CSU-METEC/MAES/commit/086daf5d484c1e53a0a8be5cd90b4eb4fa508af5))

- **readme**: Fix guide type
  ([`51f2761`](https://github.com/CSU-METEC/MAES/commit/51f27618d68f92dfcaab61a936a453b7096febde))
