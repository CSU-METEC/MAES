"""
Regression tests for simulation-window event clipping (GitHub issue #87).

The engine logs the full sampled `duration` of whatever state is in progress when
simpy stops at `simDurationSecs`, so `timestamp + duration` can exceed the window.
Left unclipped, this overrun adds spurious emission credit past the window and biases
every rate-integrated quantity (annual summaries, PDFs) upward — by tens of percent
for long / fat-tailed event classes (verified at +67.8% on a real 41.5-day overrun).

The fix clips events at `_createEmissionDF` — the single point where the emission
DataFrame is built and saved as InstEmissions (the dataset the PDF cascade reads
back) — so the rate summaries, event summaries, and PDFs are all consistent over
[0, simDurationSecs]. Only `emission_kgPerS` (the rate) is preserved; `duration_s`
and `totalEmission_kg` shrink for the overrunning event.
"""

import numpy as np
import pandas as pd

import Units as u
import Timeseries as ts
from Summaries2 import _createEmissionDF, calculateAnnualSummaries, _buildMCRunTimeseries, _aggregateEmittersByRun

SIM_DURATION_DAYS = 365.0
SIM_DURATION_SECS = SIM_DURATION_DAYS * u.SECONDS_PER_DAY  # 31_536_000

RATE = 0.001  # kg/s


def _raw_event(emitterID, timestamp, duration, rate=RATE, mc=0):
    """One raw event row as readParquetEvents produces (pre-_createEmissionDF)."""
    return {
        'mcRun': mc, 'site': 'S1', 'facilityID': 'F1', 'species': 'METHANE',
        'operator': 'op', 'psno': 'ps', 'emitterID': emitterID,
        'timestamp': float(timestamp), 'duration': float(duration),
        'emission': rate,
        'METype': 'Compressor', 'unitID': 'U1',
        'modelReadableName': 'CompressorComponentLeak',
        'modelEmissionCategory': 'FUGITIVE',
    }


def test_overrunning_event_duration_clipped_to_window():
    # interior event well inside the window; overrun event ends 100 days past T.
    start = SIM_DURATION_SECS - 10 * u.SECONDS_PER_DAY      # starts 10 days before end
    overrun_duration = 110 * u.SECONDS_PER_DAY              # would end 100 days past T
    df = pd.DataFrame([
        _raw_event('interior', timestamp=0, duration=3600.0),
        _raw_event('overrun', timestamp=start, duration=overrun_duration),
    ])
    out = _createEmissionDF(df, SIM_DURATION_SECS).set_index('emitterID')

    # interior event untouched
    assert out.loc['interior', 'duration_s'] == 3600.0
    assert abs(out.loc['interior', 'totalEmission_kg'] - RATE * 3600.0) < 1e-12

    # overrun event clipped to exactly the in-window remainder
    expected_dur = SIM_DURATION_SECS - start               # 10 days in seconds
    assert abs(out.loc['overrun', 'duration_s'] - expected_dur) < 1e-9
    # rate preserved, totalEmission recomputed from the clipped duration
    assert out.loc['overrun', 'emission_kgPerS'] == RATE
    assert abs(out.loc['overrun', 'totalEmission_kg'] - RATE * expected_dur) < 1e-9


def test_event_ending_exactly_at_window_is_unchanged():
    start = SIM_DURATION_SECS - 5 * u.SECONDS_PER_DAY
    df = pd.DataFrame([_raw_event('edge', timestamp=start, duration=5 * u.SECONDS_PER_DAY)])
    out = _createEmissionDF(df, SIM_DURATION_SECS).set_index('emitterID')
    assert abs(out.loc['edge', 'duration_s'] - 5 * u.SECONDS_PER_DAY) < 1e-9


def test_event_starting_after_window_yields_zero_duration_not_negative():
    # The engine should not produce this, but guard against a negative duration.
    df = pd.DataFrame([_raw_event('past', timestamp=SIM_DURATION_SECS + 100, duration=3600.0)])
    out = _createEmissionDF(df, SIM_DURATION_SECS).set_index('emitterID')
    assert out.loc['past', 'duration_s'] == 0.0
    assert out.loc['past', 'totalEmission_kg'] == 0.0


def test_annual_summary_uses_clipped_emission():
    """The rate-integrated annual total reflects only in-window emission."""
    start = SIM_DURATION_SECS - 10 * u.SECONDS_PER_DAY
    df = pd.DataFrame([_raw_event('overrun', timestamp=start, duration=110 * u.SECONDS_PER_DAY)])
    inst = _createEmissionDF(df, SIM_DURATION_SECS)

    AGG = {
        'total': ('emissions_kgPerYear', 'sum'),
        'count': ('emissions_kgPerYear', 'count'),
        'mean': ('emissions_kgPerYear', 'mean'),
        'min': ('emissions_kgPerYear', 'min'),
        'max': ('emissions_kgPerYear', 'max'),
        'lowerQuartile': ('emissions_kgPerYear', lambda x: np.percentile(x, 25)),
        'upperQuartile': ('emissions_kgPerYear', lambda x: np.percentile(x, 75)),
        'lowerCI': ('emissions_kgPerYear', lambda x: np.percentile(x, 2.5)),
        'upperCI': ('emissions_kgPerYear', lambda x: np.percentile(x, 97.5)),
        'readings': ('emissions_kgPerYear', list),
    }
    summ = calculateAnnualSummaries(_aggregateEmittersByRun(inst, SIM_DURATION_DAYS), AGG, mcIterations=1)

    # in-window kg = rate * 10 days; annualized over a 365-day sim == itself.
    in_window_kg = RATE * (10 * u.SECONDS_PER_DAY)
    mrn = summ[summ['CICategory'] == 'modelReadableName']
    site_total = mrn[mrn['modelReadableName'] == 'CompressorComponentLeak']['total'].iloc[0]
    assert abs(site_total - in_window_kg) < 1e-6

    # the unclipped total would have been rate * 110 days — guard against regression
    unclipped_kg = RATE * (110 * u.SECONDS_PER_DAY)
    assert site_total < 0.2 * unclipped_kg


def test_pdf_mean_matches_rate_integrated_mean_after_clip():
    """
    The motivating identity from issue #89's second use-case bullet: feeding clipped
    durations into _buildMCRunTimeseries restores mean_pdf == mean_events over [0,T].
    Build one emitter's summed timeseries from clipped InstEmissions and confirm the
    duration-weighted (PDF) mean rate equals total in-window emission / T.
    """
    start = SIM_DURATION_SECS - 10 * u.SECONDS_PER_DAY
    df = pd.DataFrame([
        _raw_event('e', timestamp=0, duration=3600.0),
        _raw_event('e', timestamp=start, duration=110 * u.SECONDS_PER_DAY),
    ])
    inst = _createEmissionDF(df, SIM_DURATION_SECS)

    ts_rle = _buildMCRunTimeseries(inst)
    pdf = ts.TimeseriesSet([ts_rle]).toPDF()
    pdf_mean = (pdf.data['value'] * (pdf.data['count'] / SIM_DURATION_SECS)).sum()  # kg/h

    in_window_kg = RATE * (3600.0 + 10 * u.SECONDS_PER_DAY)
    rate_mean = in_window_kg / SIM_DURATION_SECS * u.SECONDS_PER_HOUR  # kg/h
    assert abs(pdf_mean - rate_mean) < 1e-9
