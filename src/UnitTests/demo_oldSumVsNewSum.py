"""
demo_oldSumVsNewSum.py

Demonstrates that TimeseriesSet.oldSum() produces incorrect totals while
TimeseriesSet.sum() (the event-based rewrite) is correct.

Root cause: oldSum() builds the result by sequential addSquare() calls. Each
call must re-sample the entire accumulated timeseries at a growing set of
breakpoints. Because addSquare() uses sampleSquare(), which assigns values at
startTimes and at interior breakpoints separately, the two-pass assignment
logic can place the wrong value at a breakpoint that coincides with the end of
one interval and the start of another after many accumulation steps. The error
compounds with each additional timeseries and is negligible for a few timeseries
but grows to ~34% for 10 timeseries x 100K intervals.

Run from src/:
    conda run -n MAES python UnitTests/demo_oldSumVsNewSum.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import Timeseries as ts
import logging
logging.disable(logging.CRITICAL)

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

SEED = 42
NUM_TIMESERIES = 10

# Small: few intervals each — individual steps are visible in the plot.
# Large: many intervals — bug in oldSum() is clearly measurable.
ENTRIES_SMALL = 30
ENTRIES_LARGE = 100_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def genTS(numEntries, timeOffset, rng):
    """
    Generate a random TimeseriesRLE.

    The last interval is given a delta equal to the full timeSpan, so its
    nextTS can extend well into the next timeseries' time range.  This is
    the structure that exposes the addSquare accumulation bug.
    """
    timeSpan = numEntries * 3
    startTimes = (np.sort(rng.choice(timeSpan, size=numEntries, replace=False))
                  .astype(np.int64) + timeOffset)
    deltas = np.empty(numEntries, dtype=np.int64)
    deltas[:-1] = startTimes[1:] - startTimes[:-1]
    deltas[-1] = timeSpan
    durations = np.maximum(1, (rng.random(numEntries) * deltas).astype(np.int64))
    vals = rng.uniform(0.5, 5.0, size=numEntries)
    df = pd.DataFrame({'timestamp': startTimes,
                       'nextTS':    startTimes + durations,
                       'tsValue':   vals})
    return ts.TimeseriesRLE(df)


def rleToXY(rleTS):
    """
    Convert a TimeseriesRLE to (x, y) arrays for plt.step(..., where='post').

    At each startTime the value rises to tsValue; at each nextTS the value
    drops back to 0 (unless another interval begins exactly there).
    """
    df = rleTS.df
    if df.empty:
        return np.array([0.0]), np.array([0.0])
    events = {}
    for _, row in df.iterrows():
        t, e, v = int(row['timestamp']), int(row['nextTS']), float(row['tsValue'])
        events[t] = v
        if e not in events:
            events[e] = 0.0
    times = sorted(events)
    return np.array(times, dtype=float), np.array([events[t] for t in times])


def runAndReport(label, numEntries):
    rng = np.random.default_rng(SEED)
    stride = numEntries * 3
    tsSet = [genTS(numEntries, i * stride, rng) for i in range(NUM_TIMESERIES)]
    expected  = sum(t.total() for t in tsSet)
    oldResult = ts.TimeseriesSet(tsSet).oldSum()
    newResult = ts.TimeseriesSet(tsSet).sum()
    old_err   = (oldResult.total() - expected) / expected
    new_err   = (newResult.total() - expected) / expected
    print(f"\n{label}  ({NUM_TIMESERIES} TS x {numEntries:,} intervals each)")
    print(f"  Expected total : {expected:.4f}")
    print(f"  newSum total   : {newResult.total():.4f}  (error {new_err:+.2e})")
    print(f"  oldSum total   : {oldResult.total():.4f}  (error {old_err:+.1%})  ← WRONG")
    return tsSet, oldResult, newResult, expected


# ---------------------------------------------------------------------------
# Run both datasets
# ---------------------------------------------------------------------------

print("=" * 60)
print("TimeseriesSet.oldSum() vs sum() demonstration")
print("=" * 60)

tsSet_s, old_s, new_s, exp_s = runAndReport("SMALL", ENTRIES_SMALL)
tsSet_l, old_l, new_l, exp_l = runAndReport("LARGE", ENTRIES_LARGE)


# ---------------------------------------------------------------------------
# Plot
# ---------------------------------------------------------------------------

COLORS = plt.cm.tab10(np.arange(NUM_TIMESERIES) / 10.0)

def sampleRLE(rleTS, times):
    df = rleTS.df
    if df.empty:
        return np.zeros(len(times))
    starts = df['timestamp'].values
    ends   = df['nextTS'].values
    vals   = df['tsValue'].values
    idx    = np.searchsorted(starts, times, side='right') - 1
    result = np.zeros(len(times))
    valid  = idx >= 0
    vi, vt = idx[valid], times[valid]
    inside = vt < ends[vi]
    result[np.where(valid)[0][inside]] = vals[vi[inside]]
    return result

# Find the 200-step window with the largest oldSum - newSum area
ZOOM_WIN = 200
t_max_s  = int(max(old_s.df['nextTS'].max(), new_s.df['nextTS'].max()))
t_grid   = np.arange(t_max_s, dtype=float)
diff_grid = sampleRLE(old_s, t_grid) - sampleRLE(new_s, t_grid)
win_sums  = np.convolve(diff_grid, np.ones(ZOOM_WIN), mode='valid')
zoom_t0   = int(np.argmax(win_sums))
zoom_t1   = zoom_t0 + ZOOM_WIN

# ---------------------------------------------------------------------------
# CSV export — zoomed region values for each individual TS + new/old sum
# ---------------------------------------------------------------------------

t_zoom = np.arange(zoom_t0, zoom_t1 + 1, dtype=float)
csv_data = {'t': t_zoom.astype(int)}
for i, singleTS in enumerate(tsSet_s):
    csv_data[f'ts_{i+1}'] = sampleRLE(singleTS, t_zoom)
csv_data['new_sum'] = sampleRLE(new_s, t_zoom)
csv_data['old_sum'] = sampleRLE(old_s, t_zoom)
csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'demo_zoom_values.csv')
pd.DataFrame(csv_data).to_csv(csv_path, index=False)
print(f"Zoom CSV saved to {csv_path}")

fig, axes = plt.subplots(6, 1, figsize=(14, 21))
fig.suptitle(
    f'TimeseriesSet.sum() (new) vs oldSum() (old)\n'
    f'{NUM_TIMESERIES} timeseries — small: {ENTRIES_SMALL} intervals/TS '
    f'(top two panels)   |   large: {ENTRIES_LARGE:,} intervals/TS (bar chart)',
    fontsize=11)

# ── Panel 1: individual timeseries (small dataset) ──────────────────────────
ax = axes[0]
for i, singleTS in enumerate(tsSet_s):
    x, y = rleToXY(singleTS)
    ax.step(x, y, where='post', color=COLORS[i], linewidth=1.1,
            alpha=0.85, label=f'TS {i+1}')
ax.set_ylabel('Value')
ax.set_title(f'Individual timeseries  (seed={SEED}, {ENTRIES_SMALL} intervals each)\n'
             f'Note: last interval of each TS deliberately extends into the next TS\'s time range')
ax.legend(loc='upper right', fontsize=7, ncol=5)
# mark TS boundaries
for k in range(1, NUM_TIMESERIES):
    ax.axvline(k * ENTRIES_SMALL * 3, color='gray', linestyle=':', linewidth=0.7, alpha=0.5)

# ── Panel 2: oldSum vs newSum overlay (small dataset) ───────────────────────
ax = axes[1]
x_new, y_new = rleToXY(new_s)
x_old, y_old = rleToXY(old_s)

ax.fill_between(x_new, y_new, step='post', alpha=0.25, color='steelblue')
ax.fill_between(x_old, y_old, step='post', alpha=0.20, color='tomato')
ax.step(x_new, y_new, where='post', color='steelblue', linewidth=1.8,
        label=f'sum()  total={new_s.total():.2f}  (error {(new_s.total()-exp_s)/exp_s:+.1e})')
ax.step(x_old, y_old, where='post', color='tomato',    linewidth=1.3, linestyle='--',
        label=f'oldSum()  total={old_s.total():.2f}  (error {(old_s.total()-exp_s)/exp_s:+.1%})')

for k in range(1, NUM_TIMESERIES):
    ax.axvline(k * ENTRIES_SMALL * 3, color='gray', linestyle=':', linewidth=0.7, alpha=0.5)

ax.set_ylabel('Summed value')
ax.set_title('oldSum() vs sum()  —  small dataset (full range)')
ax.legend(fontsize=9)

# ── Panel 3: 200-step zoom — region of maximum divergence ───────────────────
ax = axes[2]
zoom_mask_new = (x_new >= zoom_t0) & (x_new <= zoom_t1 + 1)
zoom_mask_old = (x_old >= zoom_t0) & (x_old <= zoom_t1 + 1)
# prepend/append boundary points so fill starts and ends cleanly
def clip_xy(x, y, t0, t1):
    mask = (x >= t0) & (x <= t1 + 1)
    xc, yc = x[mask], y[mask]
    if len(xc) == 0 or xc[0] > t0:
        xc = np.concatenate([[t0], xc])
        yc = np.concatenate([[sampleRLE(new_s if y is y_new else old_s,
                                        np.array([float(t0)]))[0]], yc])
    return xc, yc

xn_z = np.concatenate([[zoom_t0], x_new[zoom_mask_new], [zoom_t1]])
yn_z = np.concatenate([[sampleRLE(new_s, np.array([float(zoom_t0)]))[0]],
                        y_new[zoom_mask_new],
                        [sampleRLE(new_s, np.array([float(zoom_t1)]))[0]]])
xo_z = np.concatenate([[zoom_t0], x_old[zoom_mask_old], [zoom_t1]])
yo_z = np.concatenate([[sampleRLE(old_s, np.array([float(zoom_t0)]))[0]],
                        y_old[zoom_mask_old],
                        [sampleRLE(old_s, np.array([float(zoom_t1)]))[0]]])

ax.fill_between(xn_z, yn_z, step='post', alpha=0.30, color='steelblue', label='sum() area')
ax.fill_between(xo_z, yo_z, step='post', alpha=0.22, color='tomato',    label='oldSum() area')
ax.step(xn_z, yn_z, where='post', color='steelblue', linewidth=2.0, label='sum()')
ax.step(xo_z, yo_z, where='post', color='tomato',    linewidth=1.4, linestyle='--', label='oldSum()')

# shade the error region between the two curves
all_x = np.union1d(xn_z, xo_z)
s_new = sampleRLE(new_s, all_x)
s_old = sampleRLE(old_s, all_x)
ax.fill_between(all_x, s_new, s_old, where=(s_old > s_new), step='post',
                alpha=0.35, color='gold', label='error (oldSum − sum)')

ax.set_xlim(zoom_t0, zoom_t1)
ax.set_ylabel('Summed value')
ax.set_title(f'Zoom: {ZOOM_WIN}-step window of maximum divergence  [t={zoom_t0}…{zoom_t1}]\n'
             f'Gold region = area overcounted by oldSum()')
ax.legend(fontsize=9)

# ── Panel 4: zoom with individual timeseries + oldSum + sum ─────────────────
ax = axes[3]
for i, singleTS in enumerate(tsSet_s):
    x, y = rleToXY(singleTS)
    mask = (x >= zoom_t0) & (x <= zoom_t1 + 1)
    if mask.any():
        xz = np.concatenate([[zoom_t0], x[mask], [zoom_t1]])
        yz = np.concatenate([[sampleRLE(singleTS, np.array([float(zoom_t0)]))[0]],
                              y[mask],
                              [sampleRLE(singleTS, np.array([float(zoom_t1)]))[0]]])
        ax.step(xz, yz, where='post', color=COLORS[i], linewidth=0.9,
                alpha=0.7, label=f'TS {i+1}')
ax.fill_between(all_x, s_new, s_old, where=(s_old > s_new), step='post',
                alpha=0.35, color='gold', label='error (oldSum − sum)')
ax.step(xn_z, yn_z, where='post', color='steelblue', linewidth=2.2, label='sum()')
ax.step(xo_z, yo_z, where='post', color='tomato',    linewidth=1.6, linestyle='--', label='oldSum()')
ax.set_xlim(zoom_t0, zoom_t1)
ax.set_ylabel('Value')
ax.set_title(f'Zoom [t={zoom_t0}…{zoom_t1}] — individual timeseries + sum() + oldSum()\n'
             f'Gold = overcounted area; blue=correct sum, red dashed=oldSum')
ax.legend(loc='upper right', fontsize=7, ncol=4)

# ── Panel 5: t=627…650 detail — all TS + sum() + oldSum() ───────────────────
DETAIL_T0, DETAIL_T1 = 627, 650
ax = axes[4]
detail_x = np.arange(DETAIL_T0, DETAIL_T1 + 1, dtype=float)
s_new_d = sampleRLE(new_s, detail_x)
s_old_d = sampleRLE(old_s, detail_x)
for i, singleTS in enumerate(tsSet_s):
    x, y = rleToXY(singleTS)
    mask = (x >= DETAIL_T0) & (x <= DETAIL_T1 + 1)
    if mask.any() or sampleRLE(singleTS, np.array([float(DETAIL_T0)]))[0] != 0:
        xd = np.concatenate([[DETAIL_T0], x[(x >= DETAIL_T0) & (x <= DETAIL_T1 + 1)], [DETAIL_T1]])
        yd = np.concatenate([[sampleRLE(singleTS, np.array([float(DETAIL_T0)]))[0]],
                              y[(x >= DETAIL_T0) & (x <= DETAIL_T1 + 1)],
                              [sampleRLE(singleTS, np.array([float(DETAIL_T1)]))[0]]])
        if yd.max() > 0:
            ax.step(xd, yd, where='post', color=COLORS[i], linewidth=1.2,
                    alpha=0.75, label=f'TS {i+1}')
ax.fill_between(detail_x, s_new_d, s_old_d, where=(s_old_d > s_new_d), step='post',
                alpha=0.35, color='gold', label='phantom overcount')
ax.step(detail_x, s_new_d, where='post', color='steelblue', linewidth=2.2, label='sum()  (correct)')
ax.step(detail_x, s_old_d, where='post', color='tomato', linewidth=1.6, linestyle='--', label='oldSum()  (buggy)')
ax.set_xlim(DETAIL_T0, DETAIL_T1)
ax.set_xticks(np.arange(DETAIL_T0, DETAIL_T1 + 1, 2))
ax.set_ylabel('Value')
ax.set_title(f'Detail t={DETAIL_T0}…{DETAIL_T1} — individual timeseries + sum() + oldSum()\n'
             f'Gold = phantom area overcounted by oldSum(); ts_7 active throughout, ts_8 pulses')
ax.legend(loc='upper left', fontsize=8, ncol=4)

# ── Panel 6: total comparison bar chart (large dataset) ─────────────────────
ax = axes[5]
labels = ['Expected\n(sum of individuals)', 'sum()\nnew implementation', 'oldSum()\nold implementation']
values = [exp_l, new_l.total(), old_l.total()]
bar_colors = ['#4d4d4d', 'steelblue', 'tomato']
bars = ax.bar(labels, values, color=bar_colors, width=0.45, edgecolor='black', linewidth=0.8)

for bar, val in zip(bars, values):
    err = (val - exp_l) / exp_l
    label = f'{val:,.1f}' if abs(err) < 1e-9 else f'{val:,.1f}\nerror {err:+.1%}'
    ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height() + exp_l * 0.008,
            label, ha='center', va='bottom', fontsize=10, fontweight='bold')

ax.set_ylabel('Total  (integral over time)')
ax.set_title(f'Total comparison — large dataset  ({NUM_TIMESERIES} TS × {ENTRIES_LARGE:,} intervals)')
ax.set_ylim(0, max(values) * 1.16)

plt.tight_layout()
out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'demo_oldSumVsNewSum.png')
plt.savefig(out_path, dpi=150, bbox_inches='tight')
print(f"\nPlot saved to {out_path}")
