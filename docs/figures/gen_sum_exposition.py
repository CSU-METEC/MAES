"""
gen_sum_exposition.py

Generates figures for the TimeseriesSet.sum() exposition in Timeseries.md.

Run from MAES/docs/figures/:
    conda run -n MAES python gen_sum_exposition.py
"""

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch
import os

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Concrete example used throughout all figures
# ---------------------------------------------------------------------------
#
#  TS A: [0,5) = 2,  [8,12) = 3
#  TS B: [3,9) = 1,  [11,15) = 4
#  TS C: [6,10) = 2
#
# Expected sum breakpoints and values:
#   [0,3)=2  [3,5)=3  [5,6)=1  [6,8)=3  [8,9)=6  [9,10)=5
#   [10,11)=3  [11,12)=7  [12,15)=4

TS_A = [(0,  5,  2.0), (8,  12, 3.0)]
TS_B = [(3,  9,  1.0), (11, 15, 4.0)]
TS_C = [(6, 10,  2.0)]
ALL_TS = [('A', TS_A, '#4878CF'), ('B', TS_B, '#6ACC65'), ('C', TS_C, '#D65F5F')]

EVENTS_RAW = [
    (0,  +2.0, 'A start'),
    (3,  +1.0, 'B start'),
    (5,  -2.0, 'A end'),
    (6,  +2.0, 'C start'),
    (8,  +3.0, 'A start'),
    (9,  -1.0, 'B end'),
    (10, -2.0, 'C end'),
    (11, +4.0, 'B start'),
    (12, -3.0, 'A end'),
    (15, -4.0, 'B end'),
]

SUM_INTERVALS = [
    (0,  3,  2.0),
    (3,  5,  3.0),
    (5,  6,  1.0),
    (6,  8,  3.0),
    (8,  9,  6.0),
    (9,  10, 5.0),
    (10, 11, 3.0),
    (11, 12, 7.0),
    (12, 15, 4.0),
]

T_MAX = 16


def rle_xy(intervals, t_max):
    """Convert list of (start, end, val) to step-plot (x, y) arrays."""
    events = {}
    for s, e, v in intervals:
        events[s] = v
        if e not in events:
            events[e] = 0.0
    events[0]    = events.get(0,    0.0)
    events[t_max] = events.get(t_max, 0.0)
    times = sorted(events)
    return np.array(times, dtype=float), np.array([events[t] for t in times])


# ---------------------------------------------------------------------------
# Figure 1: inputs + event table + cumsum/output  (overview)
# ---------------------------------------------------------------------------

fig, axes = plt.subplots(3, 1, figsize=(11, 10),
                         gridspec_kw={'height_ratios': [2.5, 2, 2.5]})
fig.suptitle('TimeseriesSet.sum() — Algorithm Overview', fontsize=13, fontweight='bold')

# ── Panel 1: three input timeseries ─────────────────────────────────────────
ax = axes[0]
for label, intervals, color in ALL_TS:
    x, y = rle_xy(intervals, T_MAX)
    ax.fill_between(x, y, step='post', alpha=0.18, color=color)
    ax.step(x, y, where='post', color=color, linewidth=2.2, label=f'TS {label}')
    # annotate intervals
    for s, e, v in intervals:
        ax.annotate(f'{v:.0f}', xy=((s + e) / 2, v + 0.1), ha='center', va='bottom',
                    fontsize=9, color=color, fontweight='bold')

for t, _, lbl in EVENTS_RAW:
    ax.axvline(t, color='#aaaaaa', linewidth=0.7, linestyle=':')

ax.set_xlim(0, T_MAX)
ax.set_ylim(-0.4, 8.5)
ax.set_ylabel('Value')
ax.set_title('Step 1 — Three input RLE timeseries', fontsize=10)
ax.legend(loc='upper right', fontsize=9)
ax.set_xticks(range(T_MAX + 1))

# ── Panel 2: signed event table ─────────────────────────────────────────────
ax = axes[1]
event_times  = [e[0] for e in EVENTS_RAW]
event_deltas = [e[1] for e in EVENTS_RAW]
event_labels = [e[2] for e in EVENTS_RAW]
colors_bar   = ['#4878CF' if 'A' in l else '#6ACC65' if 'B' in l else '#D65F5F'
                for l in event_labels]
bar_alphas   = [0.85 if d > 0 else 0.5 for d in event_deltas]

bars = ax.bar(event_times, event_deltas, width=0.5,
              color=colors_bar, edgecolor='black', linewidth=0.7)
for bar, delta, lbl in zip(bars, event_deltas, event_labels):
    ypos = delta + (0.1 if delta >= 0 else -0.25)
    sign = '+' if delta > 0 else ''
    ax.text(bar.get_x() + bar.get_width() / 2, ypos,
            f'{sign}{delta:.0f}\n({lbl})', ha='center', va='bottom' if delta >= 0 else 'top',
            fontsize=7.5, color='black')

ax.axhline(0, color='black', linewidth=0.8)
ax.set_xlim(0, T_MAX)
ax.set_ylim(-5.5, 5.5)
ax.set_ylabel('Δ value')
ax.set_title('Step 2 — Signed event table  (+v at interval start, −v at interval end)', fontsize=10)
ax.set_xticks(range(T_MAX + 1))

# ── Panel 3: cumsum → output ─────────────────────────────────────────────────
ax = axes[2]

# compute cumsum at each event time
event_df = pd.DataFrame({'time': event_times, 'delta': event_deltas})
grouped  = event_df.groupby('time')['delta'].sum().sort_index()
times_cs = grouped.index.values
cumsums  = grouped.values.cumsum()

# plot output sum
x_sum, y_sum = rle_xy(SUM_INTERVALS, T_MAX)
ax.fill_between(x_sum, y_sum, step='post', alpha=0.20, color='#8B6FBF')
ax.step(x_sum, y_sum, where='post', color='#8B6FBF', linewidth=2.5, label='sum()')

# overlay cumsum dots at each event breakpoint
for t, cs in zip(times_cs[:-1], cumsums[:-1]):
    ax.plot(t, cs, 'o', color='#8B6FBF', markersize=7, zorder=5)
    ax.annotate(f'{cs:.0f}', xy=(t, cs), xytext=(t + 0.15, cs + 0.25),
                fontsize=8.5, color='#8B6FBF', fontweight='bold')

for t, _, _ in EVENTS_RAW:
    ax.axvline(t, color='#aaaaaa', linewidth=0.7, linestyle=':')

ax.set_xlim(0, T_MAX)
ax.set_ylim(-0.5, 8.5)
ax.set_ylabel('Summed value')
ax.set_title('Step 3 — Cumulative sum of events gives the correct sum at every breakpoint', fontsize=10)
ax.legend(loc='upper right', fontsize=9)
ax.set_xticks(range(T_MAX + 1))

plt.tight_layout()
out = os.path.join(OUT_DIR, 'sum_overview.png')
plt.savefig(out, dpi=150, bbox_inches='tight')
print(f'Saved {out}')
plt.close()


# ---------------------------------------------------------------------------
# Figure 2: correctness invariant — active-set snapshot at t=8.5
# ---------------------------------------------------------------------------

T_PROBE = 8.5

fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
fig.suptitle(f'Correctness Invariant — Snapshot at t = {T_PROBE}', fontsize=12, fontweight='bold')

# Left: all three timeseries with vertical probe line
ax = axes[0]
for label, intervals, color in ALL_TS:
    x, y = rle_xy(intervals, T_MAX)
    ax.fill_between(x, y, step='post', alpha=0.18, color=color)
    ax.step(x, y, where='post', color=color, linewidth=2.0, label=f'TS {label}')

ax.axvline(T_PROBE, color='black', linewidth=2.0, linestyle='--', label=f't = {T_PROBE}')

# annotate active values at probe
active = [(lbl, v, col) for lbl, ivs, col in ALL_TS
          for s, e, v in ivs if s <= T_PROBE < e]
y_offset = 6.5
for lbl, v, col in active:
    ax.annotate(f'TS {lbl} = {v:.0f}  ✓', xy=(T_PROBE, v),
                xytext=(T_PROBE + 0.4, y_offset),
                fontsize=9, color=col, fontweight='bold',
                arrowprops=dict(arrowstyle='->', color=col, lw=1.2))
    y_offset -= 0.9

total = sum(v for _, v, _ in active)
ax.annotate(f'Sum = {total:.0f}', xy=(T_PROBE, total),
            xytext=(T_PROBE + 0.4, y_offset - 0.2),
            fontsize=10, color='#8B6FBF', fontweight='bold',
            arrowprops=dict(arrowstyle='->', color='#8B6FBF', lw=1.5))

ax.set_xlim(0, T_MAX)
ax.set_ylim(-0.4, 8.5)
ax.set_ylabel('Value')
ax.set_title('Active intervals at the probe time')
ax.legend(loc='upper right', fontsize=8)
ax.set_xticks(range(T_MAX + 1))

# Right: cumsum trace up to probe
ax = axes[1]
all_times = sorted(set([e[0] for e in EVENTS_RAW]))
cum = 0
prev_t = 0
prev_cum = 0
palette = plt.cm.tab10(np.arange(10) / 10.0)
for i, (t, delta, lbl) in enumerate(EVENTS_RAW):
    color = '#4878CF' if 'A' in lbl else '#6ACC65' if 'B' in lbl else '#D65F5F'
    new_cum = cum + delta
    # draw horizontal segment at current cumsum level
    ax.plot([prev_t, t], [cum, cum], color='#8B6FBF', linewidth=2.2, zorder=3)
    # draw the step
    ax.plot([t, t], [cum, new_cum], color=color, linewidth=1.8, linestyle='--', zorder=4)
    ax.plot(t, new_cum, 'o', color=color, markersize=7, zorder=5)
    sign = '+' if delta > 0 else ''
    ax.annotate(f'{sign}{delta:.0f} ({lbl.split()[0]})',
                xy=(t, (cum + new_cum) / 2),
                xytext=(t + 0.2, (cum + new_cum) / 2),
                fontsize=7.5, color=color, va='center')
    prev_t, cum = t, new_cum
    if t >= T_PROBE:
        break

ax.axvline(T_PROBE, color='black', linewidth=2.0, linestyle='--', label=f't = {T_PROBE}')
ax.plot(T_PROBE, total, '*', color='#8B6FBF', markersize=14, zorder=6,
        label=f'cumsum = {total:.0f}')
ax.set_xlim(0, T_MAX)
ax.set_ylim(-0.5, 8.5)
ax.set_ylabel('Cumulative sum of events')
ax.set_title('Cumsum trace — value equals sum of active intervals at every point')
ax.legend(loc='upper left', fontsize=8)
ax.set_xticks(range(T_MAX + 1))

plt.tight_layout()
out = os.path.join(OUT_DIR, 'sum_correctness.png')
plt.savefig(out, dpi=150, bbox_inches='tight')
print(f'Saved {out}')
plt.close()


# ---------------------------------------------------------------------------
# Figure 3: complexity — O(M log M) vs O(N·M)
# ---------------------------------------------------------------------------

Ns = [2, 5, 10, 20, 50, 100]
M  = 10_000
ops_new = [M * np.log2(n * M) for n in Ns]
ops_old = [n * M for n in Ns]  # simplified O(N·M) per addSquare step

fig, ax = plt.subplots(figsize=(8, 4.5))
ax.plot(Ns, ops_new, 'o-', color='steelblue', linewidth=2.0, markersize=7,
        label='sum()   O(M log NM)')
ax.plot(Ns, ops_old, 's--', color='tomato',    linewidth=2.0, markersize=7,
        label='oldSum()  O(N·M)')
ax.set_xlabel('Number of timeseries (N)  —  M = 10,000 intervals each')
ax.set_ylabel('Approximate operations')
ax.set_title('Complexity scaling: event-based sum() vs addSquare accumulation (oldSum())')
ax.legend(fontsize=10)
ax.set_yscale('log')
ax.grid(True, which='both', alpha=0.3)

plt.tight_layout()
out = os.path.join(OUT_DIR, 'sum_complexity.png')
plt.savefig(out, dpi=150, bbox_inches='tight')
print(f'Saved {out}')
plt.close()

# ---------------------------------------------------------------------------
# Figure 4: corner conditions
#
# Case 1 — Coincident start times
#   TS A: [3, 8) = 2    TS B: [3, 6) = 3
#   At t=3: +2 and +3 grouped → cumsum jumps by 5 in one step
#   Result: [3,6)=5, [6,8)=2
#
# Case 2 — Coincident end times
#   TS A: [1, 7) = 4    TS B: [4, 7) = 2
#   At t=7: -4 and -2 grouped → cumsum drops by 6 in one step
#   Result: [1,4)=4, [4,7)=6
#
# Case 3 — End of A equals start of B (adjacent, no gap)
#   TS A: [0, 5) = 3    TS B: [5, 10) = 4
#   At t=5: -3 (A end) and +4 (B start) grouped → net +1
#   Result: [0,5)=3, [5,10)=4   (no phantom overlap)
# ---------------------------------------------------------------------------

CASES = [
    {
        'title': 'Case 1 — Coincident start times (t = 3)',
        'corner_t': 3,
        'A': (3, 8, 2.0),
        'B': (3, 6, 3.0),
        'sum_intervals': [(3, 6, 5.0), (6, 8, 2.0)],
        'events': [(3, +2.0, 'A start'), (3, +3.0, 'B start'),
                   (6, -3.0, 'B end'),   (8, -2.0, 'A end')],
        'event_note': '+2 (A) and +3 (B)\ngrouped → net +5',
        'xlim': (0, 10),
    },
    {
        'title': 'Case 2 — Coincident end times (t = 7)',
        'corner_t': 7,
        'A': (1, 7, 4.0),
        'B': (4, 7, 2.0),
        'sum_intervals': [(1, 4, 4.0), (4, 7, 6.0)],
        'events': [(1, +4.0, 'A start'), (4, +2.0, 'B start'),
                   (7, -4.0, 'A end'),   (7, -2.0, 'B end')],
        'event_note': '−4 (A) and −2 (B)\ngrouped → net −6',
        'xlim': (0, 10),
    },
    {
        'title': 'Case 3 — End of A equals start of B (t = 5)',
        'corner_t': 5,
        'A': (0, 5, 3.0),
        'B': (5, 10, 4.0),
        'sum_intervals': [(0, 5, 3.0), (5, 10, 4.0)],
        'events': [(0, +3.0, 'A start'), (5, -3.0, 'A end'),
                   (5, +4.0, 'B start'), (10, -4.0, 'B end')],
        'event_note': '−3 (A end) and +4 (B start)\ngrouped → net +1\n(no phantom overlap)',
        'xlim': (0, 12),
    },
]

COLOR_A   = '#4878CF'
COLOR_B   = '#6ACC65'
COLOR_SUM = '#8B6FBF'

fig, axes = plt.subplots(3, 2, figsize=(13, 11),
                         gridspec_kw={'width_ratios': [2, 1]})
fig.suptitle('TimeseriesSet.sum() — Corner Conditions', fontsize=13, fontweight='bold')

for row, case in enumerate(CASES):
    ax_ts  = axes[row][0]
    ax_evt = axes[row][1]
    ct     = case['corner_t']
    xlo, xhi = case['xlim']

    # ── left panel: individual TS + sum ──────────────────────────────────────
    sa, ea, va = case['A']
    sb, eb, vb = case['B']

    def step_xy(s, e, v, xlo, xhi):
        xs = [xlo, s, s, e, e, xhi]
        ys = [0,   0, v, v, 0, 0 ]
        return xs, ys

    xA, yA = step_xy(sa, ea, va, xlo, xhi)
    xB, yB = step_xy(sb, eb, vb, xlo, xhi)

    ax_ts.fill_between(xA, yA, alpha=0.20, color=COLOR_A, step=None)
    ax_ts.fill_between(xB, yB, alpha=0.20, color=COLOR_B, step=None)
    ax_ts.plot(xA, yA, color=COLOR_A, linewidth=2.0, label='TS A', drawstyle='steps-post')
    ax_ts.plot(xB, yB, color=COLOR_B, linewidth=2.0, label='TS B', drawstyle='steps-post')

    # sum
    sum_x = [xlo]
    sum_y = [0]
    for s, e, v in case['sum_intervals']:
        sum_x += [s, s, e, e]
        sum_y += [0, v, v, 0]
    sum_x.append(xhi); sum_y.append(0)
    ax_ts.fill_between(sum_x, sum_y, alpha=0.15, color=COLOR_SUM)
    ax_ts.plot(sum_x, sum_y, color=COLOR_SUM, linewidth=2.5, linestyle='--', label='sum()')

    # annotate interval values
    for s, e, v in [case['A']] + [case['B']]:
        col = COLOR_A if (s, e, v) == case['A'] else COLOR_B
        ax_ts.text((s + e) / 2, v + 0.15, f'{v:.0f}', ha='center', va='bottom',
                   fontsize=9, color=col, fontweight='bold')
    for s, e, v in case['sum_intervals']:
        ax_ts.text((s + e) / 2, v + 0.15, f'{v:.0f}', ha='center', va='bottom',
                   fontsize=9, color=COLOR_SUM, fontweight='bold')

    # corner time vertical line
    ax_ts.axvline(ct, color='black', linewidth=1.8, linestyle=':', alpha=0.7)
    ax_ts.text(ct + 0.1, ax_ts.get_ylim()[1] if ax_ts.get_ylim()[1] > 0 else 7.5,
               f't={ct}', fontsize=8, va='top', color='black')

    ymax = max(va, vb, sum(v for _, _, v in case['sum_intervals'])) + 1.5
    ax_ts.set_xlim(xlo, xhi)
    ax_ts.set_ylim(-0.4, ymax)
    ax_ts.set_ylabel('Value')
    ax_ts.set_title(case['title'], fontsize=10)
    ax_ts.legend(loc='upper right', fontsize=8)
    ax_ts.set_xticks(range(xlo, xhi + 1))

    # ── right panel: event table at corner time ───────────────────────────────
    corner_events = [(t, d, l) for t, d, l in case['events'] if t == ct]
    other_events  = [(t, d, l) for t, d, l in case['events'] if t != ct]

    all_evt_times  = [e[0] for e in case['events']]
    all_evt_deltas = [e[1] for e in case['events']]
    all_evt_labels = [e[2] for e in case['events']]
    bar_colors = [COLOR_A if 'A' in l else COLOR_B for l in all_evt_labels]
    bar_alphas = [1.0 if t == ct else 0.35 for t in all_evt_times]

    # use unique x positions (jitter simultaneous events slightly for display)
    x_positions = []
    seen = {}
    for t in all_evt_times:
        seen[t] = seen.get(t, -0.2)
        x_positions.append(t + seen[t])
        seen[t] += 0.25

    for xp, d, col, alp, lbl in zip(x_positions, all_evt_deltas,
                                     bar_colors, bar_alphas, all_evt_labels):
        bar = ax_evt.bar(xp, d, width=0.2, color=col, alpha=alp,
                         edgecolor='black', linewidth=0.7)
        sign = '+' if d > 0 else ''
        yoff = d + (0.1 if d >= 0 else -0.15)
        ax_evt.text(xp, yoff, f'{sign}{d:.0f}\n{lbl}',
                    ha='center', va='bottom' if d >= 0 else 'top',
                    fontsize=7.5, color='black',
                    alpha=1.0 if xp in [ct - 0.2, ct + 0.05] else 0.5)

    # net event annotation
    net = sum(d for t, d, _ in case['events'] if t == ct)
    sign = '+' if net > 0 else ''
    ax_evt.axhline(0, color='black', linewidth=0.7)
    ax_evt.axvline(ct, color='black', linewidth=1.5, linestyle=':', alpha=0.7)
    ax_evt.text(ct, -abs(net) - 0.8, case['event_note'],
                ha='center', va='top', fontsize=8,
                color='black', style='italic',
                bbox=dict(boxstyle='round,pad=0.3', fc='lightyellow', ec='gray', lw=0.8))

    ax_evt.set_xlim(xlo - 0.5, xhi + 0.5)
    ax_evt.set_ylim(min(all_evt_deltas) - 1.5, max(all_evt_deltas) + 1.0)
    ax_evt.set_ylabel('Δ value')
    ax_evt.set_title('Events  (faded = non-corner)', fontsize=9)
    ax_evt.set_xticks(range(xlo, xhi + 1))

plt.tight_layout()
out = os.path.join(OUT_DIR, 'sum_corner_cases.png')
plt.savefig(out, dpi=150, bbox_inches='tight')
print(f'Saved {out}')
plt.close()

print('All figures generated.')
