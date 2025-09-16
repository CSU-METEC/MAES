import pandas as pd
import AppUtils as au
import os
import glob
import json
import logging
import numpy as np
import Timeseries as ts
import ParquetLib as Pl
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter
from scipy.stats import lognorm, fisk, norm
from scipy.optimize import minimize

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Prevent running the expensive simulation-wide aggregation more
# than once per (simulationRoot, abnormal) in a single MAES run.
_SIM_AGG_DONE = set()          # holds tuples (simulationRoot, "ON"/"OFF")
# ------------------------------------------------------------------

SECONDSINHOUR = 3600
SECONDSINDAY = 86400
US_TO_PER_METRIC_TON = 1.10231
US_TO_PER_HOUR_TO_KG_PER_HOUR = 0.1035
SPECIES = ['METHANE','ETHANE']

def compute_cdf_for_plot(df: pd.DataFrame) -> pd.DataFrame:
    df_sorted = df.sort_values(by="CH4_EmissionRate_kg/h").reset_index(drop=True)
    df_sorted["cdf"] = df_sorted["probability"].cumsum()
    return df_sorted


def get_threshold_stats(df_sorted: pd.DataFrame, thresholds: list[int]):
    percentages = {}
    coords      = []
    for t in thresholds:
        below = df_sorted[df_sorted["CH4_EmissionRate_kg/h"] < t]
        pct   = below["probability"].sum() * 100
        y_val = below["cdf"].iloc[-1] if not below.empty else 0
        percentages[t] = pct
        coords.append((t, y_val))
    return percentages, coords



# logistic mixture CDF in log‑space
def mix2_lognorm_cdf(x, w, mu1, s1, mu2, s2):
    z1 = (np.log(x) - mu1) / s1
    z2 = (np.log(x) - mu2) / s2
    return w * norm.cdf(z1) + (1 - w) * norm.cdf(z2)

def plot_cdf(df_sorted, percentages, threshold_coords,
             abnormal, out_dir):
    # 1) Load & clean
    x = df_sorted["CH4_EmissionRate_kg/h"].to_numpy(dtype=float)
    F = df_sorted["cdf"].to_numpy(dtype=float)
    mask = (x > 0) & np.concatenate([[True], np.diff(F) > 0])
    xF, FF = x[mask], F[mask]

    # 2) Sub‑sample 200 points for fitting SSE
    idx = np.linspace(0, len(xF)-1, min(200, len(xF))).astype(int)
    xS, FS = xF[idx], FF[idx]

    # 3) Define SSE on subsample
    def sse(p):
        w, mu1, s1, mu2, s2 = p
        if not (0< w<1 and s1>0 and s2>0):
            return 1e9
        return np.sum((mix2_lognorm_cdf(xS, *p) - FS)**2)

    # 4) Initial guesses & bounds
    init = [
        0.8,
        np.log(np.percentile(xS, 1)), 1.0,
        np.log(np.percentile(xS, 50)), 1.0
    ]
    bnds = [
        (1e-3, 0.999),
        (None, None), (1e-3, 5.0),
        (None, None), (1e-3, 5.0),
    ]

    # 5) Fit with L‑BFGS‑B
    res = minimize(sse, init, bounds=bnds,
                   method='L-BFGS-B',
                   options={'maxiter':200, 'ftol':1e-6})
    w, mu1, s1, mu2, s2 = res.x

    # 6) Begin plot
    fig, ax = plt.subplots(figsize=(10,6))
    ax.set_xscale('log')
    ax.plot(xF, FF, 'k', label="Empirical CDF")

    # 7) Plot fitted mixture on a thinner grid
    xg = np.logspace(np.log10(xF.min()), np.log10(xF.max()), 250)
    ax.plot(xg, mix2_lognorm_cdf(xg, w, mu1, s1, mu2, s2),
            'r--', lw=2,
            label=(f"Mix2‑lognorm fit: w={w:.2f}, "
                   f"μ₁={mu1:.2f}, σ₁={s1:.2f}, "
                   f"μ₂={mu2:.2f}, σ₂={s2:.2f}"))

    # 8) Threshold lines
    cmap = plt.get_cmap('tab10')
    for i, (thr, _) in enumerate(threshold_coords):
        pct = percentages[thr]
        ax.axvline(thr, color=cmap(i),
                   ls='--', lw=1,
                   label=f"{pct:.2f}% < {thr} kg/h")

    # 9) Cosmetics
    ax.legend(loc="upper left", fontsize=10, title="CDF & Thresholds")
    ax.set_xlabel("CH₄ Emission Rate (kg/h, log scale)", fontsize=14)
    ax.set_ylabel("Cumulative Probability", fontsize=14)
    ax.set_title(f"CDF of CH₄ Emission Rates ({abnormal})", fontsize=16)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y:.0%}"))
    ax.tick_params(labelsize=12)
    ax.grid(which='both', ls='--', lw=0.5, alpha=0.7)
    ax.margins(x=0.05, y=0.15)

    # 10) Save
    os.makedirs(out_dir, exist_ok=True)
    fig.tight_layout()
    fig.savefig(f"{out_dir}/combined_CDF_abnormal_{abnormal}.png", dpi=200)
    plt.close()


def generate_comnbined_cdf_plot(config):
    for abnormal in ['on','off']:
        try:
            file = f"{config['simulationRoot']}/summaries/AggregatedSimulationEmissions/aggregated_sim_PDFs_abnormal_{abnormal}.csv"
            df= pd.read_csv(file)
        except FileNotFoundError:
            logger.info(f"please generate combined pdf first")
            return
        except Exception as e:
            logger.warning(f"run into an error({e}), reading {file}")
            return
        df_sorted  = compute_cdf_for_plot(df)
        percentages, coords = get_threshold_stats(df_sorted, [2, 10, 15, 25, 50, 100, 200])
        plot_cdf(df_sorted, percentages, coords, abnormal=abnormal, out_dir=f"{config['simulationRoot']}/summaries/AggregatedSimulationEmissions/CDF_Plots")

def find_files(root_dir: str, pattern: str) -> list[str]:
    """Return every file under *root_dir* whose *basename* matches *pattern*."""
    return glob.glob(os.path.join(root_dir, "**", pattern), recursive=True)


def load_pdf(path: str) -> pd.DataFrame:
    """Read one CSV and return only the two relevant columns."""
    return pd.read_csv(path, usecols=["CH4_EmissionRate_kg/h", "probability"])


def combine_pdfs(paths: list[str], round_decimals=6) -> pd.DataFrame:
    """Stack, merge duplicate emission-rate rows, and normalize probabilities."""
    if not paths:
        raise ValueError("No matching files were found.")

    df = pd.concat([load_pdf(p) for p in paths], ignore_index=True)

    if round_decimals is not None:
        df["CH4_EmissionRate_kg/h"] = df["CH4_EmissionRate_kg/h"].round(round_decimals)

    df = (
        df.groupby("CH4_EmissionRate_kg/h", as_index=False)["probability"]
          .sum()                          # add probabilities of identical rates
    )
    df["probability"] = df["probability"] / df["probability"].sum()  # normalize so Σprob = 1
    return df

def generate_site_level_pdfs(root_dir, site, abnormal):
    abnormal = abnormal.lower()
    files = find_files(f"{root_dir}/summaries/PDFs", f"PDF_for_site_abnormal_{abnormal}*.csv")

    if not files:
        logger.warning(f"Site: {site} does not have PDF_for_site_abnormal_{abnormal}")
        return

    logger.info(f"Found {len(files)} file(s).")

    combined = combine_pdfs(files)
    output_folder = os.path.join(root_dir, 'summaries', 'AggregatedSimulationEmissions')
    os.makedirs(output_folder, exist_ok=True)
    output_path = os.path.join(output_folder + '', f'aggregated_sim_PDFs_abnormal_{abnormal}.csv')

    combined.to_csv(output_path, index=False)

    logger.info(f"Combined PDF written to {output_path} ({len(combined)} rows).")


def list_all_files_by(folder_path, by):
    pdfs_path = os.path.join(folder_path, 'AnnualEmissions')
    base_depth = pdfs_path.rstrip(os.sep).count(os.sep)
    all_files = []

    for root, dirs, files in os.walk(pdfs_path):
        # Stop recursion beyond immediate subfolders
        if root.rstrip(os.sep).count(os.sep) > base_depth + 1:
            dirs.clear()
            continue
        for file in files:
            if file.endswith('.csv') and by in file:
                all_files.append(os.path.join(root, file))
    return all_files

def generate_annual_emissions_plot_for_metype(file, species):
    """
    Generates a bar plot of annual emissions by METype from the CSV file for a given species.
    Each METype is shown as a bar with 95% CI error bars.

    Parameters:
    - file: path to the CSV file.
    - species: filter for species (e.g., 'METHANE' or 'ETHANE').
    """

    # Adjustable Font Size Settings
    label_fontsize = 16  # For title, y-label, and legend
    tick_fontsize = 16   # For x-ticks and y-ticks

    try:
        df = pd.read_csv(file)
        logger.info(file)

    except FileNotFoundError:
        logger.warning(f"Plots cannot be generated because MAES did not find any AnnualEmissions or AggregatedSimulationEmissions summaries for METypes.")
        return

    except Exception as e:
        logger.info(f"Error reading {file}: {e}")
        return

    df = df[df['species'] == species]
    if df.empty:
        logger.info(f"No data for species {species} in {file}")
        return

    if 'METype' not in df.columns:
        logger.info(f"Column 'METype' not found in {file}")
        return

    unit = df['unit'].values[0]
    summed_row = df[df['METype'] == 'summed_METype']
    df = df[df['METype'] != 'summed_METype']

    if df.empty:
        logger.info(f"No METype entries to plot in {file}")
        return

    df = df.sort_values('METype')
    meTypes = df['METype'].tolist()
    mean_emissions = df['mean_emissions'].tolist()
    ci_lowers = df['95%_ci_lower'].tolist()
    ci_uppers = df['95%_ci_upper'].tolist()

    err_lower = [max(mean - low, 0) for mean, low in zip(mean_emissions, ci_lowers)]
    err_upper = [max(up - mean, 0) for mean, up in zip(mean_emissions, ci_uppers)]
    yerr = [err_lower, err_upper]

    base_dir, csv_filename = os.path.split(file)
    plot_dir = os.path.join(base_dir, "Plots")
    os.makedirs(plot_dir, exist_ok=True)
    site_name = os.path.basename(base_dir).replace('site=', '').capitalize()
    image_filename = os.path.splitext(csv_filename)[0] + "_" + species.lower() + ".png"
    output_image_path = os.path.join(plot_dir, image_filename)

    meType_colors = {
        'Compressor': '#404968',
        'Tank': '#9EB6CA',
        'Flare': '#cc0000',
        'Separator': '#5E7854',
        'Heater': 'purple',
        'Well': 'brown',
        'Dehydrator': 'pink',
        'Misc': 'orange',
        'Other': 'orange',
    }
    colors = [meType_colors.get(m, 'gray') for m in meTypes]

    fig, ax = plt.subplots(figsize=(10, 8))
    x = np.arange(len(meTypes))
    bar_width = 0.5
    ax.bar(x, mean_emissions, width=bar_width, color=colors, yerr=yerr, capsize=5,
           error_kw={'ecolor': 'black', 'alpha': 0.4})

    ax.set_xticks(x)
    ax.set_xticklabels(meTypes, rotation=45, fontsize=tick_fontsize)

    if len(meTypes) == 1:
        ax.set_xlim(-2, 2)

    ax.set_ylabel(f'{species.capitalize()} Emissions ({unit})', fontsize=label_fontsize)

    # Add CI to title using summed_METype row
    if not summed_row.empty:
        s = summed_row.iloc[0]
        summed_mean = s['mean_emissions']
        ci_low = s['95%_ci_lower']
        ci_up = s['95%_ci_upper']
        ax.set_title(f"{site_name}\nAnnual Emissions by Equipment Group (METype) in {unit}\n"
                     f"Total Mean ± 95% Confidence Interval (CI): {summed_mean:.1f} [{ci_low:.1f}, {ci_up:.1f}]",
                     fontsize=label_fontsize)
    else:
        ax.set_title(f"{site_name} - Annual Emissions by METype in {unit}\nMean ± 95% Confidence Interval (CI)",
                     fontsize=label_fontsize)

    ax.tick_params(axis='y', labelsize=tick_fontsize)
    ax.grid(alpha=0.3)


    legend_handles = []
    for m, mean, low, up, color in zip(meTypes, mean_emissions, ci_lowers, ci_uppers, colors):
        label = f"{m}: {mean:.1f} [{low:.1f}, {up:.1f}]"
        patch = Patch(facecolor=color, label=label)
        legend_handles.append(patch)

    ax.legend(handles=legend_handles, fontsize=label_fontsize, loc='best')

    plt.tight_layout()
    plt.savefig(output_image_path)
    plt.close()


def plot_annual_emissions_for_metype(path, species, plot_by=None):
    """
    Generate annual emissions plots for a single file or for all files in a folder,
    filtered by species.

    Parameters:
    - path: path to a file or folder.
    - species: species to filter by ('METHANE' or 'ETHANE').
    - plot_by: 'file' to process a single file, or 'folder' to process all matching CSV files.
    """
    if plot_by == "folder":
        files = list_all_files_by(path, by="by_METype")
        for file in files:
            generate_annual_emissions_plot_for_metype(file, species)
    elif plot_by == "file":
        generate_annual_emissions_plot_for_metype(path, species)
    else:
        logger.info("Missing or invalid 'plot_by' argument.\nPlease specify 'file' or 'folder'.")

def generate_annual_emissions_plot__site_level(file, species):
    """
    Generates a stacked bar plot of annual emissions from the CSV file for a given species.

    The bar is stacked using the 'mean_emissions' values for the categories:
    'FUGITIVE', 'VENTED', and 'COMBUSTION'. Additionally, error bars representing the 95%
    confidence intervals (from '95%_ci_lower' and '95%_ci_upper') for the 'TOTAL' emissions
    are added on the bar.

    Parameters:
    - file: path to the CSV file.
    - species: filter for species (e.g., 'METHANE' or 'ETHANE').
    """

    # Adjustable Font Size Settings
    label_fontsize = 16  # Title, y-label, legend
    tick_fontsize = 16   # x-ticks and y-ticks

    try:
        df = pd.read_csv(file)
        logger.info(file)

    except FileNotFoundError:
        logger.warning(f"Plots cannot be generated because MAES did not find any AnnualEmissions or AggregatedSimulationEmissions summaries for categories.")
        return

    except Exception as e:
        logger.info(f"Error reading {file}: {e}")
        return

    df = df[df['species'] == species]

    if df.empty:
        logger.info(f"No data for species {species} in {file}")
        return

    unit = df['unit'].values[0]
    stack_categories = [x for x in df['modelEmissionCategory'].unique().tolist() if x != 'TOTAL']

    category_colors = {
        'FUGITIVE': '#0d3b66',
        'VENTED': '#f4d35e',
        'COMBUSTION': '#92140c'
    }

    available_stack = [cat for cat in stack_categories if cat in df['modelEmissionCategory'].values]
    if not available_stack:
        logger.info(f"No stack categories available in {file}. Skipping.")
        return

    df_stack = df[df['modelEmissionCategory'].isin(available_stack)]
    df_stack = df_stack.set_index('modelEmissionCategory').reindex(stack_categories, fill_value=0)
    emissions_values = df_stack['mean_emissions'].values

    df_total = df[df['modelEmissionCategory'] == 'TOTAL']
    if df_total.empty:
        logger.info(f"No TOTAL row found in {file}")
        return
    total_row = df_total.iloc[0]
    total_emissions = total_row['mean_emissions']
    ci_lower = total_row['95%_ci_lower']
    ci_upper = total_row['95%_ci_upper']
    error_low = max(total_emissions - ci_lower, 0)
    error_high = max(ci_upper - total_emissions, 0)

    base_dir, csv_filename = os.path.split(file)
    plot_dir = os.path.join(base_dir, "Plots")
    os.makedirs(plot_dir, exist_ok=True)
    site_name = os.path.basename(base_dir).replace('site=', '').capitalize()
    image_filename = os.path.splitext(csv_filename)[0] + "_" + species.lower() + ".png"
    output_image_path = os.path.join(plot_dir, image_filename)

    bar_width = 0.3
    fig, ax = plt.subplots(figsize=(10, 8))
    x = [0]
    bottom = 0

    for cat, val in zip(stack_categories, emissions_values):
        if val > 0:
            display_name = f"{cat.capitalize()}: {val:.1f}"
            ax.bar(x, val, bottom=bottom, width=bar_width, label=display_name,
                   color=category_colors.get(cat.upper(), 'gray'))
            bottom += val

    ax.errorbar(x, [total_emissions], yerr=[[error_low], [error_high]], fmt='none', alpha=0.4,
                ecolor='black', capsize=5, label=f'95% CI')

    ax.set_ylabel(f'{species.capitalize()} Emissions ({unit})', fontsize=label_fontsize)
    ax.set_title(f"{site_name} - Site Annual Emissions in {unit}\nMean ± 95% Confidence Interval (CI): {total_emissions:.1f}"
                 f" [{total_emissions - error_low:.1f}, {total_emissions + error_high:.1f}]", fontsize=label_fontsize)
    ax.set_xticks([])
    ax.set_xlim(-2, 2)
    ax.grid(alpha=0.3)
    ax.legend(fontsize=label_fontsize)

    ax.tick_params(axis='x', labelsize=tick_fontsize)
    ax.tick_params(axis='y', labelsize=tick_fontsize)

    plt.tight_layout()
    plt.savefig(output_image_path)
    plt.close()


def plot_annual_emissions_site_level(path, species, plot_by=None):
    """
    Generate annual emissions plots for a single file or for all files in a folder,
    filtered by species.

    Parameters:
    - path: path to a file or folder.
    - species: species to filter by ('METHANE' or 'ETHANE').
    - plot_by: 'file' to process a single file, or 'folder' to process all matching CSV files.
    """
    if plot_by == "folder":
        files = list_all_files_by(path, by="by_site_abnormal")
        for file in files:
            generate_annual_emissions_plot__site_level(file, species)
    elif plot_by == "file":
        generate_annual_emissions_plot__site_level(path, species)
    else:
        logger.info("Missing or invalid 'plot_by' argument.\nPlease specify 'file' or 'folder'.")

def generate_annual_emissions_plot_for_modelReadableName(file, species):
    """
    Generates a bar plot of annual emissions by unitID-modelReadableName from the CSV file for a given species.

    Rows with 'mean_emissions' equal to 0 are removed.
    Rows where 'modelReadableName' contains 'summed' are excluded.
    Each bar represents a unique combination of {unitID} - {modelReadableName},
    with 95% confidence interval error bars.

    Parameters:
    - file: path to the CSV file.
    - species: filter for species (e.g., 'METHANE' or 'ETHANE').
    """

    # Adjustable Font Size Settings
    label_fontsize = 20  # For title, y-label, legend
    tick_fontsize = 20  # For x-ticks and y-ticks

    try:
        df = pd.read_csv(file)
        logger.info(file)
    except FileNotFoundError:
        logger.warning(f"Plots cannot be generated because MAES did not find any AnnualEmissions or AggregatedSimulationEmissions summaries for modelReadbaleNames.")
        return
    except Exception as e:
        logger.info(f"Error reading {file}: {e}")
        return

    df = df[df['species'] == species]
    if df.empty:
        logger.info(f"No data for species {species} in {file}")
        return

    # Remove rows where mean_emissions is 0
    df = df[df['mean_emissions'] != 0]
    if df.empty:
        logger.info(f"All entries have 0 emissions in {file}")
        return

    # Exclude rows where modelReadableName contains 'summed' anywhere
    df = df[~df['modelReadableName'].str.contains('summed', case=False, na=False)]
    if df.empty:
        logger.info(f"No valid rows for plotting in {file}")
        return

    # Generate unique labels: "{unitID} - {modelReadableName}"
    df['label'] = df['modelReadableName'].astype(str)

    # Sort for visual consistency
    df = df.sort_values('label')

    labels = df['label'].tolist()
    mean_emissions = df['mean_emissions'].tolist()
    ci_lowers = df['95%_ci_lower'].tolist()
    ci_uppers = df['95%_ci_upper'].tolist()

    err_lower = [max(mean - low, 0) for mean, low in zip(mean_emissions, ci_lowers)]
    err_upper = [max(up - mean, 0) for mean, up in zip(mean_emissions, ci_uppers)]
    yerr = [err_lower, err_upper]

    unit = df['unit'].values[0]
    total_mean = sum(mean_emissions)
    total_lower = sum(ci_lowers)
    total_upper = sum(ci_uppers)

    # === Output Path Settings ===
    base_dir, csv_filename = os.path.split(file)
    plot_dir = os.path.join(base_dir, "Plots")
    os.makedirs(plot_dir, exist_ok=True)
    site_name = os.path.basename(base_dir).replace('site=', '').capitalize()
    image_filename = os.path.splitext(csv_filename)[0] + "_" + species.lower() + ".png"
    output_image_path = os.path.join(plot_dir, image_filename)

    # Colors
    cmap = plt.get_cmap('viridis')
    colors = [cmap(i / max(len(labels) - 1, 1)) for i in range(len(labels))]

    # Create figure with 2 columns: 50% for plot, 50% for legend
    fig = plt.figure(figsize=(30, 10))
    gs = gridspec.GridSpec(1, 2, width_ratios=[1, 1], wspace=0.05)

    # Left: Plot axis
    ax = fig.add_subplot(gs[0])

    x = np.arange(len(labels))
    bar_width = 0.5
    ax.bar(x, mean_emissions, width=bar_width, color=colors, yerr=yerr, capsize=5,
           error_kw={'ecolor': 'black', 'alpha': 0.4})

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=90, ha='center', fontsize=tick_fontsize)

    if len(labels) == 1:
        ax.set_xlim(-2, 2)

    ax.set_ylabel(f'{species.capitalize()} Emissions ({unit})', fontsize=label_fontsize)

    ax.set_title(
        f"{site_name}\nAnnual Emissions by Emission Type (ModelReadableName) in {unit}\n"
        f"Total Mean ± 95% Confidence Interval (CI): {total_mean:.1f} [{total_lower:.1f}, {total_upper:.1f}]",
        fontsize=label_fontsize
    )

    ax.tick_params(axis='y', labelsize=tick_fontsize)
    ax.grid(alpha=0.3)

    # Right: Legend axis (invisible, just used for positioning)
    legend_ax = fig.add_subplot(gs[1])
    legend_ax.axis('off')  # Hide actual plot area

    legend_handles = [
        Patch(facecolor=color, label=f"{label}: {mean:.1f} [{low:.1f}, {up:.1f}]")
        for label, mean, low, up, color in zip(labels, mean_emissions, ci_lowers, ci_uppers, colors)
    ]

    # Add legend to the center of the legend_ax
    legend_ax.legend(
        handles=legend_handles,
        fontsize=label_fontsize,
        loc='center',
        frameon=False
    )

    plt.tight_layout
    plt.savefig(output_image_path, bbox_inches='tight')
    plt.close()


def plot_annual_emissions_for_modelReadableName(path, species, plot_by=None):
    """
    Generate annual emissions plots for a single file or for all files in a folder,
    filtered by species.

    Parameters:
    - path: path to a file or folder.
    - species: species to filter by ('METHANE' or 'ETHANE').
    - plot_by: 'file' to process a single file, or 'folder' to process all matching CSV files.
    """
    if plot_by == "folder":
        files = list_all_files_by(path, by="by_modelReadableName")
        for file in files:
            generate_annual_emissions_plot_for_modelReadableName(file, species)
    elif plot_by == "file":
        generate_annual_emissions_plot_for_modelReadableName(path, species)
    else:
        logger.info("Missing or invalid 'plot_by' argument.\nPlease specify 'file' or 'folder'.")

def generate_annual_emissions_plot_unitid_level(file, species):
    """
    Generates a bar plot of annual emissions by unitID from the CSV file for a given species.

    The DataFrame is filtered by species and rows with mean_emissions equal to 0 are removed.
    Only rows where 'modelReadableName' equals 'summed_modelReadableName' are kept, excluding
    any row where modelReadableName contains 'summed' elsewhere in the name.

    Each unitID becomes a bar representing the corresponding value in 'mean_emissions' with
    95% confidence interval error bars (from '95%_ci_lower' and '95%_ci_upper').

    Parameters:
    - file: path to the CSV file.
    - species: filter for species (e.g., 'METHANE' or 'ETHANE').
    """

    # Adjustable Font Size Settings ===
    label_fontsize = 16  # For title, y-label, legend
    tick_fontsize = 16  # For x-ticks and y-ticks
    try:
        df = pd.read_csv(file)
        logger.info(file)

    except FileNotFoundError:
        logger.warning(f"Plots cannot be generated because MAES did not find any AnnualEmissions or AggregatedSimulationEmissions summaries for unitIDs.")
        return
    except Exception as e:
        logger.info(f"Error reading {file}: {e}")
        return

    df = df[df['species'] == species]
    if df.empty:
        logger.info(f"No data for species {species} in {file}")
        return

    # Remove rows where mean_emissions is 0
    df = df[df['mean_emissions'] != 0]
    if df.empty:
        logger.info(f"All entries have 0 emissions in {file}")
        return

    try:
        # Keep only rows where modelReadableName == 'summed_modelReadableName'
        df = df[df['modelReadableName'] == 'summed_modelReadableName']

        # Exclude rows where modelReadableName contains 'summed' elsewhere
        df = df[df['unitID'] != 'summed_unitID']
    except KeyError as e:
        pass

    if df.empty:
        logger.info(f"No valid rows for plotting in {file}")
        return

    unit = df['unit'].values[0]

    df = df.sort_values('unitID')
    unitIDs = df['unitID'].astype(str).tolist()
    mean_emissions = df['mean_emissions'].tolist()
    ci_lowers = df['95%_ci_lower'].tolist()
    ci_uppers = df['95%_ci_upper'].tolist()

    err_lower = [max(mean - low, 0) for mean, low in zip(mean_emissions, ci_lowers)]
    err_upper = [max(up - mean, 0) for mean, up in zip(mean_emissions, ci_uppers)]
    yerr = [err_lower, err_upper]

    # Compute total mean and CI range
    total_mean = sum(mean_emissions)
    total_lower = sum(ci_lowers)
    total_upper = sum(ci_uppers)

    base_dir, csv_filename = os.path.split(file)
    plot_dir = os.path.join(base_dir, "Plots")
    os.makedirs(plot_dir, exist_ok=True)
    site_name = os.path.basename(base_dir).replace('site=', '').capitalize()
    image_filename = os.path.splitext(csv_filename)[0] + "_" + species.lower() + ".png"
    image_filename = image_filename.replace("modelReadableName", "unitID")
    output_image_path = os.path.join(plot_dir, image_filename)

    cmap = plt.get_cmap('viridis')
    colors = [cmap(i / max(len(unitIDs) - 1, 1)) for i in range(len(unitIDs))]

    fig, ax = plt.subplots(figsize=(10, 8))
    x = np.arange(len(unitIDs))
    bar_width = 0.5
    ax.bar(x, mean_emissions, width=bar_width, color=colors, yerr=yerr, capsize=5,
           error_kw={'ecolor': 'black', 'alpha': 0.4})

    ax.set_xticks(x)
    ax.set_xticklabels(unitIDs, rotation=45, fontsize=tick_fontsize)

    if len(unitIDs) == 1:
        ax.set_xlim(-2, 2)

    ax.set_ylabel(f'{species.capitalize()} Emissions ({unit})', fontsize=label_fontsize)

    # Add total in title
    ax.set_title(
        f"{site_name}\nAnnual Emissions by Equipment Unit (UnitID) in {unit}\n"
        f"Total Mean ± 95% Confidence Interval (CI): {total_mean:.1f} [{total_lower:.1f}, {total_upper:.1f}]",
        fontsize=label_fontsize
    )

    ax.tick_params(axis='y', labelsize=tick_fontsize)
    ax.grid(alpha=0.3)

    # Create custom legend entries per unitID
    legend_handles = []
    for unit, mean, low, up, color in zip(unitIDs, mean_emissions, ci_lowers, ci_uppers, colors):
        label = f"{unit}: {mean:.1f} [{low:.1f}, {up:.1f}]"
        patch = Patch(facecolor=color, label=label)
        legend_handles.append(patch)

    ax.legend(handles=legend_handles, fontsize=label_fontsize, loc='best')

    # plt.tight_layout()
    plt.savefig(output_image_path)
    plt.close()


def plot_annual_emissions_unitid_level(path, species, plot_by=None):
    """
    Generate annual emissions plots for a single file or for all files in a folder,
    filtered by species.

    Parameters:
    - path: path to a file or folder.
    - species: species to filter by ('METHANE' or 'ETHANE').
    - plot_by: 'file' to process a single file, or 'folder' to process all matching CSV files.
    """
    if plot_by == "folder":
        files = list_all_files_by(path, by="by_modelReadableName")
        for file in files:
            generate_annual_emissions_plot_unitid_level(file, species)
    elif plot_by == "file":
        generate_annual_emissions_plot_unitid_level(path, species)
    else:
        logger.info("Missing or invalid 'plot_by' argument.\nPlease specify 'file' or 'folder'.")



def list_all_files_in_folder_for_mii(folder_path):
    """
    Recursively list all files in the given folder, excluding any subfolders named 'Plots'.
    """
    all_files = []
    for root, dirs, files in os.walk(folder_path):
        dirs[:] = [d for d in dirs if d != "Plots"]
        for file in files:
            full_path = os.path.join(root, file)
            all_files.append(full_path)
    return all_files


def compute_cdf(emission_rates, probabilities):
    """
    Compute the CDF using sorted emission rates and their associated probabilities.
    """
    sorted_indices = np.argsort(emission_rates)
    sorted_rates = np.array(emission_rates)[sorted_indices]
    sorted_probs = np.array(probabilities)[sorted_indices]
    cdf = np.cumsum(sorted_probs)
    return sorted_rates, cdf


def process_site_for_mii(site_folder):
    """
    Processes all CSV files in a given site folder (and its subfolders) that contain '_off.csv',
    computes the emission threshold at 95% cumulative probability for each file,
    and saves the results in a CSV file within a new folder called 'MIIEmissionThresholds' in the site folder.
    """
    logger.info(f"Processing site: {os.path.basename(site_folder)}")
    files = list_all_files_in_folder_for_mii(site_folder)
    thresholds = {}

    for file in files:
        if '_off.csv' not in file:
            continue
        try:
            df = pd.read_csv(file)
        except Exception as e:
            logger.info(f"Error reading {file}: {e}")
            continue

        # Extract emissions (assumed to be the 2nd column) and probabilities
        emissions = df.iloc[:, 1]
        probabilities = df['probability']

        # Compute the CDF and determine the threshold emission rate at 95% cumulative probability
        cdf_x, cdf_y = compute_cdf(emissions, probabilities)
        y_threshold = 0.95
        index_at_y = np.argmin(np.abs(cdf_y - y_threshold))
        x_at_95 = cdf_x[index_at_y]

        # Build the dictionary key based on the file name
        filename = os.path.basename(file)
        if '_all_' in filename:
            start = filename.find("all_") + len("all_")
            end = filename.find("_", start)
            if end == -1:
                end = filename.find("_off.csv", start)
            key_str = filename[start:end]
            key = f"All {key_str}s"
        else:
            start = filename.find("_for_") + len("_for_")
            end = filename.find("_abnormal", start)
            key_str = filename[start:end]
            key = key_str
        key = key.replace('_', ' ').title()

        thresholds[key] = x_at_95

    # Save the thresholds to CSV in a site-specific MIIEmissionThresholds folder
    output_dir = os.path.join(site_folder, "MIIEmissionThresholds")
    os.makedirs(output_dir, exist_ok=True)
    df_out = pd.DataFrame(list(thresholds.items()), columns=["Category", "CH4_Abnormal_Emission_Threshold_kg/h"])
    output_file = os.path.join(output_dir, "MII_Abnormal_Emission_Thresholds.csv")
    df_out.to_csv(output_file, index=False)


def process_all_sites_mii(base_folder):
    """
    Iterates over each site folder within the 'PDFs' folder and processes the MII analysis for each.
    """
    pdfs_dir = os.path.join(base_folder, "PDFs")
    # List all entries in the PDFs folder and process only directories (sites) that are not already an output folder
    for entry in os.listdir(pdfs_dir):
        site_path = os.path.join(pdfs_dir, entry)
        if os.path.isdir(site_path) and entry != "MIIEmissionThresholds":
            process_site_for_mii(site_path)

def list_all_files_for_agg_modelReadbleName(folder_path):
    """Reads the site emissions parquet file from a given folder."""
    path = os.path.join(folder_path, 'parquet/siteEmissionsByEquip')
    return pd.read_parquet(path, engine='pyarrow')

def compute_stats_by(species, all_mcRuns, df_base, mode, by):
    df = df_base[df_base['species'] == species]
    if mode == "OFF":
        df = df[df['modelEmissionCategory'] != 'FUGITIVE']

    df = df.assign(emissions_mtPerYear = df['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON)

    grouped = df.groupby([by, 'mcRun'])['emissions_mtPerYear'].sum().reset_index()
    full_idx = pd.MultiIndex.from_product([grouped[by].unique(), all_mcRuns], names=[by, 'mcRun'])
    filled = grouped.set_index([by, 'mcRun']).reindex(full_idx, fill_value=0).reset_index()

    stats = filled.groupby(by)['emissions_mtPerYear'].agg(
        mean_emissions='mean',
        MCRuns_emission_list=lambda x: list(x),
        emissions_sum_across_mcRuns='sum',
        ci_lower=lambda x: np.percentile(x, 2.5),
        ci_upper=lambda x: np.percentile(x, 97.5)
    ).reset_index()

    total_species_emissions = filled.groupby('mcRun')['emissions_mtPerYear'].sum().sum()

    stats = stats.assign(percentage_of_total_emissions = stats['emissions_sum_across_mcRuns'] / total_species_emissions * 100)
    stats = stats.rename(columns={
        'ci_lower': '95%_ci_lower',
        'ci_upper': '95%_ci_upper'
    })
    stats['species'] = species.upper()
    stats['unit'] = 'mt/year'

    return stats[[
        'species', by, 'unit', 'mean_emissions', '95%_ci_lower',
        '95%_ci_upper','MCRuns_emission_list',
        'emissions_sum_across_mcRuns', 'percentage_of_total_emissions'
    ]]


def compute_c2_c1_ratios_by(df_base, mode, by):
    if mode == "OFF":
        df_base = df_base[df_base['modelEmissionCategory'] != 'FUGITIVE']

    df = df_base[df_base['species'].str.upper().isin(['ETHANE', 'METHANE'])]
    df['species'] = df['species'].str.upper()
    df = df.assign(emissions_mtPerYear = df['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON)

    pivot = df.pivot_table(
        index=['mcRun', by],
        columns='species',
        values='emissions_mtPerYear',
        aggfunc='sum'
    ).dropna()

    pivot = pivot.assign(ratio = pivot['ETHANE'] / pivot['METHANE'])
    pivot = pivot[np.isfinite(pivot['ratio'])]

    c2c1_stats = (
        pivot.groupby(by)['ratio']
        .agg(mean_emissions='mean',
             ci_lower=lambda x: np.percentile(x, 2.5),
             ci_upper=lambda x: np.percentile(x, 97.5))
        .reset_index()
    )
    c2c1_stats = c2c1_stats.rename(columns={
        'ci_lower': '95%_ci_lower',
        'ci_upper': '95%_ci_upper'
    }).assign(
        species='C2/C1',
        unit='unitless'
    )

    return c2c1_stats[['species', by, 'unit', 'mean_emissions', '95%_ci_lower', '95%_ci_upper']]


def summarize_emissions_by_mode_for_agg_modelReadableName_and_unitID(mode, df_all, all_mcRuns, all_species, output_folder):
    """Processes and saves the summary emissions for a specific abnormal mode (ON/OFF)."""
    all_results_md_name = [
        compute_stats_by(species, all_mcRuns, df_all, mode, by='modelReadableName')
        for species in all_species
    ]
    all_results_unitID = [
        compute_stats_by(species, all_mcRuns, df_all, mode, by='unitID')
        for species in all_species
    ]

    all_results_md_name.append(compute_c2_c1_ratios_by(df_all,mode=mode,by='modelReadableName'))
    all_results_unitID.append(compute_c2_c1_ratios_by(df_all,mode=mode,by='unitID'))

    summary_df_md_name = pd.concat(all_results_md_name, ignore_index=True)
    summary_df_md_name = summary_df_md_name.drop(summary_df_md_name[summary_df_md_name["mean_emissions"] == 0 ].index)

    summary_df_unitID = pd.concat(all_results_unitID, ignore_index=True)
    summary_df_unitID = summary_df_unitID.drop(summary_df_unitID[summary_df_unitID["mean_emissions"] == 0 ].index)

    suffix = 'abnormal_on.csv' if mode == 'ON' else 'abnormal_off.csv'

    output_folder = os.path.join(output_folder, 'summaries', 'AggregatedSimulationEmissions')

    os.makedirs(output_folder, exist_ok=True)
    output_path_md_name = os.path.join(output_folder, f'aggregated_sim_emissions_by_modelReadableName_{suffix}')
    output_path_unitID = os.path.join(output_folder, f'aggregated_sim_emissions_by_unitID_{suffix}')

    summary_df_md_name.to_csv(output_path_md_name, index=False)
    summary_df_unitID.to_csv(output_path_unitID, index=False)

    logger.info(f"\nSaved modelReadableName & unitID emissions summary for ABNORMAL = {mode} to:")
    logger.info(f"{output_path_md_name} \n {output_path_unitID}")


def run_emissions_summary_pipeline_for_modelReadableName_and_unitID(folder, abnormal):
    """Runs the emissions summary for both ABNORMAL ON and OFF modes."""
    df_all = fillEmptyDataWithZero(df=list_all_metype_files(folder), emissionCol='emissions_USTonsPerYear')
    all_mcRuns = sorted(df_all['mcRun'].unique())
    all_species = df_all['species'].unique()

    summarize_emissions_by_mode_for_agg_modelReadableName_and_unitID(abnormal, df_all, all_mcRuns, all_species, folder)



def list_all_files_for_annual_emissions_categories(folder_path):
    """
    Reads the annual emissions parquet file from a given folder.
    """
    annualEmiss_parquets = os.path.join(folder_path, 'parquet/siteEmissionsbyCat')
    annualEmissDF = pd.read_parquet(annualEmiss_parquets, engine='pyarrow')
    return annualEmissDF


def compute_total_emissions_stats_for_category(folder, abnormal):
    """
    Computes total emissions summary statistics per species and modelEmissionCategory including:
      - Mean emissions (metric tons/year) across Monte Carlo runs
      - 95% Confidence Interval (lower and upper)
      - Total emissions across all Monte Carlo runs
      - Percentage of total species emissions contributed by the current modelEmissionCategory

    For abnormal mode:
      - When abnormal == "ON": it is assumed that the dataset includes TOTAL rows. In this
        case, the species total emissions are computed using the three categories that contribute
        to the species total (COMBUSTION, VENTED, and FUGITIVE).
      - When abnormal == "OFF": only COMBUSTION and VENTED are present. Therefore, the species
        total is computed using only these two categories, so that the percentages for each species
        sum to 100%.
    """
    df_full = list_all_files_for_annual_emissions_categories(folder)
    df_full = df_full.assign(emissions_mtPerYear = df_full['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON)

    # Define which categories contribute to species totals
    categories_total = ['COMBUSTION', 'VENTED'] if abnormal == "OFF" else ['COMBUSTION', 'VENTED', 'FUGITIVE']
    df_total = df_full[df_full['modelEmissionCategory'].isin(categories_total)]

    # Species totals over all Monte Carlo runs
    species_totals = (
        df_total.groupby(['species', 'mcRun'])['emissions_mtPerYear'].sum()
        .groupby('species').sum().reset_index(name='species_total_emissions')
    )

    if abnormal == "OFF":
        df_full = df_full[df_full['modelEmissionCategory'].isin(['COMBUSTION', 'VENTED'])]
        df_full = (
            df_full.groupby(['mcRun', 'species', 'modelEmissionCategory'], as_index=False)['emissions_USTonsPerYear'].sum()
        )
        df_full = df_full.assign(emissions_mtPerYear = df_full['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON)


    # Group emissions by species/category/mcRun
    group = df_full.groupby(['species', 'modelEmissionCategory', 'mcRun'])['emissions_mtPerYear'].sum()
    emissions_stats = group.reset_index().groupby(['species', 'modelEmissionCategory'])['emissions_mtPerYear'].agg(
        MCRuns_emission_list=lambda x: list(x),
        mean_emissions='mean',
        ci_lower=lambda x: np.percentile(x, 2.5),
        ci_upper=lambda x: np.percentile(x, 97.5),
        emissions_sum='sum'
    ).reset_index()

    # Merge with species totals for percentage calculation
    emissions_stats = emissions_stats.merge(species_totals, how='left', on='species')
    emissions_stats = emissions_stats.assign(percentage_of_total_emissions = np.where(
        emissions_stats['modelEmissionCategory'] == 'TOTAL',
        100.0,
        emissions_stats['emissions_sum'] / emissions_stats['species_total_emissions'] * 100
    ))

    # Final formatting
    emissions_stats = emissions_stats.assign(
        Species=emissions_stats['species'].str.upper(),
        unit='mt/year'
    )
    emissions_stats = emissions_stats.rename(columns={
        'modelEmissionCategory': 'modelEmissionCategory',
        'mean_emissions': 'mean_emissions',
        'ci_lower': '95%_ci_lower',
        'ci_upper': '95%_ci_upper',
        'emissions_sum': 'emissions_sum_across_mcRuns'
    })

    final = emissions_stats[[
        'species', 'modelEmissionCategory', 'unit', 'mean_emissions',
        '95%_ci_lower', '95%_ci_upper',  'MCRuns_emission_list',
        'emissions_sum_across_mcRuns', 'percentage_of_total_emissions']]

    # --- Compute C2/C1 ratio ---
    df_ratio = df_full[df_full['species'].str.upper().isin(['METHANE', 'ETHANE'])]
    df_ratio['species'] = df_ratio['species'].str.upper()
    pivot = df_ratio.pivot_table(
        index=['mcRun', 'modelEmissionCategory'],
        columns='species',
        values='emissions_mtPerYear',
        aggfunc='sum'
    ).dropna()

    pivot['ratio'] = pivot['ETHANE'] / pivot['METHANE']
    pivot = pivot[np.isfinite(pivot['ratio'])]

    c2c1_stats = (
        pivot.groupby('modelEmissionCategory')['ratio']
        .agg(mean_emissions='mean',
             ci_lower=lambda x: np.percentile(x, 2.5),
             ci_upper=lambda x: np.percentile(x, 97.5))
        .reset_index()
    )
    c2c1_stats = c2c1_stats.rename(columns={
        'ci_lower': '95%_ci_lower',
        'ci_upper': '95%_ci_upper'
    }).assign(
        species='C2/C1',
        unit='unitless',
        emissions_sum_across_mcRuns=np.nan,
        percentage_of_total_emissions=np.nan,
        MCRuns_emission_list=np.nan
    )

    final = pd.concat([final, c2c1_stats[final.columns]], ignore_index=True)
    return final

def run_total_emissions_pipeline_for_category(folder, abnormal):
    """
    Runs the total emissions summary for both abnormal modes ("ON" and "OFF").
    """
    output_folder = os.path.join(folder, 'summaries', 'AggregatedSimulationEmissions')

    os.makedirs(output_folder, exist_ok=True)

    df_results = compute_total_emissions_stats_for_category(folder, abnormal)
    suffix = 'abnormal_on.csv' if abnormal == 'ON' else 'abnormal_off.csv'
    output_path = os.path.join(output_folder, f'aggregated_sim_emissions_by_category_{suffix}')
    df_results.to_csv(output_path, index=False)
    logger.info(f"Saved emissions summary for ABNORMAL = {abnormal} to:")
    logger.info(output_path)


def list_all_metype_files(folder_path):
    """Reads the site emissions parquet file from a given folder."""
    path = os.path.join(folder_path, 'parquet/siteEmissionsByEquip')
    return pd.read_parquet(path, engine='pyarrow')


def compute_stats_per_METype(species, all_mcRuns, df_base, mode):
    """Computes statistics for a given species across Major Equipment Types."""
    df = df_base[df_base['species'] == species]

    if mode == "OFF":
        df = df[df['modelEmissionCategory'] != 'FUGITIVE']

    df = df.assign(emissions_mtPerYear = df['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON)

    total_species_emissions = (
        df.groupby('mcRun')['emissions_mtPerYear'].sum()
        .reindex(all_mcRuns, fill_value=0)
        .sum()
    )

    mcdf = df.groupby(["mcRun", "METype"], as_index=False)['emissions_mtPerYear'].sum()
    meandf = mcdf.groupby("METype", as_index=False)['emissions_mtPerYear'].mean()

    emission_lists = (
        mcdf.groupby("METype")['emissions_mtPerYear']
        .apply(list)
        .rename("MCRuns_emission_list")
        .reset_index()
    )

    sumdf = mcdf.groupby("METype")['emissions_mtPerYear'].sum()

    ci_lower = mcdf.groupby("METype")['emissions_mtPerYear'].apply(lambda x: np.percentile(x, 2.5))
    ci_upper = mcdf.groupby("METype")['emissions_mtPerYear'].apply(lambda x: np.percentile(x, 97.5))

    percentage_of_total = (sumdf / total_species_emissions) * 100

    meandf = meandf.merge(ci_lower.rename('95%_ci_lower'), on=['METype'], how='left')
    meandf = meandf.merge(ci_upper.rename('95%_ci_upper'), on=['METype'], how='left')
    meandf = meandf.merge(sumdf.rename('emissions_sum_across_mcRuns'), on=['METype'], how='left')
    meandf = meandf.merge(percentage_of_total.rename('percentage_of_total_emissions'), on=['METype'], how='left')

    meandf = meandf.merge(emission_lists, on='METype', how='left')

    meandf['unit'] = 'mt/year'
    meandf['species'] = species.upper()
    meandf = meandf.rename(columns={'emissions_mtPerYear':'mean_emissions'})

    return meandf


def compute_c2_c1_ratios_for_metype(df_base, mode):
    """Computes C2 to C1 emission ratios per METype."""

    if mode == "OFF":
        df_base = df_base[df_base['modelEmissionCategory'] != 'FUGITIVE']

    df_base = df_base.assign(emissions_mtPerYear = df_base['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON)

    df_ethane = df_base[df_base['species'].str.upper() == 'ETHANE']
    df_methane = df_base[df_base['species'].str.upper() == 'METHANE']

    ethane_grouped = df_ethane.groupby(['METype', 'mcRun'])['emissions_mtPerYear'].sum().reset_index()
    methane_grouped = df_methane.groupby(['METype', 'mcRun'])['emissions_mtPerYear'].sum().reset_index()

    merged = pd.merge(ethane_grouped, methane_grouped, on=['METype', 'mcRun'], how='inner', suffixes=('_ethane', '_methane'))

    merged['ratio'] = merged['emissions_mtPerYear_ethane'] / merged['emissions_mtPerYear_methane']
    merged['ratio'] = merged['ratio'].replace([np.inf, -np.inf, np.nan], 0)
    merged = merged.drop(merged[merged["ratio"] == 0 ].index)

    summary = merged.groupby('METype').agg(
        mean_emissions=('ratio', lambda r: merged.loc[r.index, 'emissions_mtPerYear_ethane'].mean() / merged.loc[r.index, 'emissions_mtPerYear_methane'].mean()),
        ci_lower=('ratio', lambda r: np.percentile(r, 2.5)),
        ci_upper=('ratio', lambda r: np.percentile(r, 97.5))
    ).reset_index()

    summary.insert(0, 'species', 'C2/C1')
    summary.insert(2, 'unit', 'unitless')

    return summary.rename(columns={
        'ci_lower': '95%_ci_lower',
        'ci_upper': '95%_ci_upper'
    })


def summarize_metype_emissions_by_mode(mode, df_all, all_mcRuns, all_species, output_folder):
    """Processes and saves the summary emissions for a specific abnormal mode (ON/OFF)."""
    all_results = [
        compute_stats_per_METype(species, all_mcRuns, df_all, mode)
        for species in all_species
    ]
    all_results.append(compute_c2_c1_ratios_for_metype(df_all, mode=mode))

    summary_df = pd.concat(all_results, ignore_index=True)
    suffix = 'abnormal_on.csv' if mode == 'ON' else 'abnormal_off.csv'

    output_folder = os.path.join(output_folder, 'summaries', 'AggregatedSimulationEmissions')
    os.makedirs(output_folder, exist_ok=True)
    output_path = os.path.join(output_folder + '', f'aggregated_sim_emissions_by_METype_{suffix}')

    summary_df.to_csv(output_path, index=False)

    logger.info(f"\nSaved METype emissions summary for ABNORMAL = {mode} to:")
    logger.info(output_path)


def run_emissions_summary_pipeline_for_metype(folder, abnormal):
    """Runs the emissions summary for both ABNORMAL ON and OFF modes."""
    df_all = fillEmptyDataWithZero(df=list_all_metype_files(folder), emissionCol='emissions_USTonsPerYear')
    all_mcRuns = sorted(df_all['mcRun'].unique())
    all_species = df_all['species'].unique()

    summarize_metype_emissions_by_mode(abnormal, df_all, all_mcRuns, all_species, folder)

def getAverageEventCountPerMcRun(df: pd.DataFrame, unitID_name: str, model_name: str, species_name: str) -> float:
    """
    Computes the average number of emission events per Monte Carlo (MC) run
    for a given modelReadableName, species, and unitID.
    """
    # Filter the DataFrame for the specified emission type, species, and unitID
    df_filtered = df[
        (df["modelReadableName"] == model_name) &
        (df["species"] == species_name) &
        (df["unitID"] == unitID_name)
        ]

    # Get the total number of MC runs in the dataset (max mcRun + 1)
    total_mcRuns = int(df["mcRun"].max()) + 1  # Ensures all mcRuns are accounted for

    # Count occurrences per mcRun
    count_per_mcRun = df_filtered.groupby("mcRun").size()

    # Create a Series covering all mcRuns (0 to max mcRun), defaulting to 0
    all_mcRuns = pd.Series(0, index=range(total_mcRuns))

    # Merge actual counts, filling missing MC runs with zeroes
    count_per_mcRun = all_mcRuns.add(count_per_mcRun, fill_value=0)

    return count_per_mcRun.mean(), total_mcRuns


def getAverageRateAndDuration(df: pd.DataFrame, unitID_name: str, model_name: str, species_name: str):
    """
    Computes the average emission rate (kg/h) and average duration (s)
    for a given modelReadableName, species, and unitID.
    """
    # Filter the DataFrame for the specified emission type, species, and unitID
    df_filtered = df[
        (df["modelReadableName"] == model_name) &
        (df["species"] == species_name) &
        (df["unitID"] == unitID_name)
        ]

    # If no matching records exist, return 0 for both values
    if df_filtered.empty:
        return 0.0, 0.0

    return df_filtered["emissions_kgPerH"].mean(), df_filtered["duration_s"].mean()


def createSummaryTable(df, species):
    """
    Creates a summary table (DataFrame) that contains, for each unique
    combination of unitID and modelReadableName, the average event count,
    average emission rate, and average emission duration.
    """
    # Get unique combinations of unitID and modelReadableName
    unique_combinations = df[['unitID', 'modelReadableName']].drop_duplicates()
    results = []

    # Loop over each combination and compute the metrics using the functions above
    for _, row in unique_combinations.iterrows():
        unitID = row['unitID']
        model = row['modelReadableName']

        avg_event_count, _ = getAverageEventCountPerMcRun(df, unitID, model, species)
        avg_rate, avg_duration = getAverageRateAndDuration(df, unitID, model, species)

        results.append({
            'unitID': unitID,
            'modelReadableName': model,
            'species': species,
            'avg_event_count': avg_event_count,
            'avg_emission_rate (kg/h)': avg_rate,
            'avg_emission_duration (s)': avg_duration
        })

    summary_df = pd.DataFrame(results)
    return summary_df

def calcInstEmissModelReadableName(df):
    df_grouped = df.groupby(["METype", "unitID", "modelReadableName", "species"], as_index=False)[
        "emissions_kgPerH"].mean()
    df_grouped.rename(columns={"emissions_kgPerH": "mean_emissions"}, inplace=True)


    # Compute the 95% confidence interval for each group (unitID, modelReadableName, species)
    ci = 95
    alpha = 100 - ci
    ci_lower = df.groupby(["unitID", "modelReadableName", "species"])["emissions_kgPerH"].apply(
        lambda x: np.percentile(x, alpha / 2))
    ci_upper = df.groupby(["unitID", "modelReadableName", "species"])["emissions_kgPerH"].apply(
        lambda x: np.percentile(x, 100 - alpha / 2))

    # Merge CI back into grouped df
    df_grouped = df_grouped.merge(ci_lower.rename(f"{ci}%_ci_lower"), on=["unitID", "modelReadableName", "species"],
                                  how="left")
    df_grouped = df_grouped.merge(ci_upper.rename(f"{ci}%_ci_upper"), on=["unitID", "modelReadableName", "species"],
                                  how="left")
    df_grouped["unit"] = "kg/hour"
    df_grouped = df_grouped.sort_values(
        by=["species", "METype", "unitID"],
        ascending=[False, True, True]
    ).reset_index(drop=True)

    return df_grouped


def calcMdReadbleNameEmissionsSummary(emissionsDf, species):

    emissions_colmn="emissions_MetricTonsPerYear"
    emissionsDf = emissionsDf.assign(emissions_MetricTonsPerYear= emissionsDf['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON)

    ci = float(95)
    mean_header= "mean_emissions"
    ci_lower_header = f"{int(ci)}%_ci_lower"
    ci_upper_header = f"{int(ci)}%_ci_upper"
    alpha = 100 - ci
    emissionsDf = emissionsDf[emissionsDf['species'] == species]

    mcNameDf = emissionsDf.groupby(["mcRun","METype", "unitID", "modelReadableName"], as_index=False)[emissions_colmn].sum()

    # Store list of emissions per MC run
    mc_emission_lists = (
        mcNameDf.groupby(["METype", "unitID", "modelReadableName"])[emissions_colmn]
        .apply(list)
        .rename("MCRuns_emission_list")
        .reset_index()
    )

    mdNameDf = mcNameDf.groupby(["METype","unitID", "modelReadableName"], as_index=False)[emissions_colmn].mean()

    mdNameDf.rename(columns={emissions_colmn: mean_header}, inplace=True)

    ci_lower = mcNameDf.groupby(["METype", "unitID", "modelReadableName"])[emissions_colmn].apply(lambda x: np.percentile(x, alpha / 2))
    ci_upper = mcNameDf.groupby(["METype", "unitID", "modelReadableName"])[emissions_colmn].apply(lambda x: np.percentile(x, (100 - alpha / 2)))
    mdNameDf = mdNameDf.merge(ci_lower.rename(ci_lower_header), on=["METype", "unitID", "modelReadableName"], how="left")
    mdNameDf = mdNameDf.merge(ci_upper.rename(ci_upper_header), on=["METype", "unitID", "modelReadableName"], how="left")

    # Merge MCRuns_emission_list into main df
    mdNameDf = mdNameDf.merge(mc_emission_lists, on=["METype", "unitID", "modelReadableName"], how="left")

    unitIDDF = mdNameDf.groupby(["METype", "unitID"], as_index=False)[[mean_header,ci_lower_header,ci_upper_header]].sum()
    unitIDDF["modelReadableName"] = "summed_modelReadableName"

    # Sum emission lists per METype+unitID
    unit_lists = (
        mdNameDf.groupby(["METype", "unitID"])["MCRuns_emission_list"]
        .apply(lambda lists: list(np.sum(lists, axis=0)))
        .reset_index()
    )
    unitIDDF = unitIDDF.merge(unit_lists, on=["METype", "unitID"], how="left")

    uniDF = emissionsDf.groupby(["mcRun","METype"], as_index=False)[emissions_colmn].sum()
    uni_ci_lower = uniDF.groupby(["METype"])[emissions_colmn].apply(lambda x: np.percentile(x, alpha / 2))
    uni_ci_upper = uniDF.groupby(["METype"])[emissions_colmn].apply(lambda x: np.percentile(x, (100 - alpha / 2)))

    meTypeDf = unitIDDF.groupby(["METype","modelReadableName"], as_index=False)[mean_header].sum()
    meTypeDf["unitID"] = "summed_unitID"
    meTypeDf = meTypeDf.merge(uni_ci_lower.rename(ci_lower_header), on=["METype"], how="left")
    meTypeDf = meTypeDf.merge(uni_ci_upper.rename(ci_upper_header), on=["METype"], how="left")

    # Sum emission lists across METype level
    meType_lists = (
        unitIDDF.groupby("METype")["MCRuns_emission_list"]
        .apply(lambda lists: list(np.sum(lists, axis=0)))
        .reset_index()
    )
    meTypeDf = meTypeDf.merge(meType_lists, on="METype", how="left")

    final_df = pd.concat([mdNameDf,unitIDDF,meTypeDf], ignore_index=True)

    total = meTypeDf.sum(numeric_only=True, axis=0)
    total["METype"] = "summed_METype"
    total["unitID"] = "summed_unitID"
    total["modelReadableName"] = "summed_modelReadableName"

    # Sum the full emission list element-wise
    total["MCRuns_emission_list"] = list(np.sum(meTypeDf["MCRuns_emission_list"].dropna(), axis=0))

    final_df = pd.concat([final_df, total.to_frame().T], ignore_index=True)
    final_df["species"] = species
    final_df["unit"] = "mt/year"
    final_df = final_df.drop(final_df[(final_df[ci_lower_header] ==0) & (final_df[ci_upper_header] ==0) & (final_df[mean_header] ==0)].index)
    return final_df.sort_values(["METype"])


def calcSiteLevelSummary(emissCatDF, species, confidence_level=95):

    emissionsColumn = "emissions_MetricTonsPerYear"
    emissCatDF = emissCatDF.assign(emissions_MetricTonsPerYear= emissCatDF['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON)

    alpha = 100 - float(confidence_level)

    ci_lower_col = f"{confidence_level}%_ci_lower"
    ci_upper_col = f"{confidence_level}%_ci_upper"

    emissCatDF = emissCatDF[emissCatDF.species == species]

    # Group by modelEmissionCategory and mcRun to prepare for list aggregation
    mcCat = emissCatDF.groupby(["mcRun", "modelEmissionCategory"], as_index=False)[emissionsColumn].sum()

    # Get list of emissions per MC run for each category
    mc_emission_lists = (
        mcCat.groupby("modelEmissionCategory")[emissionsColumn]
        .apply(list)
        .rename("MCRuns_emission_list")
        .reset_index()
    )

    mdCat = emissCatDF.groupby(["modelEmissionCategory"], as_index=False)[emissionsColumn].mean()

    min = emissCatDF.groupby(["modelEmissionCategory"])[emissionsColumn].min()
    max = emissCatDF.groupby(["modelEmissionCategory"])[emissionsColumn].max()

    lower = emissCatDF.groupby(["modelEmissionCategory"])[emissionsColumn].apply(lambda x: np.percentile(x, 25))
    upper = emissCatDF.groupby(["modelEmissionCategory"])[emissionsColumn].apply(lambda x: np.percentile(x, 75))

    ci_lower = emissCatDF.groupby(["modelEmissionCategory"])[emissionsColumn].apply(lambda x: np.percentile(x, alpha / 2))
    ci_upper = emissCatDF.groupby(["modelEmissionCategory"])[emissionsColumn].apply(lambda x: np.percentile(x, (100 - alpha / 2)))

    mdCat = mdCat.merge(min.rename("min"), on=["modelEmissionCategory"], how="left")
    mdCat = mdCat.merge(max.rename("max"), on=["modelEmissionCategory"], how="left")

    mdCat = mdCat.merge(lower.rename("lower"), on=["modelEmissionCategory"], how="left")
    mdCat = mdCat.merge(upper.rename("upper"), on=["modelEmissionCategory"], how="left")

    mdCat = mdCat.merge(ci_lower.rename(ci_lower_col), on=["modelEmissionCategory"], how="left")
    mdCat = mdCat.merge(ci_upper.rename(ci_upper_col), on=["modelEmissionCategory"], how="left")

    # Merge emission list into final dataframe
    mdCat = mdCat.merge(mc_emission_lists, on="modelEmissionCategory", how="left")

    mdCat.rename(columns={emissionsColumn:'mean_emissions'}, inplace=True)
    mdCat["species"] = species
    mdCat["unit"] = "mt/year"

    mdCat = mdCat.drop(mdCat[(mdCat["mean_emissions"] ==0 ) & (mdCat["max"] == 0)].index)
    return mdCat

def calcAnnualEmissSummaryByMEType(emissEquipDF, species, confidence_level=95):

    emissionsColumn = "emissions_MetricTonsPerYear"
    emissEquipDF = emissEquipDF.assign(emissions_MetricTonsPerYear= emissEquipDF['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON)

    emissEquipDF = emissEquipDF[emissEquipDF["species"] == species]
    alpha = 100 - float(confidence_level)

    mcEq = emissEquipDF.groupby(["mcRun", "METype"], as_index=False)[emissionsColumn].sum()

    # Store list of emissions per METype across all mcRuns
    mc_emission_lists = (
        mcEq.groupby("METype")[emissionsColumn]
        .apply(list)
        .rename("MCRuns_emission_list")
        .reset_index()
    )

    medf = mcEq.groupby("METype", as_index=False)[emissionsColumn].mean()

    min = mcEq.groupby("METype")[emissionsColumn].min()
    max = mcEq.groupby("METype")[emissionsColumn].max()

    lower = mcEq.groupby("METype")[emissionsColumn].apply(lambda x: np.percentile(x, 25))
    upper = mcEq.groupby("METype")[emissionsColumn].apply(lambda x: np.percentile(x, 75))

    ci_lower = mcEq.groupby("METype")[emissionsColumn].apply(lambda x: np.percentile(x, alpha / 2))
    ci_upper = mcEq.groupby("METype")[emissionsColumn].apply(lambda x: np.percentile(x, (100 - alpha / 2)))

    medf = medf.merge(min.rename("min"), on=["METype"], how="left")
    medf = medf.merge(max.rename("max"), on=["METype"], how="left")
    medf = medf.merge(lower.rename("lower"), on=["METype"], how="left")
    medf = medf.merge(upper.rename("upper"), on=["METype"], how="left")
    medf = medf.merge(ci_lower.rename(f"{confidence_level}%_ci_lower"), on=["METype"], how="left")
    medf = medf.merge(ci_upper.rename(f"{confidence_level}%_ci_upper"), on=["METype"], how="left")

    # Merge list of MCrun emissions per METype
    medf = medf.merge(mc_emission_lists, on="METype", how="left")

    total = medf.sum(numeric_only=True, axis=0)
    total["METype"] = "summed_METype"

    # Sum MCrun_emission_list element-wise for the summed row
    total["MCRuns_emission_list"] = np.sum(medf["MCRuns_emission_list"].dropna(), axis=0)

    total = pd.concat([medf, total.to_frame().T], ignore_index=True)
    total.rename(columns={emissionsColumn:'mean_emissions'}, inplace=True)

    total["species"] = species
    total["unit"] = "mt/year"

    return total

def calcVirtualPneumaticMetypeSummaries(df):
    pneumaticDF = df[df['modelReadableName'].str.contains('Pneumatic')]
    nonPneumaticDF = df[~df['modelReadableName'].str.contains('Pneumatic')]
    pneumaticDF['METype'] = 'Pneumatics'
    combined_df = pd.concat([pneumaticDF, nonPneumaticDF], ignore_index=True)
    summarydf = calcAnnualEmissSummaryByMEType(combined_df, species="METHANE", confidence_level=95)
    summarydf = pd.concat([summarydf, calcAnnualEmissSummaryByMEType(combined_df, species='ETHANE', confidence_level=95)])
    summarydf = summarydf.drop(summarydf[(summarydf["mean_emissions"] ==0 ) & (summarydf["max"] == 0)].index)
    return summarydf

def dumpEmissions(summaryDF, config, summaryType, facID=None, abnormal=None):
    abnormal = abnormal.lower()

    match summaryType:
        case "facility":
            extension = f"annualEmissions_by_site_abnormal_{abnormal}"

        case "equipment":
            extension = f"annualEmissions_by_METype_abnormal_{abnormal}"

        case "pneumatics":
            extension = f"annualEmissions_by_METype_abnormal_{abnormal}"

        case "unit_level":
            extension = f"_abnormal_{abnormal}"

        case "equip_group_level":
            extension = f"_abnormal_{abnormal}"

        case "pdf_site_aggregate":
            extension = f"PDF_for_site_abnormal_{abnormal}"

        case "annual_mdReadbleName_emissions":
            extension = f"annualEmissions_by_modelReadableName_abnormal_{abnormal}"

        case "instantEmissions_emissions_summary":
            extension = f"instantEmissions_by_modelReadableName_abnormal_{abnormal}"

        case "avgERandDur":
            extension = f"avg_ER_and_duration_by_modelReadableName_abnormal_{abnormal}"

        case _:
            extension = None

    if facID is None:
        facID = summaryDF['facilityID'].unique().tolist()[0]
    # todo: would it be better to put all the facility summaries into a single .csv file?
    outFile = au.expandFilename(config['siteEmissions'], {**config, 'facilityID': 'summaries/' + facID + extension})
    summaryDF.to_csv(outFile, index=False)
    logger.info(f"Wrote {outFile}")

    return None

def aggrSet(input_df, value_column, group_options=None):
    """Aggregates a DataFrame by specified options, creating Timeseries objects."""
    timeseries_set = []
    if group_options:
        input_df = input_df[input_df[group_options[0]] == group_options[1]]
    grouping_cols = ['facilityID', 'METype'] if value_column == "state" else ['facilityID', 'unitID', 'emitterID']
    TimeseriesClass = ts.TimeseriesCategorical if value_column == "state" else ts.TimeseriesRLE
    if input_df.empty:
        logger.info(f"Where {group_options[0]} = {group_options[1]}, no timeseries data were found")
        pass
    for _, subset_df in input_df.groupby(grouping_cols):
        timeseries_set.append(TimeseriesClass(subset_df, valueColName=value_column))
    return timeseries_set

def readParquetFiles(config, site, abnormal, mergeGC, additionalEventFilters):
    siteEVDF = Pl.readParquetEvents(config, site=site, mergeGC=mergeGC, species="METHANE", additionalEventFilters=additionalEventFilters)
    siteEVDF = siteEVDF[siteEVDF["nextTS"] - siteEVDF["timestamp"] == siteEVDF["duration"]]
    siteEVDF = siteEVDF[siteEVDF['duration'] >= 0]
    siteEndSimDF = Pl.readParquetSummary(config, site=site)

    if abnormal == "OFF":
        valid_emitter_ids = siteEVDF[siteEVDF['modelEmissionCategory'] != 'FUGITIVE']['emitterID']
        siteEVDF = siteEVDF[siteEVDF['emitterID'].isin(valid_emitter_ids)]

    return siteEVDF, siteEndSimDF

def grouping(dfToGroup, siteEndSimDF, valueColName, groupOptions=None):
    AllMcRuns = {}
    for mcRun, mcRunDF in dfToGroup.groupby('mcRun'):
        EndSimDF = siteEndSimDF[siteEndSimDF['mcRun'] == mcRun]
        simDuration = EndSimDF.loc[EndSimDF['command'] == 'SIM-STOP', 'timestamp'].values[0]
        totalTimeseriesSet = ts.TimeseriesSet(aggrSet(input_df=mcRunDF.sort_values(by=['nextTS'], ascending=[True]), value_column=valueColName, group_options=groupOptions))

        if valueColName == "emission":
            tdf = totalTimeseriesSet.sum(filterZeros=False)
            tdf.df = tdf.df[tdf.df['nextTS'] <= simDuration]
            tdf.df.loc[:, 'tsValue'] = tdf.df['tsValue'] * SECONDSINHOUR
            AllMcRuns[mcRun] = tdf
        else:
            for tscat in totalTimeseriesSet.tsSetList:
                tscat.df = tscat.df[tscat.df["nextTS"] <= simDuration]

            AllMcRuns[mcRun] = totalTimeseriesSet.tsSetList

    return AllMcRuns

def calculateMeanEmissions(time_series_list, min_timestamp):
    """Calculates mean emissions for all MC runs or a specified MC run."""
    max_timestamp = max(td.df['timestamp'].max() for td in time_series_list)
    total_seconds = int((max_timestamp - min_timestamp) / SECONDSINHOUR) + 1

    emission_sum = np.zeros(total_seconds)
    emission_count = np.zeros(total_seconds)

    for tf in time_series_list:
        for i, row in tf.df.iterrows():
            start = int((row['timestamp'] - min_timestamp) / SECONDSINHOUR)
            end = int((tf.df.iloc[i + 1]['timestamp'] - min_timestamp) / SECONDSINHOUR) if i + 1 < len(tf.df) else total_seconds
            emission_sum[start:end] += row['tsValue']
            emission_count[start:end] += 1

    return emission_sum / np.where(emission_count == 0, 1, emission_count)

def plotMeanEmissions(ax, mean_emissions, fac, abnormal):
    """Plots the mean emissions on the provided axis."""
    time_range = np.arange(len(mean_emissions)) * SECONDSINHOUR / SECONDSINDAY
    ax.plot(time_range, mean_emissions, color='black', linewidth=2, label='Mean Emissions')
    ax.set_xlabel('Time (days)', fontsize=14)
    ax.set_ylabel('CH4 Emissions (kg/h)', fontsize=14)
    ax.set_title(f'Mean Emissions - Facility: {fac} \n Abnormal: {abnormal}', fontsize=14)
    ax.legend(fontsize=14)
    ax.grid(alpha=0.3)

def calcProbabilitiesAllMCs(tss):
    ts_df = pd.concat([t.df for t in tss.values()], ignore_index=True)
    combined_ts = ts.TimeseriesRLE(ts_df.sort_values(by=['nextTS'], ascending=[True]), filterZeros=False)
    pdf = combined_ts.toPDF()
    return pdf.data.rename(columns={"value": "tsValue"})

def plotTs(allTSs, site, pdf, abnormal, config):
    """Plots emissions time series for all MC runs and mean emissions."""
    fig, ax = plt.subplots(1, 2, gridspec_kw={'width_ratios': [2, 1]})

    tsf = [t.toFullTimeseries() for t in allTSs.values()]
    min_timestamp = min(df['timestamp'].min() for df in [ts.df for ts in tsf])
    mean_emissions = calculateMeanEmissions(tsf, min_timestamp)

    # Plot individual time series
    for df in [t.df for t in tsf]:
        ax[0].plot((df['timestamp'] - min_timestamp) / SECONDSINDAY, df['tsValue'], alpha=0.2, color='royalblue')

    # Plot Mean Emissions on the first axis
    plotMeanEmissions(ax[0], mean_emissions, site, abnormal)

    ax[1].hist(pdf["tsValue"], density=1, orientation='horizontal', color='royalblue')
    ax[1].set_xlabel('Probability', fontsize=14)
    ax[1].set_ylabel('CH4 kg/h', fontsize=14)
    # ax[1].set_title(f'Facility: {fac} Aggregated CH4 Emissions Time Series', fontsize=14)
    ax[1].grid(alpha=0.3)

    if site:
        plot_dir = os.path.join(config['simulationRoot'], f"summaries/TimeSeriesPlots/site={site}")
    else:
        plot_dir = os.path.join(config['simulationRoot'], "summaries/TimeSeriesPlots")

    os.makedirs(plot_dir, exist_ok=True)
    output_image_path = os.path.join(plot_dir, f"CH4_Emissions_Time_Series_abnormal_{abnormal}.png")
    plt.tight_layout()
    plt.savefig(output_image_path)
    plt.close()

    return

def plotStateTS(config, AllMCruns_states, abnormal, siteEVDF, siteEndSimDF,site=None):
    """Plots one figure per unitID in each state transition: top = time series, bottom = unitID's state transitions."""

    mcRunSelected = config['mcRunTS']
    mcRunSelected = int(mcRunSelected) if mcRunSelected else 0

    if mcRunSelected not in AllMCruns_states:
        logger.info(f"MC Run {mcRunSelected} not found in AllMCruns_states")
        return

    allStateTS = AllMCruns_states[mcRunSelected]
    fac = config['site']
    for state_ts in allStateTS:
        meType = state_ts.df["METype"].unique()[0]

        for unitid, unitidDF in state_ts.df.groupby("unitID"):
            AllMCruns = grouping(dfToGroup=siteEVDF, siteEndSimDF=siteEndSimDF, valueColName="emission", groupOptions=("unitID",unitid))

            if mcRunSelected not in AllMCruns:
                logger.info(f"MC Run {mcRunSelected} not found in AllMCruns")
                return

            tsf = AllMCruns[mcRunSelected]
            tsf = tsf.toFullTimeseries()

            if tsf.df.empty:
                # logger.info(f"no emissions for {unitid} on stat plots, abnormal = {abnormal}, mcrun = {mcRunTS}")
                continue
            _, axes = plt.subplots(2, 1, figsize=(15, 10))
            ts_ax, state_ax = axes

            start_time = tsf.df["timestamp"].min() / SECONDSINDAY
            end_time = tsf.df["timestamp"].max() / SECONDSINDAY

            ts_ax.set_xlim(left=start_time, right=end_time)
            ts_ax.plot((tsf.df['timestamp'] - tsf.df['timestamp'].min()) / SECONDSINDAY, tsf.df['tsValue'], alpha=0.2, color='royalblue')
            ts_ax.grid(True, alpha=0.3)
            ts_ax.set_xlabel('Time (days)', fontsize=14)
            ts_ax.set_ylabel('CH4 Emissions (kg/h)', fontsize=14)
            ts_ax.set_title(f"Time Series with for {unitid} \n mcRun = {mcRunSelected}", fontsize=14)

            # Plot state transitions for this unitID only
            unitts = ts.TimeseriesCategorical(unitidDF, valueColName="state").toFullTimeseries().df
            state_ax.step(unitts["timestamp"] / SECONDSINDAY, unitts["tsValue"], label=unitid)
            state_ax.set_xlim(left=start_time, right=end_time)
            state_ax.set_xlabel('Time (days)', fontsize=12)
            state_ax.set_ylabel('State', fontsize=12)
            state_ax.set_title(f'State Transitions for unitID: {unitid}\nMEType: {meType}, MCrun: {mcRunSelected}, Site: {fac}', fontsize=14)
            state_ax.legend()
            state_ax.grid(alpha=0.3)

            # Output directory and file path
            if site:
                plot_dir = os.path.join(config['simulationRoot'], f"summaries/StatesPlots/site={site}")
            else:
                plot_dir = os.path.join(config['simulationRoot'], "summaries/StatesPlots")

            os.makedirs(plot_dir, exist_ok=True)
            output_image_path = os.path.join(
                plot_dir,
                f"state_transition_mcRun={mcRunSelected}_unitID={unitid}_abnormal_{abnormal}.png"
            )

            plt.tight_layout(pad=3.0)
            plt.savefig(output_image_path)
            plt.close()

    return

def generatePDFs(config, df, abnormal, site):
    df = df[df['modelReadableName'] != 'Blowdown Event']    # exclude maintenance emissions
    df = df[df['species'] == 'METHANE']

    siteEmissions = config['siteEmiss']
    meType = config['METype']
    unitID = config['unitID']
    miiEmiss = config['miiEmiss']

    all_false = all(not x for x in [siteEmissions, meType, unitID, miiEmiss])

    if all_false or miiEmiss:
        siteEmissions = meType = unitID = miiEmiss = True

    siteEndSimDF = Pl.readParquetSummary(config, site=site)

    if siteEmissions:
        allMCruns = grouping(dfToGroup=df, siteEndSimDF=siteEndSimDF, valueColName="emission")
        pdf = calcProbabilitiesAllMCs(allMCruns)
        pdf['CH4_EmissionRate_kg/h'] = pdf['tsValue']
        pdf.drop(columns=['tsValue', 'count'], inplace=True)
        if not pdf.empty:
            dumpEmissions(pdf, config, "pdf_site_aggregate", facID=f"PDFs/site={site}/", abnormal=abnormal)

    if meType:
        for siMeType, meTyDF in df.groupby('METype'):
            meTypeAllMCruns = grouping(dfToGroup=meTyDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
            meTypepdf = calcProbabilitiesAllMCs(meTypeAllMCruns)
            meTypepdf['CH4_EmissionRate_kg/h'] = meTypepdf['tsValue']
            meTypepdf.drop(columns=['tsValue', 'count'], inplace=True)
            if meTypepdf.empty:
                continue
            dumpEmissions(meTypepdf, config, "equip_group_level", facID=f"PDFs/site={site}/PDF_for_all_{siMeType}", abnormal=abnormal)


    if unitID:
        for unitID, unitIDDF in df.groupby('unitID'):
            unitAllMCruns = grouping(dfToGroup=unitIDDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
            unitPDF = calcProbabilitiesAllMCs(unitAllMCruns)
            unitPDF['CH4_EmissionRate_kg/h'] = unitPDF['tsValue']
            unitPDF.drop(columns=['tsValue', 'count'], inplace=True)
            if unitPDF.empty:
                continue
            dumpEmissions(unitPDF, config, "unit_level", facID=f"PDFs/site={site}/PDF_for_{unitID}", abnormal=abnormal)

    if miiEmiss:
        process_all_sites_mii(base_folder=f"{config['simulationRoot']}/summaries")


def allModelReadableNamesDict():
    result_dict = {}
    folder_path = "./input/ModelFormulation"
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path) and file_name.endswith(".json"):
            with open(file_path, 'r') as file:
                data = json.load(file)

            # Extracting the "Value" for "Compressors" from "Model Parameters"
            compressor_value = None
            for param in data.get("Model Parameters", []):
                if param.get("Python Parameter") == "modelCategory" and param.get("Value"):
                    compressor_value = param["Value"]
                    break

            # Extracting "modelReadableName" and "modelEmissionCategory" values from "Emitters"
            emitters = [
                {"modelReadableName": emitter["Readable Name"], "modelEmissionCategory": emitter["Emission Category"]}
                for emitter in data.get("Emitters", [])
            ]

            if compressor_value:
                if compressor_value in result_dict:
                    result_dict[compressor_value].extend(emitters)
                else:
                    result_dict[compressor_value] = emitters

    # Remove duplicate values for each key
    for key in result_dict:
        unique_emitters = []
        seen_emitters = set()
        for emitter in result_dict[key]:
            emitter_tuple = (emitter["modelReadableName"], emitter["modelEmissionCategory"])
            if emitter_tuple not in seen_emitters:
                seen_emitters.add(emitter_tuple)
                unique_emitters.append(emitter)
        result_dict[key] = unique_emitters

    # Remove keys with empty lists
    result_dict = {key: value for key, value in result_dict.items() if value}

    return result_dict


def fillEmptyDataWithZero(df,emissionCol):
    me_df = df[df['METype'].notnull() & (df['METype'] != "")]
    unit_info = {r['unitID']: {'METype': r['METype'], 'emitterID': r['emitterID']}
                 for _, r in me_df.iterrows()}
    model_dict = allModelReadableNamesDict()
    overall_species = list(df['species'].unique())
    mcRuns, unitIDs = df['mcRun'].unique(), set(unit_info.keys())
    missing = []
    facID = df['facilityID'].unique()[0]
    site = df['site'].unique()[0]
    

    for mc in mcRuns:
        for uid in unitIDs:
            METype, emitterID = unit_info[uid]['METype'], unit_info[uid]['emitterID']
            group = df[(df['mcRun'] == mc) & (df['unitID'] == uid)]
            # group = df[(df['mcRun'] == mc) & (df['unitID'] == uid)]
            if METype not in model_dict:
                # Add missing species rows for units without a defined model dictionary.
                pres_species = set(group['species'].unique())
                for sp in set(overall_species) - pres_species:
                    missing.append({'mcRun': mc, 'unitID': uid, 'METype': METype, 'species': sp,
                                    'modelReadableName': None, 'modelEmissionCategory': None,
                                    'emitterID': emitterID, emissionCol: 0, 'facilityID': facID, 'site': site})
            else:
                # For units with a model dictionary, for each species add missing model events.
                for sp in overall_species:
                    pres_models = set(group[group['species'] == sp]['modelReadableName'].dropna().unique())
                    for m in model_dict[METype]:
                        if m['modelReadableName'] not in pres_models:
                            missing.append({'mcRun': mc, 'unitID': uid, 'METype': METype, 'species': sp,
                                            'modelReadableName': m['modelReadableName'],
                                            'modelEmissionCategory': m['modelEmissionCategory'],
                                            'emitterID': emitterID, emissionCol: 0, 'facilityID': facID, 'site': site})
    df_missing = pd.DataFrame(missing)
    df_complete = pd.concat([df, df_missing], ignore_index=True)
    df_complete[emissionCol] = df_complete[emissionCol].fillna(0)
    # df.to_csv('C:\METEC\MAES_development\output\TEST\df.csv')
    # df_missing.to_csv('C:\METEC\MAES_development\output\TEST\df_missing.csv')
    # df_complete.to_csv('C:\METEC\MAES_development\output\TEST\df_complete.csv')
    return df_complete

def generatedCsvSummaries(config, df, site, abnormal):
     # Get DFs for emissions for the summaries
    zerosDF = fillEmptyDataWithZero(df, emissionCol="emissions_USTonsPerYear")
    emissCatDF = Pl.processEmissionsCat(zerosDF)
    emissInstEquipDF = Pl.processInstantEquipEmissions(df)

    annualSummaries = config['annualSummaries']
    instantaneousSummaries = config['instantaneousSummaries']
    pdfSummaries = config['pdfSummaries']
    avgDurSummaries = config['avgDurSummaries']
    statesAndTsPloting = config['statesAndTsPloting']
    simulationEmissions = config['simulationEmissions']
    gen_plots = config['plot']

    if config['fullSummaries']:
        annualSummaries = instantaneousSummaries = pdfSummaries = avgDurSummaries = simulationEmissions = True

    if annualSummaries:
        siteEmissions = config['siteEmiss']
        meType = config['METype']
        unitID = config['unitID']
        pneumatics = config['Pneumatics']

        all_false = all(not x for x in [siteEmissions, meType, unitID, pneumatics])
        if all_false:
            siteEmissions = meType = unitID = pneumatics = True

        if unitID:
            detailed_emissionsDF = calcMdReadbleNameEmissionsSummary(zerosDF, species="METHANE")
            detailed_emissionsDF = pd.concat([detailed_emissionsDF, calcMdReadbleNameEmissionsSummary(zerosDF, species="ETHANE")])
            dumpEmissions(detailed_emissionsDF, config, "annual_mdReadbleName_emissions", facID=f"AnnualEmissions/site={site}/", abnormal=abnormal)

        if siteEmissions:
            CategorySummaryDF = calcSiteLevelSummary(emissCatDF, species='METHANE', confidence_level=95)
            CategorySummaryDF = pd.concat([CategorySummaryDF, calcSiteLevelSummary(emissCatDF, species='ETHANE', confidence_level=95)])  # add ethane summary
            dumpEmissions(CategorySummaryDF, config, "facility", facID=f"AnnualEmissions/site={site}/", abnormal=abnormal)

        if meType:
            equipEmissSummaryDF = calcAnnualEmissSummaryByMEType(zerosDF, species='METHANE', confidence_level=95)
            equipEmissSummaryDF = pd.concat([equipEmissSummaryDF, calcAnnualEmissSummaryByMEType(zerosDF, species='ETHANE', confidence_level=95)])  # add ethane summary
            dumpEmissions(equipEmissSummaryDF, config, "equipment", facID=f"AnnualEmissions/site={site}/", abnormal=abnormal)

        if pneumatics:
            pneumaticSummaryDF = calcVirtualPneumaticMetypeSummaries(df=zerosDF)
            dumpEmissions(pneumaticSummaryDF, config, "pneumatics", facID=f"AnnualEmissions/site={site}/ONGAEIR-GHGRPFormat/", abnormal=abnormal)

    if statesAndTsPloting:
        siteEVDF, siteEndSimDF = readParquetFiles(config=config, site=config['siteName'], abnormal=abnormal, mergeGC=True, additionalEventFilters=[('command', '=', 'EMISSION')])
        AllMCruns = grouping(dfToGroup=siteEVDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
        pdf = calcProbabilitiesAllMCs(AllMCruns)
        plotTs(AllMCruns, config=config, site=site, pdf=pdf, abnormal=abnormal)
        # Get state transitions
        siteEVDF_state, siteEndSimDF_state = readParquetFiles(config=config, site=config['siteName'], abnormal=abnormal, mergeGC=False, additionalEventFilters=[('command', '=', 'STATE_TRANSITION')])
        AllMCruns_states = grouping(dfToGroup=siteEVDF_state, siteEndSimDF=siteEndSimDF_state, valueColName="state")

        # Plot state transitions with mean emissions
        plotStateTS(config, AllMCruns_states, abnormal=abnormal, site=site, siteEVDF=siteEVDF, siteEndSimDF=siteEndSimDF)

    if instantaneousSummaries:
        # Get instantaneous emissions summary by modelReadableName
        instEmissByModelReadName = calcInstEmissModelReadableName(emissInstEquipDF)
        dumpEmissions(instEmissByModelReadName, config, "instantEmissions_emissions_summary", facID=f"InstantaneousEmissions/site={site}/", abnormal=abnormal)

    if pdfSummaries:
        # Get PDF at Site Level for CH4 Emissions
        generatePDFs(config=config, df=df, abnormal=abnormal, site=site)

    if avgDurSummaries:
        avgERandDur = createSummaryTable(emissInstEquipDF, species="METHANE")
        avgERandDur = pd.concat([avgERandDur,createSummaryTable(emissInstEquipDF,species="ETHANE")])
        dumpEmissions(avgERandDur, config, "avgERandDur", facID=f"AvgEmissionRatesAndDurations/site={site}/", abnormal=abnormal)

    # if simulationEmissions:
    #     run_emissions_summary_pipeline_for_modelReadableName_and_unitID(folder=config['simulationRoot'], abnormal=abnormal)
    #     run_total_emissions_pipeline_for_category(folder=config['simulationRoot'], abnormal=abnormal)
    #     run_emissions_summary_pipeline_for_metype(folder=config['simulationRoot'], abnormal=abnormal)
    #     generate_site_level_pdfs(root_dir=config['simulationRoot'], site=site, abnormal=abnormal)

        # ── simulation-level summaries: run only once per run & mode ──
    key = (config['simulationRoot'], abnormal.lower())
    if simulationEmissions and key not in _SIM_AGG_DONE:
        run_emissions_summary_pipeline_for_modelReadableName_and_unitID(
            folder=config['simulationRoot'], abnormal=abnormal)
        run_total_emissions_pipeline_for_category(
            folder=config['simulationRoot'], abnormal=abnormal)
        run_emissions_summary_pipeline_for_metype(
            folder=config['simulationRoot'], abnormal=abnormal)
        generate_site_level_pdfs(
            root_dir=config['simulationRoot'], site=None, abnormal=abnormal)
        _SIM_AGG_DONE.add(key)

    if gen_plots:
        similationLevelSummariesPath = f"{config['simulationRoot']}/summaries/AggregatedSimulationEmissions"
        annualSummariesPath = f"{config['simulationRoot']}/summaries/AnnualEmissions/site={site}"

        # 1) Per-site annual plots (if available)
        if os.path.exists(annualSummariesPath):
            for sp in SPECIES:
                plot_annual_emissions_unitid_level(
                    f"{annualSummariesPath}/annualEmissions_by_modelReadableName_abnormal_{abnormal.lower()}.csv",
                    sp, plot_by="file")
                plot_annual_emissions_for_modelReadableName(
                    f"{annualSummariesPath}/annualEmissions_by_modelReadableName_abnormal_{abnormal.lower()}.csv",
                    sp, plot_by="file")
                plot_annual_emissions_site_level(
                    f"{annualSummariesPath}/annualEmissions_by_site_abnormal_{abnormal.lower()}.csv",
                    sp, plot_by="file")
                plot_annual_emissions_for_metype(
                    f"{annualSummariesPath}/annualEmissions_by_METype_abnormal_{abnormal.lower()}.csv",
                    sp, plot_by="file")
        else:
            logger.warning("AnnualEmissions summaries not found; skipping per-site annual plots.")

        # 2) Aggregated-simulation plots (run once per (simulationRoot, abnormal))
        key = (config['simulationRoot'], abnormal.lower())
        if os.path.exists(similationLevelSummariesPath):
            if key not in _SIM_AGG_DONE:
                for sp in SPECIES:
                    plot_annual_emissions_unitid_level(
                        f"{similationLevelSummariesPath}/aggregated_sim_emissions_by_unitID_abnormal_{abnormal.lower()}.csv",
                        sp, plot_by="file")
                    plot_annual_emissions_for_modelReadableName(
                        f"{similationLevelSummariesPath}/aggregated_sim_emissions_by_modelReadableName_abnormal_{abnormal.lower()}.csv",
                        sp, plot_by="file")
                    plot_annual_emissions_site_level(
                        f"{similationLevelSummariesPath}/aggregated_sim_emissions_by_category_abnormal_{abnormal.lower()}.csv",
                        sp, plot_by="file")
                    plot_annual_emissions_for_metype(
                        f"{similationLevelSummariesPath}/aggregated_sim_emissions_by_METype_abnormal_{abnormal.lower()}.csv",
                        sp, plot_by="file")

                # Make sure the combined PDFs exist so the CDF plot can be generated
                for abn in ["on", "off"]:
                    agg_pdf = f"{similationLevelSummariesPath}/aggregated_sim_PDFs_abnormal_{abn}.csv"
                    if not os.path.exists(agg_pdf):
                        generate_site_level_pdfs(root_dir=config['simulationRoot'], site=None, abnormal=abn)

                generate_comnbined_cdf_plot(config)
                _SIM_AGG_DONE.add(key)
            else:
                logger.info(f"Skipping aggregated-simulation plots — already done for {key}")
        else:
            logger.warning("AggregatedSimulationEmissions summaries not found; skipping aggregated-simulation plots.")

    return None
