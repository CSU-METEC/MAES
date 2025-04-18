import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

def list_all_files(folder_path):
    pdfs_path = os.path.join(folder_path, 'AnnualEmissions')
    base_depth = pdfs_path.rstrip(os.sep).count(os.sep)
    all_files = []

    for root, dirs, files in os.walk(pdfs_path):
        if root.rstrip(os.sep).count(os.sep) > base_depth + 1:
            dirs.clear()
            continue
        for file in files:
            if file.endswith('.csv') and 'by_modelReadableName' in file:
                all_files.append(os.path.join(root, file))
    return all_files


def generate_annual_emissions_plot(file, species):
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
        print(file)
    except Exception as e:
        print(f"Error reading {file}: {e}")
        return

    df = df[df['species'] == species]
    if df.empty:
        print(f"No data for species {species} in {file}")
        return

    # Remove rows where mean_emissions is 0
    df = df[df['mean_emissions'] != 0]
    if df.empty:
        print(f"All entries have 0 emissions in {file}")
        return

    # Keep only rows where modelReadableName == 'summed_modelReadableName'
    df = df[df['modelReadableName'] == 'summed_modelReadableName']

    # Exclude rows where modelReadableName contains 'summed' elsewhere
    df = df[df['unitID'] != 'summed_unitID']

    if df.empty:
        print(f"No valid rows for plotting in {file}")
        return

    unit = df['Unit'].values[0]

    df = df.sort_values('unitID')
    unitIDs = df['unitID'].astype(str).tolist()
    mean_emissions = df['mean_emissions'].tolist()
    ci_lowers = df['95%_ci_lower'].tolist()
    ci_uppers = df['95%_ci_upper'].tolist()

    err_lower = [mean - low for mean, low in zip(mean_emissions, ci_lowers)]
    err_upper = [up - mean for mean, up in zip(mean_emissions, ci_uppers)]
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

    plt.tight_layout()
    plt.savefig(output_image_path)
    plt.close()


def plot_annual_emissions(path, species, plot_by=None):
    """
    Generate annual emissions plots for a single file or for all files in a folder,
    filtered by species.

    Parameters:
    - path: path to a file or folder.
    - species: species to filter by ('METHANE' or 'ETHANE').
    - plot_by: 'file' to process a single file, or 'folder' to process all matching CSV files.
    """
    if plot_by == "folder":
        files = list_all_files(path)
        for file in files:
            generate_annual_emissions_plot(file, species)
    elif plot_by == "file":
        generate_annual_emissions_plot(path, species)
    else:
        print("Missing or invalid 'plot_by' argument.\nPlease specify 'file' or 'folder'.")


def main():
    """
    This code generates annual emissions plots by unitID. Adjust the FILE (if you want to make plots
    for 1 specific site only) or FOLDER path (if you all to generate plots for all sites in that folder),
    and set the desired species ('METHANE' or 'ETHANE') accordingly.
    """

    FILE = 'C:/Users/Arthur_Santos/PycharmProjects/MAES-main/output/Mustang/MC_20250415_100248/' \
           'summaries/AnnualEmissions/site=Mustang/mustang_annualEmissions_by_modelReadableName_abnormal_on.csv'
    FOLDER = 'C:/Users/Arthur_Santos/PycharmProjects/MAES-main/output/Mustang/MC_20250415_100248/summaries/'
    SPECIES = 'METHANE'  # or 'ETHANE'

    # plot_annual_emissions(FILE, SPECIES, plot_by="file")
    plot_annual_emissions(FOLDER, SPECIES, plot_by="folder")


if __name__ == "__main__":
    main()
