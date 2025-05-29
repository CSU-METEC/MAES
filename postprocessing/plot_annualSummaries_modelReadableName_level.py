import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec


def list_all_files(folder_path):
    pdfs_path = os.path.join(folder_path, 'AnnualEmissions')
    base_depth = pdfs_path.rstrip(os.sep).count(os.sep)
    all_files = []

    for root, dirs, files in os.walk(pdfs_path):
        # Stop recursion beyond immediate subfolders
        if root.rstrip(os.sep).count(os.sep) > base_depth + 1:
            dirs.clear()
            continue
        for file in files:
            if file.endswith('.csv') and 'by_modelReadableName' in file:
                all_files.append(os.path.join(root, file))
    return all_files


def generate_annual_emissions_plot(file, species):
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

    import os
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt


    # Adjustable Font Size Settings
    label_fontsize = 20  # For title, y-label, legend
    tick_fontsize = 20  # For x-ticks and y-ticks

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

    # Exclude rows where modelReadableName contains 'summed' anywhere
    df = df[~df['modelReadableName'].str.contains('summed', case=False, na=False)]
    if df.empty:
        print(f"No valid rows for plotting in {file}")
        return

    # Generate unique labels: "{unitID} - {modelReadableName}"
    df['label'] = df['modelReadableName'].astype(str)

    # Sort for visual consistency
    df = df.sort_values('label')

    labels = df['label'].tolist()
    mean_emissions = df['mean_emissions'].tolist()
    ci_lowers = df['95%_ci_lower'].tolist()
    ci_uppers = df['95%_ci_upper'].tolist()

    err_lower = [mean - low for mean, low in zip(mean_emissions, ci_lowers)]
    err_upper = [up - mean for mean, up in zip(mean_emissions, ci_uppers)]
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

    from matplotlib.patches import Patch
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


def main(file=None, folder=None):
    """
    This code generates annual emissions plots by modelReadableName. Adjust the FILE (if you want to make plots
    for 1 specific site only) or FOLDER path (if you all to generate plots for all sites in that folder),
    and set the desired species ('METHANE' or 'ETHANE') accordingly.
    """
    # FILE = 'C:/Users/Arthur_Santos/PycharmProjects/MAES-main/output/Mustang/MC_20250404_102836/' \
    #        'summaries/AnnualEmissions/site=Mustang/mustang_annualEmissions_by_modelReadableName_abnormal_on.csv'
    # FOLDER = '/home/arthur/MAES/output/Mustang_/MC_20250321_144004/summaries/'
    SPECIES = ['METHANE','ETHANE']
    for sp in SPECIES:
        if file:
            plot_annual_emissions(file, sp, plot_by="file")
        if folder:
            plot_annual_emissions(folder, sp, plot_by="folder")


if __name__ == "__main__":
    main(folder='/home/arthur/MAES/output/Mustang_/MC_20250321_144004/summaries/')

