import pandas as pd
import matplotlib.pyplot as plt
import os

def list_all_files(folder_path):
    pdfs_path = os.path.join(folder_path, 'AnnualEmissions')
    base_depth = pdfs_path.rstrip(os.sep).count(os.sep)
    all_files = []

    for root, dirs, files in os.walk(pdfs_path):
        if root.rstrip(os.sep).count(os.sep) > base_depth + 1:
            dirs.clear()
            continue
        for file in files:
            if file.endswith('.csv') and 'by_site_abnormal' in file:
                all_files.append(os.path.join(root, file))
    return all_files


def generate_annual_emissions_plot(file, species):
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
        print(file)
    except Exception as e:
        print(f"Error reading {file}: {e}")
        return

    df = df[df['species'] == species]
    if df.empty:
        print(f"No data for species {species} in {file}")
        return

    unit = df['Unit'].values[0]
    stack_categories = [x for x in df['modelEmissionCategory'].unique().tolist() if x != 'TOTAL']

    category_colors = {
        'FUGITIVE': '#0d3b66',
        'VENTED': '#f4d35e',
        'COMBUSTION': '#92140c'
    }

    available_stack = [cat for cat in stack_categories if cat in df['modelEmissionCategory'].values]
    if not available_stack:
        print(f"No stack categories available in {file}. Skipping.")
        return

    df_stack = df[df['modelEmissionCategory'].isin(available_stack)]
    df_stack = df_stack.set_index('modelEmissionCategory').reindex(stack_categories, fill_value=0)
    emissions_values = df_stack['mean_emissions'].values

    df_total = df[df['modelEmissionCategory'] == 'TOTAL']
    if df_total.empty:
        print(f"No TOTAL row found in {file}")
        return
    total_row = df_total.iloc[0]
    total_emissions = total_row['mean_emissions']
    ci_lower = total_row['95%_ci_lower']
    ci_upper = total_row['95%_ci_upper']
    error_low = total_emissions - ci_lower
    error_high = ci_upper - total_emissions

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
    This code generates annual emissions plots at the Site level. Adjust the FILE (if you want to make plots
    for 1 specific site only) or FOLDER path (if you all to generate plots for all sites in that folder),
    and set the desired species ('METHANE' or 'ETHANE') accordingly.
    """

    # Load the uploaded CSV file
    FILE = 'C:/Users/Arthur_Santos/PycharmProjects/MAES-main/output/Mustang/MC_20250404_102836/' \
           'summaries/AnnualEmissions/site=Mustang/mustang_annualEmissions_by_site_abnormal_on.csv'
    FOLDER = 'C:/Users/Arthur_Santos/PycharmProjects/MAES-main/output/Mustang/MC_20250404_102836/' \
           'summaries/'

    SPECIES = 'METHANE'  # or 'ETHANE'

    # plot_annual_emissions(FILE, SPECIES, plot_by="file")
    plot_annual_emissions(FOLDER, SPECIES, plot_by="folder")


if __name__=="__main__":
    main()
