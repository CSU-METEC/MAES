import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import re

def list_all_files(folder_path):
    pdfs_path = os.path.join(folder_path, 'PDFs')
    base_depth = pdfs_path.rstrip(os.sep).count(os.sep)
    all_files = []

    for root, dirs, files in os.walk(pdfs_path):
        if root.rstrip(os.sep).count(os.sep) > base_depth + 1:
            dirs.clear()
            continue
        for file in files:
            if file.endswith('.csv'):
                all_files.append(os.path.join(root, file))
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


def correct_plot_string(s):
    match = re.search(r'all_+(\w+)', s, re.IGNORECASE)
    if match:
        item = match.group(1).lower()
        return f"All {item.capitalize()}s Combined"
    return s


def generate_pdf_cdf_plot_with_aerial(file, aerial_dict):
    """
    Generates PDF and CDF plots. For files whose names contain '_site_abnormal_off'
    and for which aa aerial measurement is available (via aerial_dict), overlay the aerial measurement:
      - A solid red vertical line at the aerial measured CH4 emission rate.
      - A red shaded region representing -50% to +100% uncertainty.
      - An annotation showing the probability (from the CDF) that emissions are higher than the aerial measurement.
    For all other files, a vertical red dashed line is drawn at the x value where the CDF reaches 95%.
    """
    # Font customization
    label_fontsize = 16
    tick_fontsize = 16

    print(f"Processing file: {file}")
    df = pd.read_csv(file)

    # Setup output folder: "Plots with Aerial Meas. Overlay" inside the CSV file's folder
    base_dir, csv_filename = os.path.split(file)
    plot_dir = os.path.join(base_dir, "Plots with Aerial Meas. Overlay")
    os.makedirs(plot_dir, exist_ok=True)
    image_filename = os.path.splitext(csv_filename)[0] + ".png"
    unit = image_filename.split("_for_")[1].split("_abnormal")[0]#.replace("_", " ")
    unit = correct_plot_string(unit)
    abnormal = image_filename.split("abnormal", 1)[1].replace("_", "").replace(".png", "").capitalize()

    output_image_path = os.path.join(plot_dir, image_filename)

    # Determine site name from folder name and remove 'site=' if present.
    site = os.path.basename(base_dir).replace("site=", "")
    site_title = site

    # Extract emissions and probabilities
    emissions = df.iloc[:, 1]
    probabilities = df['probability']

    # Check total probability (optional)
    total_prob = probabilities.sum()
    if not np.isclose(total_prob, 1.0, atol=1e-3):
        print(f"Warning: total probability sums to {total_prob:.3f}, not 1.0")

    # Create figure with two subplots: PDF and CDF.
    fig, axs = plt.subplots(2, 1, figsize=(10, 10), sharex=True)

    # PDF subplot
    axs[0].bar(emissions, probabilities, width=0.1, alpha=0.6, edgecolor='black')
    axs[0].set_ylabel('Probability', fontsize=label_fontsize)
    axs[0].set_title('Probability Density Function (PDF)', fontsize=label_fontsize)
    axs[0].tick_params(axis='both', labelsize=tick_fontsize)
    axs[0].grid(alpha=0.3)

    # CDF subplot
    cdf_x, cdf_y = compute_cdf(emissions, probabilities)
    axs[1].step(cdf_x, cdf_y, where='post')
    axs[1].set_ylim(0, 1.1)
    axs[1].set_xlabel('CH4 Emission Rate (kg/h)', fontsize=label_fontsize)
    axs[1].set_ylabel('Cumulative Probability', fontsize=label_fontsize)
    axs[1].set_title('Cumulative Distribution Function (CDF)', fontsize=label_fontsize)
    axs[1].tick_params(axis='both', labelsize=tick_fontsize)
    axs[1].grid(alpha=0.3)

    # Determine if we should overlay aerial measurement.
    filename = os.path.basename(file)
    use_aerial = False
    aerial_value = None
    if ("_site_abnormal_off" in csv_filename) and (aerial_dict is not None):
        aerial_value = aerial_dict.get(site, None)
        if aerial_value is not None:
            use_aerial = True

    if use_aerial:
        # Calculate uncertainty bounds: -50% and +100%
        lower_bound = aerial_value * 0.5  # -50%
        upper_bound = aerial_value * 2.0  # +100%

        # Compute the CDF value at the aerial measurement via linear interpolation.
        F_aerial = np.interp(aerial_value, cdf_x, cdf_y, left=0, right=1)
        prob_above = 1 - F_aerial

        # Overlay aerial measurement: solid red line and shaded uncertainty region.
        line_aerial = axs[1].axvline(x=aerial_value, color='darkred', linestyle='-', linewidth=2,
                                      label=f'Aerial Meas.: {aerial_value:.1f} kg/h\n'
                                            f'P(emissions > {aerial_value:.1f}) = {prob_above:.1%}')
        shade = axs[1].axvspan(lower_bound, upper_bound, color='darkred', alpha=0.05,
                               label=f'Uncertainty: [{lower_bound:.1f}, {upper_bound:.1f}] kg/h')

        # Merge legends: combine all handles into a single legend.
        handles, labels = axs[1].get_legend_handles_labels()
        axs[1].legend(handles, labels, loc="lower right", fontsize=tick_fontsize)
    else:
        # Default: plot vertical red dashed line at the point where CDF reaches 95%
        y_threshold = 0.95
        index_at_y = np.argmin(np.abs(cdf_y - y_threshold))
        x_at_95 = cdf_x[index_at_y]
        axs[1].axvline(x=x_at_95, color='red', linestyle='--', alpha=0.3,
                       label=f'95% CDF: ≤ {x_at_95:.1f} kg/h')
        axs[1].legend(loc="lower right", fontsize=tick_fontsize)

    fig.suptitle(f"{site_title}\nPDF & CDF Plots for {unit}\nAbnormal Emissions {abnormal}", fontsize=label_fontsize + 2)
    plt.tight_layout()
    plt.savefig(output_image_path)
    plt.close()


def plot_pdf_cdf(path, plot_by=None, aerial_file="Aerial Measurements.csv"):
    """
    Processes CSV files from either a single file or a folder.
    Loads aerial measurements from the CSV file if available.
    If the aerial file is not provided or a measurement for a site is missing,
    all plots revert to the default 95% threshold vertical red dashed line.
    """
    # Attempt to load aerial measurements.
    try:
        aerial_df = pd.read_csv(aerial_file)
        aerial_dict = dict(zip(aerial_df['Site'], aerial_df['CH4_Aggregated_Emission_Rate']))
    except Exception as e:
        print(f"Error loading Aerial Measurements CSV: {e}")
        aerial_dict = {}

    if plot_by == "folder":
        files = list_all_files(path)
        for file in files:
            generate_pdf_cdf_plot_with_aerial(file, aerial_dict)
    elif plot_by == "file":
        generate_pdf_cdf_plot_with_aerial(path, aerial_dict)
    else:
        print("Missing or invalid 'plot_by' argument.\nPlease specify one of the following options:\n"
              "  • 'file'   → to generate plots for a single file\n"
              "  • 'folder' → to generate plots for all files in a folder")


def main():
    """
    This code generates PDF and CDF plots for either a single Probability Density Function (PDF) summary file
    or PDFs (at unitID, METype, and site level) from multiple sites in a simulation summary folder.
    Sites with a valid aerial measurement available, should be listed in a separate file named "Aerial Measurements.csv"
    with their respective aggregated emission rate. This measurement file must have the columns 'Site' and
    'CH4_Aggregated_Emission_Rate' with the site name and its aggregated measured CH4 emission rate, respectively.
    This code reads this file and overlay the measurements on CDF plots (note: the site name utilized in the MAES
    simulations and in the 'Aerial Measurement.csv' file must match in order for the overlay plots to work).
    If no site measurement is provided, or if the site names don't match, the code will generate
    only PDFs and CDF plots with a vertical red dashed line at the point where the CDF reaches 95%.
    The resulting plots are saved in a folder called 'Plots with Aerial Meas. Overlay'.
    """
    # Define paths (update as needed)
    FILE = r'C:/Users/Arthur_Santos/PycharmProjects/MAES-main/output/Mustang/MC_20250404_102836/' \
           r'summaries/PDFs/Mustang_PDF_for_comp_1_abnormal_off.csv'
    FOLDER = r'C:/Users/Arthur_Santos/PycharmProjects/MAES-main/output/Mustang/MC_20250404_102836/' \
             r'summaries/'

    # Define where the file with Aerial Measurements aggregated by site is located
    AERIAL_MEASUREMENTS = "Aerial Measurements.csv"

    # For processing a single file:
    # plot_pdf_cdf(FILE, plot_by="file", aerial_file=AERIAL_MEASUREMENTS)

    # For processing an entire folder:
    plot_pdf_cdf(FOLDER, plot_by="folder", aerial_file=AERIAL_MEASUREMENTS)


if __name__ == "__main__":
    main()
