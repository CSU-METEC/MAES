import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os


def list_all_files_in_folder(folder_path):
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


def process_site(site_folder):
    """
    Processes all CSV files in a given site folder (and its subfolders) that contain '_off.csv',
    computes the emission threshold at 95% cumulative probability for each file,
    and saves the results in a CSV file within a new folder called 'MIIEmissionThresholds' in the site folder.
    """
    print(f"Processing site: {os.path.basename(site_folder)}")
    files = list_all_files_in_folder(site_folder)
    thresholds = {}

    for file in files:
        if '_off.csv' not in file:
            continue
        try:
            df = pd.read_csv(file)
        except Exception as e:
            print(f"Error reading {file}: {e}")
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


def process_all_sites(base_folder):
    """
    Iterates over each site folder within the 'PDFs' folder and processes the MII analysis for each.
    """
    pdfs_dir = os.path.join(base_folder, "PDFs")
    # List all entries in the PDFs folder and process only directories (sites) that are not already an output folder
    for entry in os.listdir(pdfs_dir):
        site_path = os.path.join(pdfs_dir, entry)
        if os.path.isdir(site_path) and entry != "MIIEmissionThresholds":
            process_site(site_path)


def main():
    """
    Main function that:
      1. Processes each site folder to compute the emission thresholds at 95% cumulative probability.
      2. Saves the thresholds to a CSV file in a site-specific 'MIIEmissionThresholds' folder.
    """
    # Define the base folder (update this path as needed)
    BASE_FOLDER = r'C:/Users/Arthur_Santos/PycharmProjects/MAES-main/output/Mustang/MC_20250404_102836/summaries'

    # Process all sites within the PDFs folder
    process_all_sites(BASE_FOLDER)


if __name__ == "__main__":
    main()
