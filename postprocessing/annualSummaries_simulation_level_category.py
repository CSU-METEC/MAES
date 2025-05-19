import os
import pandas as pd
import numpy as np


def list_all_files(folder_path):
    """
    Reads the annual emissions parquet file from a given folder.
    """
    annualEmiss_parquets = os.path.join(folder_path, 'parquet/siteEmissionsbyCat')
    annualEmissDF = pd.read_parquet(annualEmiss_parquets, engine='pyarrow')
    return annualEmissDF


def compute_total_emissions_stats(folder, abnormal):
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
    # Read the full dataset
    df_full = list_all_files(folder)
    # Convert emissions from USTons to metric tons
    df_full['emissions_mtPerYear'] = df_full['emissions_USTonsPerYear'] * 0.907185

    # Compute species totals based on the abnormal mode.
    if abnormal == "OFF":
        # For OFF mode, only COMBUSTION and VENTED contribute to the species total.
        categories_total = ['COMBUSTION', 'VENTED']
    else:
        # For ON mode, the species total is computed from COMBUSTION, VENTED, and FUGITIVE.
        categories_total = ['COMBUSTION', 'VENTED', 'FUGITIVE']

    df_total = df_full[df_full['modelEmissionCategory'].isin(categories_total)]
    species_totals = {}
    for species in df_total['species'].unique():
        # Sum the emissions over all Monte Carlo runs for this species,
        # using only the categories defined in categories_total.
        total = df_total[df_total['species'] == species].groupby('mcRun')['emissions_mtPerYear'].sum().sum()
        species_totals[species] = total

    # Now proceed with the abnormal filtering for computing statistics.
    df = df_full.copy()
    if abnormal == "OFF":
        # Only COMBUSTION and VENTED are available in OFF mode.
        df = df[(df['modelEmissionCategory'] == 'COMBUSTION') | (df['modelEmissionCategory'] == 'VENTED')]
        # Aggregate the data at the mcRun level per species and modelEmissionCategory.
        df = df.groupby(['mcRun', 'species', 'modelEmissionCategory'], as_index=False)['emissions_USTonsPerYear'].sum()
        df['emissions_mtPerYear'] = df['emissions_USTonsPerYear'] * 0.907185
    # For abnormal "ON", we assume the file already contains rows where modelEmissionCategory is 'TOTAL'.

    results = []

    # Compute statistics for each combination of species and modelEmissionCategory.
    unique_combinations = df[['species', 'modelEmissionCategory']].drop_duplicates()
    for species, mec in unique_combinations.itertuples(index=False, name=None):
        df_subset = df[(df['species'] == species) & (df['modelEmissionCategory'] == mec)]
        # Group by mcRun to get one emission value per Monte Carlo run.
        grouped = df_subset.groupby('mcRun')['emissions_mtPerYear'].sum()
        total_emissions = grouped.sum()  # Emissions summed over all mcRuns for this species/category
        total_emissions_list = grouped.tolist()

        mean_emissions = np.mean(total_emissions_list)
        ci_lower = np.percentile(total_emissions_list, 2.5)
        ci_upper = np.percentile(total_emissions_list, 97.5)

        # For rows where modelEmissionCategory is 'TOTAL', the percentage is by definition 100%.
        # Otherwise, compute the percentage relative to the species total computed above.
        if mec == 'TOTAL':
            percentage_of_total = 100.0
        else:
            sp_total = species_totals.get(species, 0)
            percentage_of_total = (total_emissions / sp_total * 100) if sp_total > 0 else np.nan

        results.append({
            'Species': species.upper(),
            'modelEmissionCategory': mec,
            'unit': 'mt/year',
            'mean_emissions': mean_emissions,
            '95%_ci_lower': ci_lower,
            '95%_ci_upper': ci_upper,
            'emissions_sum_across_mcRuns': total_emissions,
            'percentage_of_total_emissions': percentage_of_total
        })

    # Compute C2/C1 ratio row for each modelEmissionCategory.
    for mec in df['modelEmissionCategory'].unique():
        # Filter the dataframe for ETHANE and METHANE within the current category.
        df_ethane = df[(df['species'].str.upper() == 'ETHANE') & (df['modelEmissionCategory'] == mec)].copy()
        df_methane = df[(df['species'].str.upper() == 'METHANE') & (df['modelEmissionCategory'] == mec)].copy()

        # Group by mcRun to compute emissions for each run.
        ethane_group = df_ethane.groupby('mcRun')['emissions_mtPerYear'].sum()
        methane_group = df_methane.groupby('mcRun')['emissions_mtPerYear'].sum()

        common_mcRuns = ethane_group.index.intersection(methane_group.index)
        if not common_mcRuns.empty:
            ratio_series = ethane_group.loc[common_mcRuns] / methane_group.loc[common_mcRuns]
            ratio_series = ratio_series.replace([np.inf, -np.inf], np.nan).dropna()

            if not ratio_series.empty:
                mean_ratio = ratio_series.mean()
                ci_lower_ratio = np.percentile(ratio_series, 2.5)
                ci_upper_ratio = np.percentile(ratio_series, 97.5)

                results.append({
                    'Species': 'C2/C1',
                    'modelEmissionCategory': mec,
                    'unit': 'unitless',
                    'mean_emissions': mean_ratio,
                    '95%_ci_lower': ci_lower_ratio,
                    '95%_ci_upper': ci_upper_ratio,
                    'emissions_sum_across_mcRuns': np.nan,
                    'percentage_of_total_emissions': np.nan
                })

    return pd.DataFrame(results)


def run_total_emissions_pipeline(folder):
    """
    Runs the total emissions summary for both abnormal modes ("ON" and "OFF").
    """
    output_folder = os.path.join(folder, 'summaries', 'AggregatedSimulationEmissions')
    os.makedirs(output_folder, exist_ok=True)
    for mode in ['ON', 'OFF']:
        df_results = compute_total_emissions_stats(folder, mode)
        suffix = 'abnormal_on.csv' if mode == 'ON' else 'abnormal_off.csv'
        output_path = os.path.join(output_folder, f'aggregated_sim_emissions_by_category_{suffix}')
        df_results.to_csv(output_path, index=False)
        print(f"Saved emissions summary for ABNORMAL = {mode} to:")
        print(output_path)



def main(folder):
    # Define the folder path
<<<<<<< HEAD
    FOLDER = "C:/METEC/MAES2/output/P2_2stages_flare/MC_20250509_114055/"
    run_total_emissions_pipeline(FOLDER)
=======
    # FOLDER = '/home/arthur/MAES/output/Mustang/MC_20250404_102836'
    run_total_emissions_pipeline(folder)
>>>>>>> 0c525ae70356439ac7e655703ff31b96a4df5f47


if __name__ == "__main__":
    main(folder="/home/arthur/MAES/output/Mustang/MC_20250404_102836")
