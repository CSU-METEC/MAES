import pandas as pd
import numpy as np
import os


def list_all_files(folder_path):
    """Reads the site emissions parquet file from a given folder."""
    path = os.path.join(folder_path, 'parquet/siteEmissionsByEquip')
    return pd.read_parquet(path, engine='pyarrow')


def compute_stats_per_METype(species, all_mcRuns, df_base, mode):
    """Computes statistics for a given species across Major Equipment Types."""
    df = df_base[df_base['species'] == species].copy()

    if mode == "OFF":
        df = df[df['modelEmissionCategory'] != 'FUGITIVE']

    df['emissions_mtPerYear'] = df['emissions_USTonsPerYear'] * 0.907185


    result_rows = []

    total_species_emissions = (
        df.groupby('mcRun')['emissions_mtPerYear'].sum()
        .reindex(all_mcRuns, fill_value=0)
        .sum()
    )

    for me_type in df['METype'].unique():
        df_me = df[df['METype'] == me_type]
        summed_by_mc = df_me.groupby('mcRun')['emissions_mtPerYear'].sum()
        summed_by_mc = summed_by_mc.reindex(all_mcRuns, fill_value=0)

        mean_val = np.mean(summed_by_mc)
        ci_lower = np.percentile(summed_by_mc, 2.5)
        ci_upper = np.percentile(summed_by_mc, 97.5)
        total_sum = summed_by_mc.sum()

        percentage_of_total = (
            (total_sum / total_species_emissions) * 100
            if total_species_emissions > 0 else np.nan
        )

        result_rows.append({
            'species': species.upper(),
            'METype': me_type,
            'Unit': 'mt/year',
            'mean_emissions': mean_val,
            '95%_ci_lower': ci_lower,
            '95%_ci_upper': ci_upper,
            'emissions_sum_across_mcRuns': total_sum,
            'percentage_of_total_emissions': percentage_of_total
        })

    return pd.DataFrame(result_rows)


def compute_c2_c1_ratios(df_base):
    """Computes C2 to C1 emission ratios per METype."""
    df_ethane = df_base[df_base['species'].str.upper() == 'ETHANE'].copy()
    df_methane = df_base[df_base['species'].str.upper() == 'METHANE'].copy()

    df_ethane['emissions_mtPerYear'] = df_ethane['emissions_USTonsPerYear'] * 0.907185
    df_methane['emissions_mtPerYear'] = df_methane['emissions_USTonsPerYear'] * 0.907185

    all_me_types = sorted(set(df_base['METype'].unique()))
    result_rows = []

    for me_type in all_me_types:
        ethane_grp = df_ethane[df_ethane['METype'] == me_type].groupby('mcRun')['emissions_mtPerYear'].sum()
        methane_grp = df_methane[df_methane['METype'] == me_type].groupby('mcRun')['emissions_mtPerYear'].sum()

        common_mcRuns = ethane_grp.index.intersection(methane_grp.index)
        ratio = ethane_grp.loc[common_mcRuns] / methane_grp.loc[common_mcRuns]

        ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()

        if not ratio.empty:
            result_rows.append({
                'species': 'C2/C1',
                'METype': me_type,
                'Unit': 'unitless',
                'mean_emissions': ratio.mean(),
                '95%_ci_lower': np.percentile(ratio, 2.5),
                '95%_ci_upper': np.percentile(ratio, 97.5)
            })

    return pd.DataFrame(result_rows)


def summarize_emissions_by_mode(mode, df_all, all_mcRuns, all_species, output_folder):
    """Processes and saves the summary emissions for a specific abnormal mode (ON/OFF)."""
    all_results = [
        compute_stats_per_METype(species, all_mcRuns, df_all, mode)
        for species in all_species
    ]
    all_results.append(compute_c2_c1_ratios(df_all))

    summary_df = pd.concat(all_results, ignore_index=True)

    suffix = 'abnormal_on.csv' if mode == 'ON' else 'abnormal_off.csv'
    output_folder = os.path.join(output_folder, 'summaries', 'AggregatedSimulationEmissions')
    os.makedirs(output_folder, exist_ok=True)
    output_path = os.path.join(output_folder + '', f'aggregated_sim_emissions_by_METype_{suffix}')

    summary_df.to_csv(output_path, index=False)

    print(f"\nSaved METype emissions summary for ABNORMAL = {mode} to:")
    print(output_path)


def run_emissions_summary_pipeline(folder):
    """Runs the emissions summary for both ABNORMAL ON and OFF modes."""
    df_all = list_all_files(folder)
    all_mcRuns = sorted(df_all['mcRun'].unique())
    all_species = df_all['species'].unique()

    summarize_emissions_by_mode('ON', df_all, all_mcRuns, all_species, folder)
    summarize_emissions_by_mode('OFF', df_all, all_mcRuns, all_species, folder)


def main():
    # Define the folder path
    FOLDER = '/home/arthur/MAES/output/Mustang_/MC_20250321_144004/'
    run_emissions_summary_pipeline(FOLDER)


# Example of calling the main function
if __name__ == "__main__":
    main()
