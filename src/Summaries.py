import pandas as pd
import AppUtils as au
import os
import json
import logging
import numpy as np
import Timeseries as ts
import ParquetLib as Pl
from postprocessing import plot_annualSummaries_METype_level as ptm
from postprocessing import plot_annualSummaries_unitID_level as ptu
from postprocessing import annualSummaries_simulation_level_category as alc
from postprocessing import annualSummaries_simulation_level_by_METype as alm
from postprocessing import plot_timeseries_and_state_transitions as pst
from postprocessing import annualSummaries_simulation_level_modelReadableName as ald
from postprocessing import plot_annualSummaries_modelReadableName_level as ptd
from postprocessing import plot_annualSummaries_site_level as pts
from postprocessing import generate_MII_emiss_thresholds as gmt

logger = logging.getLogger(__name__)

SECONDSINHOUR = 3600
US_TO_PER_METRIC_TON = 1.10231
US_TO_PER_HOUR_TO_KG_PER_HOUR = 0.1035


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
        ].copy()

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
        ].copy()

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



def calcMdReadbleNameEmissionsSummary(emissionsDf, emissions_colmn, species, inst_emissions = False):
    if inst_emissions:
        emissionsDf[emissions_colmn] = emissionsDf[emissions_colmn] * US_TO_PER_HOUR_TO_KG_PER_HOUR
        mt = "kg/hour"
    else:
        emissionsDf[emissions_colmn] = emissionsDf[emissions_colmn] / US_TO_PER_METRIC_TON
        mt = "mt/year"

    ci = float(95)
    mean_header= "mean_emissions"
    ci_lower_header = f"{int(ci)}%_ci_lower"
    ci_upper_header = f"{int(ci)}%_ci_upper"
    alpha = 100 - ci
    emissionsDf = emissionsDf[emissionsDf['species'] == species]

    mcNameDf = emissionsDf.groupby(["mcRun","METype", "unitID", "modelReadableName"], as_index=False)[emissions_colmn].sum()
    mdNameDf = mcNameDf.groupby(["METype","unitID", "modelReadableName"], as_index=False)[emissions_colmn].mean()

    mdNameDf.rename(columns={emissions_colmn: mean_header}, inplace=True)

    ci_lower = mcNameDf.groupby(["METype", "unitID", "modelReadableName"])[emissions_colmn].apply(lambda x: np.percentile(x, alpha / 2))
    ci_upper = mcNameDf.groupby(["METype", "unitID", "modelReadableName"])[emissions_colmn].apply(lambda x: np.percentile(x, (100 - alpha / 2)))
    mdNameDf = mdNameDf.merge(ci_lower.rename(ci_lower_header), on=["METype", "unitID", "modelReadableName"], how="left")
    mdNameDf = mdNameDf.merge(ci_upper.rename(ci_upper_header), on=["METype", "unitID", "modelReadableName"], how="left")

    unitIDDF = mdNameDf.groupby(["METype", "unitID"], as_index=False)[[mean_header,ci_lower_header,ci_upper_header]].sum()
    unitIDDF["modelReadableName"] = "summed_modelReadableName"

    uniDF = emissionsDf.groupby(["mcRun","METype"], as_index=False)[emissions_colmn].sum()
    uni_ci_lower = uniDF.groupby(["METype"])[emissions_colmn].apply(lambda x: np.percentile(x, alpha / 2))
    uni_ci_upper = uniDF.groupby(["METype"])[emissions_colmn].apply(lambda x: np.percentile(x, (100 - alpha / 2)))

    meTypeDf = unitIDDF.groupby(["METype","modelReadableName"], as_index=False)[mean_header].sum()
    meTypeDf["unitID"] = "summed_unitID"
    meTypeDf = meTypeDf.merge(uni_ci_lower.rename(ci_lower_header), on=["METype"], how="left")
    meTypeDf = meTypeDf.merge(uni_ci_upper.rename(ci_upper_header), on=["METype"], how="left")

    final_df = pd.concat([mdNameDf,unitIDDF,meTypeDf], ignore_index=True)
    
    total = meTypeDf.sum(numeric_only=True, axis=0)
    total["METype"] = "summed_METype"
    total["unitID"] = "summed_unitID"
    total["modelReadableName"] = "summed_modelReadableName"

    final_df = pd.concat([final_df, total.to_frame().T], ignore_index=True)
    final_df["species"] = species
    final_df["unit"] = mt
    final_df = final_df.drop(final_df[(final_df[ci_lower_header] ==0) & (final_df[ci_upper_header] ==0) & (final_df[mean_header] ==0)].index)
    return final_df.sort_values(["METype"])

 
def calcSiteLevelSummary(emissCatDF, species, confidence_level=95, instantEmissions=False):
    if instantEmissions:
        emissionsColumn = "emissions_kgPerH"
        emissCatDF[emissionsColumn] = emissCatDF['emissions_USTonsPerYear'] * US_TO_PER_HOUR_TO_KG_PER_HOUR
        mt = "kg/hour"
    else:
        emissionsColumn = "emissions_MetricTonsPerYear"
        emissCatDF[emissionsColumn] = emissCatDF['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON # convert from US tons to metric tons
        mt = "mt/year"

    alpha = 100 - float(confidence_level)

    ci_lower_col = f"{confidence_level}%_ci_lower"
    ci_upper_col = f"{confidence_level}%_ci_upper"

    emissCatDF = emissCatDF[emissCatDF.species == species]
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

    mdCat.rename(columns={emissionsColumn:'mean_emissions'}, inplace=True)
    mdCat["species"] = species
    mdCat["unit"] = mt

    mdCat = mdCat.drop(mdCat[(mdCat["mean_emissions"] ==0 ) & (mdCat["max"] == 0)].index)
    return mdCat

def calcEmissSummaryByMEType(emissEquipDF, species, confidence_level=95, instantEmissions = False):
    if instantEmissions:
        emissionsColumn = "emissions_kgPerH"
        mt = "kg/hour"
        emissEquipDF[emissionsColumn] = emissEquipDF['emissions_USTonsPerYear'] * US_TO_PER_HOUR_TO_KG_PER_HOUR
    else:
        emissionsColumn = "emissions_MetricTonsPerYear"
        emissEquipDF[emissionsColumn] = emissEquipDF['emissions_USTonsPerYear'] / US_TO_PER_METRIC_TON
        mt = "mt/year"

    emissEquipDF = emissEquipDF[emissEquipDF["species"] == species]
    alpha = 100 - float(confidence_level)

    mcEq = emissEquipDF.groupby(["mcRun", "METype"], as_index=False)[emissionsColumn].sum()

    medf = mcEq.groupby("METype", as_index=False)[emissionsColumn].mean()

    min = mcEq.groupby("METype")[emissionsColumn].min()
    max = mcEq.groupby("METype")[emissionsColumn].max()

    lower = mcEq.groupby("METype")[emissionsColumn].apply(lambda x : np.percentile(x, 25))
    upper = mcEq.groupby("METype")[emissionsColumn].apply(lambda x : np.percentile(x, 75))

    ci_lower = mcEq.groupby("METype")[emissionsColumn].apply(lambda x : np.percentile(x, alpha / 2))
    ci_upper = mcEq.groupby("METype")[emissionsColumn].apply(lambda x : np.percentile(x, (100 - alpha / 2)))

    medf = medf.merge(min.rename("min"), on=["METype"], how="left")
    medf = medf.merge(max.rename("max"), on=["METype"], how="left")
    medf = medf.merge(lower.rename("lower"), on=["METype"], how="left")
    medf = medf.merge(upper.rename("upper"), on=["METype"], how="left")
    medf = medf.merge(ci_lower.rename(f"{confidence_level}%_ci_lower"), on=["METype"], how="left")
    medf = medf.merge(ci_upper.rename(f"{confidence_level}%_ci_upper"), on=["METype"], how="left")

    total = medf.sum(numeric_only=True, axis=0)
    total["METype"] = "summed_METype"
    total = pd.concat([medf, total.to_frame().T], ignore_index=True)
    total.rename(columns={emissionsColumn:'mean_emissions'}, inplace=True)

    total["species"] = species
    total["unit"] = mt

    return total


def dumpEmissions(summaryDF, config, summaryType, facID=None, abnormal=None):
    abnormal = abnormal.lower()

    match summaryType:
        case "facility":
            extension = f"annualEmissions_by_site_abnormal_{abnormal}"

        case "equipment":
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
    return outFile

def aggrSet(input_df, value_column, group_options=None):
    """Aggregates a DataFrame by specified options, creating Timeseries objects."""
    timeseries_set = []
    if group_options:
        input_df = input_df[input_df[group_options[0]] == group_options[1]]
    grouping_cols = ['facilityID', 'METype'] if value_column == "state" else ['facilityID', 'unitID', 'emitterID']
    TimeseriesClass = ts.TimeseriesCategorical if value_column == "state" else ts.TimeseriesRLE
    if input_df.empty:
        logger.warning(f"Where {group_options[0]} = {group_options[1]} and the selected abnormal emissions options do not match input data")
        pass
    for _, subset_df in input_df.groupby(grouping_cols):
        timeseries_set.append(TimeseriesClass(subset_df, valueColName=value_column))
    return timeseries_set

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

def calcProbabilitiesAllMCs(tss):
    combined_ts_df = pd.concat([t.df for t in tss], ignore_index=True)
    combined_ts = ts.TimeseriesRLE(combined_ts_df.sort_values(by=['nextTS'], ascending=[True]), filterZeros=False)
    pdf = combined_ts.toPDF()
    return pdf.data

def generatePDFs(config, df, abnormal, fac):
    df = df[df['modelReadableName'] != 'Blowdown Event']    # exclude maintenance emissions
    facilityDF = df[df['species'] == 'METHANE']

    siteEmissions = config['siteEmiss']
    meType = config['METype']
    unitID = config['unitID']
    miiEmiss = config['miiEmiss']

    all_false = all(not x for x in [siteEmissions, meType, unitID, miiEmiss])
    
    if all_false or miiEmiss:
        siteEmissions = meType = unitID = miiEmiss = True

    for site, Sdf in facilityDF.groupby('site'):
        siteDF = Sdf[Sdf['site'] == site]
        siteEndSimDF = Pl.readParquetSummary(config, site=site)

        if siteEmissions:
            allMCruns = grouping(dfToGroup=siteDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
            pdf = calcProbabilitiesAllMCs(allMCruns.values())
            pdf['CH4_EmissionRate_kg/h'] = pdf['value']
            pdf.drop(columns=['value', 'count'], inplace=True)
            dumpEmissions(pdf, config, "pdf_site_aggregate", facID=f"PDFs/site={fac}/", abnormal=abnormal)

        if meType:
            for siMeType, meTyDF in siteDF.groupby('METype'):
                meTypeAllMCruns = grouping(dfToGroup=meTyDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
                meTypepdf = calcProbabilitiesAllMCs(meTypeAllMCruns.values())
                meTypepdf['CH4_EmissionRate_kg/h'] = meTypepdf['value']
                meTypepdf.drop(columns=['value', 'count'], inplace=True)
                dumpEmissions(meTypepdf, config, "equip_group_level", facID=f"PDFs/site={fac}/PDF_for_all_{siMeType}", abnormal=abnormal)

        
        if unitID:
            for unitID, unitIDDF in siteDF.groupby('unitID'):
                unitAllMCruns = grouping(dfToGroup=unitIDDF, siteEndSimDF=siteEndSimDF, valueColName="emission")
                unitPDF = calcProbabilitiesAllMCs(unitAllMCruns.values())
                unitPDF['CH4_EmissionRate_kg/h'] = unitPDF['value']
                unitPDF.drop(columns=['value', 'count'], inplace=True)
                dumpEmissions(unitPDF, config, "unit_level", facID=f"PDFs/site={fac}/PDF_for_{unitID}", abnormal=abnormal)

        if miiEmiss:
            gmt.main(folder=f"{config['simulationRoot']}/summaries")

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
            group = df[(df['mcRun'] == mc) & (df['unitID'] == uid)]
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
    return df_complete

def generatedCsvSummaries(config, df, fac, abnormal):
    fac = str(fac).capitalize()
     # Get DFs for emissions for the summaries
    zerosDF = fillEmptyDataWithZero(df.copy(), emissionCol="emissions_USTonsPerYear")
    # zerosDF = zerosDF[zerosDF['METype']=='Compressor']
    # logging.info("Creating dataframes for Emission by Categories...")
    emissCatDF = Pl.processEmissionsCat(zerosDF.copy())
    # logging.info("Creating dataframes for Emission by Equipment...")
    # emissEquipDF = processEquipEmissions(zerosDF)
    # logging.info("Creating dataframes for Instantaneous Emissions by Equipment...")
    emissInstEquipDF = Pl.processInstantEquipEmissions(df)

    annualSummaries = config['annualSummaries']
    instantaneousSummaries = config['instantaneousSummaries']
    pdfSummaries = config['pdfSummaries']
    avgDurSummaries = config['avgDurSummaries']
    
    if config['fullSummaries']:
        annualSummaries = instantaneousSummaries = pdfSummaries = avgDurSummaries = True

    if annualSummaries:
        siteEmissions = config['siteEmiss']
        meType = config['METype']
        unitID = config['unitID']
        simulationEmissions = config['simulationEmissions']
        statesAndTsPloting = config['statesAndTsPloting']

        all_false = all(not x for x in [siteEmissions, meType, unitID, simulationEmissions, statesAndTsPloting])
        if all_false:
            siteEmissions = meType = unitID = simulationEmissions = statesAndTsPloting =True

        if unitID:
            detailed_emissionsDF = calcMdReadbleNameEmissionsSummary(zerosDF.copy(), emissions_colmn="emissions_USTonsPerYear", species="METHANE")
            detailed_emissionsDF = pd.concat([detailed_emissionsDF, calcMdReadbleNameEmissionsSummary(zerosDF.copy(), emissions_colmn="emissions_USTonsPerYear", species="ETHANE")])
            unit_summary_path = dumpEmissions(detailed_emissionsDF, config, "annual_mdReadbleName_emissions", facID=f"AnnualEmissions/site={fac}/", abnormal=abnormal)
            if config['plot']:
                ptu.main(file=unit_summary_path)
                ptd.main(file=unit_summary_path)

        if siteEmissions:
            CategorySummaryDF = calcSiteLevelSummary(emissCatDF.copy(), species='METHANE', confidence_level=95)
            CategorySummaryDF = pd.concat([CategorySummaryDF, calcSiteLevelSummary(emissCatDF.copy(), species='ETHANE', confidence_level=95)])  # add ethane summary
            site_summary_path = dumpEmissions(CategorySummaryDF, config, "facility", facID=f"AnnualEmissions/site={fac}/", abnormal=abnormal)
            if config['plot']:
                pts.main(file=site_summary_path)

        if meType:
            equipEmissSummaryDF = calcEmissSummaryByMEType(zerosDF.copy(), species='METHANE', confidence_level=95)
            equipEmissSummaryDF = pd.concat([equipEmissSummaryDF, calcEmissSummaryByMEType(zerosDF.copy(), species='ETHANE', confidence_level=95)])  # add ethane summary
            metype_summary_path = dumpEmissions(equipEmissSummaryDF, config, "equipment", facID=f"AnnualEmissions/site={fac}/", abnormal=abnormal)
            if config['plot']:
                ptm.main(file=metype_summary_path)

        if simulationEmissions:
            alc.main(folder=config['simulationRoot'])
            ald.main(folder=config['simulationRoot'])
            alm.main(folder=config['simulationRoot'])

        if statesAndTsPloting:
            mcRunTs = config['mcRunTs']
            mcRunStates = config['mcRunStates']
            pst.main(config=config, abnormal="OFF", mcRunTs=mcRunTs, mcRunStates=mcRunStates)

    if instantaneousSummaries:
        # Get instantaneous emissions summary by modelReadableName
        instEmissByModelReadName = calcInstEmissModelReadableName(emissInstEquipDF.copy())
        dumpEmissions(instEmissByModelReadName, config, "instantEmissions_emissions_summary", facID=f"InstantaneousEmissions/site={fac}/", abnormal=abnormal)
     
    if pdfSummaries: 
        # Get PDF at Site Level for CH4 Emissions
        generatePDFs(config=config, df=df.copy(), abnormal=abnormal, fac=fac)

    if avgDurSummaries:
        avgERandDur = createSummaryTable(emissInstEquipDF.copy(), species="METHANE")
        avgERandDur = pd.concat([avgERandDur,createSummaryTable(emissInstEquipDF.copy(),species="ETHANE")])
        dumpEmissions(avgERandDur, config, "avgERandDur", facID=f"AvgEmissionRatesAndDurations/site={fac}/", abnormal=abnormal)

    return None
   
def filterAbnormalEmissions(df):
    valid_emitter_ids = df[df['modelEmissionCategory'] != 'FUGITIVE']['emitterID']
    df = df[df['emitterID'].isin(valid_emitter_ids)]
    return df
