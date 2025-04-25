import matplotlib.pyplot as plt
import pandas as pd
import argparse
import os
import Timeseries as ts
import numpy as np
from matplotlib.gridspec import GridSpec

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)       # Adjust the display width to avoid line wrapping

def getTotalMC(parq):
    mcRun = parq["mcRun"].max()
    return mcRun

def getEmitterID(ie, md, unitID, modelReadableName):
    reqMD = md[md["modelReadableName"] == modelReadableName]
    reqMD = reqMD[reqMD["unitID"] == unitID]
    reqMD = reqMD[reqMD["equipmentType"] != "ActivityFactor"]
    emIDs = reqMD["emitterID"].unique()

    return emIDs

def plotEmAcross(parq, unitID, allModelReadableNames, saveIn):
    parq = parq[parq["unitID"] == unitID]
    interval_days = 5.0
    ieNew = parq[parq['species'] == "METHANE"]
    ieNew['timestamp'] = ieNew['timestamp_s']
    ieNew['timestamp_days'] = ieNew['timestamp'] / 86400.0
    ieNew['tsValue'] = ieNew['emissions_kgPerH']
    ieNew['nextTS'] = ieNew['timestamp'] + ieNew['duration_s']
    listOfMc = ieNew["mcRun"].unique()
    listOfMc = np.sort(listOfMc)
    for modelReadableName in allModelReadableNames:
        temp = ieNew[ieNew["modelReadableName"] == modelReadableName]
        if temp.empty:
            continue
        plt.figure(figsize=(10, 5))
        maxVal = temp['tsValue'].max() 
        # allModelReadableNames = temp["modelReadableName"].unique()
        for mcRun in listOfMc:
            temp2 = temp[temp["mcRun"] == mcRun]
            if temp2.empty:
                continue
            for emitterID in temp2["emitterID"].unique():
                temp3 = temp2[temp2["emitterID"] == emitterID]
                dataTS = ts.TimeseriesRLE(temp3)    # Convert the dataframe to a timeseries
                dataFull = dataTS.toFullTimeseries()
                tsD = dataFull.df['timestamp'] / 86400.0
                plt.plot(tsD, dataFull.df['tsValue'])
                # maxVal = temp2['tsValue'].max() 
                plt.ylim(0, maxVal * 1.2)  
                plt.xlabel('Timestamp (Days)')
                plt.ylabel(f'{modelReadableName} Emissions (kg/h)')
                plt.title(f'{modelReadableName} for {unitID} with emitterID {emitterID}') 
        
        temp['interval'] = np.floor(temp['timestamp_days'] / interval_days) * interval_days
        mean_emissions = temp.groupby('interval')['tsValue'].mean().reset_index()

        plt.plot(mean_emissions['interval'], mean_emissions['tsValue'], label='Mean Emissions', color='black', linewidth=2)
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        # plt.show()
        plt.savefig(saveIn + f"timeseries_{modelReadableName}_{unitID}.png")
        plt.close()
    pass

def plotSiteInstantEmissionByEquip(parq, unitIDs, saveIn, facName):
    parq = pd.read_parquet(parq + 'siteInstantEmissionsByEquip/' + f'facilityID={facName}/' + 'siteInstantEmissionsByEquip-0.parquet')
    if unitIDs is None:
        unitIDs = parq["unitID"].unique()
    # unitIDs = ['tank_battery_OIL']
    # modelReadableNames = ['Tank Vents PRV', 'Tank Vents Thief Hatch']
    modelReadableNames = parq["modelReadableName"].unique()
    for unitID in unitIDs:
        plotEmAcross(parq, unitID, modelReadableNames, saveIn)
    pass


def plotStateMachineTS(ie, unitIDList, saveIn, facName, mc='0'):
    ie = ie[ie["command"] == "STATE_TRANSITION"]
    
    if not unitIDList:
        unique_unitIDs = ie["unitID"].unique()
    else:
        unique_unitIDs = unitIDList
    
    num_units = len(unique_unitIDs)
    fig, axes = plt.subplots(num_units, 1, figsize=(10, 5 * num_units), sharex=True)
    
    if num_units == 1:
        axes = [axes]
    
    for ax, unitID in zip(axes, unique_unitIDs):
        ieTank = ie[ie["unitID"] == unitID]
        
        isTs = ts.TimeseriesCategorical(ieTank)
        isTS = isTs.toFullTimeseries()
        tsToPlot = isTs.df
        
        category_mapping = {category: idx for idx, category in enumerate(tsToPlot['state'].unique())}
        tsToPlot['numericValue'] = tsToPlot['state'].map(category_mapping)
        
        tsToPlot.plot(x='timestamp', y='numericValue', legend=False, ax=ax)
        ax.set_title(f"State Machine for UnitID: {unitID}")
        ax.set_xlabel('Timestamp')
        ax.set_ylabel('State')
        ax.set_yticks(list(category_mapping.values()))
        ax.set_yticklabels(list(category_mapping.keys()))
        ax.grid(True)
    
    plt.tight_layout()
    plt.savefig(saveIn + f"stateMachine_mcRun_{mc}.png")
    plt.close()
    pass


def preprocessTs(inFolder):
    # Load the CSV files
    dfInstantaneous = pd.read_csv(inFolder + "/instantaneousEvents.csv")
    dfTs = pd.read_csv(inFolder + "/emissionTimeseries.csv")
    tsMerged = pd.merge(
        dfInstantaneous,
        dfTs,
        how="left",
        on="tsKey"
    )
    tsMerged = tsMerged.sort_values(by="eventID")

    return tsMerged


def plotPDFsAndCDFsUnitID(parq, byType, indType=None, saveIn=None, facName=None, excludeModelReadableName=None, spPlotName=None, mcRuns=None, compareWith=None):
    # Load the Parquet file
    parq = pd.read_parquet(parq + 'siteInstantEmissionsByEquip/' + f'facilityID={facName}/' + 'siteInstantEmissionsByEquip-0.parquet')
    if excludeModelReadableName is not None:
        for modelReadableName in excludeModelReadableName:
            parq = parq[parq["modelReadableName"] != modelReadableName]
        # parq = parq[parq["modelReadableName"] != excludeModelReadableName]
    if not byType:
        byType = 'unitID'
    allType = parq[byType].unique()
    if indType:
        allType = indType
    # allType = ['tank_battery_OIL']
    
    for eachType in allType:
        parq2 = parq[parq[byType] == eachType]
        parq2 = parq2[parq2["species"] == "METHANE"]
        parq2['tsValue'] = parq2['emissions_kgPerH']
        # parq2 = parq2.dropna(subset=["tsValue"])
        parq2['nextTS'] = parq2['timestamp_s'] + parq2['duration_s']
        parq2.sort_values(by="timestamp_s")
        # mean_emissions = parq2['tsValue'].mean()
        mean_emissions = calcMean(parq2, mcruns=mcRuns)
        fig, axes = plt.subplots(2, 1, figsize=(10, 10), sharex=True)
        
        # Plot PDF (Probability Density Function) in the first subplot
        axes[0].hist(parq2['tsValue'], bins=100, density=True, alpha=0.7, label=f'{byType}: {eachType}')
        axes[0].axvline(mean_emissions, color='red', linestyle='--', label=f'Mean: {mean_emissions:.6f} kg/h')
        if compareWith is not None:
            axes[0].axvline(compareWith, color='green', linestyle='--', label=f'ONGAEIR Tank Emissions Estimate: {compareWith:.6f} kg/h')
        axes[0].set_ylabel('Probability Density')
        axes[0].set_title(f'PDF of Emissions for {byType}: {eachType}')
        axes[0].legend()
        axes[0].grid(True)

        
        # Plot CDF (Cumulative Distribution Function) in the second subplot
        sorted_values = np.sort(parq2['tsValue'])
        cdf = np.arange(1, len(sorted_values) + 1) / len(sorted_values)
        axes[1].plot(sorted_values, cdf, label=f'{byType}: {eachType}', color='orange')
        axes[1].axvline(mean_emissions, color='red', linestyle='--', label=f'Mean: {mean_emissions:.6f} kg/h')
        if compareWith is not None:
            axes[1].axvline(compareWith, color='green', linestyle='--', label=f'ONGAEIR Tank Emissions Estimate: {compareWith:.6f} kg/h')
        axes[1].set_xlabel('Emissions (kg/h)')
        axes[1].set_ylabel('Cumulative Probability')
        axes[1].set_title(f'CDF of Emissions for {byType}: {eachType}')
        axes[1].legend()
        axes[1].grid(True)
        
        plt.tight_layout()
        if spPlotName is not None:
            filename = f"{saveIn}/pdf_and_cdf_{eachType} [{spPlotName}].png"
        else:
            filename = f"{saveIn}/pdf_and_cdf_{eachType}.png"
        plt.savefig(filename)
        # print(f"Plot saved: {filename}")
        plt.close()  
        pass

def plot_standard_timeseries(ax, parq2, interval_days=5.0, compareWith=None):
    """
    Plots the standard time series on the given axis with a simplified legend.
    If compareWith is provided, plots a horizontal line for it and shows an expanded Y-axis on the right.
    """
    # Plot all mcRun data with unique colors
    for mcRun in parq2["mcRun"].unique():
        temp2 = parq2[parq2["mcRun"] == mcRun]
        if temp2.empty:
            continue
        for emitterID in temp2["emitterID"].unique():
            temp3 = temp2[temp2["emitterID"] == emitterID]
            dataTS = ts.TimeseriesRLE(temp3)
            dataFull = dataTS.toFullTimeseries()
            tsD = dataFull.df['timestamp'] / 86400.0
            ax.plot(tsD, dataFull.df['tsValue'], label=None)  # Avoid adding individual mcRun to the legend

    # Calculate mean emissions over intervals
    parq2['interval'] = np.floor(parq2['timestamp_days'] / interval_days) * interval_days
    mean_emissions = parq2.groupby('interval')['tsValue'].sum() / (interval_days * 3600.0)
    mean_emissions = mean_emissions.reset_index()
    ax.plot(mean_emissions['interval'], mean_emissions['tsValue'], label='Simulation Mean Emissions', color='black', linewidth=2)

    # Add a single legend entry for all mcRun data
    ax.plot([], [], label='Colors = Timeseries Emissions from Individual MC Runs', color='blue')  # Add a dummy plot for the legend

    # Add a secondary Y-axis for compareWith if provided
    if compareWith is not None:
        ax_compare = ax.twinx()  # Create a secondary Y-axis
        ax_compare.axhline(compareWith, color='green', linestyle='--', label=f'ONGAEIR: {compareWith:.6f} kg/h')
        simMean = calcMean(parq2, filtersMRN=['Tank Battery Component Leak', 'Tank Vents PRV'], mcruns=100)
        ax_compare.axhline(simMean, color='mediumblue', linestyle='--', label=f'Simulation Estimate: {simMean:.6f} kg/h')
        # ax_compare.plot(mean_emissions['interval'], mean_emissions['tsValue'], label='Mean Emissions', color='black', linewidth=2)
        ax_compare.set_ylabel('CompareWith (kg/h)', color='green')
        ax_compare.tick_params(axis='y', labelcolor='green')
        ax_compare.grid(False)  # Disable grid for the secondary Y-axis
        ax_compare.set_ylim(0, compareWith*1.5)  # Set limits for the secondary Y-axis
        
        # Align the zeros of both Y-axes
        # primary_ylim = ax.get_ylim()
        # ax_compare.set_ylim(primary_ylim)  # Set the secondary Y-axis limits to match the primary Y-axis

        # Add legend for the secondary Y-axis
        ax_compare.legend(loc='upper right')

    # Customize the primary plot
    ax.set_xlabel('Timestamp (Days)')
    ax.set_ylabel('Emissions (kg/h)')
    ax.legend(loc='upper left')
    ax.grid(True)
    i = 10

def calcMean(parq, filtersMRN=None, mcruns=None):
    if filtersMRN:
        for modelReadableName in filtersMRN:
            parq = parq[parq["modelReadableName"] != modelReadableName]
    parq['tsValue'] = parq['emissions_kgPerH']
    # parq['timestamp_days'] = parq['timestamp_s'] / 86400.0
    parq['total'] = parq['tsValue'] * (parq['duration_s'] / 3600.0)
    parq2Em = parq['total'].sum()/(mcruns*8760)
    return parq2Em


def plot_pdf(ax, parq2, mcRuns=None):
    ax.hist(parq2['tsValue'], bins=50, density=True, orientation='horizontal', alpha=0.7, color='orange')
    # mean_emissions_value = parq2['tsValue'].mean()
    mean_emissions_value = calcMean(parq2, mcruns=mcRuns)
    ax.axhline(mean_emissions_value, color='red', linestyle='--', label=f'Mean: {mean_emissions_value:.6f} kg/h')

    ax.set_xlabel('Probability Density')
    ax.set_title('PDF of Emissions')
    ax.legend()
    ax.grid(True)

def plotTimeseriesPerUnitID(parq, unitIDs, saveIn, facName, pdfOnRight=True, interval_days=5.0, excludeModelReadableName=None, mcRuns=None, compareWith=None):
    parq = pd.read_parquet(parq + 'siteInstantEmissionsByEquip/' + f'facilityID={facName}/' + 'siteInstantEmissionsByEquip-0.parquet')
    
    if excludeModelReadableName is not None:
        for modelReadableName in excludeModelReadableName:
            parq = parq[parq["modelReadableName"] != modelReadableName]

    if unitIDs is None: 
        unitIDs = parq["unitID"].unique()
    
    for unitID in unitIDs:
        parq2 = parq[parq["unitID"] == unitID]
        parq2 = parq2[parq2["species"] == "METHANE"]
        parq2['tsValue'] = parq2['emissions_kgPerH']
        parq2['timestamp'] = parq2['timestamp_s']
        parq2['timestamp_days'] = parq2['timestamp'] / 86400.0
        parq2['nextTS'] = parq2['timestamp'] + parq2['duration_s']
        # parq2 = parq2[parq2['modelReadableName'] != 'Tank Vents PRV']
        # parq2['total'] = parq2['tsValue'] * (parq2['duration_s'] / 3600.0)
        # parq2Em = parq2['total'].sum()/(25*8760)
        # print(f"Total Emissions for {unitID} = {parq2Em * 9.656} us tonne/yr")

        if pdfOnRight:
            fig = plt.figure(figsize=(12, 6))
            gs = GridSpec(1, 2, width_ratios=[3, 1], wspace=0.3)

            # Time series plot (left)
            ax_ts = fig.add_subplot(gs[0, 0])
            plot_standard_timeseries(ax_ts, parq2, interval_days)
            ax_ts.set_title(f'Time Series for UnitID: {unitID}')

            # PDF plot (right)
            ax_pdf = fig.add_subplot(gs[0, 1], sharey=ax_ts)
            plot_pdf(ax_pdf, parq2, mcRuns)

            plt.tight_layout()
            filename = f"{saveIn}/timeseries_with_pdf_{unitID}.png"
            plt.savefig(filename)
            print(f"Plot saved: {filename}")
            plt.close()

        else:
            fig, ax = plt.subplots(figsize=(10, 5))
            plot_standard_timeseries(ax, parq2, interval_days, compareWith)
            ax.set_title(f'Time Series for UnitID: {unitID}')

            plt.tight_layout()
            filename = f"{saveIn}/timeseries_{unitID}.png"
            plt.savefig(filename)
            print(f"Plot saved: {filename}")
            plt.close()

def plotTimeseriesWithPDF(parq, unitID, saveIn, facName, interval_days=5):
    # modelReadableName = "Tank Vents PRV"
    parq = pd.read_parquet(parq + 'siteInstantEmissionsByEquip/' + f'facilityID={facName}/' + 'siteInstantEmissionsByEquip-0.parquet')
    parq = parq[parq["unitID"] == unitID]
    parq = parq[parq["species"] == "METHANE"]
    parq['timestamp_days'] = parq['timestamp_s'] / 86400.0
    parq['tsValue'] = parq['emissions_kgPerH']
    parq = parq.dropna(subset=["tsValue"])

    # Create a GridSpec layout
    fig = plt.figure(figsize=(12, 6))
    gs = GridSpec(1, 2, width_ratios=[3, 1], wspace=0.3)

    # Time series plot (left)
    ax_ts = fig.add_subplot(gs[0, 0])
    for mcRun in parq["mcRun"].unique():
        temp = parq[parq["mcRun"] == mcRun]
        dataTS = ts.TimeseriesRLE(temp)
        dataFull = dataTS.toFullTimeseries()
        tsD = dataFull.df['timestamp'] / 86400.0
        ax_ts.plot(tsD, dataFull.df['tsValue'], label=f"mcRun {mcRun}")

    # Calculate mean emissions over intervals
    parq['interval'] = np.floor(parq['timestamp_days'] / interval_days) * interval_days
    mean_emissions = parq.groupby('interval')['tsValue'].mean().reset_index()
    ax_ts.plot(mean_emissions['interval'], mean_emissions['tsValue'], label='Mean Emissions', color='black', linewidth=2)

    # Customize time series plot
    ax_ts.set_xlabel('Timestamp (Days)')
    ax_ts.set_ylabel('Emissions (kg/h)')
    ax_ts.set_title(f'Time Series for UnitID: {unitID}')
    ax_ts.legend()
    ax_ts.grid(True)

    # PDF plot (right)
    ax_pdf = fig.add_subplot(gs[0, 1], sharey=ax_ts)
    ax_pdf.hist(parq['tsValue'], bins=50, density=True, orientation='horizontal', alpha=0.7, color='orange')
    mean_emissions_value = parq['tsValue'].mean()
    ax_pdf.axhline(mean_emissions_value, color='red', linestyle='--', label=f'Mean: {mean_emissions_value:.6f} kg/h')

    # Customize PDF plot
    ax_pdf.set_xlabel('Probability Density')
    ax_pdf.set_title('PDF of Emissions')
    ax_pdf.legend()
    ax_pdf.grid(True)

    # Save the plot
    plt.tight_layout()
    filename = f"{saveIn}/timeseries_with_pdf_{unitID}.png"
    plt.savefig(filename)
    print(f"Plot saved: {filename}")
    plt.close()

def main():
    parser = argparse.ArgumentParser(description="Process and plot state machines.")
    parser.add_argument("-o", "--output", type=str, required=False, help="Input folder containing CSV files")
    parser.add_argument("-f", "--facilityName", type=str, help="Facility Name in parquet file 'facilityID=123-5391'")
    parser.add_argument("-s", "--site", type=str, help="Site Name in input study sheet")
    args = parser.parse_args()

    if not args.output:
        # Automatically find the last created folder in the 'output' directory
        output_base = f'output/{args.site}/'
        all_folders = [os.path.join(output_base, d) for d in os.listdir(output_base) if os.path.isdir(os.path.join(output_base, d))]
        if not all_folders:
            raise FileNotFoundError("No folders found in the 'output' directory.")
        last_created_folder = max(all_folders, key=os.path.getctime)
        outputMC = f'{last_created_folder}/{args.site}/'
        print(f"Using the last created folder: {last_created_folder}")
    else:
        nextPara = args.output.split('/')
        outputMC = f'{args.output}/{nextPara[1]}/'

    saveIn = os.path.join(args.output if args.output else last_created_folder, 'parquet', 'plots/')
    parq = os.path.join(args.output if args.output else last_created_folder, 'parquet/')
    os.makedirs(saveIn, exist_ok=True)

    byTypes = ['modelReadableName', 'unitID', 'modelEmissionCategory']
    mcRuns = len(os.listdir(outputMC)) - 1

    reqMC = ['97']

    # for mc in os.listdir(outputMC): 
    for mc in reqMC:
        opFolder = os.path.join(outputMC, mc)
        if mc == 'template':
            continue
        tsMerged = preprocessTs(opFolder)
        plotStateMachineTS(tsMerged, unitIDList=['05-123-12697', 'sep_stage1_1', 'sep_stage2_1', 'tank_battery_OIL'], saveIn=saveIn, facName=args.facilityName, mc=mc)
    plotSiteInstantEmissionByEquip(parq=parq, unitIDs=None, saveIn=saveIn, facName=args.facilityName)
    plotTimeseriesPerUnitID(parq=parq, unitIDs=None, saveIn=saveIn, facName=args.facilityName, excludeModelReadableName=None, mcRuns=mcRuns)
    # plotTimeseriesPerUnitID(parq=parq, unitIDs=None, saveIn=saveIn, facName=args.facilityName, excludeModelReadableName=['Tank Battery Component Leak', 'Tank Vents PRV'])
    # for byType in byTypes:
        # plotPDFsAndCDFsUnitID(parq=parq, byType=byType, saveIn=saveIn, facName=args.facilityName)
    plotPDFsAndCDFsUnitID(parq=parq, byType='unitID', saveIn=saveIn, facName=args.facilityName, 
                          excludeModelReadableName=None, mcRuns=mcRuns)
    # plotTimeseriesPerUnitID(parq=parq, unitIDs=['tank_battery_OIL'], saveIn=saveIn, mcRuns=mcRuns,
    #                         excludeModelReadableName=['Tank Battery Component Leak', 'Tank Vents PRV'], facName=args.facilityName, interval_days=5, pdfOnRight=False, compareWith=0.00006)
    # plotPDFsAndCDFsUnitID(parq=parq, byType='unitID', indType = ['tank_battery_OIL'], saveIn=saveIn, facName=args.facilityName, 
    #                       excludeModelReadableName=None, spPlotName='With', mcRuns=mcRuns, compareWith=0.00006)
    # plotPDFsAndCDFsUnitID(parq=parq, byType='unitID', indType = ['tank_battery_OIL'], saveIn=saveIn, facName=args.facilityName, 
    #                       excludeModelReadableName=['Tank Battery Component Leak', 'Tank Vents PRV'], spPlotName='Without2', mcRuns=mcRuns, compareWith=0.00006)
    pass


if __name__ == "__main__":
    main()