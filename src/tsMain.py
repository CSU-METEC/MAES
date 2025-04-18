import matplotlib.pyplot as plt
import pandas as pd
import argparse
import os
import Timeseries as ts
import numpy as np
from matplotlib.gridspec import GridSpec

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)       # Adjust the display width to avoid line wrapping

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


def plotStateMachineTS(ie, unitIDList, saveIn, facName):
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
    plt.savefig(saveIn + f"stateMachine.png")
    plt.close()
    pass


def preprocessTs(inFolder):
    # Load the CSV files
    dfInstantaneous = pd.read_csv(inFolder + "instantaneousEvents.csv")
    dfTs = pd.read_csv(inFolder + "emissionTimeseries.csv")
    tsMerged = pd.merge(
        dfInstantaneous,
        dfTs,
        how="left",
        on="tsKey"
    )
    tsMerged = tsMerged.sort_values(by="eventID")

    return tsMerged


def plotPDFsAndCDFsUnitID(parq, byType, saveIn, facName, excludeModelReadableName=None, spPlotName=None):
    # Load the Parquet file
    parq = pd.read_parquet(parq + 'siteInstantEmissionsByEquip/' + f'facilityID={facName}/' + 'siteInstantEmissionsByEquip-0.parquet')
    if excludeModelReadableName is not None:
        parq = parq[parq["modelReadableName"] != excludeModelReadableName]
    if not byType:
        byType = 'unitID'
    allType = parq[byType].unique()
    # allType = ['tank_battery_OIL']
    
    for eachType in allType:
        parq2 = parq[parq[byType] == eachType]
        parq2 = parq2[parq2["species"] == "METHANE"]
        parq2['tsValue'] = parq2['emissions_kgPerH']
        parq2 = parq2.dropna(subset=["tsValue"])
        mean_emissions = parq2['tsValue'].mean()
        fig, axes = plt.subplots(2, 1, figsize=(10, 10), sharex=True)
        
        # Plot PDF (Probability Density Function) in the first subplot
        axes[0].hist(parq2['tsValue'], bins=100, density=True, alpha=0.7, label=f'{byType}: {eachType}')
        axes[0].axvline(mean_emissions, color='red', linestyle='--', label=f'Mean: {mean_emissions:.2f} kg/h')
        axes[0].set_ylabel('Probability Density')
        axes[0].set_title(f'PDF of Emissions for {byType}: {eachType}')
        axes[0].legend()
        axes[0].grid(True)
        
        # Plot CDF (Cumulative Distribution Function) in the second subplot
        sorted_values = np.sort(parq2['tsValue'])
        cdf = np.arange(1, len(sorted_values) + 1) / len(sorted_values)
        axes[1].plot(sorted_values, cdf, label=f'{byType}: {eachType}', color='orange')
        axes[1].axvline(mean_emissions, color='red', linestyle='--', label=f'Mean: {mean_emissions:.2f} kg/h')
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

def plot_standard_timeseries(ax, parq2, interval_days=5.0):
    for mcRun in parq2["mcRun"].unique():
        temp2 = parq2[parq2["mcRun"] == mcRun]
        if temp2.empty:
            continue
        for emitterID in temp2["emitterID"].unique():
            temp3 = temp2[temp2["emitterID"] == emitterID]
            dataTS = ts.TimeseriesRLE(temp3)
            dataFull = dataTS.toFullTimeseries()
            tsD = dataFull.df['timestamp'] / 86400.0
            ax.plot(tsD, dataFull.df['tsValue'], label=f'EmitterID: {emitterID}, mcRun: {mcRun}')

    parq2['interval'] = np.floor(parq2['timestamp_days'] / interval_days) * interval_days
    mean_emissions = parq2.groupby('interval')['tsValue'].mean().reset_index()
    ax.plot(mean_emissions['interval'], mean_emissions['tsValue'], label='Mean Emissions', color='black', linewidth=2)

    ax.set_xlabel('Timestamp (Days)')
    ax.set_ylabel('Emissions (kg/h)')
    # ax.legend()
    ax.grid(True)

def plot_pdf(ax, parq2):
    ax.hist(parq2['tsValue'], bins=50, density=True, orientation='horizontal', alpha=0.7, color='orange')
    mean_emissions_value = parq2['tsValue'].mean()
    ax.axhline(mean_emissions_value, color='red', linestyle='--', label=f'Mean: {mean_emissions_value:.2f} kg/h')

    ax.set_xlabel('Probability Density')
    ax.set_title('PDF of Emissions')
    ax.legend()
    ax.grid(True)

def plotTimeseriesPerUnitID(parq, unitIDs, saveIn, facName, pdfOnRight=True, interval_days=5.0):
    parq = pd.read_parquet(parq + 'siteInstantEmissionsByEquip/' + f'facilityID={facName}/' + 'siteInstantEmissionsByEquip-0.parquet')
    
    if unitIDs is None: 
        unitIDs = parq["unitID"].unique()
    
    for unitID in unitIDs:
        parq2 = parq[parq["unitID"] == unitID]
        parq2 = parq2[parq2["species"] == "METHANE"]
        parq2['tsValue'] = parq2['emissions_kgPerH']
        parq2['timestamp'] = parq2['timestamp_s']
        parq2['timestamp_days'] = parq2['timestamp'] / 86400.0
        parq2['nextTS'] = parq2['timestamp'] + parq2['duration_s']

        if pdfOnRight:
            fig = plt.figure(figsize=(12, 6))
            gs = GridSpec(1, 2, width_ratios=[3, 1], wspace=0.3)

            # Time series plot (left)
            ax_ts = fig.add_subplot(gs[0, 0])
            plot_standard_timeseries(ax_ts, parq2, interval_days)
            ax_ts.set_title(f'Time Series for UnitID: {unitID}')

            # PDF plot (right)
            ax_pdf = fig.add_subplot(gs[0, 1], sharey=ax_ts)
            plot_pdf(ax_pdf, parq2)

            plt.tight_layout()
            filename = f"{saveIn}/timeseries_with_pdf_{unitID}.png"
            plt.savefig(filename)
            print(f"Plot saved: {filename}")
            plt.close()

        else:
            fig, ax = plt.subplots(figsize=(10, 5))
            plot_standard_timeseries(ax, parq2, interval_days)
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
    ax_pdf.axhline(mean_emissions_value, color='red', linestyle='--', label=f'Mean: {mean_emissions_value:.2f} kg/h')

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
    parser.add_argument("-o", "--output", type=str, required=True, help="Input folder containing CSV files")
    parser.add_argument("-f", "--facilityName", type=str, help="Facility Name in parquet file 'facilityID=123-5391'")
    args = parser.parse_args()
    outputMC = args.output + '/' + args.output.split("/")[1] + '/0/'
    saveIn = args.output + '/parquet/' + 'plots/'
    parq = args.output + '/parquet/'
    os.makedirs(saveIn, exist_ok=True)
    byTypes = ['modelReadableName', 'unitID', 'modelEmissionCategory']

    tsMerged = preprocessTs(outputMC)
    plotStateMachineTS(tsMerged, unitIDList=['05-123-12697', 'sep_stage1_1', 'sep_stage2_1', 'tank_battery_OIL'], saveIn=saveIn, facName=args.facilityName)
    plotSiteInstantEmissionByEquip(parq=parq, unitIDs=None, saveIn=saveIn, facName=args.facilityName)
    plotTimeseriesPerUnitID(parq=parq, unitIDs=None, saveIn=saveIn, facName=args.facilityName)
    for byType in byTypes:
        plotPDFsAndCDFsUnitID(parq=parq, byType=byType, saveIn=saveIn, facName=args.facilityName)
    plotPDFsAndCDFsUnitID(parq=parq, byType='unitID', saveIn=saveIn, facName=args.facilityName, excludeModelReadableName='Tank Vents PRV', spPlotName='NO_PRV')
    # plotTimeseriesPerUnitID(parq=args.parquet, unitIDs=['tank_battery_OIL'], saveIn=saveIn, facName=args.facilityName, interval_days=5, pdfOnRight=True)
    pass


if __name__ == "__main__":
    main()