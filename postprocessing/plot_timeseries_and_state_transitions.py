import src.Timeseries as ts
import matplotlib.pyplot as plt
import numpy as np
import math
import os
import src.ParquetLib as pq

SECONDSINDAY = 86400
SECONDSINHOUR = 3600

def aggrSet(input_df, value_column, group_options=None):
    """Aggregates a DataFrame by specified options, creating Timeseries objects."""
    timeseries_set = []
    if group_options:
        input_df = input_df[input_df[group_options[0]] == group_options[1]]
    
    grouping_cols = ['facilityID', 'METype'] if value_column == "state" else ['facilityID', 'unitID', 'emitterID']
    TimeseriesClass = ts.TimeseriesCategorical if value_column == "state" else ts.TimeseriesRLE

    for _, subset_df in input_df.groupby(grouping_cols):
        timeseries_set.append(TimeseriesClass(subset_df, valueColName=value_column))

    return timeseries_set

def readParquetFiles(config, site, abnormal, mergeGC, additionalEventFilters):
    siteEVDF = pq.readParquetEvents(config, site=site, mergeGC=mergeGC, species="METHANE", additionalEventFilters=additionalEventFilters)
    siteEVDF = siteEVDF[siteEVDF["nextTS"] - siteEVDF["timestamp"] == siteEVDF["duration"]]
    siteEVDF = siteEVDF[siteEVDF['duration'] >= 0]
    siteEndSimDF = pq.readParquetSummary(config, site=site)

    if abnormal == "OFF":
        valid_emitter_ids = siteEVDF[siteEVDF['modelEmissionCategory'] != 'FUGITIVE']['emitterID']
        siteEVDF = siteEVDF[siteEVDF['emitterID'].isin(valid_emitter_ids)]

    return siteEVDF, siteEndSimDF

def grouping(siteEVDF, siteEndSimDF, valueColName, groupOptions):
    AllMcRuns = {}
    for mcRun, mcRunDF in siteEVDF.groupby('mcRun'):
        EndSimDF = siteEndSimDF[siteEndSimDF['mcRun'] == mcRun]
        simDuration = EndSimDF.loc[EndSimDF['command'] == 'SIM-STOP', 'timestamp'].values[0]
        timeseries_set = aggrSet(input_df=mcRunDF.sort_values(by=['nextTS'], ascending=[True]), value_column=valueColName, group_options=groupOptions)
        if not timeseries_set:
            continue
        totalTimeseriesSet = ts.TimeseriesSet(timeseries_set)

        if valueColName == "emission":
            tdf = totalTimeseriesSet.sum()
            tdf.df = tdf.df[tdf.df['nextTS'] <= simDuration]
            tdf.df.loc[:, 'tsValue'] = tdf.df['tsValue'] * SECONDSINHOUR
            AllMcRuns[mcRun] = tdf
        else:
            for tscat in totalTimeseriesSet.tsSetList:
                tscat.df = tscat.df[tscat.df["nextTS"] <= simDuration]

            AllMcRuns[mcRun] = totalTimeseriesSet.tsSetList
    if not AllMcRuns:
        raise ValueError("Group options do not match input data")
    return AllMcRuns

def calculateMeanEmissions(time_series_list, min_timestamp):
    """Calculates mean emissions for all MC runs or a specified MC run."""
    max_timestamp = max(td.df['timestamp'].max() for td in time_series_list)
    total_seconds = int((max_timestamp - min_timestamp) / SECONDSINHOUR) + 1

    emission_sum = np.zeros(total_seconds)
    emission_count = np.zeros(total_seconds)

    for tf in time_series_list:
        for i, row in tf.df.iterrows():
            start = int((row['timestamp'] - min_timestamp) / SECONDSINHOUR)
            end = int((tf.df.iloc[i + 1]['timestamp'] - min_timestamp) / SECONDSINHOUR) if i + 1 < len(tf.df) else total_seconds
            emission_sum[start:end] += row['tsValue']
            emission_count[start:end] += 1

    return emission_sum / np.where(emission_count == 0, 1, emission_count)

def plotMeanEmissions(ax, mean_emissions, fac, abnormal):
    """Plots the mean emissions on the provided axis."""
    time_range = np.arange(len(mean_emissions)) * SECONDSINHOUR / SECONDSINDAY
    ax.plot(time_range, mean_emissions, color='black', linewidth=2, label='Mean Emissions')
    ax.set_xlabel('Time (days)', fontsize=14)
    ax.set_ylabel('CH4 Emissions (kg/h)', fontsize=14)
    ax.set_title(f'Mean Emissions - Site: {fac} \n Abnormal: {abnormal}', fontsize=14)
    ax.legend(fontsize=14)
    ax.grid(alpha=0.3)

def plotStateTS(config, AllMCruns_states, AllMCruns, groupOptions, abnormal, mcRunTs=None, mcRunStates=None):
    """Plots Mean Emissions as the first subplot and State Transitions for each run, with a max of 4 subplots per figure."""
    
    mcRunStates = 0 if not mcRunStates else int(mcRunStates)

    if mcRunStates not in AllMCruns_states:
        print(f"MC Run {mcRunStates} not found in AllMCruns_states")
        return

    if not mcRunTs:
        tsf = [t.toFullTimeseries() for t in AllMCruns.values()]
    else:
        mcRunTs = int(mcRunTs)
        tS = AllMCruns[mcRunTs].toFullTimeseries()

    allStateTS = AllMCruns_states[mcRunStates]
    num_states = len(allStateTS)
    fac = config['site']
    plot_num = 0
    # Plot in batches of 3 state transitions per figure (plus 1 for the time series)
    for batch_start in range(0, num_states, 3):
        # Define the number of subplots in this figure (1 time series + up to 3 state transitions)
        num_plots_in_figure = min(4, num_states - batch_start + 1)
        num_rows = math.ceil(num_plots_in_figure / 1)
        fig, axes = plt.subplots(num_rows, 1, figsize=(15, 5 * num_rows))
        axes = axes.flatten()
        if mcRunTs:
            start_time = tS._startTimes.min() / SECONDSINDAY
            end_time = tS._startTimes.max() / SECONDSINDAY
            ticks = tS._startTimes / SECONDSINDAY
            axes[0].set_xlim(left=start_time, right=end_time)
            axes[0].set_xticks(ticks, minor=True)
            # Plot Time series in the first subplot
            axes[0].plot(ticks, tS._values, color='blue', label=f"McRun = {mcRunStates}")
            axes[0].set_xlabel('Time (days)', fontsize=14)
            axes[0].set_ylabel('CH4 Emissions (kg/h)', fontsize=14)
            axes[0].set_title(f'TimeSeries - Site: {fac} \n Abnormal: {abnormal}', fontsize=14)
            if groupOptions:
                axes[0].set_title(f'TimeSeries - Site: {fac} \n {groupOptions[0]}: {groupOptions[1]} \n Abnormal: {abnormal}', fontsize=14)
            axes[0].legend(fontsize=14)
            axes[0].grid(alpha=0.3)
        else:
            min_timestamp = min(df['timestamp'].min() for df in [ts.df for ts in tsf])
            mean_emissions = calculateMeanEmissions(tsf, min_timestamp)
            for df in [t.df for t in tsf]:
                start_time = df["timestamp"].min() / SECONDSINDAY
                end_time = df["timestamp"].max() / SECONDSINDAY
                axes[0].set_xlim(left=start_time, right=end_time)
                axes[0].set_xticks(df['timestamp'] / SECONDSINDAY, minor=True)
                axes[0].plot((df['timestamp'] - df['timestamp'].min()) / SECONDSINDAY, df['tsValue'], alpha=0.2, color='royalblue')
            plotMeanEmissions(axes[0], mean_emissions, fac, abnormal)
        # Plot each state transition in subsequent subplots
        for i, state_ts in enumerate(allStateTS[batch_start:batch_start + 3], start=1):
            ax = axes[i]
            states = state_ts.toFullTimeseries().df
            ax.step(states["timestamp"] / SECONDSINDAY, states["tsValue"])
            ax.set_xlim(left=start_time, right=end_time)
            ax.set_xticks(states["timestamp"] / SECONDSINDAY, minor=True)
            ax.set_xlabel('Time (days)', fontsize=12)
            ax.set_ylabel('State', fontsize=12)
            
            if groupOptions and groupOptions[0] == "unitID":
                ax.set_title(f"State Transitions -\n {groupOptions[0]}: {groupOptions[1]}\nMCrun: {mcRunStates}", fontsize=14)
            else:
                for unitid, unitidDF in state_ts.df.groupby("unitID"):
                    unitts = ts.TimeseriesCategorical(unitidDF, valueColName="state").toFullTimeseries().df
                    ax.step(unitts["timestamp"] / SECONDSINDAY, unitts["tsValue"], label=unitid)
                ax.set_title(f'State Transitions - {state_ts.df["METype"].unique()[0]} for {fac}\nMCrun: {mcRunStates}', fontsize=14)
            
            ax.legend()
            ax.grid(alpha=0.3)
        
        # Hide any unused subplots in this figure
        for j in range(num_plots_in_figure, len(axes)):
            fig.delaxes(axes[j])
        if config["stpDirectory"]:
            dr = config["stpDirectory"]
            plot_dir = os.path.join(config['simulationRoot'], f"summaries/{dr}")
        else:
            plot_dir = os.path.join(config['simulationRoot'], "summaries/StatesPlots")
        os.makedirs(plot_dir, exist_ok=True)
        output_image_path = os.path.join(plot_dir, f"state_transition_by_mcRun={mcRunStates}_plot={plot_num}.png")
        plot_num += 1
 
        plt.tight_layout(pad=10.0)
        plt.savefig(output_image_path)
        plt.close()


def main(config, groupOptions=None, abnormal="ON", mcRunTs=None, mcRunStates=None):
    siteEVDF, siteEndSimDF = readParquetFiles(config=config, site=config['siteName'], abnormal=abnormal, mergeGC=True, additionalEventFilters=[('command', '=', 'EMISSION')])
    AllMCruns = grouping(siteEVDF=siteEVDF, siteEndSimDF=siteEndSimDF, valueColName="emission", groupOptions=groupOptions)

    # Get state transitions
    siteEVDF_state, siteEndSimDF_state = readParquetFiles(config=config, site=config['siteName'], abnormal=abnormal, mergeGC=False, additionalEventFilters=[('command', '=', 'STATE_TRANSITION')])
    AllMCruns_states = grouping(siteEVDF=siteEVDF_state, siteEndSimDF=siteEndSimDF_state, valueColName="state", groupOptions=groupOptions)

    # Plot state transitions with mean emissions
    plotStateTS(config, AllMCruns_states, AllMCruns, groupOptions, abnormal=abnormal, mcRunTs=mcRunTs,mcRunStates=mcRunStates) 