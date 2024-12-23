from bokeh.plotting import figure, output_file, show, save
from bokeh.models import HoverTool
from bokeh.layouts import column
import bokeh.palettes as ps
import logging
import AppUtils as au
import GraphUtils as gu
import Timeseries as ts
import pandas as pd
import Units as u
import numpy as np
import math
import json

logger = logging.getLogger(__name__)

KEYCOLSET = set(['facilityID', 'unitID', 'emitterID'])

# todo: clean up / rewrite core graph function
# todo: clean up y axis label
# todo: resample
# todo: multiple timeseries per graph -- named by unique (facilityID, unitID, emitterID) triplets??
# todo: error checking on empty dataframe after filter
# todo: integration with mdIndex file

timeUnitChanges = {
    "seconds": 1,
    "hours": u.SECONDS_PER_HOUR,
    "days": u.SECONDS_PER_DAY,
    "months": u.SECONDS_PER_MONTH,
    "years": u.SECONDS_PER_YEAR,
}


def filterEvents(eventDF, **kwargs):
    TRUE_MASK = pd.Series(True, index=eventDF.index)

    def _createFilteredMask(eventDF, paramName, paramVal, trueMask=TRUE_MASK):
        outMask = trueMask
        if paramVal is not None:
            outMask = eventDF[paramName] == paramVal
        return outMask

    fullMask = TRUE_MASK
    for singleMaskName, singleMaskVal in kwargs.items():
        if isinstance(singleMaskVal, float) and math.isnan(singleMaskVal):
            continue
        singleMask = _createFilteredMask(eventDF, singleMaskName, singleMaskVal)
        fullMask = fullMask & singleMask

    return eventDF[fullMask]


def determineRange(spec, filteredDF, colName):
    if spec.get('categorical', False):
        calcYRange = list(pd.Categorical(filteredDF[colName].drop_duplicates()))
    else:
        maxVal = filteredDF[colName].astype(float).max() * 1.1
        calcYRange = [0.0, maxVal if maxVal != 0.0 else 0.0001]  # Give a little margin for maximum rates
    return calcYRange


def toWorkTS(spec, filteredDF, colName):
    if spec.get('categorical', False) and spec['categorical']:
        workTS = ts.TimeseriesCategorical(filteredDF, valueColName=colName).toFullTimeseries()
        calcYVals = workTS.df['categories'].astype(str)
    else:
        filteredDF[colName] = filteredDF[colName].astype(float)
        filteredDF = pd.pivot_table(filteredDF, index=['timestamp', 'nextTS'], values=[colName], aggfunc=np.sum)
        filteredDF = filteredDF.reset_index()
        if 'tsUnits' in filteredDF.columns:
            unitsName = filteredDF['tsUnits'].unique()  # should check that this is unique
        else:
            unitsName = ''
        workTS = ts.TimeseriesRLE(filteredDF, units=unitsName, valueColName=colName, name=colName).toFullTimeseries()
        calcYVals = workTS.df['rate']

    return workTS, calcYVals


def graphTimeseries(inDF, spec, basePlot=None, timeUnits="Days"):
    logger.info(f"Graphing {spec}")
    if spec.get('filterSpec', False):
        filteredDF = filterEvents(inDF, **spec['filterSpec'])
    else:
        filteredDF = inDF

    if filteredDF.empty:
        logger.warning(f"Skipping empty filter: {spec}")
        return None

    instanceCols = list(KEYCOLSET - set(spec['filterSpec'].keys()))
    if instanceCols:
        instanceDF = filteredDF[['facilityID', 'unitID', 'emitterID']].drop_duplicates(subset=instanceCols)
    else:
        instanceDF = filteredDF[['facilityID', 'unitID', 'emitterID']].drop_duplicates()
    colName = spec['cols'][0]
    calcYRange = determineRange(spec, filteredDF, colName)
    xlabel_text = timeUnits
    ylabelDict = filteredDF.iloc[0].to_dict()
    ylabel = spec['yLabelFormat'].format(**ylabelDict)

    f1 = figure(width=1200, height=300,
                x_axis_label=f'{xlabel_text} since simulation start',
                y_axis_label=ylabel,
                y_range=calcYRange,
                title=spec['title']
                )

    numInstances = len(instanceDF)
    if numInstances <= 2:
        palette = ['red', 'blue']
        lineMult = 1
    elif numInstances <= 20:
        palette = ps.Category20[numInstances]
        lineMult = 1
    else:
        palette = ps.Inferno256
        lineMult = len(palette) // numInstances

    aggregateGraphs = spec.get("aggregate", "False").lower() == "true"
    aggregation = None
    timeUnits = timeUnits.strip().lower()
    if basePlot is not None:
        f1.x_range = basePlot.x_range

    for rowNum, singleInstance in instanceDF.reset_index(drop=True).iterrows():
        singleInstanceDict = singleInstance.to_dict()
        filteredInstanceDF = filterEvents(filteredDF, **singleInstanceDict)
        if aggregateGraphs:
            if not aggregation:
                aggregation = ts.TimeseriesRLE(filteredInstanceDF)
            else:
                thisTS = ts.TimeseriesRLE(filteredInstanceDF)
                aggregation = aggregation.addSquare(thisTS)
        workTS, calcYVals = toWorkTS(spec, filteredInstanceDF, colName)
        if not spec.get('categorical', False) and spec.get('resample', False):
            workTS = workTS.periodicAverage(
                list(range(workTS.df['timestamp'][:1].iat[-1].astype('int64'),
                           workTS.df['timestamp'][-1:].iat[-1].astype('int64'),
                           timeUnitChanges[timeUnits]))).toFullTimeseries()
        workTS.df = workTS.df.assign(origTimestamp=workTS.df['timestamp'],
                                     timestamp=workTS.df['timestamp'] / timeUnitChanges[timeUnits],
                                     )
        if basePlot is not None:
            f1.x_range = basePlot.x_range

        lineFormat = spec.get('lineFormat', colName)
        lineName = lineFormat.format(**singleInstanceDict)
        f1_line = f1.line(x=workTS.df['timestamp'], y=calcYVals,
                          line_color=palette[rowNum * lineMult], line_width=2, legend_label=lineName)
        f1.add_tools(HoverTool(line_policy='nearest', renderers=[f1_line], tooltips=[('x', '@x'), ('y', '@y')]))

    if len(f1.legend) > 0:
        f1.legend.click_policy = "hide"
        f1.legend.location = "top_left"

    return f1


def graphSpecList(eventTSDF, graphSpec, allFigs=None):
    if allFigs is None:
        allFigs = []
    for singleFig in graphSpec:
        graphs = []
        basePlot = None
        for singleSpec in singleFig['graphSpecs']:
            graph = graphTimeseries(eventTSDF, singleSpec, basePlot=basePlot, timeUnits=singleFig["timeUnits"])
            if basePlot is None:
                basePlot = graph
            if graph:
                graphs.append(graph)
        thisFig = {'graphName': singleFig['graphName'], 'figList': graphs}
        allFigs.append(thisFig)
    return allFigs


def outputGraph(config, graphName, figList):
    for singleFig in figList:
        outputFileTempl = singleFig.get('outputFilename', config['graphTemplate'])
        outputFilename = au.expandFilename(outputFileTempl, {**config, 'graphName': graphName})
        output_file(outputFilename)
        show(column(*singleFig['figList']))


#
# By default, generate graph specs that graph the states for each piece of major equipment,
#  as well as each emission for all emission categories
#

def generateGraphSpec(config, fullTSDF):
    allGraphs = []
    for (singleFacility, singleUnitID), unitGrp in fullTSDF.groupby(['facilityID', 'unitID']):
        if not singleFacility or not singleUnitID:
            continue
        graphSpecs = []
        graphName = f'{singleFacility}_{singleUnitID}'
        if unitGrp[unitGrp['modelCategory'] == "Probe"].empty:
            stateFilterSpec = {
                "title": f'State Transitions ({singleUnitID})',
                "cols": ['state'],
                'categorical': True,
                'yLabelFormat': 'State',
                'filter': True,
                'filterSpec': {
                    'facilityID': singleFacility,
                    'unitID': singleUnitID,
                    'command': 'STATE_TRANSITION',
                    'event': 'START',
                }
            }
            graphSpecs.append(stateFilterSpec)

            emissions = unitGrp[unitGrp['command'] == 'EMISSION']
            for singleCategory, emitterGrp in emissions.groupby(['modelCategory']):
                catFilterSpec = {
                    "title": f'{singleUnitID} {singleCategory} emissions (METHANE)',
                    "cols": ['emission'],
                    'yLabelFormat': f'Methane (kg/s)',
                    "lineFormat": "{emitterID}",
                    'filter': True,
                    'filterSpec': {
                        'facilityID': singleFacility,
                        'unitID': singleUnitID,
                        'command': 'EMISSION',
                        'modelCategory': singleCategory,
                        'species': 'METHANE'
                    }
                }
                graphSpecs.append(catFilterSpec)
            singleGraph = {'outputFilename': au.expandFilename(config['graphTemplate'], {**config, 'graphName': graphName}),
                           "timeUnits": "Days", 'graphSpecs': graphSpecs}
            allGraphs.append(singleGraph)

    for singleMDGroup, mdGrp in fullTSDF.groupby('mdGroup'):
        graphSpecs = []
        fluidFlows = mdGrp[mdGrp['command'] == 'FLUID-FLOW']
        graphName = f'{singleMDGroup}_FluidFlows'
        if not fluidFlows.empty:
            for (singleFacility, singleUnitID), unitGrp in fluidFlows.groupby(['facilityID', 'unitID']):
                if not singleFacility or not singleUnitID:
                    continue

                flowUnits = unitGrp['driverUnits'].iloc[0]
                mdFilterSpec = {
                    "title": f'{singleMDGroup} {singleFacility}-{singleUnitID} Fluid Flows',
                    "cols": ['driverRate'],
                    'categorical': False,
                    'yLabelFormat': f'Fluid  ({flowUnits}/s)',
                    "lineFormat": "{unitID}",
                    'filter': True,
                    'filterSpec': {
                        'facilityID': singleFacility,
                        'unitID': singleUnitID,
                        'command': 'FLUID-FLOW',
                        'mdGroup': singleMDGroup
                    }
                }
                graphSpecs.append(mdFilterSpec)
        singleGraph = {'outputFilename': au.expandFilename(config['graphTemplate'], {**config, 'graphName': graphName}),
                       "timeUnits": "Days", 'graphSpecs': graphSpecs}
        allGraphs.append(singleGraph)
    return allGraphs


def getGraphSpec(config, fullTSDF):
    if config.get("graphSpec", None) and config["graphSpec"] != "":
        graphSpecFilename = au.expandFilename(config['graphSpec'], config)
        with open(graphSpecFilename, "r") as iFile:
            graphSpec = json.load(iFile)
    else:
        graphSpec = generateGraphSpec(config, fullTSDF)
    return graphSpec


def graphMain(config):
    logging.basicConfig(level=logging.INFO)
    eventTSDF, metadata = gu.readCompleteEvents(config)
    graphSpec = getGraphSpec(config, eventTSDF)
    graphs = graphSpecList(eventTSDF, graphSpec)
    outputGraph(config, config['studyName'], graphs)


if __name__ == "__main__":
    config, args = au.getConfig()
    if not args.scenarioTimestamp:
        config = gu.findMostRecentScenario(config, args)
    graphMain(config)
