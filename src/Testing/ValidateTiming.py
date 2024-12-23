import GraphUtils as gu
import AppUtils as au
import logging
import Distribution as d
import Units as u
import SummaryMain as sm
import SimDataManager as sdm


logger = logging.getLogger(__name__)

def getStateCountWideDF(stateTimingDF, metadata, equipmentType, listOfStates):
    eventsWithMD = stateTimingDF.merge(metadata, on=['facilityID', 'unitID', 'emitterID'])
    contSepEvents = eventsWithMD[eventsWithMD['equipmentType'] == equipmentType]
    if contSepEvents.empty:
        return contSepEvents

    stateCountWideDF = (contSepEvents.set_index(['facilityID', 'unitID', 'emitterID', 'state'])
                        [['count', 'duration', 'maxDuration', 'minDuration', 'meanDuration']]
                        .unstack()
                        .reset_index()
                        )
    contSepEvents.loc[:, 'mcIter'] = 0
    counts = contSepEvents.pivot_table(values=['totalDuration', 'totalCount', 'mcIter'],
                                   index=['unitID'])

    if not counts.empty:
        counts.loc[counts['mcIter']==0, 'mcIter'] = ""

    counts.columns = gu.pd.MultiIndex.from_product([counts.columns, ['']])
    stateCountWideDF = stateCountWideDF.merge(counts, on='unitID')
    stateCountWideDF = stateCountWideDF.set_index(['facilityID', 'unitID', 'emitterID', 'mcIter'])

    # Assign CountPct Columns
    listOfStatesVal = [x.lower() + "CountPct" for x in listOfStates]
    kwargs = {}
    for i in range(len(listOfStates)):
        kwargs[listOfStatesVal[i]] = stateCountWideDF['count', listOfStates[i]] / stateCountWideDF['totalCount']
    stateCountWideDF = stateCountWideDF.assign(**kwargs)

    # Assign DurPct Columns
    listOfStatesVal = [x.lower() + "DurPct" for x in listOfStates]
    kwargs = {}
    for i in range(len(listOfStates)):
        kwargs[listOfStatesVal[i]] = stateCountWideDF['duration', listOfStates[i]] / stateCountWideDF['totalDuration']
    stateCountWideDF = stateCountWideDF.assign(**kwargs)

    return stateCountWideDF

def calcExpOpMinDur(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['opDurMin']*u.SECONDS_PER_DAY

def calcExpOpMeanDur(key, eqTable):
    eqEntry = eqTable[key]
    mean = (eqEntry['opDurMin'] + eqEntry['opDurMax'])/2*u.SECONDS_PER_DAY
    return mean

def calcExpOpMaxDur(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['opDurMax']*u.SECONDS_PER_DAY

def calcExpMalfMinDur(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['malfDurMin']*u.SECONDS_PER_DAY

def calcExpMalfMeanDur(key, eqTable):
    eqEntry = eqTable[key]
    mean = (eqEntry['malfDurMin'] + eqEntry['malfDurMax'])/2*u.SECONDS_PER_DAY
    return mean

def calcExpMalfMaxDur(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['malfDurMax']*u.SECONDS_PER_DAY

def calcExpUnlitMinDur(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['unlitDurMin']*u.SECONDS_PER_DAY

def calcExpUnlitMeanDur(key, eqTable):
    eqEntry = eqTable[key]
    mean = (eqEntry['unlitDurMin'] + eqEntry['unlitDurMax'])/2*u.SECONDS_PER_DAY
    return mean

def calcExpUnlitMaxDur(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['unlitDurMax']*u.SECONDS_PER_DAY

def calcPMalfunction(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['pMalfunction']

def calcPUnlit(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['pUnlit']

def calcExpProdMinDur(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['productionDurationMinSeconds']

def calcExpProdMeanDur(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['meanProductionTimeSeconds']

def calcExpProdMaxDur(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['productionDurationMaxSeconds']

def calcExpProdMeanDurConstWell(key, eqTable):
    eqEntry = eqTable[key]
    mean = (eqEntry['productionDurationMinSeconds'] + eqEntry['productionDurationMaxSeconds'])/2
    return mean

def calcExpShutInMinDur(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['shutInDurationMinSeconds']

def calcExpShutInMaxDur(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['shutInDurationMaxSeconds']

def calcExpShutInMeanDur(key, eqTable):
    eqEntry = eqTable[key]
    mean = (eqEntry['shutInDurationMinSeconds'] + eqEntry['shutInDurationMaxSeconds'])/2
    return mean

def calcExpDumpTime(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['dumpTime']

def calcExpSDVFailDurMin(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['stuckDumpValveDurMinDays']*u.SECONDS_PER_DAY

def calcExpSDVFailDurMean(key, eqTable):
    eqEntry = eqTable[key]
    mean = (eqEntry['stuckDumpValveDurMinDays'] + eqEntry['stuckDumpValveDurMaxDays'])/2*u.SECONDS_PER_DAY
    return mean

def calcExpSDVFailDurMax(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['stuckDumpValveDurMaxDays']*u.SECONDS_PER_DAY

def calcExpFailDumpValDurMean(key, eqTable):
    eqEntry = eqTable[key]
    return (eqEntry['stuckDumpValveDurMinDays']+eqEntry['stuckDumpValveDurMaxDays'])/2*u.SECONDS_PER_DAY

def calcExpStuckDumpValPLeak(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['stuckDumpValvepLeak']

def calcExpStuckDumpValCC(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['stuckDumpValveCC']

def calcExpOPVentMTTRmin(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['tankOverpressureMTTRMinDays']*u.SECONDS_PER_DAY

def calcExpOPVentMTTRmean(key, eqTable):
    eqEntry = eqTable[key]
    return (eqEntry['tankOverpressureMTTRMinDays'] + eqEntry['tankOverpressureMTTRMaxDays'])/2*u.SECONDS_PER_DAY

def calcExpOPVentMTTRmax(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['tankOverpressureMTTRMaxDays']*u.SECONDS_PER_DAY

def calcExpFlareStateCounts(key, eqTable, newConfig):
    eqEntry = eqTable[key]
    opDurMean = (eqEntry['opDurMin'] + eqEntry['opDurMax'])/2
    malfDurMean = (eqEntry['malfDurMin'] + eqEntry['malfDurMax'])/2 * eqEntry['pMalfunction']
    unlitDurMean = (eqEntry['unlitDurMin'] + eqEntry['unlitDurMax'])/2 * eqEntry['pUnlit']
    oneCycle = opDurMean + malfDurMean + unlitDurMean
    totalNumberOfStates  = newConfig.get('simDurationSeconds', 0)/u.SECONDS_PER_DAY / oneCycle * 2
    opStateCount = totalNumberOfStates/2
    malfStateCount = totalNumberOfStates/2*eqEntry['pMalfunction']
    unlitStateCount = totalNumberOfStates/2*eqEntry['pUnlit']
    return opStateCount, malfStateCount, unlitStateCount

def calcExpFlareOpCounts(key, eqTable, newConfig):
    opStateCount,_,_ = calcExpFlareStateCounts(key, eqTable, newConfig)
    return opStateCount

def calcExpFlareMalfCounts(key, eqTable, newConfig):
    _,malfStateCount,_ = calcExpFlareStateCounts(key, eqTable, newConfig)
    return malfStateCount

def calcExpFlareUnlitCounts(key, eqTable, newConfig):
    _,_,unlitStateCount = calcExpFlareStateCounts(key, eqTable, newConfig)
    return unlitStateCount

def calcCSExpStatesCount(key, eqTable, newConfig):
    eqEntry = eqTable[key]
    pLeak = eqEntry['stuckDumpValvepLeak']
    MTTR = (eqEntry['stuckDumpValveDurMinDays'] + eqEntry['stuckDumpValveDurMaxDays'])/2
    MTBF = MTTR * (1-pLeak)/pLeak
    failureCycle = (MTTR + MTBF) * u.SECONDS_PER_DAY
    simDuration = newConfig.get('simDurationSeconds', 0)
    SDVStateCount = round(simDuration/failureCycle, 0) - 1 #-1 because I'm dropping initial and last state
    opStateCount = SDVStateCount
    expOpDurMean = MTBF * u.SECONDS_PER_DAY
    expOpDurMax = eqEntry['stuckDumpValveDurMaxDays'] * (1 - pLeak)/pLeak * u.SECONDS_PER_DAY
    expOpDurMin = eqEntry['stuckDumpValveDurMinDays'] * (1 - pLeak)/pLeak * u.SECONDS_PER_DAY
    return opStateCount, SDVStateCount, expOpDurMin, expOpDurMean, expOpDurMax

def calcCSExpOpStateCount(key, eqTable, newConfig):
    opStateCount, _, _, _, _ = calcCSExpStatesCount(key, eqTable, newConfig)
    return opStateCount

def calcCSExpSDVStateCount(key, eqTable, newConfig):
    _, SDVStateCount, _, _, _ = calcCSExpStatesCount(key, eqTable, newConfig)
    return SDVStateCount

def calcCSExpOpDurMax(key, eqTable, newConfig):
    _, _, _, _, expOpDurMax = calcCSExpStatesCount(key, eqTable, newConfig)
    return expOpDurMax

def calcCSExpOpDurMin(key, eqTable, newConfig):
    _, _, expOpDurMin, _, _ = calcCSExpStatesCount(key, eqTable, newConfig)
    return expOpDurMin

def calcCSExpOpDurMean(key, eqTable, newConfig):
    _, _, _, expOpDurMean, _ = calcCSExpStatesCount(key, eqTable, newConfig)
    return expOpDurMean

def calcExpOPLeakStateCount(key, eqTable, newConfig):
    OPLeakStateCount, _, _, _ = calcExpOPLeakStateDurAndCount(key, eqTable, newConfig)
    return OPLeakStateCount

def calcExpOPLeakDurMin(key, eqTable, newConfig):
    _, expOPLeakDurMin, _, _ = calcExpOPLeakStateDurAndCount(key, eqTable, newConfig)
    return expOPLeakDurMin

def calcExpOPLeakDurMean(key, eqTable, newConfig):
    _, _, expOPLeakDurMean, _ = calcExpOPLeakStateDurAndCount(key, eqTable, newConfig)
    return expOPLeakDurMean

def calcExpOPLeakDurMax(key, eqTable, newConfig):
    _, _, _, expOPLeakDurMax = calcExpOPLeakStateDurAndCount(key, eqTable, newConfig)
    return expOPLeakDurMax

def calcExpOPVentStateCount(key, eqTable, newConfig):
    eqEntry = eqTable[key]
    pLeak = eqEntry['tankOverpressurePLeak']
    MTTR = (eqEntry['tankOverpressureMTTRMinDays'] + eqEntry['tankOverpressureMTTRMaxDays'])/2
    MTBF = MTTR * (1-pLeak)/pLeak
    failureCycle = (MTTR + MTBF)*u.SECONDS_PER_DAY
    simDuration = newConfig.get('simDurationSeconds', 0)
    OPStateCount = simDuration/failureCycle
    return OPStateCount

def calcDSSDVStateCount(key, eqTable, newConfig):
    eqEntry = eqTable[key]
    pLeak = eqEntry['stuckDumpValvepLeak']
    MTTR = (eqEntry['stuckDumpValveDurMinDays'] + eqEntry['stuckDumpValveDurMaxDays'])/2
    MTBF = MTTR * (1-pLeak)/pLeak
    failureCycle = (MTTR + MTBF)*u.SECONDS_PER_DAY
    simDuration = newConfig.get('simDurationSeconds', 0)
    SDVStateCount = simDuration/failureCycle
    return SDVStateCount

def calcExpOPLeakStateDurAndCount(key, eqTable, newConfig):
    eqEntry = eqTable[key]
    pLeak = eqEntry['tankOverpressurePLeak']
    MTTR = (eqEntry['tankOverpressureMTTRMinDays'] + eqEntry['tankOverpressureMTTRMaxDays'])/2
    MTBF = MTTR * (1-pLeak)/pLeak
    failureCycle = (MTTR + MTBF)*u.SECONDS_PER_DAY
    simDuration = newConfig.get('simDurationSeconds', 0)
    OPLeakStateCount = simDuration/failureCycle
    expOpDurMean = MTBF * u.SECONDS_PER_DAY
    expOpDurMax = eqEntry['tankOverpressureMTTRMaxDays'] * (1 - pLeak)/pLeak * u.SECONDS_PER_DAY
    expOpDurMin = eqEntry['tankOverpressureMTTRMinDays'] * (1 - pLeak)/pLeak * u.SECONDS_PER_DAY
    expOPLeakDurMin = eqEntry['tankOverpressureMTTRMinDays'] * u.SECONDS_PER_DAY
    expOPLeakDurMean = MTTR * u.SECONDS_PER_DAY
    expOPLeakDurMax = eqEntry['tankOverpressureMTTRMaxDays'] * u.SECONDS_PER_DAY
    return OPLeakStateCount, expOPLeakDurMin, expOPLeakDurMean, expOPLeakDurMax

def validateCycWellStates(stateTimingDF, metadata, eqTable, newConfig, val_tol):
    equipmentType = "MEETWellCycledProduction"
    listOfStates = ["PRODUCTION", "SHUT_IN"]
    stateCountWideDF = getStateCountWideDF(stateTimingDF, metadata, equipmentType, listOfStates)

    stateCountWideDF = stateCountWideDF.assign(
        expProdMinDuration=stateCountWideDF.index.map(lambda x: calcExpProdMinDur(x, eqTable)),
        expProdMeanDuration=stateCountWideDF.index.map(lambda x: calcExpProdMeanDur(x, eqTable)),
        expProdMaxDuration=stateCountWideDF.index.map(lambda x: calcExpProdMaxDur(x, eqTable)),
        expShutInMinDuration=stateCountWideDF.index.map(lambda x: calcExpShutInMinDur(x, eqTable)),
        expShutInMeanDuration=stateCountWideDF.index.map(lambda x: calcExpShutInMeanDur(x, eqTable)),
        expShutInMaxDuration=stateCountWideDF.index.map(lambda x: calcExpShutInMaxDur(x, eqTable)),
        expProdStateCount = newConfig.get('simDurationSeconds', 0)/u.SECONDS_PER_DAY,
        expShutInStateCount = newConfig.get('simDurationSeconds', 0)/u.SECONDS_PER_DAY
    )

    # Validate expected and observed results for cycling well states
    stateCountWideDF = stateCountWideDF.assign(
        valProdTimeMin = lambda x: x['minDuration', 'PRODUCTION'] >= x['expProdMinDuration'],
        valProdTimeMean = lambda x: x['meanDuration', 'PRODUCTION'].between(x['expProdMeanDuration'] * (1 - val_tol),
                                                                          x['expProdMeanDuration'] * (1 + val_tol)),
        valProdTimeMax = lambda x: x['maxDuration', 'PRODUCTION'] <= x['expProdMaxDuration'],
        valShutInTimeMin = lambda x: x['minDuration', 'SHUT_IN'] >= x['expShutInMinDuration'],
        valShutInTimeMean = lambda x: x['meanDuration', 'SHUT_IN'].between(x['expShutInMeanDuration'] * (1 - val_tol),
                                                                          x['expShutInMeanDuration'] * (1 + val_tol)),
        valShutInTimeMax = lambda x: x['maxDuration', 'SHUT_IN'] <= x['expShutInMaxDuration'],
        valProdStateCount = lambda x: x['count', 'PRODUCTION'].between(x['expProdStateCount'] * (1 - val_tol),
                                                                          x['expProdStateCount'] * (1 + val_tol)),
        valShutInStateCount=lambda x: x['count', 'SHUT_IN'].between(x['expShutInStateCount'] * (1 - val_tol),
                                                                      x['expShutInStateCount'] * (1 + val_tol)),
    )
    sm.dumpSummary(stateCountWideDF, newConfig, 'cycWellValidationTemplate')
    print(stateCountWideDF.to_string())
    pass

def validateContWellStates(stateTimingDF, metadata, eqTable, newConfig, val_tol):
    equipmentType = "MEETWellContinuousProduction"
    listOfStates = ['PRODUCTION']
    stateCountWideDF = getStateCountWideDF(stateTimingDF, metadata, equipmentType, listOfStates)

    stateCountWideDF = stateCountWideDF.assign(
        expProdMinDuration=stateCountWideDF.index.map(lambda x: calcExpProdMinDur(x, eqTable)),
        expProdMeanDuration=stateCountWideDF.index.map(lambda x: calcExpProdMeanDurConstWell(x, eqTable)),
        expProdMaxDuration=stateCountWideDF.index.map(lambda x: calcExpProdMaxDur(x, eqTable)),
        expOpStateCount = 1
    )

    # Validate expected and observed results for cycling well states
    stateCountWideDF = stateCountWideDF.assign(
        valProdTimeMin = lambda x: x['minDuration', 'PRODUCTION'] >= x['expProdMinDuration'],
        valProdTimeMean = lambda x: x['meanDuration', 'PRODUCTION'].between(x['expProdMeanDuration'] * (1 - val_tol),
                                                                          x['expProdMeanDuration'] * (1 + val_tol)),
        valProdTimeMax = lambda x: x['maxDuration', 'PRODUCTION'] <= x['expProdMaxDuration'],
        valOpStateCount = lambda x: x['count', 'PRODUCTION'] == 1
    )
    sm.dumpSummary(stateCountWideDF, newConfig, 'constWellValidationTemplate')
    print(stateCountWideDF.to_string())
    pass

def calcCSExpStatesCount(key, eqTable, newConfig):
    eqEntry = eqTable[key]
    pLeak = eqEntry['stuckDumpValvepLeak']
    MTTR = (eqEntry['stuckDumpValveDurMinDays'] + eqEntry['stuckDumpValveDurMaxDays'])/2
    MTBF = MTTR * (1-pLeak)/pLeak
    failureCycle = (MTTR + MTBF)*u.SECONDS_PER_DAY
    simDuration = newConfig.get('simDurationSeconds', 0)
    SDVStateCount = simDuration/failureCycle
    opStateCount = SDVStateCount + 1
    expOpDurMean = MTBF * u.SECONDS_PER_DAY
    expOpDurMax = eqEntry['stuckDumpValveDurMaxDays'] * (1 - pLeak)/pLeak * u.SECONDS_PER_DAY
    expOpDurMin = eqEntry['stuckDumpValveDurMinDays'] * (1 - pLeak)/pLeak * u.SECONDS_PER_DAY
    return opStateCount, SDVStateCount, expOpDurMin, expOpDurMean, expOpDurMax

def calcCSExpOpStateCount(key, eqTable, newConfig):
    opStateCount, _, _, _, _ = calcCSExpStatesCount(key, eqTable, newConfig)
    return opStateCount

def calcCSExpSDVStateCount(key, eqTable, newConfig):
    _, SDVStateCount, _, _, _ = calcCSExpStatesCount(key, eqTable, newConfig)
    return SDVStateCount

def calcCSExpOpDurMax(key, eqTable, newConfig):
    _, _, _, _, expOpDurMax = calcCSExpStatesCount(key, eqTable, newConfig)
    return expOpDurMax

def calcCSExpOpDurMin(key, eqTable, newConfig):
    _, _, expOpDurMin, _, _ = calcCSExpStatesCount(key, eqTable, newConfig)
    return expOpDurMin
def validateDumpSepStates(stateTimingDF, metadata, eqTable, newConfig):
    eventsWithMD = stateTimingDF.merge(metadata, on=['facilityID', 'unitID', 'emitterID'])
    contSepEvents = eventsWithMD[eventsWithMD['equipmentType'] == 'MEETDumpingSeparator']
    stateCountWideDF = (contSepEvents.set_index(['facilityID', 'unitID', 'emitterID', 'state'])
                        [['count', 'duration', 'maxDuration', 'minDuration', 'meanDuration']]
                        .unstack()
                        .reset_index()
                        )
    contSepEvents.loc[:, 'mcIter'] = 0
    counts = contSepEvents.pivot_table(values=['totalDuration', 'totalCount', 'mcIter'],
                                   index=['unitID'])
    if counts.empty:
        return

    counts.loc[counts['mcIter']==0, 'mcIter'] = ""

def calcCSExpOpDurMean(key, eqTable, newConfig):
    _, _, _, expOpDurMean, _ = calcCSExpStatesCount(key, eqTable, newConfig)
    return expOpDurMean

def validateContSepStates(stateTimingDF, metadata, eqTable, newConfig, val_tol):
    equipmentType = "MEETContinuousSeparator"
    listOfStates = ["OPERATING", "STUCK_DUMP_VALVE"]
    stateCountWideDF = getStateCountWideDF(stateTimingDF, metadata, equipmentType, listOfStates).fillna(0)

    stateCountWideDF = stateCountWideDF.assign(
        pLeakSDV=lambda x: x['count', 'STUCK_DUMP_VALVE'] / x['totalCount'],
    )

    stateCountWideDF = stateCountWideDF.assign(
        expOpDurMin=stateCountWideDF.index.map(lambda x: calcCSExpOpDurMin(x, eqTable, newConfig)),
        expOpDurMean=stateCountWideDF.index.map(lambda x: calcCSExpOpDurMean(x, eqTable, newConfig)),
        expOpDurMax=stateCountWideDF.index.map(lambda x: calcCSExpOpDurMax(x, eqTable, newConfig)),
        expFailDumpValDurMin=stateCountWideDF.index.map(lambda x: calcExpSDVFailDurMin(x, eqTable)),
        expFailDumpValDurMean=stateCountWideDF.index.map(lambda x: calcExpSDVFailDurMean(x, eqTable)),
        expFailDumpValDurMax=stateCountWideDF.index.map(lambda x: calcExpSDVFailDurMax(x, eqTable)),
        expOpStateCount=stateCountWideDF.index.map(lambda x: calcCSExpOpStateCount(x, eqTable, newConfig)),
        expSDVStateCount=stateCountWideDF.index.map(lambda x: calcCSExpSDVStateCount(x, eqTable, newConfig)),

        expSDVPLeak=stateCountWideDF.index.map(lambda x: calcExpStuckDumpValPLeak(x, eqTable)),
        #expStuckDumpValCC=stateCountWideDF.index.map(lambda x: calcExpStuckDumpValCC(x, eqTable)),

    )

    # Validate expected and observed results for dump separator states
    stateCountWideDF = stateCountWideDF.assign(
        valOpDurMin=lambda x: x['minDuration', 'OPERATING'] >= x['expOpDurMin'],
        valOpDurMean=lambda x: x['meanDuration', 'OPERATING'].between(x['expOpDurMean'] * (1 - val_tol), x['expOpDurMean'] * (1 + val_tol)),
        valOpDurMax=lambda x: x['maxDuration', 'OPERATING'] <= x['expOpDurMax'],
        valSDVFailDurMin=lambda x: x['minDuration', 'STUCK_DUMP_VALVE'] >= x['expFailDumpValDurMin'],
        valSDVFailDurMean=lambda x: x['meanDuration', 'STUCK_DUMP_VALVE'].between(x['expFailDumpValDurMean']* (1 - val_tol), x['expFailDumpValDurMean']* (1 + val_tol)),
        valSDVFailDurMax=lambda x: x['maxDuration', 'STUCK_DUMP_VALVE'] <= x['expFailDumpValDurMax'],
        valOpStateCount=lambda x: x['count', 'OPERATING'].between(x['expOpStateCount'] * (1 - val_tol), x['expOpStateCount'] * (1 + val_tol)),
        valSDVStateCount = lambda x: x['count', 'STUCK_DUMP_VALVE'].between(x['expSDVStateCount']* (1 - val_tol), x['expSDVStateCount']* (1 + val_tol)),
        valSDVPLeak = lambda x: x['pLeakSDV'].between(x['expSDVPLeak'] * (1-val_tol), x['expSDVPLeak'] * (1+val_tol)),
    )
    print(stateCountWideDF.to_string())
    sm.dumpSummary(stateCountWideDF, newConfig, 'contSepValidationTemplate')

def calcExpDumpStateCount(key, eqTable, newConfig):
    eqEntry = eqTable[key]
    dumpVol = eqEntry['dumpVolume']
    simDuration = newConfig.get('simDurationSeconds', 0)
    return simDuration

def calcWellProd(key, eqTable):
    eqEntry = eqTable[key]
    wellProd = eqEntry['QiOilBblPerDay'] + eqEntry['QiWaterBblPerDay']
    return wellProd

def calcDumpVol(key, eqTable):
    eqEntry = eqTable[key]
    return eqEntry['dumpVolume']

def calcDSFillingDurMean(key, eqTable):
    eqEntry = eqTable[key]
    dumpVol = eqEntry['dumpVolume']
    return eqEntry['dumpVolume']

def validateDumpSepStates(stateTimingDF, metadata, eqTable, newConfig, val_tol):
    equipmentType = "MEETWellContinuousProduction"
    listOfStates = ['PRODUCTION']
    stateCountWideDFWell = getStateCountWideDF(stateTimingDF, metadata, equipmentType, listOfStates)

    equipmentType = "MEETDumpingSeparator"
    listOfStates = ["DUMPING", "FILLING", "STUCK_DUMP_VALVE"]
    stateCountWideDF = getStateCountWideDF(stateTimingDF, metadata, equipmentType, listOfStates)

    stateCountWideDF = stateCountWideDF.assign(
        fluidDailyProd=stateCountWideDFWell.index.map(lambda x: calcWellProd(x, eqTable)),
        dumpVol=stateCountWideDF.index.map(lambda x: calcDumpVol(x, eqTable))
    )

    simDurDays = newConfig.get('simDurationSeconds', 0)/u.SECONDS_PER_DAY
    stateCountWideDF = stateCountWideDF.assign(
        expFillingStateCount=lambda x: x['fluidDailyProd']*simDurDays/x['dumpVol'],
        expFillingMeanDur=lambda x: x['dumpVol']/x['fluidDailyProd']*u.SECONDS_PER_DAY,
        expSDVStateCount=stateCountWideDF.index.map(lambda x: calcDSSDVStateCount(x, eqTable, newConfig)),
        expSDVPLeak=stateCountWideDF.index.map(lambda x: calcExpStuckDumpValPLeak(x, eqTable))
    )

    stateCountWideDF = stateCountWideDF.assign(
        expDumpStateCount=lambda x: x['expFillingStateCount'] - x['expSDVStateCount'],
        expDumpTime=stateCountWideDF.index.map(lambda x: calcExpDumpTime(x, eqTable)),
        expFailDumpValDurMin=stateCountWideDF.index.map(lambda x: calcExpSDVFailDurMin(x, eqTable)),
        expFailDumpValDurMean=stateCountWideDF.index.map(lambda x: calcExpSDVFailDurMean(x, eqTable)),
        expFailDumpValDurMax=stateCountWideDF.index.map(lambda x: calcExpSDVFailDurMax(x, eqTable)),
    )

    # Validate expected and observed results for dump separator states
    stateCountWideDF = stateCountWideDF.assign(
        valFillingDurMean=lambda x: x['meanDuration', 'FILLING'].between(x['expFillingMeanDur'] * (1 - val_tol),
                                                                     x['expFillingMeanDur'] * (1 + val_tol)),
        valFillingStateCount=lambda x: x['count', 'FILLING'].between(x['expFillingStateCount']* (1 - val_tol), x['expFillingStateCount']* (1 + val_tol)),
        valDumpStateCount=lambda x: x['count', 'DUMPING'].between(x['expDumpStateCount'] * (1 - val_tol),
                                                                     x['expDumpStateCount'] * (1 + val_tol)),
        valDumpTimeMin=lambda x: x['minDuration', 'DUMPING'] == x['expDumpTime'],
        valDumpTimeMean=lambda x: x['meanDuration', 'DUMPING'] == x['expDumpTime'],
        valDumpTimeMax=lambda x: x['maxDuration', 'DUMPING'] == x['expDumpTime'],
        valSDVFailDurMin=lambda x: x['minDuration', 'STUCK_DUMP_VALVE'] >= x['expFailDumpValDurMin'],
        valSDVFailDurMean=lambda x: x['meanDuration', 'STUCK_DUMP_VALVE'].between(x['expFailDumpValDurMean']* (1 - val_tol), x['expFailDumpValDurMean']* (1 + val_tol)),
        valSDVFailDurDurMax=lambda x: x['maxDuration', 'STUCK_DUMP_VALVE'] <= x['expFailDumpValDurMax'],
        valSDVStateCount=lambda x: x['count', 'STUCK_DUMP_VALVE'].between(x['expSDVStateCount'] * (1 - val_tol), x['expSDVStateCount'] * (1 + val_tol)),
        #valSDVPLeak=lambda x: x['pLeakSDV'].between(x['expSDVPLeak'] * (1 - val_tol), x['expSDVPLeak'] * (1 + val_tol))
    )

    print(stateCountWideDF.to_string())
    sm.dumpSummary(stateCountWideDF, newConfig, 'dumpSepValidationTemplate')

def validateFlareStates(stateTimingDF, metadata, eqTable, newConfig, val_tol):
    equipmentType = "MEETFlare"
    listOfStates = ["MALFUNCTIONING", "OPERATING", "UNLIT"]
    stateCountWideDF = getStateCountWideDF(stateTimingDF, metadata, equipmentType, listOfStates)

    stateCountWideDF = stateCountWideDF.assign(
        nonOperatingCount=stateCountWideDF['totalCount'] - stateCountWideDF['count', 'OPERATING'],
        pMalfunction=lambda x: x['count', 'MALFUNCTIONING'] / x['nonOperatingCount'],
        pUnlit=lambda x: x['count', 'UNLIT'] / x['nonOperatingCount'],
    )

    stateCountWideDF = stateCountWideDF.assign(
        expOpDurMin=stateCountWideDF.index.map(lambda x: calcExpOpMinDur(x, eqTable)),
        expOpDurMean=stateCountWideDF.index.map(lambda x: calcExpOpMeanDur(x, eqTable)),
        expOpDurMax=stateCountWideDF.index.map(lambda x: calcExpOpMaxDur(x, eqTable)),
        expMalfDurMin=stateCountWideDF.index.map(lambda x: calcExpMalfMinDur(x, eqTable)),
        expMalfDurMean=stateCountWideDF.index.map(lambda x: calcExpMalfMeanDur(x, eqTable)),
        expMalfDurMax=stateCountWideDF.index.map(lambda x: calcExpMalfMaxDur(x, eqTable)),
        expUnlitDurMin=stateCountWideDF.index.map(lambda x: calcExpUnlitMinDur(x, eqTable)),
        expUnlitDurMean=stateCountWideDF.index.map(lambda x: calcExpUnlitMeanDur(x, eqTable)),
        expUnlitDurMax=stateCountWideDF.index.map(lambda x: calcExpUnlitMaxDur(x, eqTable)),
        expMalfStateCount=stateCountWideDF.index.map(lambda x: calcExpFlareMalfCounts(x, eqTable, newConfig)),
        expOpStateCount=stateCountWideDF.index.map(lambda x: calcExpFlareOpCounts(x, eqTable, newConfig)),
        expUnlitStateCount=stateCountWideDF.index.map(lambda x: calcExpFlareUnlitCounts(x, eqTable, newConfig)),
        expPMalfunction=stateCountWideDF.index.map(lambda x: calcPMalfunction(x, eqTable)),
        expPUnlit=stateCountWideDF.index.map(lambda x: calcPUnlit(x, eqTable))
    )

    # Validate expected and observed results for flare states
    stateCountWideDF = stateCountWideDF.assign(
        valOpDurMin=lambda x: x['minDuration', 'OPERATING'] >= x['expOpDurMin'],
        valOpDurMean=lambda x: x['meanDuration', 'OPERATING'].between(x['expOpDurMean']* (1 - val_tol), x['expOpDurMean']* (1 + val_tol)),
        valOpDurMax=lambda x: x['maxDuration', 'OPERATING'] <= x['expOpDurMax'],
        valMalFuncDurMin=lambda x: x['minDuration', 'MALFUNCTIONING'] >= x['expMalfDurMin'],
        valMalFuncDurMean=lambda x: x['meanDuration', 'MALFUNCTIONING'].between(x['expMalfDurMean']* (1 - val_tol), x['expMalfDurMean']* (1 + val_tol)),
        valMalFuncDurMax=lambda x: x['maxDuration', 'MALFUNCTIONING'] <= x['expMalfDurMax'],
        valUnlitDurMin=lambda x: x['minDuration', 'UNLIT'] >= x['expUnlitDurMin'],
        valUnlitDurMean=lambda x: x['meanDuration', 'UNLIT'].between(x['expUnlitDurMean']* (1 - val_tol), x['expUnlitDurMean']* (1 + val_tol)),
        valUnlitDurMax=lambda x: x['maxDuration', 'UNLIT'] <= x['expUnlitDurMax'],
        valMalfStateCount=lambda x: x['count', 'MALFUNCTIONING'].between(x['expMalfStateCount'] * (1 - val_tol), x['expMalfStateCount'] * (1 + val_tol)),
        valOpStateCount=lambda x: x['count', 'OPERATING'].between(x['expOpStateCount']* (1 - val_tol), x['expOpStateCount']* (1 + val_tol)),
        valUnlitStateCount=lambda x: x['count', 'UNLIT'].between(x['expUnlitStateCount']* (1 - val_tol), x['expUnlitStateCount']* (1 + val_tol)),
        valPMalfunction=lambda x: x['pMalfunction'].between(x['expPMalfunction'] * (1 - val_tol), x['expPMalfunction'] * (1 + val_tol)),
        valPUnlit=lambda x: x['pUnlit'].between(x['expPUnlit'] * (1 - val_tol), x['expPUnlit'] * (1 + val_tol)),
    )
    print(stateCountWideDF.to_string())
    sm.dumpSummary(stateCountWideDF, newConfig, 'flareValidationTemplate')
    pass

def validateTankBatStates(stateTimingDF, metadata, eqTable, newConfig, val_tol):
    equipmentType = "MEETBattery"
    listOfStates = ["OPERATING", "OVERPRESSURE_VENT", "OVERPRESSURE_LEAK"]
    stateCountWideDF = getStateCountWideDF(stateTimingDF, metadata, equipmentType, listOfStates).fillna(0)
    if stateCountWideDF.empty:
        return

    stateCountWideDF = stateCountWideDF.assign(
        expOPLeakStateCount=stateCountWideDF.index.map(lambda x: calcExpOPLeakStateCount(x, eqTable, newConfig)),
        expOPLeakDurMin=stateCountWideDF.index.map(lambda x: calcExpOPLeakDurMin(x, eqTable, newConfig)),
        expOPLeakDurMean=stateCountWideDF.index.map(lambda x: calcExpOPLeakDurMean(x, eqTable, newConfig)),
        expOPLeakDurMax=stateCountWideDF.index.map(lambda x: calcExpOPLeakDurMax(x, eqTable, newConfig)),
        # expTankHatchMTTRmin=stateCountWideDF.index.map(lambda x: calcExpTankHatchMTTRmin(x, eqTable)),
        # expTankHatchMTTRmean=stateCountWideDF.index.map(lambda x: calcExpTankHatchMTTRmean(x, eqTable)),
        # expTankHatchMTTRmax=stateCountWideDF.index.map(lambda x: calcExpTankHatchMTTRmax(x, eqTable)),
        # expTankVentMTTRmin=stateCountWideDF.index.map(lambda x: calcExpTankVentMTTRmin(x, eqTable)),
        # expTankVentMTTRmean=stateCountWideDF.index.map(lambda x: calcExpTankVentMTTRmean(x, eqTable)),
        # expTankVentMTTRmax=stateCountWideDF.index.map(lambda x: calcExpTankVentMTTRmax(x, eqTable)),
        # expOPVentMTTRmin=stateCountWideDF.index.map(lambda x: calcExpOPVentMTTRmin(x, eqTable)),
        # expOPVentMTTRmean=stateCountWideDF.index.map(lambda x: calcExpOPVentMTTRmean(x, eqTable)),
        # expOPVentMTTRmax=stateCountWideDF.index.map(lambda x: calcExpOPVentMTTRmax(x, eqTable)),
    )

    # Validate expected and observed results for dump separator states
    stateCountWideDF = stateCountWideDF.assign(
        valOPLeakStateCount=lambda x: x['count', 'OVERPRESSURE_LEAK'].between(x['expOPLeakStateCount']* (1 - val_tol), x['expOPLeakStateCount']* (1 + val_tol)),
        valOPLeakDurMin=lambda x: x['minDuration', 'OVERPRESSURE_LEAK'] >= x['expOPLeakDurMin'],
        valOPLeakDurMean=lambda x: x['meanDuration', 'OVERPRESSURE_LEAK'].between(x['expOPLeakDurMean']* (1 - val_tol), x['expOPLeakDurMean']* (1 + val_tol)),
        valOPLeakDurMax=lambda x: x['maxDuration', 'OVERPRESSURE_LEAK'] <= x['expOPLeakDurMax'],
        #valOPVentMTTRmin=lambda x: x['minDuration', 'OPERATING'] >= x['expFailDumpValDurMin'],
        #valOPVentMTTRmean=lambda x: x['maxDuration', 'OPERATING'] <= x['expFailDumpValDurMean'],
        #valOPVentMTTRmax=lambda x: x['maxDuration', 'OPERATING'] <= x['expFailDumpValDurMax']
        # valStuckDumpValCount = lambda x: x['count', 'STUCK_DUMP_VALVE'].between(x['expStuckDumpValCount'] * (1-val_tol), x['expStuckDumpValCount'] * (1+val_tol)),
    )
    print(stateCountWideDF.to_string())
    sm.dumpSummary(stateCountWideDF, newConfig, 'tankBatValidationTemplate')

def graphMain(config):
    VAL_TOL = 0.15 #error tolerance in percentage for validation
    logging.basicConfig(level=logging.INFO)
    coalesceEventTSDF, _ = gu.readCompleteEvents(config)

    eventDF, tsDF, gcDF, metadata, newConfig = gu.readCoreTables(config, config['studyName'], config['runNumber'])
    eqTable = gu.readEquipmentFile(config, config['studyName'], config['runNumber'])

    stateTiming = gu.calculateStateTiming(coalesceEventTSDF)

    # Start units validation
    #validateCycWellStates(stateTiming, metadata, eqTable, newConfig, VAL_TOL)
    #validateContWellStates(stateTiming, metadata, eqTable, newConfig, VAL_TOL)
    #validateContSepStates(stateTiming, metadata, eqTable, newConfig, VAL_TOL)
    validateDumpSepStates(stateTiming, metadata, eqTable, newConfig, VAL_TOL)
    #validateFlareStates(stateTiming, metadata, eqTable, newConfig, VAL_TOL)
    #validateTankBatStates(stateTiming, metadata, eqTable, newConfig, VAL_TOL)

    pass


if __name__ == "__main__":
    config, args = au.getConfig()
    if not args.scenarioTimestamp:
        config = gu.findMostRecentScenario(config, args)
    graphMain(config)