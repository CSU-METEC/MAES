import pathlib
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# data reference: https://mountainscholar.org/handle/10217/194765
# download data before running this file
# 'Working directory' pointing to downloaded and unzipped data

INTERMITTENT = ['A-1', 'D-1', 'D-4', 'D-6', 'H-4', 'H-6', 'I-3', 'I-4', 'I-5',
                'J-1', 'N-1', 'N-3', 'O-1', 'S-2', 'S-6', 'V-4', 'D-2', 'H-1',
                'S-4', 'T-4', 'U-5', 'V-6', 'A-2', 'A-3', 'A-4', 'A-6', 'G-5',
                'N-2', 'N-4', 'O-6', 'P-1', 'P-2', 'P-5', 'Q-5', 'S-1', 'S-5',
                'T-5', 'T-6', 'U-6', 'V-2', 'A-5', 'D-5', 'L-6', 'O-4', 'O-5',
                'P-3', 'P-4', 'T-1', 'U-1', 'U-4']

LOW_BLEED = ['G-2', 'G-4', 'I-2', 'J-2', 'J-6', 'L-2', 'L-3', 'N-6', 'O-2', 'O-3',
             'Q-1', 'Q-6', 'T-2', 'U-2', 'U-3', 'V-1', 'Y-1', 'Y-4', 'D-3', 'J-4',
             'N-5', 'Q-2', 'V-3', 'V-5', 'H-2', 'H-3', ]

HIGH_BLEED = ['G-3', 'G-6', 'J-3', 'L-4', 'Q-4', 'Y-2', 'Y-5', 'Y-6', 'L-5']

CONTINUOUS_INTERMITTENT = ['A-4', 'D-1', 'D-4', 'G-5', 'I-5', 'N-1', 'N-2', 'N-4',
                           'S-2', 'S-4', 'V-2', 'V-6']

EXTENDED_RAMP_INT = ['D-1', 'H-1', 'O-6', 'P-5', 'T-4', 'T-5', 'T-6', 'U-5',
                     'U-6', 'V-6']

NO_RET_TO_ZERO_INT = ['A-3', 'D-1', 'D-4', 'D-6', 'H-6', 'V-6']

IRREGULAR_INT = ['D-1', 'D-4', 'D-6', 'G-5', 'H-6', 'I-5', 'N-2', 'N-3', 'O-6',
                 'P-1', 'S-2', 'S-4', 'T-6', 'V-2']


def updataDictDF(dataFile, df):
    df.update({str(dataFile).replace('.csv', ''): pd.read_csv(dataFile)})


def strReplaceCsv(dataFile):
    return str(dataFile).replace('.csv', '')


def getData(pcTypeGlobal):
    p = pathlib.Path()
    retDF = {}
    for dataFile in p.iterdir():
        if strReplaceCsv(dataFile) in pcTypeGlobal:
            updataDictDF(dataFile, retDF)
    return pd.concat(retDF)


def getProbsFromHist(histo):
    totalCount = histo[0].sum()
    counts = histo[0]
    return counts/totalCount


def getDataFromHist(histo):
    points = histo[1]
    ret = [(points[i]+points[i+1])/2 for i in range(len(points)-1)]
    return ret


def getFactorsFromProbsData(histo):
    return pd.DataFrame({'probs': getProbsFromHist(histo), 'data': getDataFromHist(histo)})

def reduceData(df, nBins):
    samplesPerBin = int(len(df)/nBins)
    sampleSizes = []
    aucArray = []
    for i in range(nBins+1):
        sampleSizes.append(samplesPerBin*i)
    for eachBin in sampleSizes:
        if eachBin < len(df):
            vals = df.iloc[eachBin:eachBin+samplesPerBin]['CorrectedFlow_scfh']
            auc = sum(vals)/len(vals)  # averaging == area under curve?
            aucArray.append(auc)
    return aucArray


def calcLowBleedEmissionFactors(lowBleedDF):
    emissionRatesOperating = lowBleedDF[(lowBleedDF['CorrectedFlow_scfh'] <= 6)]
    a = reduceData(emissionRatesOperating, 500)
    emissionRatesAbnormal = lowBleedDF[(lowBleedDF['CorrectedFlow_scfh'] > 6)]
    opEmiRatesHist = plt.hist(emissionRatesOperating['CorrectedFlow_scfh'], bins=50)
    opEmissionFactors = getFactorsFromProbsData(opEmiRatesHist)
    abEmiRatesHist = plt.hist(emissionRatesAbnormal['CorrectedFlow_scfh'], bins=50)
    abEmissionFactors = getFactorsFromProbsData(abEmiRatesHist)
    return opEmissionFactors, abEmissionFactors


def calcHighBleedEmissionFactors(highBleedDF):
    emissionRatesHist = plt.hist(highBleedDF['CorrectedFlow_scfh'], bins=50)
    emissionFactors = getFactorsFromProbsData(emissionRatesHist)
    return emissionFactors


def calcContIntermittentEmissionFactors(contIntDF):
    emissionRatesHist = plt.hist(contIntDF['CorrectedFlow_scfh'], bins=50)
    emissionFactors = getFactorsFromProbsData(emissionRatesHist)
    return emissionFactors


def calcIntermittentStateTimes(extRampDF):
    extRampDF['mask'] = (extRampDF['CorrectedFlow_scfh'] < 60)
    extRampDF['shiftedMask'] = extRampDF['mask'].shift(1)
    extRampDF['diff'] = extRampDF['mask'] - extRampDF['shiftedMask']
    a = extRampDF.index[extRampDF['diff'] == 1]
    c = []
    for i in range(a.size):
        if i < (a.size - 1):
            c.append(a[i + 1][1] - a[i][1])
    d = [i for i in c if i > 0]
    d = np.array(d)
    meanWaitDuration = d.mean()
    waitDurDist = getFactorsFromProbsData(plt.hist(d, bins=50))
    return meanWaitDuration, waitDurDist


def calcIntermittentEmissionFactors(intDataDF):
    i = 10
    allGroups = intDataDF.groupby(level=0)
    allDF = [i[1] for i in allGroups]
    emFacWait = []
    emFacInt = []
    for each in allDF:
        low = each['CorrectedFlow_scfh'].max()*0.2
        high = each['CorrectedFlow_scfh'].max()*0.7
        emFacWait.append(each[(each['CorrectedFlow_scfh'] < low)])
        emFacInt.append(each[(each['CorrectedFlow_scfh'] > high)])

    emFacWait = pd.concat(emFacWait)
    emFacWaitHist = plt.hist(emFacWait['CorrectedFlow_scfh'], bins=50)
    emFacWaitAll = getFactorsFromProbsData(emFacWaitHist)

    emFacInt = pd.concat(emFacInt)
    emFacIntHist = plt.hist(emFacInt['CorrectedFlow_scfh'], bins=50)
    emFacIntAll = getFactorsFromProbsData(emFacIntHist)

    i = 2
    return emFacWaitAll, emFacIntAll


def convertToSCFS(ef):
    ef['data_scfs'] = ef['data'] / 3600
    return ef


def main():
    p = pathlib.Path('C:\death\METEC_MEET\Data_LuckEtAl')

    opLowBleedEF, abLowBleedEF = calcLowBleedEmissionFactors(getData(LOW_BLEED))
    opLowBleedEF = convertToSCFS(opLowBleedEF)
    abLowBleedEF = convertToSCFS(abLowBleedEF)
    opLowBleedEF.to_csv(p / 'opLowBleedEF.csv', index=False)
    abLowBleedEF.to_csv(p / 'abLowBleedEF.csv', index=False)

    highBleedEF = calcHighBleedEmissionFactors(getData(HIGH_BLEED))
    highBleedEF = convertToSCFS(highBleedEF)
    highBleedEF.to_csv(p / 'highBleedEF.csv', index=False)

    contIntermittentEF = calcContIntermittentEmissionFactors(getData(CONTINUOUS_INTERMITTENT))
    contIntermittentEF = convertToSCFS(contIntermittentEF)
    contIntermittentEF.to_csv(p / 'contIntermittentEF.csv', index=False)

    randIntermittentEF = calcContIntermittentEmissionFactors(getData(INTERMITTENT))
    randIntermittentEF = convertToSCFS(randIntermittentEF)
    randIntermittentEF.to_csv(p / 'randIntermittentEF.csv', index=False)

    irregularIntermittentDF = calcContIntermittentEmissionFactors(getData(IRREGULAR_INT))
    irregularIntermittentDF = convertToSCFS(irregularIntermittentDF)
    irregularIntermittentDF.to_csv(p / 'irregularIntermittentDF.csv', index=False)

    stateTimes, waitDurDist = calcIntermittentStateTimes(getData(EXTENDED_RAMP_INT))
    waitDurDist.to_csv(p / 'waitDurDist.csv', index=False)

    efWait, efInt = calcIntermittentEmissionFactors(getData(INTERMITTENT))
    efWait = convertToSCFS(efWait)
    efInt = convertToSCFS(efInt)
    efWait.to_csv(p / 'efWait.csv', index=False)
    efInt.to_csv(p / 'efInt.csv', index=False)

    i = 10
    pass


if __name__ == '__main__':
    main()
