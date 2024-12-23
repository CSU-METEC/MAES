import numpy as np
import pandas as pd
import random

# This code generates a histogram of flash fraction values
# Inputs: gas production, observed emission rate.
# These production and emission values are obtained from Omara data by filtering out the facilities
# that caused most emissions. Top 8 highest emission values were used here. Gas production values are
# the mid-point of these 8 highest emission facilities. Observed emission rate is a normal distribution
# of lowest to highest emission rates from Omara data. Inputs 'cyclesPerDay' is an assumption for
# cycling wells where the wells cycle between 2-10 cycles per day. Cycle Time is the time between each
# production cycle. The algorithm generates an array of fractions by dividing emission rate by production
# flow rate. If the fraction comes out to be >1, we reset that value to 1. The array of fractions is then
# used to create a histogram of 10 values. This histogram is used as an input in the study sheets as
# a random variable.


def main():
    gasProd = np.random.normal(370, 100, 10)/1.5  # kg/d,
    # Mid-point gas production from Omara data that caused most emissions/avg no of wells
    obsEmissionRate = np.random.normal(2.3, 0.5, 10)  # kg/hr, observed emission rates, Omara data

    cyclesPerDay = np.random.randint(2, 10, 10)  # 2-10 cycles per day, estimated assumption
    # cycleTime2 = np.random.randint(1, 12, 10)  # hours
    cycleTime = 24/cyclesPerDay  # cycle time in hours, 24/cyclesPerDay

    processedData = np.array([])

    for cycle in cyclesPerDay:
        for tc in cycleTime:
            Gc = gasProd / cycle  # prod per cycle
            Fg = Gc / tc   # flow rate
            frac = obsEmissionRate / Fg
            frac[frac > 1] = 1
            processedData = np.append(processedData, frac)
            i = 10

    i = 10
    # divide data into a histogram
    p1 = np.count_nonzero(processedData < 0.1) / processedData.size  # <0.1
    p2 = np.count_nonzero((processedData > 0.1) & (processedData < 0.2)) / processedData.size  # 0.1 < x < 0.2
    p3 = np.count_nonzero((processedData > 0.2) & (processedData < 0.3)) / processedData.size
    p4 = np.count_nonzero((processedData > 0.3) & (processedData < 0.4)) / processedData.size
    p5 = np.count_nonzero((processedData > 0.4) & (processedData < 0.5)) / processedData.size
    p6 = np.count_nonzero((processedData > 0.5) & (processedData < 0.6)) / processedData.size
    p7 = np.count_nonzero((processedData > 0.6) & (processedData < 0.7)) / processedData.size
    p8 = np.count_nonzero((processedData > 0.7) & (processedData < 0.8)) / processedData.size
    p9 = np.count_nonzero((processedData > 0.8) & (processedData < 0.9)) / processedData.size
    p10 = np.count_nonzero((processedData > 0.9) & (processedData <= 1.0)) / processedData.size

    pass


if __name__ == '__main__':
    main()
