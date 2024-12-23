import DistributionProfile as dp
import Distribution
import numpy as np
import pandas as pd
import logging

class EmissionTimeseries():
    def __init__(self, distProfile, ts, units=None):
        self.distProfile = distProfile
        self.ts = ts
        self.units = units if units is not None else self.distProfile.md['Units']

    def instantaneousEmission(self, t):
        tsDF = self.ts.points
        ret = np.interp([t], tsDF['time'], tsDF['value'])
        return ret[0]

    def integratedEmission(self, t0, t1):
        ts = self.ts.points
        t0Interp = self.instantaneousEmission(t0)
        t1Interp = self.instantaneousEmission(t1)
        edgeDF = pd.DataFrame(data={
            'time':  [0, t0,       t0,       t1, t1],
            'value': [0,  0, t0Interp, t1Interp,  0]})
        interiorPoints = ts[np.logical_and(ts['time'] > t0, ts['time'] < t1)]
        evalDF = edgeDF.append(interiorPoints).sort_values(['time']).drop_duplicates()
        ret = np.trapz(evalDF['value'], evalDF['time'])
        return ret

    def toDict(self):
        return self.ts.points.to_dict()

    def scale(self, scaleFactor, newTimeBasis):
        newMetadata = {**self.distProfile.md, 'Time Basis': newTimeBasis}
        newDistribution = dp.DistributionProfile(newMetadata, self.distProfile.distribution)
        newTS = self.ts.scale(scaleFactor)
        return EmissionTimeseries(newDistribution, newTS)

class ConstantEmissionTimeseries():
    def __init__(self, constVal=0.0, units=None):
        self.constVal = constVal
        self.units = units

    def instantaneousEmission(self, t):
        return self.constVal

    def integratedEmission(self, t0, t1):
        return self.constVal * (t1 - t0)

    def toDict(self):
        return {'units': self.units, 'constVal': self.constVal}

    def scale(self, scaleFactor, newTimeBasis):
        # newMetadata = {**self.distProfile.md, 'Time Basis': newTimeBasis}
        # newDistribution = dp.DistributionProfile(newMetadata, self.distProfile.distribution)
        newConst = self.constVal * scaleFactor
        return ConstantEmissionTimeseries(units=self.units, constVal=newConst)


class EmissionDriver():
    def __init__(self, nameParm=None, filename=None):
        try:
            self.distProfile = dp.DistributionProfile.readFile(filename)
            name = nameParm if nameParm is not None else self.distProfile.md.get('Name', str(filename))
            self.md = {'name': name, **self.distProfile.md}
        except Exception as e:
            logging.exception(f"EmissionDriver.__init__ failed, nameParam: {nameParm}, filename: {filename}", exc_info=True)
            raise e

    def pick(self):
        try:
            if isinstance(self.distProfile.distribution, Distribution.Timeseries):
                return EmissionTimeseries(self.distProfile, self.distProfile.distribution)
            return ConstantEmissionTimeseries(units=self.distProfile.md['Units'], constVal=self.distProfile.pick())
        except Exception as e:
            logging.exception(f"EmissionDriver.pick() failed, name: {self.md['name']}", exc_info=True)
            raise e

class ManualEmissionDriver(EmissionDriver):
    def __init__(self, name, distribution, md):
        self.distProfile = dp.DistributionProfile(md, distribution)
        self.md = {'name': name, **md}

class ActivityProfile():
    def __init__(self, name, filename):
        self.distProfile = dp.DistributionProfile.readFile(filename)
        self.md = {'name': name, **self.distProfile.md}

    def pick(self):
        return self.distProfile.pick()



