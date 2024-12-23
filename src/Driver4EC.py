from abc import ABC
import Distribution

class Driver4EC(ABC):
    def __init__(self, units='scf_wholegas', name=None, gcFingerprint="default"):
        self.units = units
        self.name = name
        self.gcFingerprint = gcFingerprint

    def setUnits(self, units):
        self.units=units

    def setName(self, name):
        self.name = name

    def setFingerprint(self, gcFingerprint):
        self.gcFingerprint = gcFingerprint

    def pick(self):
        raise NotImplementedError

    def instantaneousEmission(self, ts, emissionName=None):
        raise NotImplementedError

    def integratedEmission(self, t0, t1, emissionName=None):
        raise NotImplementedError

class ConstantDriver4EC(Driver4EC):
    def __init__(self, rate, units=None):
        if not isinstance(rate, dict):
            rate = {'default': rate}
        self.rate = rate
        self.units = units

    def instantaneousEmission(self, ts, emissionName=None):
        eName = emissionName
        if eName is None:
            eName = list(self.rate.keys())[0]
        return self.rate[eName], self.units

    def integratedEmission(self, t0, t1, emissionName=None):
        eName = emissionName
        if eName is None:
            eName = list(self.rate.keys())[0]
        return self.rate[eName] * (t1 - t0), self.units

    def pick(self):
        return self

    def serialize(self):
        return {'driver4ECClass': self.__class__, 'rate': self.rate, 'units': self.units}

    @classmethod
    def fromPandas(cls, df):
        return ConstantDriver4EC(df.iloc[0].to_dict())  # row[0], col [ConstVal]

class HistogramDriver4EC(ConstantDriver4EC):
    def __init__(self, df, units=None):
        hDist = Distribution.Histogram.fromPandas(df, probCol='Probability')
        rate = hDist.pick()
        return super().__init__(rate, units)

    def instantaneousEmission(self, ts, emissionName=None):
        eName = emissionName
        if eName is None:
            eName = list(self.rate.keys())[0]
        return self.rate[eName], self.units

    def integratedEmission(self, t0, t1, emissionName=None):
        eName = emissionName
        if eName is None:
            eName = list(self.rate.keys())[0]
        return self.rate[eName] * (t1 - t0), self.units

    def pick(self):
        constVal = self.hDist.pick()
        return ConstantDriver4EC(rate=constVal, units=self.units)

    @classmethod
    def fromPandas(cls, df):
        return HistogramDriver4EC(df)  # row[0], col [ConstVal]