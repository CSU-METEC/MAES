class EmissionRate():
    def instantaneousEmission(self, ts, emissionName=None):
        pass

    def integratedEmission(self, t0, t1, emissionName=None):
        pass

    def setUnits(self, units):
        self.units = units

class ConstantEmissionRate(EmissionRate):
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

    @classmethod
    def fromPandas(cls, df):
        return ConstantEmissionRate(df.iloc[0].to_dict())  # row[0], col [ConstVal]

