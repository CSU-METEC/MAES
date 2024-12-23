import numbers
import logging
import numpy as np
import math
import random
import json
import pandas as pd

logger = logging.getLogger(__name__)

RELATIVE_TOLERANCE = .01

DISTRIBUTION_DEBUG = False

class Distribution:

    ATTRIBUTES_LIST = []

    def pick(self):
        pass

    def test(self, seq):
        pass

    def pick_residualtime(self):
        """Return residual time distribution, assuming Distribution is use for stateDuration"""
        raise NotImplementedError()

    def expected_value(self):
        raise NotImplementedError()

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, self.__class__):
            return False
        
        for attri in self.__class__.ATTRIBUTES_LIST:
           if self.__class__.__getattribute__(self,attri) != value.__getattribute__(attri):
               return False
           return True

class Constant(Distribution):

    def __init__(self, constVal):
        if isinstance(constVal, dict):
            self.constDict = constVal
        else:
            self.constDict = {"ConstVal": constVal}

    def pick(self, name=None):
        if name is None:
            vals = list(self.constDict.values())
            return vals[0]
        else:
            return self.constDict[name]

    def test(self, seq, name='ConstVal'):
        return len(list(filter(lambda x: x != self.constDict[name], seq))) == 0

    def __str__(self):
        return f"<Constant Distribution {self.constDict}>"

    @classmethod
    def fromPandas(cls, df):
        return Constant(df.iloc[0].to_dict())  # row[0], col [ConstVal]


class Normal(Distribution):

    # todo: this style of passing arguments is non-standard.  Move to kwargs with defaults, and us **kwargs in caller
    def __init__(self, json):
        mu = json['mu']
        sigma = json['sigma']
        self.mu = mu
        self.sigma = sigma

    def pick(self, _=None):
        if DISTRIBUTION_DEBUG:
            return self.mu
        mu = self.mu
        sigma = self.sigma
        ret = np.random.normal(mu, sigma)
        return ret

    def test(self, seq):
        calcMean = np.mean(seq)
        calcStd = np.std(seq)
        meanClose = math.isclose(self.mu, calcMean, rel_tol=RELATIVE_TOLERANCE)
        stdClose = math.isclose(self.sigma, calcStd, rel_tol=RELATIVE_TOLERANCE)
        return (meanClose and stdClose)

    def __str__(self):
        return f"<Normal Distribution, mu: {self.mu}, sigma: {self.sigma}>"

    @classmethod
    def fromPandas(cls, df):
        jDict = {"mu": df.iat[0, 0], "sigma": df.iat[0, 1]}
        return Normal(jDict)

    def toJson(self):
        return {'distribution': 'normal', 'mu': self.mu, 'sigma': self.sigma}

    def __eq__(self, other):
        if not isinstance(other, Normal):
            return False
        return self.mu == other.mu and self.sigma == other.sigma

class BoundedNormal(Normal):
    def __init__(self, json):
        super().__init__(json)
        self.min = json.get('min', 0)

    def pick(self, _=None):
        ret = super().pick()
        if ret < self.min:
            ret = self.min
        return ret

    @classmethod
    def fromPandas(cls, df):
        jDict = {"mu": df.iat[0, 0], "sigma": df.iat[0, 1], "min": df.iat[0, 2]}
        return BoundedNormal(jDict)


class Lognormal(Distribution):

    ATTRIBUTES_LIST = ["mean", "sigma"]

    def __init__(self, json):
        self.mean = json['mu']
        self.sigma = json['sigma']

    def pick(self, _=None):
        ret = np.random.lognormal(self.mean, self.sigma)
        return ret

    def test(self, seq):
        return True
    
    def toJson(self):
        return {'distribution': 'lognormal', 'mu': self.mean, 'sigma': self.sigma}

    def __str__(self):
        return f"<Lognormal Distribution, mu: {self.mean}, sigma: {self.sigma}>"

    @classmethod
    def fromPandas(cls, df):
        jDict = {"mu": df.iat[0, 0], "sigma": df.iat[0, 1]}
        return Lognormal(jDict)


class Triangular(Distribution):

    ATTRIBUTES_LIST = ["left","mode","right"]

    def __init__(self, json):
        self.left = json['min']
        self.mode = json['mean']
        self.right = json['max']

    def pick(self, _=None):
        ret = np.random.triangular(self.left, self.mode, self.right)
        return ret

    def test(self, seq):
        minVal = min(seq)
        maxVal = max(seq)
        return True

    def toJson(self):
        return {'distribution': 'triangular', 'min': self.left, 'mean': self.mode, 'max': self.right}

    def __str__(self):
        return f"<Triangular Distribution, left: {self.left}, mode: {self.mode}, right: {self.right}>"

    @classmethod
    def fromPandas(cls, df):
        jDict = {"min": df.iat[0, 0], "mean": df.iat[0, 1], "max": df.iat[0, 2]}
        return Triangular(jDict)

class Uniform(Distribution):

    def __init__(self, json):
        self.low = json['min']
        self.high = json['max']

    def pick(self, _=None):
        if DISTRIBUTION_DEBUG:
            return (self.high + self.low) / 2.0
        # Samples are uniformly distributed over the half-open interval [low, high) (includes low, but excludes high).
        ret = np.random.uniform(self.low, self.high)
        return ret

    def test(self, seq):
        minVal = min(seq)
        maxVal = max(seq)
        return True

    def __str__(self):
        return f"<Uniform Distribution, left: {self.low}, right: {self.high}>"

    @classmethod
    def fromPandas(cls, df):
        jDict = {"min": df.iat[0, 0], "max": df.iat[0, 1]}
        return Uniform(jDict)

    def mean(self):
        return (self.low + self.high) / 2.0


class Scaled(Distribution):
    def __init__(self, json):
        self.dist = json['dist']
        self.scaleFactor = json['factor']

    def pick(self, _=None):
        # ret = self.dist.pick() * self.scaleFactor         # ?? this needs work
        ret = self.dist * self.scaleFactor

        return ret

    @classmethod
    def fromPandas(cls, df):
        jDict = {"dist": df.iat[0, 0], "factor": df.iat[0, 1]}
        return Scaled(jDict)


class Exponential(Distribution):

    ATTRIBUTES_LIST = ["scale"]

    def __init__(self, json):
        self.scale = json['scale']

    def pick(self, _=None):
        ret = np.random.exponential(self.scale)
        return ret

    def pick_residualtime(self):
        return np.random.exponential(self.scale)

    def expected_value(self):
        return self.scale

    def test(self, seg):
        self.pick()
        return True

    def __str__(self):
        return f"<Exponential Distribution, scale: {self.scale}>"

    def toJson(self):
        return {'distribution': 'exponential', 'scale': self.scale}

    @classmethod
    def fromPandas(cls, df):
        jDict = {"scale": df.iat[0, 0]}
        return Exponential(jDict)


class Sampled(Distribution):
    """Sampled from observations"""

    def __init__(self, json):
        if isinstance(json, dict):
            self.obs = json
        else:
            self.obs = {'observations': json}

    def pick(self, obsName=None):
        if obsName is None:
            keyNames = list(self.obs.keys())
            obsForPick = self.obs[keyNames[0]]
        else:
            obsForPick = self.obs[obsName]
        ret = random.choice(obsForPick)
        return ret

    @classmethod
    def fromPandas(cls, df):
        dict1 = df.to_dict()
        dict2 = {}
        for key, data in dict1.items():
            newData = list(filter(lambda x: not math.isnan(x), data.values()))
            dict2[key] = newData
        return Sampled(dict2)

class Histogram(Distribution):
    """Sampled from observations"""

    def __init__(self, df):
        colNames = df.columns
        defaultCol = colNames[0] if colNames[0] != 'Frequency' else colNames[1]
        self.df = df
        self.defaultCol = defaultCol

    def pick(self, col=None):
        r = random.random()
        ndx = self.df['cProb'].searchsorted(r)
        valCol = col if col is not None else self.defaultCol
        pk = self.df.loc[ndx][valCol]
        return pk

    @classmethod
    def fromPandas(cls, df, probCol='Probability'):
        N = df[probCol].sum()
        df['prob'] = df[probCol] / N
        df['cProb'] = df['prob'].cumsum()
        return Histogram(df)

class Timeseries(Distribution):
    def __init__(self, points):
        self.points = points

    @classmethod
    def fromPandas(cls, df):
        return Timeseries(df)

    @classmethod
    def constantTS(cls, constVal):
        points = pd.DataFrame(data={'time': [0, np.inf], 'value': [constVal, constVal]})
        return Timeseries(points)

    def scale(self, scaleFactor):
        scaledDF = self.points.copy(deep=True)
        scaledDF['value'] = scaledDF['value'] * scaleFactor
        return Timeseries(scaledDF)

class GasComposition(Distribution):
    def __init__(self, gcTable):
        self.gcTable = gcTable

    @classmethod
    def fromPandas(cls, df):
        inDF = df.set_index('Species')
        return GasComposition(inDF)


DISTRIBUTION_MAP = {
    'constant': Constant,
    'normal': Normal,
    'boundednormal': BoundedNormal,
    'lognormal': Lognormal,
    'exponential': Exponential,
    'triangular': Triangular,
    'scaled': Scaled,
    'sampled': Sampled,
    'histogram': Histogram,
    'uniform': Uniform
}


def distFactory(json):
    if isinstance(json, numbers.Number):
        return Constant(json)
    if isinstance(json, str):
        if json == 'inf':
            return Constant(math.inf)
        logger.warning(f"Unknown distribution type {json}")
        raise NotImplementedError
    if isinstance(json, list):
        return Sampled(json)
    if 'distribution' not in json:
        logger.warning(f"key 'distribution' not found in {json}")
        raise NotImplementedError
    distName = json['distribution']
    if distName not in DISTRIBUTION_MAP:
        logger.warning(f"distribution {distName} not found in known distributions ({DISTRIBUTION_MAP.keys()})")
        raise NotImplementedError

    cls = DISTRIBUTION_MAP[distName]
    return cls(json)

class JSONEncode(json.JSONEncoder):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def default(self, obj):
        if isinstance(obj, Distribution):
            return obj.toJson()
        return super().default(obj)


class JSONDecode(json.JSONDecoder):
    def __init__(self, **kwargs):
        super().__init__(object_hook=self.object_hook, **kwargs)

    def object_hook(self, obj):
        if 'distribution' not in obj:
            return obj
        return distFactory(obj)


