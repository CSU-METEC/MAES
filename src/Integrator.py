from abc import ABC, abstractmethod
import logging

class Integrator(ABC):
    def __init__(self, units, profile_id, fingerprint):
        self.units = units
        self.profile_id = profile_id
        self.fingerprint = fingerprint

    @abstractmethod
    def integrate(self, t0, t1):
        pass

class ConstantIntegrator(Integrator):


    def __init__(self, emissionRate, units='scf_whole_gas', profile_id=None, fingerprint=None):
        super().__init__(units, profile_id, fingerprint)
        self.emissionRate = emissionRate
        self.profile_id = profile_id
        self.fingerprint = fingerprint

    def integrate(self, t0, t1):
        res = (t1 - t0) * self.emissionRate, self.units, self.profile_id, self.fingerprint
        # logging.debug(f"ts: {t1}, res: {res}")
        return res

    def getUnits(self):
        return self.units

class EmpericalIntegrator(Integrator):

    def __init__(self, observations, units='scf_whole_gas', profile_id=None, fingerprint=None):
        self.observations = observations
        super().__init__(units, profile_id, fingerprint)
        self.profile_id = profile_id
        self.fingerprint = fingerprint
        self.numObs = len(self.observations)
        self.mean = sum(self.observations) / len(self.observations)
        self.base = None

    def integrate(self, t0, t1):
        deltat = (t1 - t0)
        if deltat > len(self.observations):
            return deltat*self.mean
        numObs = self.numObs
        if not self.base or t0 > self.base + numObs:
            self.base = t0
        startTime = (t0-self.base) % numObs
        endTime = t1 if t1 == numObs else (t1-self.base) % numObs
        return sum(self.observations[startTime:endTime]), self.units, self.profile_id, self.fingerprint

class AnalyticalIntegrator(Integrator):
    """use this when you have analysticaly solved integrals"""

    def __init__(self, cumfnc, units='scf_whole_gas', profile_id=None, fingerprint=None):
        self.cumfnc = cumfnc
        super().__init__(units, profile_id, fingerprint)

    def integrate(self, t0, t1):
        if t1 == t0:
            return 0.0, self.units, self.profile_id, self.fingerprint
        v1 = self.cumfnc(t1)
        v0 = self.cumfnc(t0)
        return v1 - v0, self.units, self.profile_id, self.fingerprint

