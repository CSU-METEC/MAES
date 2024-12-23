from EmissionCategory import EmissionCategory
from Integrator import ConstantIntegrator
import logging

class EmissionAccumulator:

    EmissionTemplate = {'accumulatedMass': 0, 'eventStartTS': 0}
    IntegratorTemplate = {'integrator': None}

    @staticmethod
    def newIntegratorTable():
        return {}

    @staticmethod
    def setIntegrator(iTable, eCategory, integrator):
        if eCategory not in iTable:
            iTable[eCategory] = EmissionAccumulator.IntegratorTemplate.copy()
        itEntry = iTable[eCategory]
        itEntry['integrator'] = integrator

    def __init__(self):
        self.emissionTable = dict(map(lambda x: (x, self.EmissionTemplate.copy()), EmissionCategory))
        self.defaultIntegratorTable = self.newIntegratorTable()
        self.zeroIntegrator = ConstantIntegrator(0.0)

    def integratorFromParam(self, integratorTable):
        return integratorTable if integratorTable is not None else self.defaultIntegratorTable

    def setIntegrator(self, eCategory, integrator, integratorTable=None):
        iTable = self.integratorFromParam(integratorTable)
        EmissionAccumulator.setIntegrator(iTable, eCategory, integrator)

    def handleEmission(self, category, ts, integratorTable=None):
        iTable = self.integratorFromParam(integratorTable)
        ecData = self.emissionTable[category]
        startTS = ecData['eventStartTS']
        ecData['eventStartTS'] = ts
        if category not in iTable:
            return
        itEntry = iTable[category]
        emissionMass = itEntry.integrate(startTS, ts)
        ecData['accumulatedMass'] += emissionMass

    def handleEmissionEvent(self, event):
        eventCategory = event['category']
        eventAction = event.get('action', 'STOP')

        self.handleEmission(event['category'], event['timestamp'])

        self.emissionTable[eventCategory]['integrator'] = self.emissionTable[eventCategory][eventAction]

    def updateEmissions(self, ts, integratorTable=None):
        iTable = self.integratorFromParam(integratorTable)
        for category, integrator in iTable.items():
            if category not in self.emissionTable:
                logging.warning(f"unknown category {category} in emission table -- creating entry")  # This should only happen during debugging
                self.emissionTable[category] = self.EmissionTemplate
            ecData = self.emissionTable[category]
            emissions, units, profile_id, fingerprint = integrator.integrate(ecData['eventStartTS'], ts)
            ecData['accumulatedMass'] += emissions
            ecData['eventStartTS'] = ts
            if 'units' not in ecData:
                ecData['units'] = units
            else:
                if ecData.get('units', 'none') != units:
                    logging.warning(f"Units mismatch -- expected {ecData['units']}, got: {units}")
                    raise ValueError
            if 'profile_id' not in ecData:
                ecData['profile_id'] = profile_id
                ecData['fingerprint'] = fingerprint
            return emissions
        return None

    def updateTimestamp(self, ts, integratorTable=None):  # Just like the above function, but does not update the accumulated mass
        iTable = self.integratorFromParam(integratorTable)
        for category, integrator in iTable.items():
            if category not in self.emissionTable:
                logging.warning(f"unknown category {category} in emission table -- creating entry")  # This should only happen during debugging
                self.emissionTable[category] = self.EmissionTemplate
            ecData = self.emissionTable[category]
            ecData['eventStartTS'] = ts

    def resetCumulativeEmissions(self, t=0):
        et = self.emissionTable
        for singleEC in EmissionCategory:
            et[singleEC]['accumulatedMass'] = 0
            et[singleEC]['eventStartTS'] = t

    def timestamp(self, ts, integratorTable=None):
        self.updateEmissions(ts, integratorTable)
        ret = dict(map(lambda x: (x[0], {'value': x[1]['accumulatedMass'], 'units': x[1].get('units', 'scf_whole_gas'), 'profile_id': x[1].get('profile_id', None), 'fingerprint': x[1].get('fingerprint', None)}), self.emissionTable.items()))
        self.resetCumulativeEmissions(ts)
        return ret
