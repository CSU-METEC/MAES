"""Wrapper around lookup.py for multiple conditions"""

from Lookup import Lookup, CATEGORIZED_SPECIES
import AppUtils as au
from InputMain import InputMain
from Timer import Timer

import pandas as pd
import numpy as np
import sqlite3
import json
import os
import logging
import requests
import math

DEFAULT_CONFIG = "config/defaultConfig.json"
# make DBG_LOOKUP to be True so that
#  - Get CSV export of lookup results, inputs, match status
#  - Search database for all "thermodynamic" composition sepcification (ignores input fingerprint.json for them)
#DBG_LOOKUP = False
DBG_LOOKUP = True

# i just define here
MWt = {
    # some molecular weights (g/mol):
    'C1': 16.04,
    'C2': 30.07,
    'C3': 44.01,
    'C4': 58.1,
    'iC4': 58.1,
    'C5': 72.2,
    'C6': 86.18,
    'CO2': 44.009,
    'N2': 28.014,
}




class LookupManager:
    lookup = None
    def __init__(self, config, dbg_lookup):
        self.config = config.copy()
        self.dbg_lookup = dbg_lookup

        self.intakeSpreadsheet = au.expandFilename(config['facilityEquipTmpl'], config)
        self.defaultFpFile = au.expandFilename(config['defaultFingerprintFile'], config)
        self.fingerprintFile = au.expandFilename(config['gcFingerprintFile'], config)
        self.thermodbFile = au.expandFilename(self.config['thermoDatabaseFile'], self.config)
        if self.dbg_lookup:
            self.dbgOutFile = au.expandFilename(self.config['gcFingerprintDebugOutputFile'], self.config)

        self.defaultFp = {}
        self.read_fp()
        self.readIntakeFile(config)

        # dont load until it is needed
        self.lookup = None

        # TODO bad idea to hard-wire URL in code...s
        self.known_thermodbFiles = {
            'thermodynamics_database_v2_20200211.db': 'https://utexas.box.com/s/yf61dp1hqnt9h4j8i49f4zqumep0k8f3',
            'thermodynamics_database_v2_20200305.db': 'https://utexas.box.com/s/wsyycno7to5uwyhp0y2rz81mdnu8cuy7',
        }



    def read_fp(self):

        with open(self.defaultFpFile, 'r') as f:
            for line in f:
                v = json.loads(line)
                self.defaultFp[v['GC_profile_id']] = v


    def thermo_lookup(self, param_values, prof_id):

        if not LookupManager.lookup:
            if not os.path.exists(self.thermodbFile):
                url = self.known_thermodbFiles.get(os.path.basename(self.thermodbFile))
                if url:
                    # TODO can try download but file is large i dont want with mess with network troubles...
                    raise FileNotFoundError("Please download database file {} from {}".format(self.thermodbFile, url))
                else:
                    raise FileNotFoundError(" thermodynamics database file not found: {}".format(self.thermodbFile))

            LookupManager.lookup = Lookup(self.thermodbFile)

        # get df of candidate match, based on block level search
        df_candidates, _stats = LookupManager.lookup.search(param_values)

        # subset of values to be used to calculate distance (excludes temperature and pressure)
        #param_values_for_distance = {k: v for (k, v) in param_values.items() if k not in ('temp', 'p1', 'p2')}
        param_values_for_distance = {k: v for (k, v) in param_values.items() if k not in ('no_condensate', 'n_stages')}
        #param_values_for_distance = {k: v for (k, v) in param_values.items()
        #        if k not in ('no_condensate', 'n_stages','temp', 'p1', 'p2')}

        # calculate distance for each candidate recoerds
        df_candidates['dst'] = LookupManager.lookup.euclid_distance(param_values_for_distance, df_candidates)
        df_candidates = df_candidates.sort_values('dst')

        # get fingerprint of the best match
        fp = LookupManager.lookup.get_fingerprint(
            **(df_candidates.iloc[0, :].loc[['n_stages', 'comp_param_idx', 'comp_idx',
                                             'param_idx']]))

        best_params = df_candidates.iloc[0,:].to_dict()
        best_params['dst'] = math.sqrt(best_params['dst'])
        best_params.update( {'box_dimension': _stats['r'] *2 + 1, 'count_in_box': _stats['n']} )

        # add extra attributes
        myunits = {
            'wellstream': 'volume_fraction',
            'produced_gas': 'volume_fraction',
            'cond_tank': 'kg/bbl',
            'water_tank_flash': 'kg/bbl',
            'cond_tank_instant_flash': 'kg/bbl',
            'mwt': 'g/mol',
        }

        logging.info('GC_profile_id = {}, n_expand = {}, n_returned = {}, distance = {}'.format(prof_id, _stats['r'], _stats['n'], best_params['dst']))
        fp = {k: {'unit': myunits[k], 'values': v} for (k,v) in fp.items()}
        return fp, best_params

    def cardoso_corr(self, intake, prof_id):

        c1 = intake['c1']

        spnames = np.array(['CO2', 'N2', 'C1', 'C2', 'C3', 'C4', 'iC4', 'C5', 'C6', 'VOC'])
        species_fullnames = ['CARBON_DIOXIDE', 'NITROGEN', 'METHANE', 'ETHANE', 'PROPANE', 'N_BUTANE', 'ISOBUTANE',
                             'N_PENTANE', 'N_HEXANE', 'ALL_VOC']
        molf = pd.Series([0] * len(spnames), dtype=float, index=spnames)
        molf['C1'] = c1

        # First start with the inerts. These are median values from PVT reports and Allen, et al.
        molf['CO2'] = .017
        molf['N2'] = .001

        # now the calculations will begin. First the calculation of ethane
        # The regression equation is:
        # C2/C1 = 1.370 - 0.014*C1
        # Where C2/C1 is the mass ratio, and C1 is the molar percentage.

        # Note that this is a correlation, and at very high methane values, the ethane to methane ratio will be negative
        # Based on the numbers from the equation shown above, at C1 molar percentage larger than 97.85, the C2/C1 ratio will
        # be negative, which doesn't make physical sense. So if the C1 molar percentage is larger than 97, since
        # this will be very dry gas, will assume that  ethane fills the majority of the portion that will make it sum to 100, and
        # propane a small amount. No other species

        if molf['C1'] > .96:
            molf['C3'] = max(1 - molf['C1'], .005)
            molf['C2'] = 1 - molf['C1'] - molf['C3']
            molf[:'N2'] = 0

        else:
            # if the methane molar percentage is smaller than 96, then will use the correlation to estimate the c2_molar fraction,
            # and the C2_C1_mass_ratio
            C2_C1_mass_ratio = 1.37 - 0.014 * molf['C1'] * 100
            molf['C2'] = C2_C1_mass_ratio * MWt['C1'] / MWt['C2'] * molf['C1']

            # Now will check if the numbers exceed 1 or not, and then based on that might make changes, otherwise will continue.
            if molf[:'C2'].sum() > 1:
                molf['C2'] = 1 - molf[:'C1'].sum()
            else:
                # if not, will just continue with propane

                #####################################
                # Calculations for propane. The regression equation is:
                # C3/C1 = 0.969 - 0.010*C1
                # Where C3/C1 is the mass ratio, and C1 is the molar percentage, so:
                C3_C1_mass_ratio = 0.969 - .010 * molf['C1'] * 100
                molf['C3'] = C3_C1_mass_ratio * MWt['C1'] / MWt['C3'] * molf['C1']

                # Now will check if the numbers exceed 1 or not, and then based on that might make changes, otherwise will continue
                if molf[:'C3'].sum() > 1:
                    molf[:'C3'] = 1 - molf[:'C2'].sum()

                else:
                    # if not, will continue with butane

                    ######################################
                    # Calculations for butane. The regression equation is:
                    # C4/C1 = 0.405 - 0.004*C1
                    # Where C4/C1 is the mass ratio, and C1 is the molar percentage, so:
                    C4_C1_mass_ratio = 0.405 - 0.004 * molf['C1'] * 100
                    molf['C4'] = C4_C1_mass_ratio * MWt['C1'] / MWt['C4'] * molf['C1']

                    # Now will check if the numbers exceed 1 or not, and then based on that might make changes, otherwise will continue
                    if molf[:'C4'].sum() > 1:
                        molf['C4'] = 1 - molf[:'C3'].sum()
                    else:
                        # if not, will continue with isobutane

                        ##########################################
                        # Calculations for isobutane. The regression equation is:
                        # iC4/C1 = 0.231 - 0.002*C1
                        # Where iC4/C1 is the mass ratio, and C1 is the molar percentage, so:
                        iC4_C1_mass_ratio = 0.231 - 0.002 * molf['C1'] * 100
                        molf['iC4'] = iC4_C1_mass_ratio * MWt['C1'] / MWt['iC4'] * molf['C1']

                        # Now will check if the numbers exceed 1 or not, and then based on that might make changes, otherwise will continue
                        if molf[:'iC4'].sum() > 1:
                            molf['iC4'] = 1 - molf[:'C4'].sum()
                        else:
                            ##########################################
                            # Now will estimate the VOC. Hydrocarbons with 3 or more carbon atoms are classified as VOC.
                            # Will assume no non-alkane species pressent, i.e. N2 = 0, CO2 = 0 and H2S = 0, so
                            # VOC = 1 - N2_molar_frac - CO2_molar_frac - C1_molar_frac - C2_molar_fraction
                            molf['VOC'] = 1 - molf[:'C2'].sum()
                            # will group the pentane and larger together, their molar fraction will be low in the produced gas...
                            molf['C5'] = 1 - molf[:'iC4'].sum() * .66
                            molf['C6'] = 1 - molf[:'iC4'].sum() * .34

                # also need to estimate the VOC/C1 mass ratio:
                # VOC/C1 (molar ratio) = (C3_molar_fraction + C4_molar_fraction + iC4_molar_fraction + C5_molar_fraction + C6_plus_molar_fraction) / (C1_molar_frac)
                # will assume that C6_plus is composed mainly of C6, so will assume a C6_molecular weight, then the VOC/C1 mass ratio can be
                # calculated as:
                # VOC/C1 (mass ratio) = (CC3_molar_fraction*C3_Mw + C4_molar_fraction*C4_Mw + iC4_molar_fraction*iC4_Mw + C5_molar_fraction*C5_Mw + C6_plus_molar_fraction*C6_Mw)/(C1_molar_frac*C1_Mw)
                # The numerator is also the multiplication of the VOC_molar fraction times the VOC_avg_Mw, which can be defined as:
                VOC_avg_Mw = (molf['C3':'C6'] * np.array(
                    [MWt[_] for _ in ['C3', 'C4', 'iC4', 'C5', 'C6']])).sum() / molf['C3':'C6'].sum()

                # The line below gives the same result as two lines below, but estimating the VOC_avg_Mw is better because it will be needed
                # VOC_C1_mass_ratio <- round((C3_molar_fraction*C3_Mw + C4_molar_fraction*C4_Mw + iC4_molar_fraction*iC4_Mw + C5_molar_fraction*C5_Mw + C6_plus_molar_fraction*C6_Mw)/(C1_molar_frac*C1_Mw),4)
                VOC_C1_mass_ratio = molf['VOC'] * VOC_avg_Mw / (molf['C1'] * MWt['C1'])
        ###########################################
        # Let's do a check to see if the molar fractions sum 1
        sum_light_alkane_molar_fraction = molf['C1':'iC4'].sum()
        sum_molar_fractions = molf[:'C6'].sum()

        assert np.round(sum_molar_fractions - 1, 4) == 0

        ###########################################
        ### Up to this point the calculations are ready, the numbers that will be needed to estimate emissions have been calculated,
        # and will depend on the emission factor.
        # 1) If the emission factor is C1 kg/hr, then the mass ratios will help estimate the other light alkanes and the VOC emission rates
        # in kg/hr ... this should already be coded in Python.
        # 2) If the emission factor is scfh of whole gas, then need to use the molar fraction of each species (also of VOC) to estimate
        # the scfh of each species, then use ideal gas law to convert to moles, then use molecular weight to estimate the kg/hr of each species
        # note that for C5_plus_molar_fraction the Molecular weight to be used is the one from pentane as approximation.... The conversion
        # mentioned here should already be coded.

        # # dont return
        # molf = molf[:'C6']
        # i need full species name
        molf = molf.rename({p: q for (p, q) in zip(molf.index, species_fullnames[:-1])})

        # ready to go
        return {
                   'produced_gas': {'unit': 'volume_fraction', 'values': molf.to_dict()},
                   'mwt': {'unit': 'g/mol', 'values': {'produced_gas': {'ALL_VOC': VOC_avg_Mw}}},
                }, None

    def user_prof(self, intake, prof_id):
        # voc
        voc_sp = ['PROPANE', 'N_BUTANE', 'ISOBUTANE','N_PENTANE', 'ISOPENTANE', 'N_HEXANE', ]
        voc_mw = {s:MWt['C'+str(n)] for s,n in zip(voc_sp,[3,4,4,5,5,6])}
        voc = sum(intake[_] for _ in voc_sp)
        mw = sum(intake[_] * voc_mw[_] for _ in voc_sp) / voc
        return {
                   'produced_gas': {'unit': 'volume_fraction', 'values': intake},
                   'mwt': {'unit': 'g/mol', 'values': {'produced_gas': {'ALL_VOC': mw}}},
               }, None

    def combustion_slip(self, intake, prof_id):

        if intake['profileDataSource'] in ('AP-42', 'Subpart_C'):
            profile = self.defaultFp[intake['profileDataSource']]['fingerprints']
        else:
            raise ValueError('unknwon profile data source: {}'.format(intake['profileDataSource']))

        return profile, None

    def readIntakeFile(self, config):
        with open(self.intakeSpreadsheet, "rb") as xlsFile:
            self.CompositionDF = pd.read_excel(xlsFile, sheet_name='Composition')
        im = InputMain(self.intakeSpreadsheet, self.CompositionDF, config['simulationStartDate'])
        self.intake = im.loadIntakeData('composition', assignFaclityInfo = False)

        # make sure that combustion slip fingerprints are included
        self.lstID = [_['GC_profile_id'] for _ in self.intake]
        toAdd = [_ for _ in ('AP-42', 'Subpart_C') if _ not in self.lstID]
        self.intake.extend({'GC_profile_id': _, 'profileDataSource': _} for _ in toAdd)
        self.lstID.extend(toAdd)

    def getFromDefaults(self, intake):
        try:
            fp = self.defaultFp[intake['GC_profile_id']]
        except KeyError:
            return None
        for k,v in intake.items():
            if v != fp['intake'].get(k):
                return None
        return fp



    @staticmethod
    def map_values(values, mymap):
        o = {}
        for k, v in mymap.items():
            if values[k] is not None:
                o[v.get('name', k)] = v.get('fnc', lambda x: x)(values[k])
        return o



    def processOneIntake(self, singleIntake):
        # see if it's in the default fingerprints
        fp = self.getFromDefaults(singleIntake)

        if fp and not self.dbg_lookup:
            # you found it!
            bp = {}
        else:
            # you have to process!
            if singleIntake['profileDataSource'] == 'thermodynamics':
                mymap = {
                    'condensate_prod': {'name': 'no_condensate', 'fnc': lambda x: 0 if x == 'yes' else 1},
                    'n_stages': {},
                    'api_gravity': {},
                    'gor': {'name': 'log10_gor', 'fnc': np.log10},
                    'temp': {},
                    'p1': {},
                    'p2': {},
                    'methane': {'name': 'c1'},
                    'ethane': {'name': 'c2'},
                    'propane': {'name': 'c3'},
                }
                my_fp_fnc = self.thermo_lookup
            elif singleIntake['profileDataSource'] == 'Cardoso 2019 correlation':
                mymap = {
                    'methane': {'name': 'c1'},
                    'ethane': {'name': 'c2'},
                    'propane': {'name': 'c3'},
                    'butane': {'name': 'c4'},
                    'isobutane': {'name': 'ic4'},
                    'pentane': {'name': 'c5'},
                    'isopentane': {'name': 'ic5'},
                    'hexanes': {'name': 'c6'},
                }
                my_fp_fnc = self.cardoso_corr
            elif singleIntake['profileDataSource'] == 'user specified':
                mymap = {
                    'carbon_dioxide': {'name': 'CARBON_DIOXIDE'},
                    'nitrogen': {'name': 'NITROGEN'},
                    'hydrogen_sulfide': {'name': 'HYDROGEN_SULFIDE'},
                    'methane': {'name': 'METHANE'},
                    'ethane': {'name': 'ETHANE'},
                    'propane': {'name': 'PROPANE'},
                    'butane': {'name': 'N_BUTANE'},
                    'isobutane': {'name': 'ISOBUTANE'},
                    'pentane': {'name': 'N_PENTANE'},
                    'isopentane': {'name': 'ISOPENTANE'},
                    'hexanes': {'name': 'N_HEXANE'},
                }
                my_fp_fnc = self.user_prof
            elif singleIntake['profileDataSource'] in ('AP-42', 'Subpart_C'):
                mymap = {'profileDataSource': {}}
                my_fp_fnc = self.combustion_slip
            else:
                raise ValueError('unknown profile data source: {}'.format(singleIntake['profileDataSource']))

            # select/map intake values
            param_values = self.map_values(singleIntake, mymap)

            # get fingerprint
            try:
                fp, bp = my_fp_fnc(param_values, singleIntake['GC_profile_id'])
            except:
                logging.warning('GC_profile_id = {}, FAILED!!!'.format(singleIntake['GC_profile_id']))
                fp, bp = {}, {}
                raise
            if 'mwt' in fp:
                mwt = fp['mwt']
                del fp['mwt']
            else:
                mwt = None
            fp = {'GC_profile_id': singleIntake['GC_profile_id'], 'intake': singleIntake, 'fingerprints': fp}
            if mwt is not None: fp['mwt'] = mwt
            # print(fp)

        return fp, bp, singleIntake

    def processIntakes(self, nproc=1):

        class DebugProcessor:
            def __init__(self):
                self.species = ['METHANE', 'ETHANE', 'PROPANE', 'ISOBUTANE', 'N_BUTANE', 'ISOPENTANE', 'N_PENTANE',
                           'N_HEXANE', 'PSEUDOCOMPONENT_1', 'PSEUDOCOMPONENT_2', 'C7_plus', 'ALL_VOC']
                self.vsp = ['PSEUDOCOMPONENT_1', 'PSEUDOCOMPONENT_2',
                       'C7_plus', ]  # species that mwt varies, but common across stream
                self.vsp2 = ['ALL_VOC']  # mwt varies and different across streams
                self.streams = ['wellstream', 'produced_gas', 'cond_tank', 'water_tank_flash', 'cond_tank_instant_flash']
                indices = ['comp_param_idx', 'comp_idx', 'param_idx', ]
                common_params = ['api_gravity', 'gor', 'temp', 'p1', 'p2', 'methane', 'ethane', 'propane']
                self.intake_params = ['condensate_prod', 'n_stages', ] + common_params
                self.matched_params = common_params + ['distance', 'box_dimension', 'count_in_box'] + indices

            def header(self):
                h = ['profile_id']
                for st in self.streams:
                    h += [sp + ', ' + st for sp in self.species]
                h += [p + ', ' + 'intake' for p in self.intake_params]
                h += [p + ', ' + 'matched' for p in self.matched_params]
                h += [sp + ', mwt' for sp in self.vsp]
                for st in self.streams:
                    h += [sp + ', mwt, ' + st for sp in self.vsp2]
                return h

            def body(self, results):
                fp, bp, sit = results

                if fp['intake']['profileDataSource'] != 'thermodynamics': return None
                v = [fp['profile_id']]
                try:
                    bp['gor'] = 10 ** bp['log10_gor']
                    bp['methane'] = bp['c1']
                    bp['ethane'] = bp['c2']
                    bp['propane'] = bp['c3']
                    bp['distance'] = bp['dst']
                except KeyError:
                    pass

                for st in self.streams:
                    if st in fp['fingerprints']:
                        try:
                            v += [fp['fingerprints'][st]['values'].get(sp,None) for sp in self.species]
                        except TypeError:
                            v += ([None] * len(self.species))
                    else:
                        v += ([None] * len(self.species))

                v += [sit.get(p, None) for p in self.intake_params]
                v += [bp.get(p, None) for p in self.matched_params]

                mw = fp['mwt']['values']
                v += [mw['produced_gas'].get(sp,None) for sp in self.vsp]
                for st in self.streams:
                    v += [mw.get(st,{}).get(sp,None) for sp in self.vsp2]
                return v

        with open(self.dbgOutFile if self.dbg_lookup else os.devnull, 'w', newline='') as fdbg:
            if self.dbg_lookup:
                import csv
                w = csv.writer(fdbg)
                dbp=DebugProcessor()
                w.writerow(dbp.header())

            with open(self.fingerprintFile, 'w') as f:
                if nproc > 1:
                    import multiprocessing
                    pool = multiprocessing.Pool(nproc)

                    for results in pool.imap(self.processOneIntake, self.intake):
                        fp, bp, sit = results
                        if bp:
                            logging.info('Looked up profile_id: {}'.format(fp['profile_id']))

                        f.write(json.dumps(fp) + '\n')

                        if self.dbg_lookup:
                            dbgdat = dbp.body(results)
                            if dbgdat: w.writerow(dbgdat)

                else:
                    for singleIntake in self.intake:
                        results = self.processOneIntake(singleIntake)
                        fp, bp, sit = results
                        if bp:
                            logging.info('Looked up profile_id: {}'.format(fp['profile_id']))
                        f.write(json.dumps(fp) + '\n')
                        if self.dbg_lookup:
                            dbgdat = dbp.body(results)
                            if dbgdat: w.writerow(dbgdat)


def main(config, dbg_lookup=False):
    lm = LookupManager(config, dbg_lookup)
    numProcs = config.get('processorCount', 1)
    lm.processIntakes(numProcs)
    


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config, _ = au.getConfig(DEFAULT_CONFIG)
    timer = Timer('LookupMain.main')
    timer.start()
    main(config=config, dbg_lookup=True)
    timer.stop()
    timer.report()
