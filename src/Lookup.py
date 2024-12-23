import pandas as pd
import numpy as np
import sqlite3
import logging
import os

# copied constants from thermodynamic/src/util/constants.py
CATEGORIZED_SPECIES = {
    'non_scn_species': [
        'CARBON_DIOXIDE', 'NITROGEN', 'HYDROGEN_SULFIDE',
        'METHANE', 'ETHANE', 'PROPANE',
        'ISOBUTANE', 'N_BUTANE', 'ISOPENTANE', 'N_PENTANE', 'N_HEXANE'
    ],
    'other_species': [
        'HEPTANE', 'SCN6'
    ],
    'scn_species_shared': [
        'SCN7', 'SCN8', 'SCN9', 'SCN10', 'SCN11', 'SCN12', 'SCN13', 'SCN14',
        'SCN15', 'SCN16', 'SCN17', 'SCN18', 'SCN19', 'SCN20', 'SCN21', 'SCN22',
        'SCN23', 'SCN24', 'SCN25', 'SCN26', 'SCN27', 'SCN28', 'SCN29', 'SCN30',
        'SCN31', 'SCN32', 'SCN33', 'SCN34', 'SCN35', 'SCN36', 'SCN37', 'SCN38',
        'SCN39', 'SCN40', 'SCN41', 'SCN42', 'SCN43', 'SCN45'
    ],
    'behrens_species': [
        'PSEUDOCOMPONENT_1', 'PSEUDOCOMPONENT_2'
    ]
}
# i just define here
MWt = {
    # some molecular weights (g/mol):
    'METHANE': 16.04,
    'ETHANE': 30.07,
    'PROPANE': 44.01,
    'N_BUTANE': 58.1,
    'ISOBUTANE': 58.1,
    'ISOPENTANE': 72.2,
    'N_PENTANE': 72.2,
    'N_HEXANE': 86.18,
    'CARBON_DIOXIDE': 44.009,
    'NITROGEN': 28.014,
}


class Lookup:
    def __init__(self, db_name):
        self.db_name = db_name
        logging.info(f'loading thermo datbase file {db_name}')
        if not os.path.exists(db_name):
            raise ValueError(f'thermo database file does not exists: {db_name}')
        self.conn = sqlite3.connect(db_name)
        self.c = self.conn.cursor()

        # special variables:
        # these values are treated differently when filtering data

        # known_parameters: input parameter for param_sweep.  Unlike regular variables, DB has fixed set of values
        self.known_parameters = ['temp', 'p1', 'p2']
        # known_switches: it can take only 0 or 1
        self.known_switches = ['n_stages', 'no_condensate']

        # how many divisions were made for IVR
        try:
            db_config = pd.read_sql('SELECT * FROM bld_config', self.conn)
            # for x in db_config.itertuples():
            #     print(x)
            self.db_config = {x.item:x.value for x in db_config.itertuples()}
            # print(self.db_config)
            self.ndiv_ivr = int(self.db_config['ndiv_ivr'])
            division_method = self.db_config['division_method']
            if 'np.floor' in division_method:
                self.division_method = 'use_floor' #old method
            else:
                self.division_method = 'use_trunc' # new, correct method
        except pd.io.sql.DatabaseError:
            self.db_config = {}
            self.ndiv_ivr = 10 # 10 was default in earlier version of database
        if self.ndiv_ivr >= 20:
            # start with 3x3 box in filtering
            self.initial_boxes = 3
        else:
            # start with single box in filtering
            self.initial_boxes = 1

        # scale for normalization
        self.lkup_norm = pd.read_sql('select * from lkup_norm', self.conn,
                                     index_col='measure')

        self.known_parameter_values = {}
        for param in self.known_parameters:
            df =  pd.read_sql(f'select distinct {param} from parameterization order by {param}', self.conn)
            df['k_' + param] = df[param].apply(lambda x: self.get_index(param, x))
            self.known_parameter_values[param] = df
        pass

    def normalize(self, name, value):
        q05, q95, ivr = self.lkup_norm.loc[name, ['q05', 'q95', 'ivr']]
        return (value - .5 * (q05 + q95)) / ivr

    def get_initial_pair(self, name, value, idx_center):
        """detemine low/high of initial box for three parameters, for one dimension"""

        # originally i thought that we start from single box, then expand when there is no match
        # but for Temperature the data is sparse, so i figured i need to start with  range of boxes so that
        # preset value of Temperaure is included from the start (so that i dont have to expand box just to include
        # temperature value being sweeped.

        df = self.known_parameter_values[name]

        # find the rows for at or above, at or below the user specified value
        try:
            idx_below = df[df[name] <= value].iloc[-1,-1]
        except IndexError:
            idx_below = -np.Inf
        try:
            idx_above = df[df[name] >= value].iloc[0,-1]
        except IndexError:
            idx_above = +np.Inf

        # find smaller distance to above/below
        assert idx_below <= idx_center and idx_center <= idx_above
        r = min(idx_above-idx_center, idx_center-idx_below)

        pair = [idx_center-r, idx_center+r]

        return pair

    def get_index(self, name, value):
        v = self.normalize(name, value)
        if self.division_method == 'use_floor':
            return np.floor(self.ndiv_ivr * v).astype(int)
        else:
            return np.trunc(self.ndiv_ivr * v).astype(int)

    def search(self, vals):
        """given dict of values find idx of blocks with records"""

        # behavior is going to be different between p, T and the rest of parameters:
        #
        # basic algorithm goes like this:
        # - user specifies target value for each parameter
        # - identify box that contains the parameter
        # - if any record are found in the box, search is done, and return the records
        # - if no record is found, expand grid in all direction by one, and redo the search
        #
        # for p, T,
        # - ensure that two nearest values are included i the first query
        # - after that, follow the growth of look up range as with the rest of parameters
        # - be particularly careful for T, since the default set of parameter values had only 6 values.
        #   so in first time you grab distant two index to cover two values.



        # vals is user specified dict, with key being parameters to search
        idx_fixed = {}
        idx_expandable = {}
        idx_initial_pair ={}
        for k, v in vals.items():
            if k in self.known_switches:
                # never expand the dimension for box for filter
                idx_fixed[k] = v
            else:
                # idx_expandable is mid point, the box closest to the search value.  All parameter has this value
                idx_expandable['k_' + k] = self.get_index(k, v)
                # idx_initial_pair is either (1) pair of same value as idx_expandable or (2) pair of boxes of equal
                # distance from idx_pair, such that pre-fixed parameter values (picked in parameter sweep) is included
                if k in self.known_parameters:
                    idx_initial_pair['k_' + k] = self.get_initial_pair(k, v, idx_expandable['k_' + k])
                else:
                    idx_initial_pair['k_' + k] = [idx_expandable['k_' + k]] * 2
        pass


        if self.initial_boxes == 1:
            # search with the 1x1 box
            where_expression = ' AND '.join(
                ['{} = {}'.format(k, v) for (k, v) in idx_fixed.items()] +
                ['{} >= {} AND {} <= {}'.format(k, v[0], k, v[1]) for (k, v) in idx_initial_pair.items()]
            )
            qry = f"""select * from forlookup where {where_expression}"""
            df = pd.read_sql(qry, self.conn)
            rr = 0
            r = 0
        else:
            # skip initial search.  start with r=1, which is 3x3 box
            df = pd.DataFrame({}, index=[])
            rr = 0

        # r is "radious" of the box.  size of box (# of boxes on one axis) is 2r+1 (r boxes on both side).
        # for example, if r=1, 3 boxes on one dimension, if r=2, 5 boxes and so on.
        # rr is seed for r:  I use geomtric growth in size:
        # rr=1 => r=1 => n=3;  rr=2 => r=2 => n=5, rr=3 => r=4, n=9, rr=4 => r=7 => n=15 and so on.
        # originally i was linearly increasing the size but i had performance issue working with cases where you have
        # to reach out far.
        while len(df.index) == 0:
            rr += 1
            r = rr*(rr-1)/2+1

            if r > self.ndiv_ivr * 2:
                raise ValueError(f'infinite loop... qry: {qry}')

            where_expression = ' AND '.join(
                ['{} = {}'.format(k, v) for (k, v) in idx_fixed.items()] +
                ['{} >= {} AND {} <= {}'.format(k, min(v[0], idx_expandable[k]-r),
                                                k, max(v[1], idx_expandable[k]+r))
                 for (k, v) in idx_initial_pair.items()]
            )
            qry = f"""select * from forlookup where {where_expression}"""
            df = pd.read_sql(qry, self.conn)

        # print(df)
        return df, {'n':len(df.index), 'r': r}

    def euclid_distance(self, vals, df):

        # df should have pre-selected records
        # distance calculated for all records

        try:
            dst = sum((df['n_'+k] - self.normalize(k, v)) ** 2 for (k, v) in vals.items())
        except KeyError:
            dst = sum(((df[k].apply(lambda x: self.normalize(k, x)) - self.normalize(k, v))) ** 2 for (k, v) in vals.items())
        return dst

    def get_fingerprint(self, n_stages, comp_param_idx, comp_idx, param_idx):

        fingerprints = {}
        my_mwt = MWt.copy()

        VOC_explict = CATEGORIZED_SPECIES['non_scn_species'][5:]

        # well
        wellstream_fingerprint = pd.read_sql_query(
            """SELECT {sp} FROM wellstream WHERE comp_idx = {idx};""".format(
                sp=', '.join(CATEGORIZED_SPECIES['non_scn_species']+['C7_plus', 'M7_plus']),
                idx=comp_idx
            ),
            self.conn
        )

        # save mwt of C7+
        my_mwt['C7_plus'] = wellstream_fingerprint.loc[0,'M7_plus']


        wellstream_fingerprint = wellstream_fingerprint.drop(columns='M7_plus')

        # ALL_VOC is not in database
        wellstream_fingerprint['ALL_VOC'] = wellstream_fingerprint[VOC_explict+['C7_plus']].sum(axis=1)

        # convert percentage to fraction
        wellstream_fingerprint = wellstream_fingerprint.mul(.01)

        fingerprints['wellstream'] = wellstream_fingerprint.to_dict(orient='records')[0]


        pseudocomponent_mwts = pd.read_sql_query(
            """SELECT Compound, M FROM pseudocomponent WHERE comp_idx = {idx};""".format(
                idx=comp_idx
            ),
            self.conn
        )
        mwt_pc = {k:v for k,v in zip(pseudocomponent_mwts['Compound'], pseudocomponent_mwts['M'])}
        my_mwt.update(mwt_pc)

        if n_stages == 1:
            production_stage = 'stg1'
        elif n_stages == 2:
            production_stage = 'all_sep'
        else:
            raise ValueError('unknown n_stages: {}'.format(n_stages))

        prod_gas_fp = pd.read_sql_query(
            """SELECT Compound, molar_fraction, stage FROM separators 
            WHERE comp_param_idx = '{cp_idx}' 
            AND stream = 'gas' AND stage = '{prod_stg}'""".format(
                cp_idx=comp_param_idx,
                prod_stg=production_stage
            ),
            self.conn
        ).set_index('Compound')

        fingerprints['produced_gas'] = prod_gas_fp[['molar_fraction']].transpose().to_dict(orient='records')[0]
        # ALL_VOC is not in database
        fingerprints['produced_gas']['ALL_VOC'] = sum(fingerprints['produced_gas'][_] for _ in VOC_explict+CATEGORIZED_SPECIES['behrens_species'])

        # ALL_VOC mwt
        extra_mwt = {k:v for k,v in my_mwt.items() if k not in MWt}
        fingerprints['mwt'] = {_:extra_mwt.copy() for _ in ('wellstream', 'produced_gas')}

        fingerprints['mwt']['wellstream']['ALL_VOC'] = sum(
            fingerprints['wellstream'][_]*my_mwt[_] for _ in VOC_explict+['C7_plus']) / sum(
            fingerprints['wellstream'][_] for _ in VOC_explict + ['C7_plus']
        )

        fingerprints['mwt']['produced_gas']['ALL_VOC'] = sum(
            fingerprints['produced_gas'][_]*my_mwt[_] for _ in VOC_explict+CATEGORIZED_SPECIES['behrens_species']
            if fingerprints['produced_gas'][_]>0 ) / sum(
            fingerprints['produced_gas'][_] for _ in VOC_explict + CATEGORIZED_SPECIES['behrens_species']
            if fingerprints['produced_gas'][_] > 0
        )


        # tank flash "leak"
        tanks_fp = pd.read_sql_query(
            """SELECT Compound, molar_fraction FROM tanks 
            WHERE comp_param_idx = '{cp_idx}' AND stream = 'gas' AND stage = 'tank';""".format(
                cp_idx=comp_param_idx
            ),
            self.conn
        ).set_index('Compound')
        if len(tanks_fp.index) > 0:
            fingerprints['cond_tank'] = tanks_fp.transpose().to_dict(orient='records')[0]

        # tank flash
        flash_fingerprints = pd.read_sql_query(
            """SELECT Compound, flash_kg_per_bbl, stage FROM flash_emissions 
            WHERE comp_param_idx = '{cp_idx}' 
            AND stage IN ('water_tank', 'cond_tank_instant');""".format(
                cp_idx=comp_param_idx
            ),
            self.conn
        ).set_index('Compound')

        for stage in flash_fingerprints['stage'].unique():
            fingerprints[stage + '_flash'] = \
                flash_fingerprints[flash_fingerprints['stage'] == stage][['flash_kg_per_bbl']].transpose().to_dict(
                    orient='records')[0]

        return fingerprints
