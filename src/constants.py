import numpy as np

CATEGORIZED_SPECIES = {
    'non_scn_species': [
        'CARBON_DIOXIDE',
        'NITROGEN',
        'HYDROGEN_SULFIDE',
        'METHANE',
        'ETHANE',
        'PROPANE',
        'ISOBUTANE',
        'N_BUTANE',
        'ISOPENTANE',
        'N_PENTANE',
        'N_HEXANE'
    ],
    'other_species': [
        'HEPTANE',
        'SCN6'
    ],
    'scn_species_shared': [
        'SCN7',
        'SCN8',
        'SCN9',
        'SCN10',
        'SCN11',
        'SCN12',
        'SCN13',
        'SCN14',
        'SCN15',
        'SCN16',
        'SCN17',
        'SCN18',
        'SCN19',
        'SCN20',
        'SCN21',
        'SCN22',
        'SCN23',
        'SCN24',
        'SCN25',
        'SCN26',
        'SCN27',
        'SCN28',
        'SCN29',
        'SCN30',
        'SCN31',
        'SCN32',
        'SCN33',
        'SCN34',
        'SCN35',
        'SCN36',
        'SCN37',
        'SCN38',
        'SCN39',
        'SCN40',
        'SCN41',
        'SCN42',
        'SCN43',
        'SCN44'
    ],
    'behrens_species': [
        'PSEUDOCOMPONENT_1',
        'PSEUDOCOMPONENT_2'
    ]
}

ORDERED_SPECIES = CATEGORIZED_SPECIES['non_scn_species'] + \
    CATEGORIZED_SPECIES['other_species'] + \
    CATEGORIZED_SPECIES['scn_species_shared'] + \
    CATEGORIZED_SPECIES['behrens_species']

PROPERTIES = {
    'CARBON_DIOXIDE': {
        'GAMMA': None,
        'M': 44.01,
        'TC_RANKINE': 547.578,
        'PC_PSIA': 1070.68,
        'OMEGA': 0.2667,
        'N_CARBONS': 1
    },
    'NITROGEN': {
        'GAMMA': None,
        'M': 28.0,
        'TC_RANKINE': 227.16,
        'PC_PSIA': 492.855,
        'OMEGA': 0.0372,
        'N_CARBONS': 0
    },
    'HYDROGEN_SULFIDE': {
        'GAMMA': None,
        'M': 34.08,
        'TC_RANKINE': 671.6,
        'PC_PSIA': 1305.0,
        'OMEGA': 0.081,
        'N_CARBONS': 0
    },
    'METHANE': {
        'GAMMA': None,
        'M': 16.04,
        'TC_RANKINE': 343.08,
        'PC_PSIA': 666.855,
        'OMEGA': 0.011,
        'N_CARBONS': 1
    },
    'ETHANE': {
        'GAMMA': None,
        'M': 30.07,
        'TC_RANKINE': 549.54,
        'PC_PSIA': 706.44,
        'OMEGA': 0.099,
        'N_CARBONS': 2
    },
    'PROPANE': {
        'GAMMA': 0.506,
        'M': 44.1,
        'TC_RANKINE': 665.64,
        'PC_PSIA': 615.96,
        'OMEGA': 0.153,
        'N_CARBONS': 3
    },
    'ISOBUTANE': {
        'GAMMA': 0.572,
        'M': 58.12,
        'TC_RANKINE': 734.04,
        'PC_PSIA': 522.58,
        'OMEGA': 0.183,
        'N_CARBONS': 4
    },
    'N_BUTANE': {
        'GAMMA': 0.572,
        'M': 58.12,
        'TC_RANKINE': 765.18,
        'PC_PSIA': 550.42,
        'OMEGA': 0.199,
        'N_CARBONS': 4
    },
    'ISOPENTANE': {
        'GAMMA': 0.62,
        'M': 72.15,
        'TC_RANKINE': 828.72,
        'PC_PSIA': 490.1,
        'OMEGA': 0.227,
        'N_CARBONS': 5
    },
    'N_PENTANE': {
        'GAMMA': 0.62599,
        'M': 72.15,
        'TC_RANKINE': 845.46,
        'PC_PSIA': 488.65,
        'OMEGA': 0.251,
        'N_CARBONS': 5
    },
    'N_HEXANE': {
        'GAMMA': 0.659,
        'M': 86.18,
        'TC_RANKINE': 913.68,
        'PC_PSIA': 438.625,
        'OMEGA': 0.299,
        'N_CARBONS': 6
    },
    'SCN6': {
        'GAMMA': 0.69,
        'M': 84.0,
        'TC_RANKINE': 914.0,
        'PC_PSIA': 476.0,
        'OMEGA': 0.271,
        'N_CARBONS': 6
    },
    'SCN7': {
        'GAMMA': 0.727,
        'M': 96.0,
        'TC_RANKINE': 976.0,
        'PC_PSIA': 457.0,
        'OMEGA': 0.31,
        'N_CARBONS': 7
    },
    'SCN8': {
        'GAMMA': 0.749,
        'M': 107.0,
        'TC_RANKINE': 1027.0,
        'PC_PSIA': 428.0,
        'OMEGA': 0.349,
        'N_CARBONS': 8
    },
    'SCN9': {
        'GAMMA': 0.748,
        'M': 121.0,
        'TC_RANKINE': 1077.0,
        'PC_PSIA': 397.0,
        'OMEGA': 0.392,
        'N_CARBONS': 9
    },
    'SCN10': {
        'GAMMA': 0.782,
        'M': 134.0,
        'TC_RANKINE': 1120.0,
        'PC_PSIA': 367.0,
        'OMEGA': 0.437,
        'N_CARBONS': 10
    },
    'SCN11': {
        'GAMMA': 0.793,
        'M': 147.0,
        'TC_RANKINE': 1158.0,
        'PC_PSIA': 341.0,
        'OMEGA': 0.479,
        'N_CARBONS': 11
    },
    'SCN12': {
        'GAMMA': 0.804,
        'M': 161.0,
        'TC_RANKINE': 1195.0,
        'PC_PSIA': 318.0,
        'OMEGA': 0.523,
        'N_CARBONS': 12
    },
    'SCN13': {
        'GAMMA': 0.815,
        'M': 175.0,
        'TC_RANKINE': 1228.0,
        'PC_PSIA': 301.0,
        'OMEGA': 0.561,
        'N_CARBONS': 13
    },
    'SCN14': {
        'GAMMA': 0.826,
        'M': 190.0,
        'TC_RANKINE': 1261.0,
        'PC_PSIA': 284.0,
        'OMEGA': 0.601,
        'N_CARBONS': 14
    },
    'SCN15': {
        'GAMMA': 0.836,
        'M': 206.0,
        'TC_RANKINE': 1294.0,
        'PC_PSIA': 268.0,
        'OMEGA': 0.644,
        'N_CARBONS': 15
    },
    'SCN16': {
        'GAMMA': 0.843,
        'M': 222.0,
        'TC_RANKINE': 1321.0,
        'PC_PSIA': 253.0,
        'OMEGA': 0.684,
        'N_CARBONS': 16
    },
    'SCN17': {
        'GAMMA': 0.851,
        'M': 237.0,
        'TC_RANKINE': 1349.0,
        'PC_PSIA': 240.0,
        'OMEGA': 0.723,
        'N_CARBONS': 17
    },
    'SCN18': {
        'GAMMA': 0.856,
        'M': 251.0,
        'TC_RANKINE': 1369.0,
        'PC_PSIA': 230.0,
        'OMEGA': 0.754,
        'N_CARBONS': 18
    },
    'SCN19': {
        'GAMMA': 0.861,
        'M': 263.0,
        'TC_RANKINE': 1388.0,
        'PC_PSIA': 221.0,
        'OMEGA': 0.784,
        'N_CARBONS': 19
    },
    'SCN20': {
        'GAMMA': 0.866,
        'M': 275.0,
        'TC_RANKINE': 1408.0,
        'PC_PSIA': 212.0,
        'OMEGA': 0.816,
        'N_CARBONS': 20
    },
    'SCN21': {
        'GAMMA': 0.871,
        'M': 291.0,
        'TC_RANKINE': 1428.0,
        'PC_PSIA': 203.0,
        'OMEGA': 0.849,
        'N_CARBONS': 21
    },
    'SCN22': {
        'GAMMA': 0.876,
        'M': 300.0,
        'TC_RANKINE': 1447.0,
        'PC_PSIA': 195.0,
        'OMEGA': 0.879,
        'N_CARBONS': 22
    },
    'SCN23': {
        'GAMMA': 0.881,
        'M': 312.0,
        'TC_RANKINE': 1466.0,
        'PC_PSIA': 188.0,
        'OMEGA': 0.909,
        'N_CARBONS': 23
    },
    'SCN24': {
        'GAMMA': 0.885,
        'M': 324.0,
        'TC_RANKINE': 1482.0,
        'PC_PSIA': 182.0,
        'OMEGA': 0.936,
        'N_CARBONS': 24
    },
    'SCN25': {
        'GAMMA': 0.888,
        'M': 337.0,
        'TC_RANKINE': 1498.0,
        'PC_PSIA': 175.0,
        'OMEGA': 0.965,
        'N_CARBONS': 25
    },
    'SCN26': {
        'GAMMA': 0.892,
        'M': 349.0,
        'TC_RANKINE': 1515.0,
        'PC_PSIA': 168.0,
        'OMEGA': 0.992,
        'N_CARBONS': 26
    },
    'SCN27': {
        'GAMMA': 0.896,
        'M': 360.0,
        'TC_RANKINE': 1531.0,
        'PC_PSIA': 163.0,
        'OMEGA': 1.019,
        'N_CARBONS': 27
    },
    'SCN28': {
        'GAMMA': 0.899,
        'M': 372.0,
        'TC_RANKINE': 1545.0,
        'PC_PSIA': 157.0,
        'OMEGA': 1.044,
        'N_CARBONS': 28
    },
    'SCN29': {
        'GAMMA': 0.902,
        'M': 382.0,
        'TC_RANKINE': 1559.0,
        'PC_PSIA': 152.0,
        'OMEGA': 1.065,
        'N_CARBONS': 29
    },
    'SCN30': {
        'GAMMA': 0.905,
        'M': 394.0,
        'TC_RANKINE': 1571.0,
        'PC_PSIA': 149.0,
        'OMEGA': 1.084,
        'N_CARBONS': 30
    },
    'SCN31': {
        'GAMMA': 0.909,
        'M': 404.0,
        'TC_RANKINE': 1584.0,
        'PC_PSIA': 145.0,
        'OMEGA': 1.104,
        'N_CARBONS': 31
    },
    'SCN32': {
        'GAMMA': 0.912,
        'M': 415.0,
        'TC_RANKINE': 1596.0,
        'PC_PSIA': 141.0,
        'OMEGA': 1.122,
        'N_CARBONS': 32
    },
    'SCN33': {
        'GAMMA': 0.915,
        'M': 426.0,
        'TC_RANKINE': 1608.0,
        'PC_PSIA': 138.0,
        'OMEGA': 1.141,
        'N_CARBONS': 33
    },
    'SCN34': {
        'GAMMA': 0.917,
        'M': 437.0,
        'TC_RANKINE': 1618.0,
        'PC_PSIA': 135.0,
        'OMEGA': 1.157,
        'N_CARBONS': 34
    },
    'SCN35': {
        'GAMMA': 0.92,
        'M': 445.0,
        'TC_RANKINE': 1630.0,
        'PC_PSIA': 131.0,
        'OMEGA': 1.175,
        'N_CARBONS': 35
    },
    'SCN36': {
        'GAMMA': 0.922,
        'M': 456.0,
        'TC_RANKINE': 1640.0,
        'PC_PSIA': 128.0,
        'OMEGA': 1.192,
        'N_CARBONS': 36
    },
    'SCN37': {
        'GAMMA': 0.925,
        'M': 464.0,
        'TC_RANKINE': 1650.0,
        'PC_PSIA': 126.0,
        'OMEGA': 1.207,
        'N_CARBONS': 37
    },
    'SCN38': {
        'GAMMA': 0.927,
        'M': 475.0,
        'TC_RANKINE': 1661.0,
        'PC_PSIA': 122.0,
        'OMEGA': 1.226,
        'N_CARBONS': 38
    },
    'SCN39': {
        'GAMMA': 0.929,
        'M': 484.0,
        'TC_RANKINE': 1671.0,
        'PC_PSIA': 119.0,
        'OMEGA': 1.242,
        'N_CARBONS': 39
    },
    'SCN40': {
        'GAMMA': 0.931,
        'M': 495.0,
        'TC_RANKINE': 1681.0,
        'PC_PSIA': 116.0,
        'OMEGA': 1.258,
        'N_CARBONS': 40
    },
    'SCN41': {
        'GAMMA': 0.933,
        'M': 502.0,
        'TC_RANKINE': 1690.0,
        'PC_PSIA': 114.0,
        'OMEGA': 1.272,
        'N_CARBONS': 41
    },
    'SCN42': {
        'GAMMA': 0.934,
        'M': 512.0,
        'TC_RANKINE': 1697.0,
        'PC_PSIA': 112.0,
        'OMEGA': 1.287,
        'N_CARBONS': 42
    },
    'SCN43': {
        'GAMMA': 0.936,
        'M': 521.0,
        'TC_RANKINE': 1706.0,
        'PC_PSIA': 109.0,
        'OMEGA': 1.3,
        'N_CARBONS': 43
    },
    'SCN44': {
        'GAMMA': 0.938,
        'M': 531.0,
        'TC_RANKINE': 1716.0,
        'PC_PSIA': 107.0,
        'OMEGA': 1.316,
        'N_CARBONS': 44
    }
}

NAME_MAP = {
    'C7_plus': 'C7_plus',
    'M7_plus': 'M7_plus',
    'CARBON_DIOXIDE': 'CARBON_DIOXIDE',
    'Carbon Dioxide': 'CARBON_DIOXIDE',
    'CarbonDioxide': 'CARBON_DIOXIDE',
    'Carbon_Dioxide': 'CARBON_DIOXIDE',
    'CO2': 'CARBON_DIOXIDE',
    'NITROGEN': 'NITROGEN',
    'Nitrogen': 'NITROGEN',
    'N2': 'NITROGEN',
    'HYDROGEN_SULFIDE': 'HYDROGEN_SULFIDE',
    'Hydrogen Sulfide': 'HYDROGEN_SULFIDE',
    'HydrogenSulfide': 'HYDROGEN_SULFIDE',
    'Hydrogen_Sulfide': 'HYDROGEN_SULFIDE',
    'H2S': 'HYDROGEN_SULFIDE',
    'METHANE': 'METHANE',
    'Methane': 'METHANE',
    'C1': 'METHANE',
    'ETHANE': 'ETHANE',
    'Ethane': 'ETHANE',
    'C2': 'ETHANE',
    'PROPANE': 'PROPANE',
    'Propane': 'PROPANE',
    'C3': 'PROPANE',
    'ISOBUTANE': 'ISOBUTANE',
    'Isobutane': 'ISOBUTANE',
    'iC4': 'ISOBUTANE',
    'N_BUTANE': 'N_BUTANE',
    'n-Butane': 'N_BUTANE',
    'nButane': 'N_BUTANE',
    'n_Butane': 'N_BUTANE',
    'C4': 'N_BUTANE',
    'ISOPENTANE': 'ISOPENTANE',
    'Isopentane': 'ISOPENTANE',
    'iC5': 'ISOPENTANE',
    'N_PENTANE': 'N_PENTANE',
    'n-Pentane': 'N_PENTANE',
    'nPentane': 'N_PENTANE',
    'n_Pentane': 'N_PENTANE',
    'C5': 'N_PENTANE',
    'N_HEXANE': 'N_HEXANE',
    'n-Hexane': 'N_HEXANE',
    'nHexane': 'N_HEXANE',
    'n_Hexane': 'N_HEXANE',
    'C6': 'N_HEXANE',
    'HEPTANE': 'HEPTANE',
    'Heptane': 'HEPTANE',
    'SCN7': 'SCN7',
    'SCN8': 'SCN8',
    'SCN9': 'SCN9',
    'SCN10': 'SCN10',
    'SCN11': 'SCN11',
    'SCN12': 'SCN12',
    'SCN13': 'SCN13',
    'SCN14': 'SCN14',
    'SCN15': 'SCN15',
    'SCN16': 'SCN16',
    'SCN17': 'SCN17',
    'SCN18': 'SCN18',
    'SCN19': 'SCN19',
    'SCN20': 'SCN20',
    'SCN21': 'SCN21',
    'SCN22': 'SCN22',
    'SCN23': 'SCN23',
    'SCN24': 'SCN24',
    'SCN25': 'SCN25',
    'SCN26': 'SCN26',
    'SCN27': 'SCN27',
    'SCN28': 'SCN28',
    'SCN29': 'SCN29',
    'SCN30': 'SCN30',
    'SCN31': 'SCN31',
    'SCN32': 'SCN32',
    'SCN33': 'SCN33',
    'SCN34': 'SCN34',
    'SCN35': 'SCN35',
    'SCN36': 'SCN36',
    'SCN37': 'SCN37',
    'SCN38': 'SCN38',
    'SCN39': 'SCN39',
    'SCN40': 'SCN40',
    'SCN41': 'SCN41',
    'SCN42': 'SCN42',
    'SCN43': 'SCN43',
    'SCN44': 'SCN44'
}

# coefficients in the Standing Katz density correlation
SK_COEFS = {
    'Mc1c1_4': 0.00058,
    'Mc1c1_3': 0.0133,
    'Mc1c1_2': 0.000158,
    'Mc1c1_1': 0.012,
    'Mc2c2_3': 0.379,
    'Mc2c2_2': 0.000082,
    'Mc2c2_1': 0.01386,
    'Mc2c2_4': 0.0042
}

# TODO: separate constants and conversion factors

CONSTANTS = {
    'CUBIC_M_PER_FT': 0.02832,
    'STD_REPORTING_TMP_K': 288.7,  # model assumption
    'ATMOSPHERIC_PRESS_PSIA': 14.65,  # model assumption
    'ATMOSPHERIC_PRESS_PA': 100940.0,
    'WELLSTREAM_PRESS_PSIA': 1500,
    'GAS_CONSTANT_J_K_MOL': 8.314,
    'GAL_PER_M3': 264.2,
    'GAL_PER_BBL': 42,
    'LB_FT3_PER_KG_M3': 0.06243,
    'M3_PER_GAL': 0.003785,
    'OMEGA_A': 0.45724,
    'OMEGA_B': 0.0778,
    'GAS_CONST_IMPERIAL': 10.73,  # (psiaft3)/(lb-molRankine)
    'GAS_CONST_SI': 8.314,  # JK/mol
    'DAYS_PER_HOUR': (1.0/24.0),
    'HENRY_MOL/M3*PA_TO_1/ATM': 1.83089,
    'MOL_WEIGHT_WATER_G/MOL': 18,
    'DENSITY_WATER_KG/M3': 1000,
    'G/KG': 1000,
    'H/DAY': 24,
    'SPECIFIC_GRAVITY_WATER': 1000,
    'MOLES_WATER_PER_BBL': 8831.667,
    'AMBIENT_TEMP_F': 80,
    'PA_PER_PSIA': 6894.76
}

MOLECULAR_WEIGHTS = {
    'METHANE': 16.04,
    'ETHANE': 30.07,
    'PROPANE': 44.1
}

# Sander, Rolf. "Compilation of Henry's law constants (version 4.0) for water as solvent." Atmospheric Chemistry & Physics 15.8 (2015).
# We use heptane coefficients to estimate the pseudocomponents because there is no data for Henry's law of
# pseudocomponents. Heptane represents an upper bound on their amount dissolved in water, and thus on their emissions.
# This will be relevant when calculating emissions from VOC, doesn't affect emissions of methane or other light alkanes.
HENRYS_LAW = {
    'METHANE': {
        'HCP_MOL/M3*PA': np.array([1.40E-05, 1.40E-05, 1.40E-05, 1.40E-05, 1.40E-05, 1.40E-05, 1.20E-05, 1.30E-05, 1.40E-05, 1.40E-05, 1.30E-05]),
        'DELTA_H': np.array([1900, 1600, 1600, 1500, 1600, 1700, 2400, 1400, 1600, 1600, 1900])
    },
    'ETHANE': {
        'HCP_MOL/M3*PA': np.array([1.90E-05, 1.90E-05, 1.90E-05, 1.90E-05, 1.80E-05, 2.00E-05, 1.90E-05, 1.90E-05]),
        'DELTA_H': np.array([2400, 2400, 2400, 2300, 2400, 2200, 2300, 2700])
    },
    'PROPANE': {
        'HCP_MOL/M3*PA': np.array([1.50E-05, 1.50E-05, 1.50E-05, 1.50E-05, 1.60E-05, 1.50E-05]),
        'DELTA_H': np.array([2700, 2700, 2800, 2700, 2700, 2700])
    },
    'ISOBUTANE': {
        'HCP_MOL/M3*PA': np.array([9.10E-06, 9.10E-06, 8.00E-06, 1.10E-04]),
        'DELTA_H': np.array([2700, 2700, 2700, 5100])
    },
    'N_BUTANE': {
        'HCP_MOL/M3*PA': np.array([1.20E-05, 1.20E-05, 1.30E-05, 1.20E-05, 1.30E-05]),
        'DELTA_H': np.array([3100, 3100, 3100, 3100, 2300])
    },
    'ISOPENTANE': {
        'HCP_MOL/M3*PA': np.array([8.00E-06, 1.10E-05, 8.20E-06]),
        'DELTA_H': np.array([3400, 2300, 3600])
    },
    'N_PENTANE': {
        'HCP_MOL/M3*PA': np.array([8.00E-06, 1.10E-05, 8.20E-06]),
        'DELTA_H': np.array([3400, 2300, 3600])
    },
    'N_HEXANE': {
        'HCP_MOL/M3*PA': np.array([6.10E-06, 2.40E-04, 9.90E-06, 6.70E-06, 5.90E-06]),
        'DELTA_H': np.array([3800, 8700, 7500, 4200, 4000])
    },
    'HEPTANE': {
        'HCP_MOL/M3*PA': np.array([4.40E-06, 1.20E-05, 4.20E-06]),
        'DELTA_H': np.array([4100, 3700, 4700])
    },
    'OCTANE': {
        'HCP_MOL/M3*PA': np.array([3.10E-06, 3.00E-05, 3.10E-06, 2.90E-06]),
        'DELTA_H': np.array([4300, 8000, 4100, 5400])
    },
    'PSEUDOCOMPONENT_1': { # same as heptane
        'HCP_MOL/M3*PA': np.array([4.40E-06, 1.20E-05, 4.20E-06]),
        'DELTA_H': np.array([4100, 3700, 4700])
    },
    'PSEUDOCOMPONENT_2': { # same as heptane
        'HCP_MOL/M3*PA': np.array([4.40E-06, 1.20E-05, 4.20E-06]),
        'DELTA_H': np.array([4100, 3700, 4700])
    }
}