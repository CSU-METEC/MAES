from enum import Enum, auto
import datetime
import MEETGlobals as mg
import SimDataManager as sm

SECONDS_PER_MINUTE = 60
MINUTES_PER_HOUR = 60
HOURS_PER_DAY = 24
DAYS_PER_MONTH = 30  #30.41667 Code breaks when this is not an integer
DAYS_PER_YEAR = 365

GALLONS_PER_BARREL = 42

HOURS_PER_YEAR = HOURS_PER_DAY*DAYS_PER_YEAR
SECONDS_PER_HOUR = SECONDS_PER_MINUTE * MINUTES_PER_HOUR
SECONDS_PER_DAY = SECONDS_PER_MINUTE * MINUTES_PER_HOUR * HOURS_PER_DAY
SECONDS_PER_MONTH = SECONDS_PER_DAY * DAYS_PER_MONTH
SECONDS_PER_YEAR = SECONDS_PER_DAY * DAYS_PER_YEAR

FOREVER = SECONDS_PER_YEAR * 100

JOULES_PER_BTU = 1055.06
KJ_PER_BTU = JOULES_PER_BTU/1000
W_PER_KW = 1000
JOULES_PER_KWH = W_PER_KW * SECONDS_PER_HOUR
KW_PER_HP = 0.745699872
HPHR_PER_BTU = JOULES_PER_BTU/(JOULES_PER_KWH * KW_PER_HP)
kWh_PER_MMBTU = (JOULES_PER_BTU / JOULES_PER_KWH) * 1000000
GRAMS_PER_POUND = 453.59237
GRAMS_PER_KG = 1000
KG_PER_TON = 1000

# species
MWT_CH4_GMOL = 16.043
MWT_CO2_GMOL = 44.01

# pressure
PA_PER_PSI = 6894.76

# temperature
F_TO_C = lambda f: (f - 32.) * 5. / 9.
C_TO_F = lambda c: (c * 9. / 5.) + 32.
F_TO_R = lambda f: f + 459.67
C_TO_K = lambda c: c + 273.15

# length
FEET_PER_METER = 3.28084
M_PER_FEET = 1 / FEET_PER_METER

# mass
KG_PER_LBS = 0.453592

GAS_CONSTANT_JKMOL = 8.31446261815324
KG_TO_SHORT_TONS = 0.00110231
SHORT_TONS_TO_KG = 907.185

class PlotInterval(Enum):
    SECONDS=auto()
    MINUTES=auto()
    HOURS=auto()
    DAYS=auto()
    MONTHS=auto()
    YEARS=auto()

# Indentation
# Moved to global
# Refer to constants in u directly, instead of TIME_SCALE_FACTORS

PLOT_PARAMS = {
    PlotInterval.SECONDS: {'scale': 1,                  'text': 'Seconds', 'units': 's'},
    PlotInterval.MINUTES: {'scale': SECONDS_PER_MINUTE, 'text': 'Minutes', 'units': 'min'},
    PlotInterval.HOURS:   {'scale': SECONDS_PER_HOUR,   'text': 'Hours',   'units': 'hr'},
    PlotInterval.DAYS:    {'scale': SECONDS_PER_DAY,    'text': 'Days',    'units': 'day'},
    PlotInterval.MONTHS:  {'scale': SECONDS_PER_MONTH,  'text': 'Months',  'units': 'month'},
    PlotInterval.YEARS:   {'scale': SECONDS_PER_YEAR,   'text': 'Years',   'units': 'yr'},
}

def timeScale(inPlotInterval):
    plotInterval = PlotInterval[inPlotInterval]
    return PLOT_PARAMS[plotInterval]['scale']

def secsToDays(secs):
    days = secs / SECONDS_PER_DAY
    return days

def secsToMonths(secs):
    months = secs / SECONDS_PER_MONTH
    return months

def minToSecs(minutes):
    secs = minutes * SECONDS_PER_MINUTE
    return int(secs)

def hoursToSecs(hours):
    secs = hours * SECONDS_PER_HOUR
    return int(secs)

def hoursToDays(hours):
    days = hours / HOURS_PER_DAY
    return days

def daysToSecs(days):
    secs = days * SECONDS_PER_DAY
    return int(secs)

def monthsToSecs(months):
    secs = months * SECONDS_PER_MONTH
    return int(secs)

def daysToHours(days):
    hours = days * HOURS_PER_DAY
    return int(hours)

def bblToGal(bbl):
    gals = bbl * GALLONS_PER_BARREL
    return gals

def galToBbl(gal):
    bbl = gal / GALLONS_PER_BARREL
    return bbl

def bblPerDayToGalPerSec(bblPerDay):
    galsPerDay = bblPerDay * GALLONS_PER_BARREL
    galsPerSecond = galsPerDay / SECONDS_PER_DAY
    return galsPerSecond

def bblPerDayToBblPerMonth(bblPerDay):
    bblPerMonth = bblPerDay * DAYS_PER_MONTH
    return bblPerMonth

def bblPerDayToBblPerSec(bblPerDay):
    bblPerSecond = bblPerDay / SECONDS_PER_DAY
    return bblPerSecond

def scfPerDayToScfPerSec(scfPerDay):
    scfPerSecond = scfPerDay / SECONDS_PER_DAY
    return scfPerSecond

def scfPerHourToScfPerSec(scfPerHour):
    scfPerSecond = scfPerHour / SECONDS_PER_HOUR
    return scfPerSecond

def btuPerHpHrToEfficiency(btuPerHpHr):
    efficiency = safeDivide(1, (btuPerHpHr*HPHR_PER_BTU), 1)
    return efficiency

def efficiencyToBtuPerHpHr(efficiency):
    btuPerHpHr = 1/(efficiency*HPHR_PER_BTU)
    return btuPerHpHr

def hpToKW(hp):
    kW = hp*KW_PER_HP
    return kW

def kwToHp(kW):
    hp = kW/KW_PER_HP
    return hp

def lbPerMMBtuToGramsPerJoule(lbPerMMBtu):
    gramsPerJoule = lbPerMMBtu*GRAMS_PER_POUND*1e6/JOULES_PER_BTU
    return gramsPerJoule
	
def kgCH4ToScfCH4(kgCH4, p_psi=14.73, t_f=60):
    # scf = 14.73 psi, 60 F
    # https://www.eia.gov/tools/faqs/faq.php?id=45&t=8
    t_k = C_TO_K(F_TO_C(t_f))
    p_pa = PA_PER_PSI * p_psi
    v_m3 = (kgCH4 * 1000 / MWT_CH4_GMOL) * GAS_CONSTANT_JKMOL * t_k / p_pa
    v_scf = v_m3 / (M_PER_FEET ** 3)
    return v_scf

# Converts kg CH4 to SCF whole gas, given a mole fraction gas composition
def kgCH4ToScfWholeGas(kgCH4, gas_composition):
    ScfCH4 = kgCH4ToScfCH4(kgCH4)
    percentCH4 = gas_composition['METHANE']
    scf_whole_gas = ScfCH4 / (percentCH4)
    return scf_whole_gas

def scfToKg(scf, MWT, p_psi=14.73, t_f=60):
    t_k = C_TO_K(F_TO_C(t_f))
    p_pa = PA_PER_PSI * p_psi
    mol = (scf * (M_PER_FEET ** 3)) * p_pa / GAS_CONSTANT_JKMOL / t_k
    kg = mol * MWT / 1000
    return kg

def SCFStoSCFH(SCFS):
    SCFH = SCFS*SECONDS_PER_HOUR
    return SCFH

def SCFHtoSCFS(SCFH):
    SCFS = SCFH/SECONDS_PER_HOUR
    return SCFS

def SCFMtoSCFS(SCFM):
    SCFS = SCFM/SECONDS_PER_MINUTE
    return SCFS

def metricTonsTokg(metricTons):
    kg = metricTons*KG_PER_TON
    return kg

def metricTonsPerYearToSCFH(metricTonsPerYear, gas):
    if gas == 'Methane' or gas == 'CH4':
        density = scfToKg(1, MWT_CH4_GMOL)
    if gas == 'CO2':
        density = scfToKg(1, MWT_CO2_GMOL)
    SCFH = metricTonsPerYear*KG_PER_TON/(density*HOURS_PER_YEAR)
    return SCFH

def metricTonsPerYearTokgPerSec(metricTonsPerYear):
    kgPerSec = metricTonsPerYear*KG_PER_TON/SECONDS_PER_YEAR
    return kgPerSec

def safeDivide(x, y, default):
    z = default if y == 0 else x/y
    return z

def getSimDuration():
    simdm = sm.SimDataManager.getSimDataManager()
    return simdm.config['simDurationSeconds']

def getSimDateTime(t):
    """Return time t in a datetime format
    :param t (seconds) from the start of simulation"""
    return mg.GLOBAL_CONFIG['simulationStartDatetime'] + datetime.timedelta(seconds=t)

def isWeekDay(t):
    """Return TRUE if t is during a working day (Mon - Fri)
    :param t (seconds) from the start of simulation"""
    d = getSimDateTime(t)
    return d.isoweekday() in range(1, 6)  # M - Fri will return true

def isWorkingHours(t, minHour = 6, maxHour = 16):
    """Return TRUE if t is during working hours
    :param t (seconds) from the start of simulation
    :param minHour hour of day considered start of working day (default 6)
    :param maxHour is hour of day considered end of working day (default 16)"""
    d = getSimDateTime(t)
    return d.hour in range(minHour, maxHour)  # true if minHour <= hour < maxHour

def nextWorkingSec(t, minHour = 6, maxHour = 16):
    """"If time is not weekday & working hour, return the next time (in seconds from simulation start) that is
    weekday working hours
    :param t (seconds) from the start of simulation
    :param minHour hour of day considered start of working day (default 6)
    :param maxHour is hour of day considered end of working day (default 16)"""
    d = getSimDateTime(t)
    if isWeekDay(t) and isWorkingHours(t, minHour, maxHour):
        return t  # return t
    elif isWeekDay(t) and d.hour < minHour:  # if current day is week day but before working hours
        return t - hoursToSecs(d.hour) - minToSecs(d.minute) - d.second + hoursToSecs(minHour)  # return minHour of current day
    elif isWeekDay(t + SECONDS_PER_DAY):  # if next day is week day
        return t - hoursToSecs(d.hour) - minToSecs(d.minute) - d.second + daysToSecs(1) + hoursToSecs(minHour)  # return minHour of next day
    else:  # not working day
        wd = d.isoweekday()
        return t - hoursToSecs(d.hour) - minToSecs(d.minute) - d.second + daysToSecs(8 - wd) + hoursToSecs(minHour)  # return minHour the following monday)
