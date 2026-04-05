import re

# Matches a parameter name with an optional bracketed units suffix.
# Groups: 'val' — the parameter name without units; 'units' — the units string without brackets.
UNITS_RE = '(?:(?P<val>[^\[\]]+(?![^\[]*\]))(?:.*\[(?P<units>[^\[\]]*)\].*)?)$'


def toParamKey(param: str) -> dict:
    """Normalize a parameter or column name into a lookup key dict.

    Strips bracketed units, lowercases, and joins whitespace-separated tokens
    with underscores to produce a valKey suitable for fuzzy column matching.
    Returns a dict with keys: valKey (normalized key), val (name without units,
    stripped), units (units string without brackets, or None).
    """
    try:
        m = re.match(UNITS_RE, param)
        val = m.group('val')
        valLower = val.lower()
        valElts = valLower.split(' ')
        valKey = '_'.join(filter(lambda x: x != '', valElts))
        units = m.group('units')
        if units:
            units = units.strip('[]')
        val = val.strip()
        ret = {'valKey': valKey, 'val': val, 'units': units}
        return ret
    except AttributeError:
        raise Exception("Unable to parameter key {param}; check syntax")


def colNameToPythonVar(colName: str) -> str:
    """Convert a space-separated column name to a camelCase Python variable name.

    Example: 'Compressor Type' -> 'compressorType'
    """
    fields = colName.split(' ')
    ucFields = list(map(lambda x: x.capitalize(), fields))
    ucFields[0] = ucFields[0].lower()
    ret = ''.join(ucFields)
    return ret
