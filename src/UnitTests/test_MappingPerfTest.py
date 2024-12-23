import unittest
from Timer import Timer
import logging
from enum import IntEnum, auto

NUMITER = 100000

CATEGORY_ELTS = (
    'E001', 'E002', 'E003', 'E004', 'E005',
    'E006', 'E007', 'E008', 'E009', 'E010',
    'E011', 'E012', 'E013', 'E014', 'E015',
    'E016', 'E017', 'E018', 'E019', 'E020'
)

logging.basicConfig(level=logging.INFO)

class EltEnum(IntEnum):
    E001 = auto()
    E002 = auto()
    E003 = auto()
    E004 = auto()
    E005 = auto()
    E006 = auto()
    E007 = auto()
    E008 = auto()
    E009 = auto()
    E010 = auto()
    E011 = auto()
    E012 = auto()
    E013 = auto()
    E014 = auto()
    E015 = auto()
    E016 = auto()
    E017 = auto()
    E018 = auto()
    E019 = auto()
    E020 = auto()

class EltEnum2(IntEnum):
    E001 = 1
    E002 = 2
    E003 = 3
    E004 = 4
    E005 = 5
    E006 = 6
    E007 = 7
    E008 = 8
    E009 = 9
    E010 = 10
    E011 = 11
    E012 = 12
    E013 = 13
    E014 = 14
    E015 = 15
    E016 = 16
    E017 = 17
    E018 = 18
    E019 = 19
    E020 = 20

DICT = {
    'E001': 1,
    'E002': 2,
    'E003': 3,
    'E004': 4,
    'E005': 5,
    'E006': 6,
    'E007': 7,
    'E008': 8,
    'E009': 9,
    'E010': 10,
    'E011': 11,
    'E012': 12,
    'E013': 13,
    'E014': 14,
    'E016': 15,
    'E017': 17,
    'E018': 18,
    'E019': 19,
    'E020': 20
}

DICT_INV = dict(map(lambda x: (x[1], x[0]), DICT.items()))

class CategoryEnum():
    CATEGORY_ELTS = ('E001', 'E002', 'E003', 'E004', 'E005',
            'E006', 'E007', 'E008', 'E009', 'E010',
            'E011', 'E012', 'E013', 'E014', 'E015',
            'E016', 'E017', 'E018', 'E019', 'E020'
                     )

    def __init__(self):
        pass

    def marshal(self, eltName):
        return self.CATEGORY_ELTS.index(eltName)

    def unmarshal(self, eltValue):
        return self.CATEGORY_ELTS[eltValue]

    def eltList(self):
        return CATEGORY_ELTS

    def valList(self):
        return list(map(lambda x: self.CATEGORY_ELTS.index(x), CATEGORY_ELTS))

    def name(self):
        return "tuple class"


class CategoryDict():
    CATEGORY_DICT = {
        'E001': 1,
        'E002': 2,
        'E003': 3,
        'E004': 4,
        'E005': 5,
        'E006': 6,
        'E007': 7,
        'E008': 8,
        'E009': 9,
        'E010': 10,
        'E011': 11,
        'E012': 12,
        'E013': 13,
        'E014': 14,
        'E016': 15,
        'E017': 17,
        'E018': 18,
        'E019': 19,
        'E020': 20
    }

    INV_DICT = dict(map(lambda x: (x[1], x[0]), CATEGORY_DICT.items()))

    def __init__(self):
        pass

    def marshal(self, eltName):
        return self.CATEGORY_DICT[eltName]

    def unmarshal(self, eltValue):
        return self.INV_DICT[eltValue]

    def eltList(self):
        return self.CATEGORY_DICT.keys()

    def valList(self):
        return self.CATEGORY_DICT.values()

    def name(self):
        return "dict class"


def marshal_class(clasz):
    inst = clasz()
    with Timer(f"{inst.name()} marshal") as t1:
        for i in range(NUMITER):
            for singleElt in inst.eltList():
                idx = inst.marshal(singleElt)
        t1.setCount(NUMITER * len(CATEGORY_ELTS))

    return True

def unmarshal_class(clasz):
    inst = clasz()
    inverseMap = inst.valList()

    with Timer(f"{inst.name()} unmarshal") as t1:
        for i in range(NUMITER):
            for singleInverse in inverseMap:
                elt = inst.unmarshal(singleInverse)
        t1.setCount(NUMITER * len(CATEGORY_ELTS))

    return True

class MappingPerfTest(unittest.TestCase):

    def test_marshal_tuple(self):

        with Timer("Tuple marshal") as t1:
            for i in range(NUMITER):
                for singleElt in CATEGORY_ELTS:
                    idx = CATEGORY_ELTS.index(singleElt)
            t1.setCount(NUMITER * len(CATEGORY_ELTS))

        self.assertTrue(True)

    def test_unmarshal_tuple(self):
        inverseMap = list(map(lambda x: CATEGORY_ELTS.index(x), CATEGORY_ELTS))

        with Timer("Tuple unmarshal") as t1:
            for i in range(NUMITER):
                for singleInverse in inverseMap:
                    elt = CATEGORY_ELTS[singleInverse]
            t1.setCount(NUMITER * len(CATEGORY_ELTS))

        self.assertTrue(True)

    def test_marshal_enum(self):

        with Timer("Enum marshal") as t1:
            for i in range(NUMITER):
                for singleElt in EltEnum:
                    idx = singleElt.value
            t1.setCount(NUMITER * len(CATEGORY_ELTS))

        self.assertTrue(True)

    def test_marshal_enum_class(self):
        self.assertTrue(marshal_class(CategoryEnum))

    def test_marshal_dict_class(self):
        self.assertTrue(marshal_class(CategoryDict))

    def test_unmarshal_enum(self):
        inverseMap = list(map(lambda x: x.value, EltEnum))

        with Timer("Enum unmarshal") as t1:
            for i in range(NUMITER):
                for singleInverse in inverseMap:
                    elt = EltEnum(singleInverse)
            t1.setCount(NUMITER * len(CATEGORY_ELTS))

        self.assertTrue(True)

    def test_marshal_enum2(self):

        with Timer("Enum2 marshal") as t1:
            for i in range(NUMITER):
                for singleElt in EltEnum2:
                    idx = singleElt.value
            t1.setCount(NUMITER * len(CATEGORY_ELTS))

        self.assertTrue(True)

    def test_unmarshal_enum2(self):
        inverseMap = list(map(lambda x: x.value, EltEnum2))

        with Timer("Enum2 unmarshal") as t1:
            for i in range(NUMITER):
                for singleInverse in inverseMap:
                    elt = EltEnum(singleInverse)
            t1.setCount(NUMITER * len(CATEGORY_ELTS))

        self.assertTrue(True)


    def test_marshal_dict(self):

        with Timer("Dict marshal") as t1:
            for i in range(NUMITER):
                for singleElt in DICT.keys():
                    idx = DICT[singleElt]
            t1.setCount(NUMITER * len(CATEGORY_ELTS))

        self.assertTrue(True)

    def test_unmarshal_dict(self):
        inverseMap = list(DICT.values())

        with Timer("Dict unmarshal") as t1:
            for i in range(NUMITER):
                for singleInverse in inverseMap:
                    elt = DICT_INV[singleInverse]
            t1.setCount(NUMITER * len(CATEGORY_ELTS))

        self.assertTrue(True)

    def test_unmarshal_enum_class(self):
        self.assertTrue(unmarshal_class(CategoryEnum))

    def test_unmarshal_dict_class(self):
        self.assertTrue(unmarshal_class(CategoryDict))


