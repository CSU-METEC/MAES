import unittest
from EquipmentTable import JsonEquipmentTable, Facility, MajorEquipment, Emitter, ActivityFactor
import numpy as np
import SimDataManager as sdm

class EmitterSubclass(Emitter):
    def __init__(self, val1=None, val2=None, **kwargs):
        super().__init__(**kwargs)
        self.val1 = val1
        self.val2 = val2


class NewEmitterTests(unittest.TestCase):

    def setUp(self) -> None:
        self.simdm = sdm.SimDataManager.initStubSimDataManager()
        return super().setUp()  

    def checkFields(self, equipment, instance):
        instanceDict = instance.__dict__
        for singleKey, singleVal in equipment.items():
            self.assertTrue(singleKey in instanceDict)
            self.assertEqual(singleVal, instanceDict[singleKey])

    def checkMaps(self, map1, map2):
        self.assertEqual(len(map1), len(map2))
        for key, val in map1.items():
            self.assertTrue(key in map2, f"key {key} in map2")
            val2 = map2[key]
            self.assertTrue(val == map2[key], f"key: {key}, val1 {val} equals val2 {val2}")
        return True

    def test_FacilityBasic(self):
        fid = 'fac1'
        f1 = Facility(facilityID=fid)

        # Make sure critical fields in instance are set correctly
        self.assertEqual(fid, f1.facilityID)
        self.assertIsNone(f1.unitID)
        self.assertIsNone(f1.emitterID)

        # Make sure we can find the instance in the metadata, and make sure there is only one of them
        self.assertEqual(self.simdm.getEquipmentTable().elementLookup(fid), f1)
        ea = self.simdm.getEquipmentTable().getMetadata()
        fidEquipment = ea[ea['facilityID'] == fid]
        self.assertEqual(len(fidEquipment), 1)

        # Make sure fields in instance match those in metadata
        thisEquipment = fidEquipment.iloc[0]
        self.checkFields(thisEquipment, f1)

        # Check that we get an error if we try to create a new facility with the same facilityID
        fac2 = None
        try:
            fid2 = Facility(fid)
            gotException = False
        except:
            gotException = True
        self.assertFalse(fac2)
        self.assertTrue(gotException)

    def test_EquipmentBasic(self):
        fid = 'fac1'
        f1 = Facility(facilityID=fid)

        eid1 = 'eid1'
        e1 = MajorEquipment(facilityID=f1.facilityID, unitID=eid1)

        # Make sure critical fields in instance are set correctly
        self.assertEqual(fid, e1.facilityID)
        self.assertEqual(eid1, e1.unitID)
        self.assertIsNone(e1.emitterID)

        # Make sure we can find the instance in the metadata, and make sure there is only one of them
        self.assertEqual(self.simdm.getEquipmentTable().elementLookup(e1.facilityID, e1.unitID), e1)
        ea = self.simdm.getEquipmentTable().getMetadata()
        eidEquipment = ea[np.logical_and(ea['facilityID'] == fid, ea['unitID'] == eid1)]
        self.assertEqual(len(eidEquipment), 1)

        # Make sure fields in instance match those in metadata
        thisEquipment = eidEquipment.iloc[0]
        self.checkFields(thisEquipment, e1)

        # check that we can have non-unique unitIDs
        fid2 = 'fac2'
        f2 = Facility(facilityID=fid2)
        e2 = MajorEquipment(facilityID=fid2, unitID=eid1)
        self.assertEqual(fid2, e2.facilityID)
        self.assertEqual(eid1, e2.unitID)
        self.assertIsNone(e2.emitterID)
        ea = self.simdm.getEquipmentTable().getMetadata()
        eidEquipment = ea[np.logical_and(ea['facilityID'] == fid2, ea['unitID'] == eid1)]
        self.assertEqual(len(eidEquipment), 1)
        eidEquipment = ea[ea['unitID'] == eid1]
        self.assertEqual(len(eidEquipment), 2)

    def test_EmitterBasic(self):
        fid = 'fac1'
        f1 = Facility(facilityID=fid)
        eid1 = 'eid1'
        e1 = MajorEquipment(facilityID=fid, unitID=eid1)
        emid1 = 'emid1'

        em1 = Emitter(facilityID=fid, unitID=eid1, emitterID=emid1)

        # Make sure critical fields in instance are set correctly
        self.assertEqual(fid, em1.facilityID)
        self.assertEqual(eid1, em1.unitID)
        self.assertEqual(emid1, em1.emitterID)

        # Make sure we can find the instance in the metadata, and make sure there is only one of them
        self.assertEqual(self.simdm.getEquipmentTable().elementLookup(facilityID=fid, unitID=eid1, emitterID=emid1), em1)
        ea = self.simdm.getEquipmentTable().getMetadata()
        eidEquipment = ea[ea['emitterID'] == emid1]
        self.assertEqual(len(eidEquipment), 1)

        # Make sure fields in instance match those in metadata
        thisEquipment = eidEquipment.iloc[0]
        self.checkFields(thisEquipment, em1)

    def test_MultiEquipment(self):
        fid = 'fac1'
        f1 = Facility(facilityID=fid)

        eid1 = 'eid1'
        e1 = MajorEquipment(facilityID=fid, unitID=eid1)
        eid2 = 'eid2'
        e2 = MajorEquipment(facilityID=fid, unitID=eid2)

        # Make sure critical fields in instance are set correctly
        self.assertEqual(eid1, e1.unitID)
        self.assertEqual(fid, e1.facilityID)

        self.assertEqual(eid2, e2.unitID)
        self.assertEqual(fid, e2.facilityID)

        # Make sure we can find the instance in the metadata, and make sure there is only one of them
        self.assertEqual(self.simdm.getEquipmentTable().elementLookup(facilityID=fid, unitID=eid1), e1)
        ea = self.simdm.getEquipmentTable().getMetadata()
        eidEquipment = ea[ea['facilityID'] == fid]
        self.assertEqual(len(eidEquipment), 3)

        # Make sure fields in instance match those in metadata
        for _, singleRow in eidEquipment.iterrows():
            equipmentInst = self.simdm.getEquipmentTable().elementLookup(facilityID=singleRow['facilityID'], unitID=singleRow['unitID'])
            self.checkFields(singleRow, equipmentInst)

    def test_DumpBasics(self):
        fid = 'fac1'
        f1 = Facility(facilityID=fid)

        eid1 = 'eid1'
        e1 = MajorEquipment(facilityID=fid, unitID=eid1)
        eid2 = 'eid2'
        e2 = MajorEquipment(facilityID=fid, unitID=eid2)

        em1 = Emitter(facilityID=fid, unitID=eid1)
        em2 = Emitter(facilityID=fid, unitID=eid1)
        em3 = Emitter(facilityID=fid, unitID=eid2)

        # these functions no longer exists
        # et.dumpMetadata('testOutput/mdOutput.csv')
        # et.dumpEquipment('testOutput/equipment.json')

    def test_DumpSubclass(self):
        fid = 'fac1'
        f1 = Facility(facilityID=fid)

        eid1 = 'eid1'
        e1 = MajorEquipment(facilityID=fid, unitID=eid1)
        eid2 = 'eid2'
        e2 = MajorEquipment(facilityID=fid, unitID=eid2)

        em1 = Emitter(facilityID=e1.facilityID, unitID=e1.unitID)
        em2 = EmitterSubclass(facilityID=fid, unitID=eid1, val1="val1", val2=2.0)
        em3 = Emitter(facilityID=e2.facilityID, unitID=e2.unitID)

        # et.dumpMetadata('testOutput/mdOutput2.csv')
        # et.dumpEquipment('testOutput/equipment2.json')

        # etMetadata = EquipmentTable.readMetadata('testOutput/mdOutput2.csv')
        # self.assertTrue(et.equipmentAttributes.equals(etMetadata), "Restored metadata equals metadata")
        #
        # newEt = EquipmentTable.restoreEquipment('testOutput/equipment2.json')
        #
        # self.assertIsNot(newEt, et)
        # self.assertTrue(newEt.equipmentAttributes.equals(et.equipmentAttributes), "Same equipment attributes")
        # self.checkMaps(newEt.equipmentMap, et.equipmentMap)

    def modelIDHelper(self, inst, modelID, modelCategory, et):
        self.assertEqual(inst.modelID, modelID)
        self.assertEqual(inst.modelCategory, modelCategory)
        finst1 = et.elementLookup(inst.facilityID, inst.unitID, inst.emitterID)
        self.assertEqual(finst1, inst)
        self.assertEqual(finst1.modelID, inst.modelID)
        self.assertEqual(finst1.modelCategory, inst.modelCategory)

    def test_ModelID(self):
        fid = 'fac1'
        fidModelID = 'fac1 ModelID'
        fidCategory = 'fac1 Category'
        f1 = Facility(facilityID=fid, modelID=fidModelID, modelCategory=fidCategory)

        etColumns = self.simdm.getEquipmentTable().getMetadata().columns
        self.assertIn('modelID', etColumns)
        self.assertIn('modelCategory', etColumns)

        self.modelIDHelper(f1, fidModelID, fidCategory, self.simdm.getEquipmentTable())

        eid1 = 'eid1'
        eidModelID = 'model_id_eid1'
        eidCategory = 'categorymodeleid1'
        e1 = MajorEquipment(facilityID=fid, unitID=eid1, modelID=eidModelID, modelCategory=eidCategory)
        self.modelIDHelper(e1, eidModelID, eidCategory, self.simdm.getEquipmentTable())

        em1ModelID = 'em1ModelID'
        em1Category = 'em1ModelID'
        em2ModelID = 'em2ModelID'
        em2Category = 'em2ModelID'

        em1 = Emitter(facilityID=fid, unitID=eid1, modelID=em1ModelID, modelCategory=em1Category)
        em2 = EmitterSubclass(facilityID=fid, unitID=eid1, val1="val1", val2=2.0, modelID=em2ModelID, modelCategory=em2Category)
        self.modelIDHelper(em1, em1ModelID, em1Category, self.simdm.getEquipmentTable())
        self.modelIDHelper(em2, em2ModelID, em2Category, self.simdm.getEquipmentTable())

    def test_LatLonInheritance(self):
        fid = 'fac1'
        fidLat = 1.5
        fidLon = 2.75
        f1 = Facility(facilityID=fid, latitude=fidLat, longitude=fidLon)
        self.assertEqual(f1.latitude, fidLat)
        self.assertEqual(f1.longitude, fidLon)
        finst1 = self.simdm.getEquipmentTable().elementLookup(fid)
        self.assertEqual(f1, finst1)

        eid1 = 'eid1'
        e1 = MajorEquipment(facilityID=fid, unitID=eid1)
        self.assertEqual(e1.latitude, fidLat)
        self.assertEqual(e1.longitude, fidLon)

        eid2 = 'eid2'
        e2lat = 21.5
        e2lon = 22.75
        e2 = MajorEquipment(facilityID=fid, unitID=eid2, latitude=e2lat, longitude=e2lon)
        self.assertEqual(e2.latitude, e2lat)
        self.assertEqual(e2.longitude, e2lon)

        m1 = Emitter(facilityID=fid, unitID=eid2)
        self.assertEqual(m1.latitude, e2lat)
        self.assertEqual(m1.longitude, e2lon)

        m2lat = 221.5
        m2lon = 222.75
        m2 = Emitter(facilityID=fid, unitID=eid2, latitude=m2lat, longitude=m2lon)
        self.assertEqual(m2.latitude, m2lat)
        self.assertEqual(m2.longitude, m2lon)


    def test_ActivityFactor(self):
        fid = 'fac1'
        fidLat = 1.5
        fidLon = 2.75
        f1 = Facility(facilityID=fid, latitude=fidLat, longitude=fidLon)
        self.assertEqual(f1.latitude, fidLat)
        self.assertEqual(f1.longitude, fidLon)
        finst1 = self.simdm.getEquipmentTable().elementLookup(facilityID=fid)
        self.assertEqual(f1, finst1)

        eid1 = 'eid1'
        e1 = MajorEquipment(facilityID=fid, unitID=eid1)
        self.assertEqual(e1.latitude, fidLat)
        self.assertEqual(e1.longitude, fidLon)

        eid2 = 'eid2'
        e2lat = 21.5
        e2lon = 22.75
        e2 = MajorEquipment(facilityID=fid, unitID=eid2, latitude=e2lat, longitude=e2lon)
        self.assertEqual(e2.latitude, e2lat)
        self.assertEqual(e2.longitude, e2lon)
        emID = 'emitterID'
        em1 = Emitter(facilityID=fid, unitID=eid2, emitterID=emID)

         # ActivityFactor class does nothing and dumpMetadata method does'nt exists
        # a1 = ActivityFactor(facilityID=fid, unitID=eid2, emitterID=emID, modelCategory='PRV', equipmentCount=100)
        # self.simdm.getEquipmentTable().dumpMetadata('testOutput/mdOutput3.csv')

