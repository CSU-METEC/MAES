import unittest
import MEETFluidFlow as ff
import GasComposition3 as gc
import io

class GCTests(unittest.TestCase):

    def test_BasicGC(self):
        ff1 = gc.FluidFlowGC('file1', flow='Flare', fluidFlowID='well1', gcUnits='bbl')
        ff2 = ff1.derive('sep1_1')
        ff3 = ff1.derive('sep1_1')
        pass

    def test_SerializeGCTable(self):
        gct1 = gc.GCTable()
        gc.GCTable.setGCTable(gct1)
        ff1 = gc.ManualGC(units='scf', fluidFlowID='well1', speciesDict={'METHANE': 1.0, 'BUTANE': 0.75})
        ff2 = ff1.derive('sep1_1')
        ff3 = ff2.derive('sep2_1')
        with io.StringIO() as oStream:
            gct1.serialize(oStream)
            str = oStream.getvalue()
        # gct2 = gc.GCTable()
        # with io.StringIO(str) as iStream:
        #     oldGCTable = gct2.deserialize(iStream)
        pass

class BasicFFTests(unittest.TestCase):

    def test_BasicFF(self):
        gc1 = gc.ManualGC({'METHANE': 1.0})
        ff1 = ff.FluidFlow('ff1', 2.0, 'scf', gc1)
        ff2 = ff.DependentFlow(ff1, rateTransform=lambda x: x*0.5, newName='ff2')

        self.assertEqual(2.0, ff1.driverRate)
        self.assertEqual(1.0, ff2.driverRate)

        ff1.driverRate = 4.0
        self.assertEqual(4.0, ff1.driverRate)
        self.assertEqual(2.0, ff2.driverRate)

        ff3 = ff.FluidFlow('ff3', 2.0, 'scf', gc1)
        aggFF = ff.AggregatedFlow('aff', gc1, newUnits='scf')
        aggFF.addFlow(ff1)
        aggFF.addFlow(ff3)
        self.assertEqual(6.0, aggFF.driverRate)

        ff3.driverRate = 4.0
        self.assertEqual(8.0, aggFF.driverRate)
        pass
