import unittest
from ConfigManager import ConfigManager as cm
import AppUtils as au
import json
from pathlib import Path

TEST_CONFIG = {
        "intakeSpreadsheetConfigParams": {
            "Simulation Start Date":      {"configVar": "simulationStartDate", "default": "2017-01-01"},
            "Simulation Duration [Days]": {"configVar": "testIntervalDays", "default": 60},
            "Number of Monte Carlo Iterations": {"configVar": "monteCarloIterations", "default": 3},
            "Output Directory Template": {"configVar": "outputDirTmpl",
                                          "default": "{outputRoot}/{studyName}/MC_{scenarioTimestamp}"},
            "Simulation Fixed Directory Name": {"configVar": "outputSymlink", "default": "output/MC_Last1"},
            "Activity / Emission Factor Name": {"configVar": "factorName",
                                                "default": "input/CuratedData/FactorsFileReference/Factors.csv"},
            "Graph Specification File": {"configVar": "graphSpec", "default": ""}
        },

        "phaseValues": {
            "defaultValues": {
                "inputRoot": "input",
                "outputRoot": "output",
                "scenarioTimestampFormat": "%Y%m%d_%H%M%S",
                "scenarioTimestampFormat": "20230101_000000",

                "disableSimulation": False,
                "disableGraph": False,
                "disableSummary": False
            },

            "arguments": {},

            "siteDefinitionParams": {},

            "start": {
                "studyFilename":     "{inputRoot}/Studies/{studyFilename}",
                "emitterProfileDir": "{inputRoot}/CuratedData"
            },

            "simulation": {
                "simulationRoot": "{outputRoot}/{studyName}/MC_{scenarioTimestamp}",
                "templateDir":    "{simulationRoot}/template",

                "metadataFile":   "{templateDir}/metadata.csv",
                "equipmentFile":  "{templateDir}/equipment.json",
                "mdIndexFile":    "{templateDir}/mdIndex.csv",
            },

            "MCIteration": {
                "resultsRoot":             "{simulationRoot}/{site}/{MCIteration}",

                "gcFilename":              "{resultsRoot}/gasCompositions.csv",
                "tsFilename":              "{resultsRoot}/emissionTimeseries.csv",
                "ffFilename":              "{resultsRoot}/fluidFlows.csv",
                "eventFilename":           "{resultsRoot}/instantaneousEvents.csv",
                "GasComposition":          "{resultsRoot}/gasCompositions.csv",
                "emissionDriverFilename":  "{resultsRoot}/emissionDrivers.json",
                "mdScenarioFilename":      "{resultsRoot}/metadata.csv",
                "equipmentFilename":       "{resultsRoot}/equipment.json",

                "stateSummaryFilename":    "{resultsRoot}/summaryState.csv",
                "ffSummaryFilename":       "{resultsRoot}/summaryFluidFlow.csv",
                "ffRollupFilename":        "{resultsRoot}/summaryFluidFlowRollup.csv",
                "emitterSummaryFilename":  "{resultsRoot}/summaryEmission.csv",
                "emissionSummaryFilename": "{resultsRoot}/summaryEmissionCategory.csv",

            }

        },

        "dynamicTemplates": {
            "modelTemplate": "{inputRoot}/ModelFormulation/{modelID}",
            "graphTemplate": "{resultsRoot}/graphs/{graphName}.html",
        }

    }

class BasicConfigManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cm._initializeSingleton(TEST_CONFIG)
        cm.expandPhase('defaultValues')
        cm.expandPhase('start', studyFilename="study1.xlsx", studyName="testStudy")
        cm.expandPhase('simulation')
        cm.expandPhase('MCIteration', site='home', MCIteration=123)

    def test_basics(self):
        v1 = cm.getConfigVar('inputRoot')
        self.assertEqual(v1, "input")
        v1a = cm.getConfigVar('outputRoot')
        self.assertEqual(v1a, "output")

        v2 = cm.getConfigVar('simulationRoot')
        self.assertEqual(v2, "output/testStudy/MC_20230101_000000")
        v3 = cm.getConfigVar('inputRoot')
        self.assertEqual(v3, "input")
        v4 = cm.getConfigVar('metadataFile')
        self.assertEqual(v4, "output/testStudy/MC_20230101_000000/template/metadata.csv")

        v5 = cm.getConfigVar('gcFilename')
        self.assertEqual(v5, 'output/testStudy/MC_20230101_000000/home/123/gasCompositions.csv')
        pass

    def test_expandDynamicTemplate(self):
        v1 = cm.expandDynamicTemplate("modelTemplate", modelID="model123")
        self.assertEqual(v1, 'input/ModelFormulation/model123')

        v2 = cm.expandDynamicTemplate("graphTemplate", graphName="graph1")
        self.assertEqual(v2, 'output/testStudy/MC_20230101_000000/home/123/graphs/graph1.html')

class CLITests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cm._initializeSingleton(TEST_CONFIG)
        cm.expandPhase('defaultValues')

        argDict = au.getArgs().__dict__
        filteredDict = dict(filter(lambda x: x[1], argDict.items()))
        cm.expandPhase("arguments", **{**filteredDict, 'outputRoot': 'testOutput'}) # override outputRoot

    def test_basicArguments(self):
        v1 = cm.getConfigVar("inputRoot")
        self.assertEqual(v1, "input")

        v2 = cm.getConfigVar("configFile")
        self.assertEqual(v2, "config/defaultConfig.json")

        # test that CLI arguments override defaults
        v3 = cm.getConfigVar("outputRoot")
        self.assertEqual(v3, "testOutput")

        v4 = cm.getConfigVar("NonexistentVar")  # I'd like to come up with something better than None for nonexistent values
        self.assertIsNone(v4)

    def test_override(self):

        # test the original config values
        v2 = cm.getConfigVar("configFile")
        self.assertEqual(v2, "config/defaultConfig.json")
        v3 = cm.getConfigVar("outputRoot")
        self.assertEqual(v3, "testOutput")

        cm.expandPhase("arguments", outputRoot="overrideOutput")

        # test that CLI arguments override defaults
        v2 = cm.getConfigVar("configFile")
        self.assertEqual(v2, "config/defaultConfig.json")
        v3 = cm.getConfigVar("outputRoot")
        self.assertEqual(v3, "overrideOutput")

class StartupTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        au.getConfig(commandArgs=['-c', 'testInput/testConfig.json', '-s', 'testSite.xlsx', '-i', 'testInput'])


    def test_startup(self):
        studyName = cm.getConfigVar("studyName")
        self.assertEqual(studyName, "testSite")  # derived from studyFilename

        stRoot = cm.getConfigVar("studyRoot")
        self.assertEqual(stRoot, "output/testSite")

        sr = cm.getConfigVar("simulationRoot")
        self.assertEqual(sr, "output/testSite/MC_20230101_000000")

    def test_mcIteration(self):
        cm.expandPhase("MCIteration", MCIteration=0)
        rr0 = cm.getConfigVar("resultsRoot")
        self.assertEqual(rr0, "output/testSite/MC_20230101_000000//0")

        cm.expandPhase("MCIteration", MCIteration=1)
        rr1 = cm.getConfigVar("resultsRoot")
        self.assertEqual(rr1, "output/testSite/MC_20230101_000000//1")

        cm.expandPhase("MCIteration", site="testSite2", MCIteration=2)
        rr2 = cm.getConfigVar("resultsRoot")
        self.assertEqual(rr2, "output/testSite/MC_20230101_000000/testSite2/2")

    def test_siteDefinitionVariableExpansion(self):
        ssd = cm.getConfigVar("simulationStartDate")
        self.assertEqual(ssd, "1/1/2021")

        tiDays = cm.getConfigVar("testIntervalDays")
        self.assertEqual(tiDays, 1072)

        mcIterations = cm.getConfigVar("monteCarloIterations")
        self.assertEqual(mcIterations, 2)

        factorName = cm.getConfigVar("factorName")
        self.assertEqual(factorName, "input/Studies/C3/Operators/Factors.csv")

        fixedDirName = cm.getConfigVar("outputSymLink")
        self.assertEqual(fixedDirName, "output/Test/MC_Last")

        gcFilename = cm.getConfigVar("gcFilename")
        self.assertEqual("./GCTool/Results/Production_Fac_Gas_Compositions/PS5.csv", gcFilename)

class StartupTestsNoVars(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        au.getConfig(commandArgs=['-c', 'testInput/testConfig.json', '-s', 'testSiteNoVars.xlsx', '-i', 'testInput'])

    def test_defaultStartup(self):

        ssd = cm.getConfigVar("simulationStartDate")
        self.assertEqual(ssd, "1/1/2021")

        tiDays = cm.getConfigVar("testIntervalDays")
        self.assertEqual(30, tiDays)

        mcIterations = cm.getConfigVar("monteCarloIterations")
        self.assertEqual(10, mcIterations)









