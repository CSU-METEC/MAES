import unittest

def expandFilename(tmpl, config):
    expandedFilename = tmpl.format(**config)
    #
    # can do other stuff here -- create directory hierarchies, etc.
    #
    return expandedFilename


class FilenameExpansionTests(unittest.TestCase):

    def test_basicExpansion(self):
        config = {
            "outputDir": "output",

            "desOutputFile": "{outputDir}/desOut.log"
        }

        eFilename = expandFilename(config['desOutputFile'], config)
        self.assertEqual(eFilename, "output/desOut.log")

    def test_addtlExpansion(self):
        config = {
            "outputDir": "output",

            "scenarioOutputFile": "{outputDir}/scenarioOut_{scenarioName}.log",
            "scenarioOutputFile2": "{outputDir}/{scenarioName}/scenarioOut.log"
        }

        eFilename = expandFilename(config['scenarioOutputFile'], {**config, "scenarioName": "0001"})
        self.assertEqual(eFilename, "output/scenarioOut_0001.log")
        eFilename = expandFilename(config['scenarioOutputFile2'], {**config, "scenarioName": "0001"})
        self.assertEqual(eFilename, "output/0001/scenarioOut.log")





