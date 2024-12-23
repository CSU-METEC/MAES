import argparse
import AppUtils as au
import EquipmentTable as et
import os
import ModelFormulation as mf
from datetime import datetime
import json
import logging
import MEETExceptions as me


def toJson(table, config):
    with open(
            config.outputDir + datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + config.rootClass + "_SubclassTree.json",
            'w') as fp:
        json.dump(table, fp)
    pass


def buildSubclassTable(rootClass):
    table = {}
    for subclass in rootClass.__subclasses__():
        table[subclass.__name__] = buildSubclassTable(subclass)
    return table


def main(args):
    ## Make sure the program is running with DES1 as the working directory
    # config, _ = au.getConfig()
    classMap = et.getAllSubclasses(et.EquipmentTableEntry)
    rootClass = None
    for aClass in classMap:
        if args.rootClass == aClass.__name__:
            rootClass = aClass
            break
    try:
        table = {rootClass.__name__: buildSubclassTable(rootClass)}
        toJson(table, args)
    except TypeError:
        msg = f"{rootClass} was not found as a valid EquipmentTable Class"
        raise TypeError(msg)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-c", "--rootClass", default="EquipmentTableEntry",
                        help="Valid Class in DES1, will default to EquipmentTableEntry")
    parser.add_argument("-o", "--outputDir", default="output/", help="Output directory")

    args = parser.parse_args()
    main(args)
