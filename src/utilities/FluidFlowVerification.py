import pandas as pd
import numpy as np
import AppUtils as au
import networkx as nx
import Units


def ffCheck(config):
    errorDumps = []
    errorDumps1 = []
    errorDumps2 = []
    dumpPath = f"{config['outputDir']}/{config['runNumber']}/FFDump50.csv"
    sheetPath = config['studyFullName']
    dumpsheet = pd.read_csv(dumpPath)
    wellsheet = pd.read_excel(sheetPath, sheet_name='Wells')
    productionData = wellsheet.loc[:, ['Facility ID', 'Unit ID', 'Gas Production [Mscf/d]', 'Water Production [bbl/d]',
                                       'Oil Production [bbl/d]']]  # 3 dataframe columns
    productionData['Gas Production [Mscf/d]'] *= 1000  # Mscf to scf
    ffLinkSheet = pd.read_excel(sheetPath, sheet_name='FFLinks')
    G = nx.MultiDiGraph()
    # map fluid flow links by facility, then wells
    for _, ffInst in ffLinkSheet.iterrows():
        if not G.has_node((ffInst["Outlet FacilityID"], ffInst["Outlet UnitID"])):
            G.add_node((ffInst["Outlet FacilityID"], ffInst["Outlet UnitID"]))
        a = G.nodes[(ffInst["Outlet FacilityID"], ffInst["Outlet UnitID"])][ffInst['Flow Name']] = 0
        if not G.has_node((ffInst["Inlet FacilityID"], ffInst["Inlet UnitID"])):
            G.add_node((ffInst["Inlet FacilityID"], ffInst["Inlet UnitID"]))
        G.add_edge((ffInst["Outlet FacilityID"], ffInst["Outlet UnitID"]),
                   (ffInst["Inlet FacilityID"], ffInst["Inlet UnitID"]),
                   attr={"FlowName": ffInst['Flow Name'], "FlowSecondaryID": ffInst['Flow SecondaryID']})
    for _, dumpInst in dumpsheet.iterrows():
        # if pd.isna(dumpInst['secondaryID']):
        if dumpInst['dir'] == 'outletFluidFlows':
            # check for inlet
            dumpInst1 = dumpsheet[dumpsheet.dir == 'inletFluidFlows']
            dumpInst1 = dumpInst1[dumpInst1.serialNumber == dumpInst['serialNumber']]
            if not dumpInst1.empty and len(dumpInst1) == 1:
                dumpInst1 = dumpInst1.iloc[0]
                if dumpInst.driverRate == dumpInst1.driverRate and dumpInst.gc == dumpInst1.gc and dumpInst.driverUnits == dumpInst1.driverUnits:
                    inNode = G.nodes[dumpInst['facilityID'], dumpInst['unitID']]
                    if dumpInst['name'] in inNode:
                        G.nodes[(dumpInst['facilityID'], dumpInst['unitID'])][dumpInst['name']] += dumpInst[
                            'driverRate']
                    else:
                        G.nodes[(dumpInst['facilityID'], dumpInst['unitID'])][dumpInst['name']] = dumpInst[
                            'driverRate']
                else:
                    errorDumps1.append(dumpInst['serialNumber'])
            else:
                errorDumps.append(dumpInst['serialNumber'])
    for _, wellInst in productionData.iterrows():
        wellNode = G.nodes[(wellInst['Facility ID'], wellInst['Unit ID'])]
        oilTotal = 0
        for value in wellNode:
            if value == 'Water':
                if wellNode[value] == Units.bblPerDayToBblPerSec(wellInst['Water Production [bbl/d]']):
                    pass
                else:
                    errorDumps2.append((wellInst['Facility ID'], wellInst['Unit ID'], 'Water', wellNode[value], Units.bblPerDayToBblPerSec(wellInst['Water Production [bbl/d]'])))
            elif value == 'Condensate':
                oilTotal += wellNode[value]
                # if wellNode[value] == wellInst['Oil Production [bbl/d]']:
                #     pass
                # else:
                #     errorDumps2.append((wellInst['Facility ID'], wellInst['Unit ID']))
            elif value == 'Flash':
                oilTotal += wellNode[value]
                # if wellNode[value] == wellInst['Gas Production [Mscf/d]']:
                #     pass
                # else:
                #     errorDumps2.append((wellInst['Facility ID'], wellInst['Unit ID']))
        if oilTotal == Units.bblPerDayToBblPerSec(wellInst['Gas Production [Mscf/d]']):
            pass
        else:
            errorDumps2.append((wellInst['Facility ID'], wellInst['Unit ID'], 'Condensate', oilTotal, Units.scfPerDayToScfPerSec(wellInst['Gas Production [Mscf/d]'])))
    return errorDumps, errorDumps1, errorDumps2


def main():
    config, _ = au.getConfig()
    a, b, c = ffCheck(config)
    if len(a) == 0 and len(b) == 0 and len(c) == 0:
        print("All Fluid Flows are validated")
    else:
        if len(a) > 0:
            print(f"Extra Fluid Flow entries - Serial Numbers:{a}")
        if len(b) > 0:
            print(f"Input and output values do not match - Serial Numbers:{b}")
        if len(c) > 0:
            print(f"Fluid flow values don't match well production values: Wells:{c}")


if __name__ == "__main__":
    main()
