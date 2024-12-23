import pandas as pd

dumpsheet = pd.read_csv(r'C:\death\METEC_MEET\PtE\trunk\MEET2Models\output\TestSiteFF\MC_20211006_102631\0\FFDump50.csv')

wellsheet = pd.read_excel(r'C:\death\METEC_MEET\PtE\trunk\MEET2Models\input\Studies\OGCI\Test_Site_FFCheck'
                          r'\TestSiteFF.xlsx', sheet_name='Wells')

FFLinks = pd.read_excel(r'C:\death\METEC_MEET\PtE\trunk\MEET2Models\input\Studies\OGCI\Test_Site_FFCheck'
                          r'\TestSiteFF.xlsx', sheet_name='FFLinks')
FFLinks = FFLinks.dropna(how='all')

instEvents = pd.read_csv(r'C:\death\METEC_MEET\PtE\trunk\MEET2Models\output\TestSiteFF\MC_20211006_102631\0\InstantaneousEvents.csv')


def unitsSec2Hours(persec):  # input dataframe only
    perday = persec * 24 * 3600
    return perday


def unitsHours2Sec(perday):  # input dataframe only
    persec = perday / 24 / 3600
    return persec


wellsheet = wellsheet.loc[:, ['Unit ID', 'Gas Production [Mscf/d]', 'Water Production [bbl/d]', 'Oil Production [bbl/d]']]  # 3 df cols
wellsheet['Gas Production [Mscf/d]'] *= 1000                                            # Mscf to scf
dumpsheet['driverRate'] = unitsSec2Hours(dumpsheet['driverRate'])                             # units to match input

# check for wells
def checkFluidFlowWells(wells, dump):  # dataframes only
    dumpOilIndex = dump.loc[dump['gc'] == 'Well-Condensate'].index       # find index of productions in dump
    dumpWaterIndex = dump.loc[dump['gc'] == 'Well-Water'].index
    # dumpGasIndex = dump.loc[dump['gc'] == 'Well-Gas'].index

    for i in range(len(wells)):

        if len(dumpOilIndex) > 0:
            if abs(dump.loc[dumpOilIndex[i], 'driverRate'] - wells.loc[i, 'Oil Production [bbl/d]']) < 1:
                print('Oil Production matches Well-Condensate')
            else:
                print('Oil Production does not match Well-Condensate')
        else:
            print('Well-Condensate not OKAY')

        if len(dumpWaterIndex) > 0:
            if abs(dump.loc[dumpWaterIndex[i], 'driverRate'] - wells.loc[i, 'Water Production [bbl/d]']) < 1:
                print('Water Production matches Well-Water')
            else:
                print('Water Production does not match Well-Water')
        else:
            print('Well-Water not OKAY')
    return wells, dump


def checkFluidFlowCommonHeader(wells, dump):
    chCondensate = dump[dump['unitID'] == 'common_header_1']
    chWater = dump[dump['unitID'] == 'common_header_2']

    if abs(chCondensate['driverRate'].iloc[-1] - sum(wells['Oil Production [bbl/d]'])) < 1:
        print('Common Header Condensate outlet aggregate is OK')
    else:
        print('Common Header Condensate outlet aggregate is not OK')

    if abs(chWater['driverRate'].iloc[-1] - sum(wells['Water Production [bbl/d]'])) < 1:
        print('Common Header Water outlet aggregate is OK')
    else:
        print('Common Header Water outlet aggregate is not OK')


def InOutFFcheck(prev, present):
    # prev = previous FF in dump50 that is tested; df
    # present = present FF in dump50 to be tested; df
    if 'AggregatedFlow' in str(prev['type']):         # if not aggregated flow, add inlet/outlet flows
        prev = prev['driverRate'].to_list()[0]        # aggregated flow must have one value
    else:
        prev = sum(present[(present['dir']) == 'inletFluidFlows']['driverRate'].to_list())  # add any one of inlet or outlet bec it is already tested
        pass
    presentIn = sum(present[(present['dir']) == 'inletFluidFlows']['driverRate'].to_list())     # add inlet/outlet flows
    presentOut = sum(present[(present['dir']) == 'outletFluidFlows']['driverRate'].to_list())
    if abs(prev-presentIn) < 1:
        if abs(presentIn-presentOut) < 1:
            print(str(present['unitID'].iloc[-1])+' '+str(present['name'].iloc[-1])+' is OK')
    pass


def checkFluidFlowSeparator(dump):
    # water sep stage 1
    prevWater = dump[(dump['unitID'] == 'common_header_2') & (dump['type'] == 'AggregatedFlow')]
    sep1Water = dump[(dump['unitID'] == 'sep_stage1_1') & (dump['name'] == 'Water')]
    InOutFFcheck(prevWater, sep1Water)

    # condensate sep stage 1
    prevCondensate = dump[(dump['unitID'] == 'common_header_1') & (dump['type'] == 'AggregatedFlow')]
    sep1Condensate = dump[(dump['unitID'] == 'sep_stage1_1') & (dump['name'] == 'Condensate')]
    InOutFFcheck(prevCondensate, sep1Condensate)

    # water sep stage 2
    sep2Water = dump[(dump['unitID'] == 'sep_stage2_1') & (dump['name'] == 'Water')]
    InOutFFcheck(sep1Water, sep2Water)

    # condensate sep stage 2
    sep2Condensate = dump[(dump['unitID'] == 'sep_stage2_1') & (dump['name'] == 'Condensate')]
    InOutFFcheck(sep1Condensate, sep2Condensate)


if ('well' in str(dumpsheet['unitID'].unique())) & ('well' in str(FFLinks['Outlet UnitID'].unique())):
    checkFluidFlowWells(wellsheet, dumpsheet)
if ('common_header' in str(dumpsheet['unitID'].unique())) & ('common_header' in str(FFLinks['Inlet UnitID'].unique())):
    checkFluidFlowCommonHeader(wellsheet, dumpsheet)
if ('sep_stage' in str(dumpsheet['unitID'].unique())) & ('sep_stage' in str(FFLinks['Inlet UnitID'].unique())):
    checkFluidFlowSeparator(dumpsheet)


# check for states
state_transitions = instEvents[(instEvents['command'] == 'STATE_TRANSITION')]
start_events = instEvents[(instEvents['event'] == 'START')]

