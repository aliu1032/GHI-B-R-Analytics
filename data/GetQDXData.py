'''
Created on Sep 7, 2017
@author: aliu

- Sep 12 : adding OLI data prep steps; adding option to refresh dumped data and read from local
- Sep 15 : adding Appeal data prep steps;
- Sep 22 : split into GetQDXData and GetGHIData. Keep the QDX data in this module
- Sep 22 : adding the steps for preparing QDX claim and payment data
'''


import pyodbc
import pandas as pd
#import numpy as np
from datetime import datetime

import project_io_config as cfg

server = cfg.QDX_DB_Server
database = cfg.QDX_DB

#################################################
#   Appeal Case & Status                        #
#################################################
def appeal_case_status (folder, refresh = 1):
    
    print ("function : GetQDXData: appeal_case_status :: start :: ",datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('refresh = ', refresh, '\nfolder is ', folder, "\n\n")

    #database = 'Quadax'
    target = server + '_' + database + '_' + 'QDX_appeal_case_status.txt'

    f = open(cfg.sql_folder + 'QDX_appeal.sql')
    tsql = f.read()
    f.close()
   
    if refresh:
        cnxn = pyodbc.connect('Trusted_Connection=yes',DRIVER='{ODBC Driver 13 for SQL Server}',\
                              SERVER=server,DATABASE=database)
                
        output = pd.read_sql(tsql, cnxn)
        output.to_csv(folder + target, sep='|', index=False)
        cnxn.close()
    else:
        output = pd.read_csv(folder + target, sep="|", encoding="ISO-8859-1", error_bad_lines=False, dtype=object)

    #output.appealCaseNumber = output.appealCaseNumber.astype(str)
    output.appealEntryDt = pd.to_datetime(output.appealEntryDt, format = "%Y%m%d", errors='coerce')
    output.appealDenialLetterDt = pd.to_datetime(output.appealDenialLetterDt, format = "%Y%m%d", errors='coerce')
    output.appealLetDt = pd.to_datetime(output.appealLetDt, format = "%Y%m%d", errors='coerce')
    output.appealDenReason = output.appealDenReason.str.lstrip('0')

    return output

'''
 query to translate QDX code to GHI payor name
  select Tier1PayorID, Tier1PayorName, Tier2PayorID, Tier2PayorName, 
  Tier4PayorID, Tier4PayorName, Tier4PayorQuadaxInsuranceCode,
  Tier4PayorFinancialCategoryDescription
  from EDWDB.dbo.dimPayor
  where Tier4PayorQuadaxInsuranceCode = '8863'
  '''

#################################################
#   Completed appealed claims                   #
#################################################
def complete_appeal_case(folder, refresh=1):
    
    print ("function : GetQDXData: complete_appeal_case :: start :: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('refresh = ', refresh, '\nfolder is ', folder, "\n\n")

    #database = 'Quadax'
    target = server + '_' + database + '_' + 'QDX_complete_appeal_case.txt'
    
    ''' Read data '''    
    f = open(cfg.sql_folder + 'QDX_appealSuccess.sql')
    tsql = f.read()
    f.close()

    if refresh:
        cnxn = pyodbc.connect('Trusted_Connection=yes',DRIVER='{ODBC Driver 13 for SQL Server}',\
                              SERVER=server,DATABASE=database)
                        
        output = pd.read_sql(tsql, cnxn)
        output.to_csv(folder + target, sep='|', index=False)
        cnxn.close()
            
    else:
        output = pd.read_csv(folder + target, sep="|", encoding="ISO-8859-1", error_bad_lines=False)

    ##############################
    #  Prepare & transform data  #
    ##############################
    output.appealCaseNum = output.appealCaseNum.astype(str)
    output.appealTickNum = output.appealTickNum.astype(str)
    QDX_GHI_Test_Code = {'GL':'IBC','GLD':'DCIS','GLC':'Colon','MMR':'MMR','GLP':'Prostate','UNK':'Unknown','ARV7': 'Prostate-AR-V7'}
    
    for test in list(QDX_GHI_Test_Code.keys()):
        temp = output[(output['appealPH'] == test)].index
        #output.set_value(temp, 'appealPH', QDX_GHI_Test_Code[test])
        output.loc[temp,'appealPH'] = QDX_GHI_Test_Code[test]

    
    for a in ['appealAmtClmRec','appealAmtAplRec']:
        output[a] = pd.to_numeric(output[a])
    
    # 0.0*-1 = -0.0
    # Nov1: comment these 2 out
    #output.appealAmtClmRec = abs(output.appealAmtClmRec)
    #output.appealAmtAplRec = abs(output.appealAmtAplRec)
    
    output.appealSuccess = output.appealSuccess.astype('int').astype('str')
    output.appealDenReason = output.appealDenReason.astype('int').astype('str')
    
    output.appealRptDt = pd.to_datetime(output.appealRptDt, format = "%Y%m%d", errors='coerce')
    output.appealDOS = pd.to_datetime(output.appealDOS, format = "%Y%m%d", errors='coerce')
    
    output = output[['appealTickNum', 'appealCaseNum', 'appealReqNum',
       'appealAccession', 'appealDOS', 'appealInsCode', 'appealInsFC',
       'appealDenReason', 'appealDenReasonDesc', 'appealAmtChg', 'appealAmtChgExp', 'appealAmtAllow',
       'appealAmtClmRec', 'appealAmt', 'appealAmtAplRec', 'appealRptDt','appealSuccess','appealCurrency']]
    
    # not using the appealCnt from this file until QDX clarify the definition. The cnt is not matching the number of rows in appealcase.txt
    return output

#################################################
#   QDX Case.txt : Claim case status            #
#################################################
def claim_case_status(usage, folder, refresh=1):

    print ("function : GetQDXData: claim_case_status :: start :: ",datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('usage = ', usage, '\nrefresh = ', refresh, '\nfolder is ', folder, "\n\n")
    
    #database = 'Quadax'
    target = server + '_' + database + '_' + 'QDX_claim_case_status.txt'

    prep_file_name = "QDX_ClaimDataPrep.xlsx"
    
    prep_note = pd.read_excel(cfg.prep_file_path+prep_file_name, sheet_name = "QDXCases", skiprows=1, usecols = "B,F:I", encoding='utf-8-sig')
#    #rename_columns = dict(zip(prep_note.QDX_stdCaseFile, prep_note.Synonyms))
    data_type = dict(zip(prep_note.QDX_stdCaseFile, prep_note.Type))
    
    ''' Read data '''
    f = open(cfg.sql_folder + 'QDX_cases.sql')
    tsql = f.read()
    f.close()
 
    if refresh:    
        cnxn = pyodbc.connect('Trusted_Connection=yes',DRIVER='{ODBC Driver 13 for SQL Server}',\
                              SERVER=server,DATABASE=database)
        
        output = pd.read_sql(tsql, cnxn)
        output.to_csv(folder + target, sep='|', index=False)
        cnxn.close()
            
    else:
        output = pd.read_csv(folder + target, sep="|", encoding="ISO-8859-1", error_bad_lines=False, dtype=data_type)
     
   
    output.caseUpdateDate = pd.to_datetime(output.caseUpdateDate, format = "%Y%m%d", errors='coerce')
    output.caseEntryYrMth = pd.to_datetime(output.caseEntryYrMth.astype('int').astype('str'), format = "%Y%m", errors='coerce')
    output.caseCaseNum = output.caseCaseNum.astype('int').astype('str')
    output.caseTicketNum = output.caseTicketNum.astype('int').astype('str')
    
    output['caseAddedDateTime'] = pd.to_datetime(output.caseDateAdded + " " + output.caseTimeAdded, format='%Y%m%d %H:%M:%S',errors='coerce' )
    
    # For appeal process only need the Case#, Ticket# and OLI#
    # drop the case Entry month, update date, no case status. OLI has Billing Case Status
    if usage == 'case_reference':
        select_column = prep_note[(prep_note.case_reference == 1)]['Synonyms']
    elif usage == 'QDXClaim_CaseStatus':
        select_column = prep_note[(prep_note.QDXClaim_CaseStatus == 1)]['Synonyms']
    
    return output[select_column]

#################################################
#   QDX stdPayment.txt                          #
#################################################
def stdPayment(usage, folder, refresh=0):
    
    print ("function : GetQDXData: stdPayment :: start ::",datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('usage = ', usage,'\nrefresh = ', refresh, '\nfolder is ', folder, "\n\n")

    #database = 'Quadax'
    target = server + '_' + database + '_' + 'QDX_stdPayment.txt'
    
    prep_file_name = "QDX_ClaimDataPrep.xlsx"
    
    Adj_code = pd.read_excel(cfg.prep_file_path+prep_file_name, sheet_name = "AdjustmentCode", usecols="A,C:E,G", encoding='utf-8-sig')
    Adj_code.columns = [cell.strip() for cell in Adj_code.columns]
    
    pymnt_note = pd.read_excel(cfg.prep_file_path+prep_file_name, sheet_name = "QDXPayment", skiprows=1, usecols = "B:H", encoding='utf-8-sig')
    rename_columns = dict(zip(pymnt_note.QDX_stdPaymentFile, pymnt_note.Synonyms))
    data_type = dict(zip(pymnt_note.QDX_stdPaymentFile, pymnt_note.Type))

    ''' Read data '''
    
    f = open(cfg.sql_folder + 'QDX_stdPayment.sql')
    tsql = f.read()
    f.close()
    
    if refresh:
        cnxn = pyodbc.connect('Trusted_Connection=yes',DRIVER='{ODBC Driver 13 for SQL Server}',\
                              SERVER=server,DATABASE=database)
        
        output = pd.read_sql(tsql, cnxn)
        output.to_csv(folder + target, sep='|', index=False)
        cnxn.close()
    
    else:
        output = pd.read_table(folder+target, sep="|", dtype = data_type)
        
    output.rename(columns = rename_columns, inplace=True)

    # all records in stdPayment has QDXTicketNum
    # there are records have OLIID = null and OLIID = 'NONE'
    # rows with OLIID = 'NONE' has USD
    
    ############################
    #     Data Preparation     #
    ############################    
    
    
    # Translate into GHI Test Codes
    QDX_GHI_Test_Code = {'GL':'IBC','GLD':'DCIS','GLC':'Colon','MMR':'MMR','GLP':'Prostate','UNK':'Unknown','ARV7': 'Prostate-AR-V7'}
    for test in list(QDX_GHI_Test_Code.keys()):
        temp = output[(output['Test'] == test)].index
        #output.set_value(temp, 'Test', QDX_GHI_Test_Code[test])
        output.loc[temp,'Test'] = QDX_GHI_Test_Code[test]


    # Remove Allowable, Deductible and Coinsurance from Adjustment Lines
#    a = output.TXNType.isin(['AC','AD'])
#    output.loc[a,['stdPymntAllowedAmt','stdPymntDeductibleAmt','stdPymntCoinsAmt']] = np.nan
        
    # Get the PaymentInsPlan_code for RI and RP from the QDXAdjustmentCode field
    a = output.TXNType.isin(['RI','RP'])  #this return True and False
    output.loc[a,'PymntInsPlan_QDXCode'] = output.loc[a,'QDXAdjustmentCode']
    output.loc[a,['QDXAdjustmentCode','GHIAdjustmentCode']] = ''

    # if the ticket payor information is blank, fill the information with Primary Ins data
    ticket_payor_map = {'TicketInsComp_QDXCode' : 'PrimaryInsComp_QDXCode'
                         #, 'TicketInsPlan_QDXCode' : 'PrimaryInsPlan_QDXCode'
                         #, 'TicketInsCompName' : 'PrimaryInsCompName'
                         #, 'TicketInsPlanName' : 'PrimaryInsPlanName'
                         #, 'TicketInsFC' : 'PrimaryInsFC'
                         , 'TicketInsComp_GHICode' : 'PrimaryInsComp_GHICode' 
                         , 'TicketInsPlan_GHICode' : 'PrimaryInsPlan_GHICode'
                         }
        
    for a in list(ticket_payor_map.keys()):
        output.loc[output.TicketInsPlan_QDXCode.isnull(),a] = output.loc[output.TicketInsPlan_QDXCode.isnull(),ticket_payor_map.get(a)]

    a = output.TicketInsPlan_QDXCode == 'ROST'
    output.loc[a,'TicketInsPlan_QDXCode'] = output.loc[a,'TicketInsPlan_GHICode']

    # insert a flag to indicate if the RI payment from the TicketInsurance.
    # value = false : the payment could be from 2nd, 3rd, etc patient insurance
    output.loc[output.TXNType=='RI','IsPrimaryInsPymnt'] = int(1)
    output.loc[((output.TXNType=='RI') & 
                ~(output.TicketInsPlan_QDXCode.isnull()) & ~(output.PymntInsPlan_QDXCode.isnull()) &\
                (output.TicketInsPlan_QDXCode != output.PymntInsPlan_QDXCode))
               ,'IsPrimaryInsPymnt'] = int(0)
    
    # update old GHI adjustment code for PRAC to GH07, Financial Assistance Adj for Financial Assistance
    output.loc[output.QDXAdjustmentCode=='PRAC', 'GHIAdjustmentCode'] = 'GH07'
    output.loc[output.QDXAdjustmentCode=='PRA', 'GHIAdjustmentCode'] = 'GH07'
    
    output.loc[output.QDXAdjustmentCode== 'BAO', 'GHIAdjustmentCode'] = 'GH13'
    output.loc[output.QDXAdjustmentCode== 'CNCA', 'GHIAdjustmentCode'] = 'GH16'
        
    # resolve the adjustment code with description
    output = pd.merge(output, Adj_code, how='left', left_on='QDXAdjustmentCode', right_on='Code')
    output = output.drop(['Code','Category'],1)   
    
    ## data format change
#    for a in ['TXNAmount','stdPymntAllowedAmt', 'stdPymntDeductibleAmt', 'stdPymntCoinsAmt']:
    for a in ['TXNAmount']:
        output[a] = pd.to_numeric(output[a])
        
    output.OLIDOS = pd.to_datetime(output.OLIDOS, format = "%Y%m%d")
    output.TXNDate = pd.to_datetime(output.TXNDate, format = "%Y%m%d")
    output.TXNLineNumber = output.TXNLineNumber.astype('int')
   
    ######################
    #   Data Selection   #
    ######################
    
    if usage == 'Adjustment_analysis':
        select_columns = pymnt_note[(pymnt_note['Adjustment_analysis'] == 1)]['Synonyms']
        output_data = output[((output.PymntCategoryType == 'CA') & \
                                (output.OLIDOS >= "2016-01-01") & (output['OLIDOS'] < "2016-04-01"))]
        
    elif usage == 'UnitPrice_analysis':
        select_columns = pymnt_note[(pymnt_note['UnitPrice_analysis'] == 1)]['Synonyms']
        '''output = output[(output['PymntCategoryType'] == 'CR') & ~(output['OLIID'].isnull()) &\
                               (output['OLIDOS'] >= "2017-01-01") & (output['OLIDOS'] < "2017-04-01")]\
                               [select_columns].copy()
        '''                       
    elif usage == 'Claim2Rev':
        select_columns = pymnt_note[(pymnt_note['Claim2Rev']==1)]['Synonyms']
     
    elif usage == 'ClaimTicket':
        select_columns = pymnt_note[(pymnt_note['ClaimTicket']==1)]['Synonyms']
   
    return output[select_columns]

#################################################
#   Read QDX stdClaim.txt                       #
#################################################
def stdClaim(usage, folder, refresh=0):


# if data is refreshed and read from the database, the charges become object
# need to convert the amount columns into float before return if read from database

    print ("function : GetQDXData: stdClaim :: start :: ",datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('usage = ', usage,'\nrefresh = ', refresh, '\nfolder is ', folder, "\n\n")

    #database = 'Quadax'
    target = server + '_' + database + '_' + 'QDX_stdClaimFile.txt'
    
    prep_file_name = "QDX_ClaimDataPrep.xlsx"
    claim_note = pd.read_excel(cfg.prep_file_path+prep_file_name, sheet_name = "QDXClaim", skiprows=1, encoding='utf-8-sig')
    rename_columns = dict(zip(claim_note.QDX_stdClaimFile, claim_note.Synonyms))
    data_type = dict(zip(claim_note.QDX_stdClaimFile, claim_note.Type))
    
    f = open(cfg.sql_folder + 'QDX_stdClaim.sql')
    tsql = f.read()
    f.close()
    
    ''' Read data '''
    if refresh:        
        cnxn = pyodbc.connect('Trusted_Connection=yes', DRIVER='{ODBC Driver 13 for SQL Server}',\
                              SERVER=server,DATABASE=database)
                
        output = pd.read_sql(tsql, cnxn)
        output.to_csv(folder + target, sep='|', index=False)
        cnxn.close()
         
    else:
        output = pd.read_table(folder+target, sep="|" , dtype=data_type)
    
    output.rename(columns = rename_columns, inplace=True)
    
    ###########################################
    # Clean, fix, translate, interpolate data #
    ###########################################
    
    ## may need to convert the amount to float before returning when reading from database

    QDX_GHI_Test_Code = {'GL':'IBC','GLD':'DCIS','GLC':'Colon','MMR':'MMR','GLP':'Prostate','UNK':'Unknown','ARV7': 'Prostate-AR-V7'}
    for test in list(QDX_GHI_Test_Code.keys()):
        temp = output[(output['Test'] == test)].index
        #output.set_value(temp, 'Test', QDX_GHI_Test_Code[test])
        output.loc[temp,'Test'] = QDX_GHI_Test_Code[test]
        
    output.OLIDOS = pd.to_datetime(output.OLIDOS, format = "%Y%m%d")
    output.TXNDate = pd.to_datetime(output.TXNDate, format = "%Y%m%d")
    
    for a in ['stdClmInitBillDt','stdClmInitPymntDt','stdClmLastBillDt','stdClmLastPymntDt']:
        output[a] = pd.to_datetime(output[a], format = "%Y%m%d", errors='coerce')
    
    cond = (~output.stdClmInitBillDt.isnull()) & (~output.stdClmInitPymntDt.isnull())
    output['Days_toInitPymnt'] = (output.loc[cond,'stdClmInitPymntDt'] - output.loc[cond,'stdClmInitBillDt']).astype('timedelta64[D]')
    output[(output.Test=='Prostate') & (~output.Days_toInitPymnt.isnull())].Days_toInitPymnt.describe()

    cond = (~output.stdClmInitBillDt.isnull()) & (~output.stdClmLastPymntDt.isnull())    
    output['Days_toLastPymnt'] = (output.loc[cond,'stdClmLastPymntDt'] - output.loc[cond,'stdClmInitBillDt']).astype('timedelta64[D]')
    output[(output.Test=='Prostate') & (~output.Days_toLastPymnt.isnull())].Days_toLastPymnt.describe()
   
    for a in ['TXNAmount','ClmAmtAdj','ClmAmtRec','ClmTickBal']:
        output[a] = pd.to_numeric(output[a])

    output['TXNLineNumber'] = int(0)
    output['TXNType'] = 'CL'   
    
    output.loc[output.BillingCaseStatusSummary2 == 'Claim In Process', 'BillingCaseStatusSummary2'] = 'Claim in Process'

    # Find the Claim Ticket Payor from the ReRouted Insurance. 
    # if the ReRouted values are not available, read from the Primary Insurance
    
    ticket_payor_map1 = {'TicketInsComp_QDXCode' : 'ReRoutedPrimaryInsComp_QDXCode'
                        , 'TicketInsPlan_QDXCode' : 'ReRoutedPrimaryInsPlan_QDXCode'
                        , 'TicketInsCompName' : 'ReRoutedPrimaryInsCompName'
                        , 'TicketInsPlanName' : 'ReRoutedPrimaryInsPlanName'
                        , 'TicketInsFC' : 'ReRoutedPrimaryInsFC'
                        , 'TicketInsComp_GHICode' : 'ReRoutedPrimaryInsComp_GHICode'
                        , 'TicketInsPlan_GHICode' : 'ReRoutedPrimaryInsPlan_GHICode'
                        }
    
    ticket_payor_map2 = {'TicketInsComp_QDXCode' : 'PrimaryInsComp_QDXCode'
                         , 'TicketInsPlan_QDXCode' : 'PrimaryInsPlan_QDXCode'
                         , 'TicketInsCompName' : 'PrimaryInsCompName'
                         , 'TicketInsPlanName' : 'PrimaryInsPlanName'
                         , 'TicketInsFC' : 'PrimaryInsFC'
                         , 'TicketInsComp_GHICode' : 'PrimaryInsComp_GHICode' 
                         , 'TicketInsPlan_GHICode' : 'PrimaryInsPlan_GHICode'
                         }
        
    for a in ticket_payor_map1:
        output[a] = output[ticket_payor_map1.get(a)]
        output.loc[output.ReRoutedPrimaryInsPlan_QDXCode.isnull(),a] = output.loc[output.ReRoutedPrimaryInsPlan_QDXCode.isnull(),ticket_payor_map2.get(a)]
 
    # Quadax does not assign plan id to GHI Roster Account.
    a = output.TicketInsPlan_QDXCode == 'ROST'
    output.loc[a,'TicketInsPlan_QDXCode'] = output.loc[a,'TicketInsPlan_GHICode']

    ##########################################
    # Select and return data based on usage   #
    ###########################################
    
    if usage == 'Claim2Rev':
        select_columns = claim_note[(claim_note.Claim2Rev == 1)]['Synonyms']
    elif usage == 'ClaimTicket':    
        select_columns = claim_note[(claim_note.ClaimTicket == 1)]['Synonyms']

    return output[select_columns]

#################################################
#   Read QDX worklist.txt                       #
#################################################
def workListRecs(usage, folder, refresh=0):
    
    print ("function : GetQDXData: workListRecs :: start :: ",datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('usage = ', usage,'\nrefresh = ', refresh, '\nfolder is ', folder, "\n\n")

    prep_file_name = "QDX_ClaimDataPrep.xlsx"

    #workList_note = pd.read_excel(prep_file_path+prep_file_name, sheet_name = "QDXWorkList", skiprows=1, parse_cols="B:F", encoding='utf-8-sig')
    workList_note = pd.read_excel(cfg.prep_file_path+prep_file_name, sheet_name = "QDXWorkList", skiprows=1, usecols="B:F", encoding='utf-8-sig')

    rename_columns = dict(zip(workList_note.workListsRecs, workList_note.Synonyms))
    data_type = dict(zip(workList_note.workListsRecs, workList_note.Type))
    select_columns = workList_note[(workList_note.WO_Condition == 1)]['Synonyms']
    condition_code = pd.read_excel(cfg.prep_file_path+prep_file_name, sheet_name = "ConditionCode", usecols="A:C", encoding='utf-8-sig')
    
    file_name="workListRecs.txt"
    folder = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\QDX USD-Jul12\\"
    
    input_data = pd.read_table(folder+file_name, sep="|" , dtype=data_type)
    input_data.rename(columns = rename_columns, inplace=True)
    input_data.workListRecsCondition = input_data.workListRecsCondition.str.strip()
    
    #input_data[((input_data.workListTicketNum=='186796') & (input_data.workListCode == 'HS7X'))]
    
    if usage == 'wo_condition':
        wo_condition = input_data[(input_data.workListCode == 'HS7X') & (input_data.workListRecsCondition != '')][select_columns]
        wo_condition = pd.merge(wo_condition, condition_code, how='left', left_on=['workListCode','workListRecsCondition'],\
                                right_on=['conditionWorkList','conditionCode'])
        wo_condition.drop(['workListRecsCondition','conditionWorkList'],1, inplace=True)
        return wo_condition

    return input_data
    
#################################################
#   Read priorAuth.txt for pre claim status     #
#################################################
def priorAuth(usage, folder, refresh=0):
    
    print ("function : GetQDXData: priorAuth :: start :: ",datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('usage = ', usage,'\nrefresh = ', refresh, '\nfolder is ', folder, "\n\n")
    
    #database = 'Quadax'
    target = server + '_' + database + '_' + 'priorAuth.txt'
    
    f = open(cfg.sql_folder + 'QDX_priorAuth.sql')
    tsql = f.read()
    f.close()

    # Read data #
    if refresh:        
        cnxn = pyodbc.connect('Trusted_Connection=yes', DRIVER='{ODBC Driver 13 for SQL Server}',\
                              SERVER=server,DATABASE=database)
        output = pd.read_sql(tsql, cnxn)
        output.to_csv(folder + target, sep='|', index=False)
        cnxn.close()
         
    else:
        output = pd.read_table(folder+target, sep="|" )
    
    
    output.priorAuthCaseNum = output.priorAuthCaseNum.astype(str)
    return(output)

#################################################
#   Read inscodes.txt Quadax Ins info           #
#################################################
def insCodes(usage, folder, refresh=0):
    
    print ("function : GetQDXData: insCodes :: start :: ",datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('usage = ', usage,'\nrefresh = ', refresh, '\nfolder is ', folder, "\n\n")
    
    #database = 'Quadax'
    target = server + '_' + database + '_' + 'insCodes.txt'
    
    f = open(cfg.sql_folder + 'QDX_insCodes.sql')
    tsql = f.read()
    f.close()

    # Read data #
    if refresh:        
        cnxn = pyodbc.connect('Trusted_Connection=yes', DRIVER='{ODBC Driver 13 for SQL Server}',\
                              SERVER=server,DATABASE=database)
        output = pd.read_sql(tsql, cnxn)
        output.to_csv(folder + target, sep='|', index=False)
        cnxn.close()
         
    else:
        output = pd.read_table(folder+target, sep="|" )
    
    return(output)

