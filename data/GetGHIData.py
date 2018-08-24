'''
Created on Sep 7, 2017
@author: aliu

- Sep 12 : adding OLI data prep steps; adding option to refresh dumped data and read from local
- Sep 15 : adding Appeal data prep steps;
'''

import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime
from openpyxl.styles.builtins import output

import project_io_config as cfg
server = cfg.GHI_DB_Server

#prep_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\workspace\\Supplement\\"
#sql_folder = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\workspace\\SQL\\"
 
#folder="C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\May022018\\"
#refresh = 0

#################################################
#   Order Line Detail for appeal analysis       #
#################################################
def OLI_detail(usage, folder, refresh=1, PayorHierarchy = 'At_OrderCapture'):
    # usage = ['appeal', 'revenue_receipt', 'utilization']
    # refresh = [1: refresh data from database and save to the given folder,
    #            0: get the data from the given folder
    # folder - if not blank: location to save the output & folder to get the local data
    
    print ("function : GetGHIData: OLI_detail :: start :: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('usage = ', usage,'\nrefresh = ', refresh, '\nfolder is ', folder, "\n\n")
    
    #server = 'EDWStage'
    #database = 'StagingDB'
    database = 'EDWDB'
    
    #if PayorHierarchy == 'At_OrderCapture':
    target = server + '_' + database + '_' + 'OrderLineDetail.txt'
    #else:
    #    target = server + '_' + database + '_' + 'OrderLineDetail_CurrentPayorHierarchy.txt'
        
    if refresh:    
        cnxn = pyodbc.connect('Trusted_Connection=yes',DRIVER='{ODBC Driver 13 for SQL Server}', SERVER=server, DATABASE=database)
       
        #if PayorHierarchy == 'At_OrderCapture':
        f = open(cfg.sql_folder + 'EDWDB_fctOrderLineItem.sql')
        tsql = f.read()
        f.close()
        #else:
        #    f = open(cfg.sql_folder + 'EDWDB_fctOrderLineItem_CurrentPayorHierarchy.sql')
        #    tsql = f.read()
        #    f.close()
            
        output = pd.read_sql(tsql, cnxn)
        output.to_csv(folder + target, sep='|', index=False)

    
    #prep_file_name = "GHI_OLI_Claim_Data_Prep.xlsx"
    #prep_note = pd.read_excel(prep_file_path+prep_file_name, sheet_name = "OrderLineDetail", skiprows=1, encoding='utf-8-sig')
    #########
    prep_file_name = "GHI_vwFctOrderLineItem.xlsx"
    prep_note = pd.read_excel(cfg.prep_file_path+prep_file_name, sheet_name = "OrderLineItem",\
                              skiprows=1,  usecols="I:L", encoding='utf-8-sig')
    prep_note = prep_note[~(prep_note.Synonyms.isnull())]
    #rename_columns = dict(zip(prep_note.StageDB_OrderDetail, prep_note.Synonyms))
    data_type = dict(zip(prep_note.Synonyms, prep_note.Type))
    
    if ~refresh:
        #read the data from local
        #use dtype=object until know where is the type error
        output = pd.read_csv(folder+target, sep="|", encoding="ISO-8859-1",  dtype=data_type)

    #utput.rename(columns = rename_columns, inplace=True)

    #################################################
    #  Prepare Order Line Item Data                 #
    #################################################

    temp = ['BilledAmount','ListPrice','TotalPayment','PayorPaid','PatientPaid','TotalAdjustment','CurrentOutstanding','ContractedPrice']
    for a in temp:
        output.loc[(output[a] == 0.0), a] = np.NaN
        
    temp = ['TestDeliveredDate','DateOfService']
    for a in temp:
        output[a] = pd.to_datetime(output[a].astype(str), format = "%Y%m%d.0", errors='coerce')

    temp = ['OrderStartDate','OLIStartDate']
    for a in temp:
        output[a] = pd.to_datetime(output[a].astype(str), format = "%Y%m%d", errors='coerce')
        
    '''
    ###df.loc
    ### standardize zero contract price to null, to mean there is no contract price for the OLI since we do not have an agreement with the payer & insurance plan
    output.loc[(output.ContractedPrice == 0.0), 'ContractedPrice'] = np.NaN

    temp = ['BilledAmount','ListPrice','TotalPayment','PayorPaid','PatientPaid','TotalAdjustment','CurrentOutstanding']
    output[temp] = output[temp].fillna(0.0)

    for a in temp:
        output[a] = pd.to_numeric(output[a])
    '''
        
    # Construct Tier1Payor, Tier2Payor and Tier4Payor that is not available from StagingDB.Analytics.stgOrderDetail
    output['Tier1Payor'] = output.Tier1PayorName + " (" + output.Tier1PayorID + ")"
    output['Tier2Payor'] = output.Tier2PayorName + " (" + output.Tier2PayorID + ")"
    output['Tier4Payor'] = output.Tier4PayorName + " (" + output.Tier4PayorID + ")"
  
    #convert QDXTicketNumber to string
    #QDXTicketNumber has NaN, thus it will always be a float. 
    #first convert the NaN into 0, convert to int, convert to str, and then convert the 0 into np.NaN
    # e.g. Test not delivered and no claim would not have Ticket Number
    output.CurrentTicketNumber = output.CurrentTicketNumber.fillna(0.0)
    output.CurrentTicketNumber = output.CurrentTicketNumber.astype(float).astype(int).astype('str')
    output.CurrentTicketNumber = output.CurrentTicketNumber.replace('0',np.nan)  
    
    output.loc[output.RiskGroup.isnull(),'RiskGroup'] = 'Unknown'
    
    output['HCPProvidedGleasonScore'].fillna('', inplace = True)
    output['HCPProvidedGleasonScore'] = output['HCPProvidedGleasonScore'].astype(str)
    output['HCPProvidedClinicalStage'].fillna('', inplace = True)
    output['HCPProvidedClinicalStage'] = output['HCPProvidedClinicalStage'].astype(str)

    #output['HCPProvidedPSA'].fillna('', inplace = True)
    
    ################################################################################
    # Reporting Group is stamped by SFDC: lookup the formula for ReportingGroup(D) #
    # Per Cris M, Micromet is not Node Positive                                    #
    # Update the label using the NodalStatus value                                 #
    ################################################################################
    a = output.ReportingGroup == 'Node Positive (Micromet)'
    output.loc[a,'ReportingGroup'] = output.loc[a,'NodalStatus'] 
    
    #########################################################################################
    #  Enrich Data                                                                          #
    #  - Reading the Financial Category code from GHI. GHI import and track the FC for      #
    #    T4 payor and not for self pay, roster,etc                                          #
    #  - Add on QDX FC to match QDX ASR; use the QDX FC to fill in missing FC in OLI file   #
    #########################################################################################
    ## Read QDX Ins Plan Txt for Financial Category where GHI does not import the FC for the payor
    inscode = pd.read_csv(cfg.prep_file_path+'insCodes.txt', sep="|", quoting=3, encoding='utf-8-sig', error_bad_lines=False) 
    inscode = inscode[~(inscode['insAltId'].isnull())][['insCode','insFC','insAltId']]
    inscode.rename(columns = {'insCode' : 'QDXInsCode', 'insFC':'QDXInsFC'}, inplace=True)
    ## need to check if the QDXinsPlanCode is matching the current Tier4Payor or the appealInsCode
    
    ## add the QDX insFC to order_appeal_history
    output = pd.merge(output, inscode, how='left',\
                                    left_on=['Tier4PayorID'], right_on=['insAltId'])
    output = output.drop(['QDXInsCode','insAltId'],1)
       
    output['SFDCSubmittedNCCNRisk'] = output['SubmittedNCCNRisk']
    '''
    Calculate the NCCN refinedS Intermediate Favorable and UnFavorable Risk
    '''
    def NCCN_Update(Gleason, PSA, Stage, OrgNCCN):
        if ((Gleason=='') and (Stage=='')):
            return OrgNCCN # 'Intermediate favorability indeterminate'
        if (Gleason==''):
            #or (Stage=='') or (PSA==0.0)):
            return OrgNCCN # 'Intermediate favorability indeterminate'
        elif (Gleason == '4+3'):
            return 'Intermediate Unfavorable'
        elif Gleason == '3+3':
            if PSA <10:
                return 'Intermediate Favorable'
            elif PSA < 20:  ## PSA 10 to 19
                if (Stage == ''):
                    return OrgNCCN # 'Intermediate favorability indeterminate'
                elif Stage in ['T1c', 'T2a', 'T1C', 'T2A']:
                    return 'Intermediate Favorable'
                else:
                    return 'Intermediate Unfavorable'
            else:
                return 'Intermediate Unfavorable'
        elif Gleason == '3+4':
            if PSA < 10:
                if (Stage == ''):
                    return OrgNCCN # 'Intermediate favorability indeterminate'
                elif Stage in ['T1c','T2a','T1C','T2A']:
                    return 'Intermediate Favorable'
                else:
                    return 'Intermediate Unfavorable'
            else:
                return 'Intermediate Unfavorable'
    '''
    temp = output[(output.OrderStartDate>='2017-05-01') & (output.Test=='Prostate')]
    temp['RevisedNCCN'] = temp.apply(lambda x : NCCN_Update(x['HCPProvidedGleasonScore'], x['HCPProvidedPSA'], x['HCPProvidedClinicalStage'], x['SFDCSubmittedNCCNRisk']), axis = 1)
    output_file = 'Check_NCCN_Calculation.txt'
    output_file_path = folder
    temp.to_csv(output_file_path+output_file, sep='|',index=False)
    '''
    ###
    cond = (output.OrderStartDate>='2017-01-01') & (output.Test=='Prostate') & (output.SFDCSubmittedNCCNRisk == 'Intermediate Risk')
    output.loc[cond ,'SubmittedNCCNRisk'] = output.loc[cond].apply(lambda x : NCCN_Update(x['HCPProvidedGleasonScore'], x['HCPProvidedPSA'], x['HCPProvidedClinicalStage'], x['SFDCSubmittedNCCNRisk']), axis = 1)
    
    #################################################
    #  Output OLI detail for the given usage        #
    #################################################

    if usage == 'Claim2Rev':
        select_column = prep_note[(prep_note['Claim2Rev'] == 1)]['Synonyms']
    elif usage == 'ClaimTicket':
        select_column = prep_note[(prep_note['ClaimTicket'] == 1)]['Synonyms']
    '''   
    elif usage == 'appeal':
        select_column = prep_note[(prep_note.Claim_Appeal==1)]['Synonyms']
    elif usage == 'Utilization':
        select_column = prep_note[(prep_note['Utilization'] == 1)]['Synonyms']
    elif usage == 'ClaimTicket_wCriteria':
        select_column = prep_note[(prep_note['ClaimTicket_wCriteria'] == 1)]['Synonyms']
    elif usage == 'Claim_Cycle':
        select_column = prep_note[(prep_note['Claim_Cycle'] == 1)]['Synonyms']
        return output[~(output.TestDeliveredDate.isnull())][select_column]
    elif usage == 'OLI_Payment_Revenue':
        select_column = prep_note[(prep_note['OLI_Payment_Revenue'] == 1)]['Synonyms']
    '''
    return output[select_column]

#################################################
#   Order Line Detail for appeal analysis       #
#################################################
def revenue_data(usage, folder, refresh=1):
    '''
    Get GHI Revenue data from StagingDB.Analytics.mvwRevenue
    Remove all no revenue impact rows, i.e. TotalRevenue & TotalAccural & TotalCash & TotalUSDRevenue & TotalUSCAccural & TotalUSDCash are 0.00 or null
    when call a refresh from the database, I will save a copy into the local for local use until the next refresh
    '''
    print ("function : GetGHIData: revenue data :: start :: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('usage = ', usage,'\nrefresh = ', refresh, '\nfolder is ', folder, "\n\n")
    
    #server = 'EDWStage'    
    database = 'EDWDB'
    target = server + '_' + database + '_' + 'RevenueDetail.txt'
    
    if refresh:
        cnxn = pyodbc.connect('Trusted_Connection=yes',DRIVER='{ODBC Driver 13 for SQL Server}',SERVER=server)

        f = open(cfg.sql_folder + 'EDWDB_fctRevenue.sql')
        tsql = f.read()
        f.close()

        output = pd.read_sql(tsql, cnxn)
        
        #Remove the rows that have none of the Revenue numbers        
        Rev_Noimpact = output[(  (output.TotalRevenue.isnull() | output.TotalRevenue == 0.0)
                                       & (output.TotalAccrualRevenue.isnull() | output.TotalAccrualRevenue == 0.0)
                                       & (output.TotalCashRevenue.isnull() | output.TotalCashRevenue == 0.0)
                                       & (output.TotalUSDRevenue.isnull() | output.TotalUSDRevenue == 0.0)
                                       & (output.TotalUSDAccrualRevenue.isnull() | output.TotalUSDAccrualRevenue == 0.0)
                                       & (output.TotalUSDCashRevenue.isnull() | output.TotalUSDCashRevenue == 0.0)
                                      ) | 
                                      (output.TicketNumber.isnull())
                                     ]
        
        output = output[~output.index.isin(Rev_Noimpact.index)]
        #Rev_Noimpact.to_csv(folder + 'Rev_NoImpact.csv', sep = '|', index = False)

        output.to_csv(folder + target, sep='|', index=False)

        
    prep_file_name = 'GHI_fctRevenue.xlsx'
    prep_note = pd.read_excel(cfg.prep_file_path+prep_file_name, sheet_name = "Revenue_data", skiprows=1, usecols = 'L:Q', encoding='utf-8-sig')
    prep_note = prep_note[~(prep_note.TargetColumn.isnull())].reset_index(drop=True)
    
    rename_columns = dict(zip(prep_note.TargetColumn, prep_note.Synonyms))
    data_type = dict(zip(prep_note.TargetColumn, prep_note.Type))

    if ~refresh:
        output = pd.read_csv(folder + target, sep="|", encoding="ISO-8859-1", error_bad_lines=False, dtype=data_type)
          
    output.rename(columns=rename_columns, inplace=True)
    
    #########################################
    #  Prepare Revenue Data                 #
    #########################################
    ''' need when extract from fctRevenue '''
    temp = ['AccountingPeriodDate','ClaimPeriodDate','TestDeliveredDate']
    for a in temp:
        output[a] = pd.to_datetime(output[a].astype(str), format = "%Y-%m-%d", errors = 'ignore')

    if output['TestDeliveredDate'].dtypes == 'O' :
        output['TestDeliveredDate'] = pd.to_datetime(output.TestDeliveredDate.str[:8],format = "%Y-%m-%d", errors = 'ignore')
    output['ClaimPeriodDate'] = pd.to_datetime(output.ClaimPeriodDate.str[:8],format = "%Y-%m-%d", errors = 'ignore')
       
    output['OLIID'].fillna('Unknown()', inplace=True)
    
    output['Tier1Payor'] = output.Tier1PayorName + " (" + output.Tier1PayorID + ")"
    output['Tier2Payor'] = output.Tier2PayorName + " (" + output.Tier2PayorID + ")"
    output['Tier4Payor'] = output.Tier4PayorName + " (" + output.Tier4PayorID + ")"

    ## pd.nan == np.nan is false
    ## np.nan == np.NaN is false
    ## Null TicketNumber is read as 'NaN' is equals isnull. Thus fillna will replace the value
    ## if converted to str, null TicketNumber become 'nan' and is not equals isnull(). Thus fillna does not replace the value
    
    #convert QDXTicketNumber to string
    #QDXTicketNumber has NaN, thus it will always be a float. 
    #first convert the NaN into 0, convert to int, convert to str, and then convert the 0 into np.NaN
    output.TicketNumber = output.TicketNumber.fillna(0.0)
    output.TicketNumber = output.TicketNumber.astype(float).astype(int).astype('str')
    output.TicketNumber = output.TicketNumber.replace('0',np.nan)  

    ## Read QDX Ins Plan Txt for Financial Category where GHI does not import the FC for the payor
    inscode = pd.read_csv(cfg.prep_file_path+'insCodes.txt', sep="|", quoting=3, encoding='utf-8-sig', error_bad_lines=False) 
    inscode = inscode[~(inscode['insAltId'].isnull())][['insCode','insFC','insAltId']]
    inscode.rename(columns = {'insCode' : 'QDXInsCode', 'insFC':'QDXInsFC'}, inplace=True)
    ## need to check if the QDXinsPlanCode is matching the current Tier4Payor or the appealInsCode

    ## add the QDX insFC to order_appeal_history
    output = pd.merge(output, inscode, how='left',\
                                    left_on=['Tier4PayorID'], right_on=['insAltId'])
    output = output.drop(['QDXInsCode','insAltId'],1)

    #################################################
    #  Output Revenue detail for the given usage    #
    #################################################

    if usage == 'Claim2Rev':
        select_column = prep_note[(prep_note['Claim2Rev'] == 1)]['Synonyms']
    elif usage == 'ClaimTicket':
        select_column = prep_note[(prep_note['ClaimTicket'] == 1)]['Synonyms']
    elif usage == 'OLI_Payment_Revenue':
        select_column = prep_note[(prep_note['OLI_Payment_Revenue'] == 1)]['Synonyms']

    return output[select_column]


#################################################
#   Payor Test Criteria                         #
#################################################
def getPTC (usage, folder, refresh = 1):
    
    print ("function : GetGHIData: get PTC :: start :: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('usage = ', usage,'\nrefresh = ', refresh, '\nfolder is ', folder, "\n\n")
    
    #server = 'EDWStage'    
    database = 'StagingDB'
    target = server + '_' + database + '_' + 'SFDC_PTC.txt'
    
    if refresh:
        cnxn = pyodbc.connect('Trusted_Connection=yes',DRIVER='{ODBC Driver 13 for SQL Server}',SERVER=server)

        f = open(cfg.sql_folder + 'StagingDB_SFDC_PTC.sql')
        tsql = f.read()
        f.close()

        output = pd.read_sql(tsql, cnxn)
        output.to_csv(folder + target, sep='|', index=False)
        
    else:
        output = pd.read_csv(folder + target, sep="|", encoding="ISO-8859-1")
        
    return(output)

#################################################
#   getPTV                                      #
#################################################

def getPTV (usage, folder, refresh = 1):
    
    print ("function : GetGHIData: get PTV :: start :: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('usage = ', usage,'\nrefresh = ', refresh, '\nfolder is ', folder, "\n\n")
    
    #server = 'EDWStage'    
    database = 'StagingDB'
    target = server + '_' + database + '_' + 'SFDC_PTV.txt'
    
    if refresh:
        cnxn = pyodbc.connect('Trusted_Connection=yes',DRIVER='{ODBC Driver 13 for SQL Server}',SERVER=server)

        f = open(cfg.sql_folder + 'StagingDB_SFDC_PTV.sql')
        tsql = f.read()
        f.close()

        output = pd.read_sql(tsql, cnxn)
        output.to_csv(folder + target, sep='|', index=False)
        
    else:
        output = pd.read_csv(folder + target, sep="|", encoding="ISO-8859-1")
        
    return(output)

#################################################
#   stgBills                                    #
#################################################

def getPayors (usage, folder, refresh = 1):
    
    print ("function : GetGHIData: get Payors :: start :: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('usage = ', usage,'\nrefresh = ', refresh, '\nfolder is ', folder, "\n\n")
    
    #server = 'EDWStage'    
    database = 'StagingDB'
    target = server + '_' + database + '_' + 'SFDC_Payors.txt'
    
    if refresh:
        cnxn = pyodbc.connect('Trusted_Connection=yes',DRIVER='{ODBC Driver 13 for SQL Server}',SERVER=server)

        f = open(cfg.sql_folder + 'SFDC_Payor_Plan_HCO_Hierarchy.sql')
        tsql = f.read()
        f.close()

        output = pd.read_sql(tsql, cnxn)
        output.to_csv(folder + target, sep='|', index=False)
        
    else:
        output = pd.read_csv(folder + target, sep="|", encoding="ISO-8859-1")
    
    
    output['Tier1Payor'] = output.Tier1PayorName + " (" + output.Tier1PayorID + ")"
    output['Tier2Payor'] = output.Tier2PayorName + " (" + output.Tier2PayorID + ")"
    output['Tier4Payor'] = output.Tier4PayorName + " (" + output.Tier4PayorID + ")"

    select_column = ['Tier1Payor', 'Tier1PayorName', 'Tier1PayorID',
                      'Tier2Payor', 'Tier2PayorName', 'Tier2PayorID',
                      'Tier4PayorID'
                      #'Tier4Payor', 'Tier4PayorName', 
                    ]
  
    return(output[select_column])
#################################################
#   stgBills                                    #
#################################################

def stgBills_data(usage, folder, refresh=1):
    # usage = there is only one usage now
    # refresh = [1: refresh data from database and save to the given folder,
    #            0: get the data from the given folder
    # folder - if not blank: location to save the output & folder to get the local data
    
    print ("function : GetGHIData: stgBills_data :: start :: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('usage = ', usage,'\nrefresh = ', refresh, '\nfolder is ', folder, "\n\n")
    
    #server = 'EDWStage'
    database = 'StagingDB'
    target = server + '_' + database + '_' + 'stgBills.txt'

    if refresh:    
        cnxn = pyodbc.connect('Trusted_Connection=yes',DRIVER='{ODBC Driver 13 for SQL Server}',\
                              SERVER=server,DATABASE=database)

        tsql = """
                SELECT *
                FROM [StagingDB].[dbo].[stgBills]
                where ClaimPeriodDate >= '2015-12-01'
                """
                
        output = pd.read_sql(tsql, cnxn)
        output.to_csv(folder + target, sep='|', index=False)
        
    else:
        output = pd.read_csv(folder + target, sep="|", encoding="ISO-8859-1")
        
    return output


#################################################
#   stgPayment                                  #
#################################################

def stgPayment_data(usage, folder, refresh=1):
    # usage = there is only one usage now
    # refresh = [1: refresh data from database and save to the given folder,
    #            0: get the data from the given folder
    # folder - if not blank: location to save the output & folder to get the local data
    
    print ("function : GetGHIData: stgPayments_data :: start :: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print ('usage = ', usage,'\nrefresh = ', refresh, '\nfolder is ', folder, "\n\n")
    
    #server = 'EDWStage'
    database = 'StagingDB'
    target = server + '_' + database + '_' + 'stgPayments.txt'

    if refresh:    
        cnxn = pyodbc.connect('Trusted_Connection=yes',DRIVER='{ODBC Driver 13 for SQL Server}',\
                              SERVER=server,DATABASE=database)

        tsql = """
                Select *
                from StagingDB.dbo.stgPayments
                where AccountingPeriodDate >='2016-01-01'
                """
                
        output = pd.read_sql(tsql, cnxn)
        output.to_csv(folder + target, sep='|', index=False)
                
    else:
        output = pd.read_csv(folder + target, sep="|", encoding="ISO-8859-1")
        
    return output


def getProductListPrice():
    prep_file_name = "ProductListPrice.xlsx"

    output = pd.read_excel(cfg.prep_file_path+prep_file_name, sheet_name = "ProductListPrice", encoding='utf-8-sig')
    return output
