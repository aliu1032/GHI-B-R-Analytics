'''
Created on Aug 17, 2018

@author: aliu
'''

import pandas as pd
from datetime import datetime

import project_io_config as cfg
import random
refresh = cfg.refresh

pd.options.display.max_columns=999

################################
# Read Input data

target = 'OLI_PTx.txt'
Claim2Rev = pd.read_csv(cfg.output_file_path+target, sep="|", encoding="ISO-8859-1")
Claim2Rev.TestDeliveredDate = pd.to_datetime(Claim2Rev.TestDeliveredDate.astype(str), format = "%Y-%m-%d", errors='coerce')

target = 'Wide_' + 'Prostate' +'_PTC.txt'
PTC = pd.read_csv(cfg.input_file_path+target, sep="|", encoding="ISO-8859-1")
PTC_criteria_enum = pd.read_excel(cfg.prep_file_path+'Enum.xlsx', sheet_name = "Criteria_ENUM", encoding='utf-8-sig', usecols="A:C,E:F")

target = 'Wide_' + 'Prostate' +'_PTV.txt'
PTV = pd.read_csv(cfg.input_file_path+target, sep="|", encoding="ISO-8859-1")
PTV_PA_enum = pd.read_excel(cfg.prep_file_path+'Enum.xlsx', sheet_name = "PA_ENUM", encoding='utf-8-sig', usecols="A:C,E:F")

########################################################
# Retrieve Patient clinical criteria captured with OLI #
########################################################
## gender
## Procedure Type
## Age
## Tumor size

select_columns = [
 'OrderID', 'OLIID', 'Test',
 'TestDeliveredDate', 'OrderStartDate',

 'Tier1PayorID', 'Tier1PayorName', 'Tier1Payor',
 'Tier2PayorID', 'Tier2Payor', 'Tier2PayorName',
 'Tier4PayorID', 'Tier4Payor', 'Tier4PayorName',
 'QDXInsPlanCode', 'FinancialCategory',
 
 'ProcedureType',
 'Specialty',
 'IsOrderingHCPCTR',
 'SubmittedNCCNRisk',
 'HCPProvidedGleasonScore', 'HCPProvidedPSA',
 'HCPProvidedClinicalStage','MaxPctOfTumorInvolvementInAnyCore',
 'HCPProvidedNumberOfPositiveCores',
 'NumberOf4Plus3Cores',  
 
 'BillingCaseStatusSummary2', 'BillingCaseStatus',
 'PreClaim_Failure'
 ]

OLI_data = Claim2Rev[(Claim2Rev.Test=='Prostate') &\
                     ~(Claim2Rev.CurrentQDXTicketNumber.isnull()) &\
                     (Claim2Rev.OrderStartDate >=' 2017-01-01')][select_columns].copy()

#######################################
# Standardized OLI Clinical Criteria   #
#######################################

OLI_data['MedOnc_Order'] = (OLI_data[~OLI_data.Specialty.isnull()].Specialty == 'Oncologist').map({True:'Med Onc Order', False:'Non-Med Onc Order'})
OLI_data['IsOrderingHCPCTR'] = (OLI_data[~OLI_data.IsOrderingHCPCTR.isnull()].IsOrderingHCPCTR).map({1:'Yes', 0:'No'})

OLI_data.loc[OLI_data.MaxPctOfTumorInvolvementInAnyCore == '> 50%', 'MaxPctOfTumorInvolvementInAnyCore'] = 'less than 50%'
OLI_data.loc[OLI_data.MaxPctOfTumorInvolvementInAnyCore == 'â�\x89¤ 50%','MaxPctOfTumorInvolvementInAnyCore'] = 'Greater than or Equal to 50%'

OLI_data.loc[OLI_data.SubmittedNCCNRisk == 'Favorable Intermediate', 'SubmittedNCCNRisk'] = 'Intermediate Favorable'
OLI_data.loc[OLI_data.SubmittedNCCNRisk == 'Unfavorable Intermediate', 'SubmittedNCCNRisk'] = 'Intermediate Unfavorable'

########################################################
# Read and prepare the Plans' PTC information
# there are 19 distinct clinical criteria in IBC PTC
# 13 of the 19 has corresponding Patient data to compare with
########################################################

select_columns = ['Name',
# 'Policy',
 'Test',
# 'Tier2PayorName', 'Tier2PayorID',
# 'Tier4PayorName',
 'Tier4PayorID',
# 'QDX_InsPlan_Code', 'Financial_Category', 'Line_of_Business',
# 'GHI_RecentlyNewlyDiagnosis__c',
 'GHI_AgeOfBiopsy__c',
 'Prostate_Patient_life_expectance_10_20__c',
 'GHI_NCCNRiskCategory__c',
 'GHI_HCPProvidedGleasonScore__c',
 'GHI_HCPProvidedClinicalStage__c',
 'GHI_MaxTumorInvolvement__c',
 'GHI_ProcedureType__c',
 'GHI_MedOncOrder__c',
 'GHI_CTR__c',
 'MP_PTC_Available',
 'CT_PTC_Available'
]

PTC_Criteria = select_columns[3:-2]

'''
Add a check to flag a blank PTC: all criteria and set to xx_PTC_Available = 'No'
'''
def flag_blank_PTC(record):
    if sum(record[PTC_Criteria].isnull()) == len(PTC_Criteria):
        if record.Policy == 'MP':
            record['MP_PTC_Available'] = 'Blank PTC'
        else:
            record['CT_PTC_Available'] = 'Blank PTC'
    return(record)

PTC = PTC.apply(lambda x: flag_blank_PTC(x), axis=1)

MP_PTC_data = PTC[~(PTC.Tier4PayorID.isnull()) & (PTC.Policy=='MP')][select_columns[:-1]]
new_names = {}
for i in PTC_Criteria:
    new_name = 'MP_' + i
    new_names[i]=new_name
MP_PTC_data.rename(columns={'Name':'MP_PTC'}, inplace=True)
MP_PTC_data.rename(columns=new_names, inplace=True)

    
CT_PTC_data = PTC[~(PTC.Tier4PayorID.isnull()) & (PTC.Policy=='CT')][select_columns[:-2]+select_columns[-1:]]
new_names = {}
for i in PTC_Criteria:
    new_name = 'CT_' + i
    new_names[i]=new_name
CT_PTC_data.rename(columns={'Name':'CT_PTC'}, inplace=True)
CT_PTC_data.rename(columns=new_names, inplace=True)

########################################################
# Read and prepare the Plans' PTV information
########################################################

select_columns = ['Name','Test','Tier4PayorID','PA_Required']
PTV_data = PTV[~(PTV.Tier4PayorID.isnull())][select_columns]
PTV_data.rename(columns = {'Name':'PTV'}, inplace=True)

########################################################
# Connect the OLI record with the Plan PTC record 
########################################################

Data = pd.merge(OLI_data, MP_PTC_data, how='left', on = ['Tier4PayorID', 'Test'])
Data.loc[Data.MP_PTC.isnull(),'MP_PTC_Available'] = 'No'
Data['MP_PTC_Available'] = Data['MP_PTC_Available'].fillna('Yes')

Data = pd.merge(Data, CT_PTC_data, how='left', on = ['Tier4PayorID', 'Test'])
Data.loc[Data.CT_PTC.isnull(),'CT_PTC_Available'] = 'No'
Data['CT_PTC_Available'] = Data['CT_PTC_Available'].fillna('Yes')

Data = pd.merge(Data, PTV_data, how='left', on = ['Tier4PayorID', 'Test'])
Data['PTV_Available'] = 'Yes'
Data.loc[Data.PTV.isnull(),'PTV_Available'] = 'No'

####################################################################
# Prepare the data for comparison
#
# Current algorithm compare 10 of the 13 comparable clinical criteria
# Not comparing: Gender, Tumor Size, Test Execution
####################################################################
Prostate_compare = {
    #'Pa' :'MP_GHI_AgeOfBiopsy__c',
            'SubmittedNCCNRisk' : 'MP_GHI_NCCNRiskCategory__c',
            'HCPProvidedGleasonScore' : 'MP_GHI_HCPProvidedGleasonScore__c',
#               'HCPProvidedPSA' : 'MP_'GHI_HCPProvidedPSANgMl__c',
            'HCPProvidedClinicalStage' : 'MP_GHI_HCPProvidedClinicalStage__c',
#               'HCPProvidedNumberOfPositiveCores' : 'GHI_HCPProvidedNumberOfPositiveCores__c'
#               'NumberOf4Plus3Cores' : 'GHI_NumberOfCores__c',
            'MaxPctOfTumorInvolvementInAnyCore' : 'MP_GHI_MaxTumorInvolvement__c',
            'ProcedureType' :  'MP_GHI_ProcedureType__c',
            'IsOrderingHCPCTR' : 'MP_GHI_CTR__c',
            'MedOnc_Order' : 'MP_GHI_MedOncOrder__c',  # data issue
#               CTR: 'GHI_CTR__c'
            #'PreClaim_Failure' : 'PA_Required',
                }

for i in list(Prostate_compare.keys()):
    Data[i] = Data[i].fillna("Unknown")
    Data[Prostate_compare[i]] = Data[Prostate_compare[i]].fillna("")
    Data[Prostate_compare[i]] = Data[Prostate_compare[i]].apply(lambda x : x.split(sep=";") if x else '')

Data['PreClaim_Failure'] = Data['PreClaim_Failure'].fillna("Unknown") 
Data['PA_Required'] = Data['PA_Required'].fillna('.PTV unknown')

####################################################################
# Calculate IN/OUT criteria            
# Algorithm 1:
# The OLI is OUT when there is not PTC (including Plan has no PTC record and Plan has a blank PTC record
# For each clinical criteria
# If Patient's clinical criteria is available
#      check with Plan's PTC
#      OLI is covered if Plan PTC has the patient's clinical criteria; else OLI is not covered
#      By pass the check if Plan does not have the PTC: This is to handle the case when Payor/Plan Medical Policy does not mention the criteria
#      
# If Patient's clinical criteria is unavailable
#      If Plan has a PTC, then the OLI is not covered since we cannot provide the 'required' information
#      If Plan does not have a PTC, then by pass the check
#
####################################################################
def In_or_Out_1 (record):
    
    # kick it to 'OUT' if PTC is not available, either no PTC record or PTC is blank
    if record.MP_PTC_Available != 'Yes':
        record['MP_InCriteria_1'] = '.PTC unknown'
        return(record)
    
    InCriteria_1_temp = [] 
    for i in list(Prostate_compare.keys())[:-1]: #exclude MedOncOrder until data issue is fixed
        #print ('OLI: ', record[i], ' vs ', record[Prostate_compare[i]])
        comparing = Prostate_compare[i][:-3] + '_coverage_1'
        
        if (record[i] != '') and (record[i] != 'Unknown'):  # Patient clinical criteria is captured in OLI, then compare
            if (type(record[Prostate_compare[i]]) == list):   # PTC clinical criteria is entered
                record[comparing] = '..In' if (record[i] in record[Prostate_compare[i]]) else '.Out'
                InCriteria_1_temp.append((record[i] in record[Prostate_compare[i]]))
            else:                                        # PTC clinical criteria is blank
                record[comparing] = '.MP_Criteria blank'
                # by pass when both patient & ptc have no information
                
        else:                                            # Patient clinical criteria is unavailable
            if (type(record[Prostate_compare[i]]) == list):   # PTC clinical criteria is entered
                record[comparing] = '.Out'
                InCriteria_1_temp.append(0)          # Patient clinical criteria is not captured in OLI, set to 'Out' as no information to compare
            else:
                record[comparing] = 'Patient & MP_Criteria blank'
                # by pass when both patient & ptc have no information

    comparing = Prostate_compare['MedOnc_Order'][:-3] + '_coverage_1'
    if record['MedOnc_Order'] != '' and (record['MedOnc_Order'] != 'Unknown'):
        if (type(record['MP_GHI_MedOncOrder__c'] == list)):
            record[comparing] = '..In' if (record['MedOnc_Order'] in record['MP_GHI_MedOncOrder__c']) else '.Out'
        else:
            record[comparing] = '.MP_Criteria blank'
    else:
        if (type(record[Prostate_compare[i]]) == list):   # PTC clinical criteria is entered
            record[comparing] = '.Out'
        else:
            record[comparing] = 'Patient & MP_Criteria blank'
            
    # Quadax PreClaim status precede GHI PA Required Flag
    # compare PTV.OSM_PA_Required__c.unique() with PreClaim_Failure
    # for PA_Required = True and PreClaim_Failure = 'Non Failure' then IN
    # PA_Required = True and PreClaim_Failure = 'Failure' or = blank, then OUT
    # for PA_Required = False, then does not matter what is PreClaim_Failure
    comparing = 'PA_requirement_1'
    
    if record['PreClaim_Failure'] == 'Failure':
        record[comparing] = '.Out'
        InCriteria_1_temp.append(0)
    else: # blank and non failure
        record[comparing] = '..In'
        InCriteria_1_temp.append(1)
       
    
    # len(InCriteria_1_temp) is 0 when the Plan has no PTC
    # 0 in InCriteria_1_temp)) when at least 1 of the criteria is out
    record['MP_Criteria_1_considered'] = len(InCriteria_1_temp)
    
    if ((len(InCriteria_1_temp) == 0) or (0 in InCriteria_1_temp)):
        record['MP_InCriteria_1'] = '.Out'
    else:
        record['MP_InCriteria_1'] = '..In'
 
    return(record)

Data = Data.apply(lambda x: In_or_Out_1(x), axis=1)

####################################################################
# Calculate IN/OUT criteria            
# Algorithm 2:
# The OLI is OUT when there is not PTC (including Plan has no PTC record and Plan has a blank PTC record
# For each clinical criteria
# If Patient's clinical criteria is available
#      check with Plan's PTC
#      OLI is covered if Plan PTC has the patient's clinical criteria; else OLI is not covered
#      PTC clinical criteria is unavailable (the criteria is blank in PTC), this is OUT
#      
# If Patient's clinical criteria is unavailable
#      If Plan has a PTC, then the OLI is not covered since we cannot provide the 'required' information
#      If Plan does not have a PTC, then by pass the check
#
#
# An OLI is OUT for all PreClaim Failure
# OLI is IN if it is not a PreClaim Failure or PreClaim status is null
####################################################################

def In_or_Out_2 (record):

    # kick it to 'OUT' if PTC is not available, either no PTC record or PTC is blank
    if record.MP_PTC_Available != 'Yes':
        record['MP_InCriteria_2'] = '.Out'
        return(record)
    
    InCriteria_2_temp = [] 
    for i in list(Prostate_compare.keys())[:-1]:  
        #print ('OLI: ', record[i], ' vs ', record[Prostate_compare[i]])
        comparing = Prostate_compare[i][:-3] + '_coverage_2'
        
        if (record[i] != '') and (record[i] != 'Unknown'):  # Patient clinical criteria is captured in OLI, then compare
            if (type(record[Prostate_compare[i]]) == list):   # PTC clinical criteria is entered
                record[comparing] = '..In' if (record[i] in record[Prostate_compare[i]]) else '.Out'
                InCriteria_2_temp.append((record[i] in record[Prostate_compare[i]]))
            else:                                        # PTC clinical criteria is blank
                record[comparing] = '.Out'
                InCriteria_2_temp.append(0)
                
        else:                               # Patient clinical criteria is unavailable
            if (type(record[Prostate_compare[i]]) == list):   # PTC clinical criteria is entered
                record[comparing] = '.Out'
                InCriteria_2_temp.append(0)          # Patient clinical criteria is not captured in OLI, set to 'Out' as no information to compare
            else:
                record[comparing] = 'Patient & MP_Criteria blank'
                # by pass when both patient & ptc have no information
        
    # compare PTV.OSM_PA_Required__c.unique() with PreClaim_Failure
    # for PA_Required = True and PreClaim_Failure = 'Non Failure' then IN
    # PA_Required = True and PreClaim_Failure = 'Failure' or = blank, then OUT
    # for PA_Required = False, then does not matter what is PreClaim_Failure
    comparing = 'PA_requirement_2'
    
    if record['PreClaim_Failure'] == 'Failure':
        record[comparing] = '.Out'
        InCriteria_2_temp.append(0)
    else: # blank and non failure
        record[comparing] = '..In'
        InCriteria_2_temp.append(1)
        
    
    # len(InCriteria_1_temp) is 0 when the Plan has no PTC
    # 0 in InCriteria_1_temp)) when at least 1 of the criteria is out
    if ((len(InCriteria_2_temp) == 0) or (0 in InCriteria_2_temp)):
        record['MP_InCriteria_2'] = '.Out'
    else:
        record['MP_InCriteria_2'] = '..In'
 
    return(record)

Data = Data.apply(lambda x: In_or_Out_2(x), axis=1)


###################################################################
# Write the output
####################################################################
output_file = 'In_or_Out_Prostate.txt'
Data.to_csv(cfg.output_file_path+output_file, sep='|',index=False)

print("Payment Assessment Data Prep Done Done")

