'''
Created on Aug 17, 2018

@author: aliu
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pandas as pd
from datetime import datetime

import project_io_config as cfg
import random
refresh = cfg.refresh

pd.options.display.max_columns=999

################################
# Read Input data

target = 'OLI_PTx.txt'
Claim2Rev = pd.read_csv(cfg.output_file_path+target, sep="|", encoding= "ISO-8859-1")
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
 'ClaimEntryDate','Days_toInitPymnt', 'Days_toLastPymnt',
 'CurrentQDXTicketNumber',
 'QDXTickCnt', 'QDXCaseCnt',
 
 'BillingCaseStatusSummary2', 'BillingCaseStatusCode', 'BillingCaseStatus',
 
 'Total Billed', 'Charge', 'Total Payment', 'PayorPaid', 'PatientPaid','AllowedAmt',

 'All other Adjustment', 'Charged in Error',
 'GHI Adjustment', 'Insurance Adjustment', 'Refund & Refund Reversal', 'Revenue Impact Adjustment',

 'Tier1PayorID', 'Tier1PayorName', 'Tier1Payor',
 'Tier2PayorID', 'Tier2Payor', 'Tier2PayorName',
 'Tier4PayorID', 'Tier4Payor', 'Tier4PayorName',
 'QDXInsPlanCode', 'FinancialCategory',
 
 'TestDelivered', 'IsClaim', 'IsFullyAdjudicated',
 'Rerouted_Ticket', 'Status',
 
 'BusinessUnit', 'InternationalArea', 'Division', 'Country',
 'OrderingHCPName', 'OrderingHCPCity', 'OrderingHCPState', 'OrderingHCPCountry',
 'Territory', 'TerritoryRegion', 'TerritoryArea',
 
 'ProcedureType',
 'Age_Of_Specimen',
 'Specialty',
 'IsOrderingHCPCTR',
 'SubmittedNCCNRisk',
 'HCPProvidedGleasonScore', 'HCPProvidedPSA',
 'HCPProvidedClinicalStage','MaxPctOfTumorInvolvementInAnyCore',
 'HCPProvidedNumberOfPositiveCores',
 'NumberOf4Plus3Cores',  
 
 'IBC_Candidate_for_Adj_Chemo', 'SOMN_Status',
 
 'appealDenReason','appealDenReasonDesc', 'appealSuccess', 'appealResult',
 
 'priorAuthResult', 'priorAuthResult_Category','priorAuthNumber','PreClaim_Failure'
 ]

OLI_data = Claim2Rev[(Claim2Rev.Test=='Prostate') &\
                     ~(Claim2Rev.CurrentQDXTicketNumber.isnull()) &\
                     (Claim2Rev.OrderStartDate >=' 2017-01-01')][select_columns].copy()

#######################################
# Standardized OLI Clinical Criteria   #
#######################################

OLI_data['MedOnc_Order'] = (OLI_data[~OLI_data.Specialty.isnull()].Specialty == 'Oncologist').map({True:'Med Onc Order', False:'Non-Med Onc Order'})
OLI_data['IsOrderingHCPCTR'] = (OLI_data[~OLI_data.IsOrderingHCPCTR.isnull()].IsOrderingHCPCTR).map({1:'Yes', 0:'No'})

OLI_data.loc[OLI_data.MaxPctOfTumorInvolvementInAnyCore == '> 50%', 'MaxPctOfTumorInvolvementInAnyCore'] = 'Greater than or Equal to 50%'
OLI_data.loc[OLI_data.MaxPctOfTumorInvolvementInAnyCore == 'Ã¢Â\x89Â¤ 50%','MaxPctOfTumorInvolvementInAnyCore'] = 'less than 50%'
#compare_string = chr(8804) + ' 50%'
#OLI_data.loc[OLI_data.MaxPctOfTumorInvolvementInAnyCore == compare_string,'MaxPctOfTumorInvolvementInAnyCore'] = 'less than 50%'
#ord('≤')

OLI_data.loc[OLI_data.SubmittedNCCNRisk == 'Favorable Intermediate', 'SubmittedNCCNRisk'] = 'Intermediate Favorable'
OLI_data.loc[OLI_data.SubmittedNCCNRisk == 'Unfavorable Intermediate', 'SubmittedNCCNRisk'] = 'Intermediate Unfavorable'

# need to clarify 180 days, it is the last day of 6th month, 
OLI_data['Age_Of_Biopsy'] = 'Unknown'
cond = (~OLI_data.Age_Of_Specimen.isnull()) & (OLI_data.Age_Of_Specimen >= 0) & (OLI_data.Age_Of_Specimen < 180)
OLI_data.loc[cond,'Age_Of_Biopsy'] = 'Less than 6 months'

cond = (~OLI_data.Age_Of_Specimen.isnull()) & (OLI_data.Age_Of_Specimen >= 180) & (OLI_data.Age_Of_Specimen <= 1080)
OLI_data.loc[cond,'Age_Of_Biopsy'] = '6-36 months'

########################################################
# Read and prepare the Plans' PTC information
# there are 19 distinct clinical criteria in IBC PTC
# 13 of the 19 has corresponding Patient data to compare with
########################################################

select_columns = ['Name', 'Policy_Status',
# 'Policy',
 'Test',
# 'Tier2PayorName', 'Tier2PayorID',
# 'Tier4PayorName',
 'Tier4PayorID',
# 'QDX_InsPlan_Code', 'Financial_Category', 'Line_of_Business',
 'GHI_AgeOfBiopsy__c',
 'Prostate_Patient_life_expectance_10_20__c',
 'GHI_NCCNRiskCategory__c',
 'GHI_HCPProvidedGleasonScore__c',
# PSA 
 'GHI_HCPProvidedClinicalStage__c',
# Number of positive cores
# number of 4+3 cores
 'GHI_MaxTumorInvolvement__c',
 'GHI_ProcedureType__c',
 'GHI_MedOncOrder__c',
 'GHI_CTR__c',
 'Medical_Policy_Status1',
 'Contracted_Policy_Status1'
]

PTC_Criteria = select_columns[4:-2]

'''
Add a check to flag a blank PTC: all criteria and set to xx_PTC_Available = 'No'
'''
def flag_blank_PTC(record):
    if sum(record[PTC_Criteria].isnull()) == len(PTC_Criteria):
        if record.Policy == 'MP':
            record['Medical_Policy_Status1'] = 'Blank PTC'
        else:
            record['Contracted_Policy_Status1'] = 'Blank PTC'
    return(record)

PTC = PTC.apply(lambda x: flag_blank_PTC(x), axis=1)

MP_PTC_data = PTC[~(PTC.Tier4PayorID.isnull()) & (PTC.Policy=='MP')][select_columns[:-1]]
new_names = {}
for i in PTC_Criteria:
    new_name = 'MP_' + i
    new_names[i]=new_name
MP_PTC_data.rename(columns={'Name':'MP_PTC','Policy_Status':'MP_Policy_Status'}, inplace=True)
MP_PTC_data.rename(columns=new_names, inplace=True)

    
CT_PTC_data = PTC[~(PTC.Tier4PayorID.isnull()) & (PTC.Policy=='CT')][select_columns[:-2]+select_columns[-1:]]
new_names = {}
for i in PTC_Criteria:
    new_name = 'CT_' + i
    new_names[i]=new_name
CT_PTC_data.rename(columns={'Name':'CT_PTC','Policy_Status':'CT_Policy_Status'}, inplace=True)
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
Data.loc[Data.MP_PTC.isnull(),'Medical_Policy_Status1'] = 'No PTC'
Data['Medical_Policy_Status1'] = Data['Medical_Policy_Status1'].fillna('Yes PTC')
Data['Medical_Policy_Status'] = Data['Medical_Policy_Status1'].map({'Blank PTC':'No policy', 'No PTC': 'No policy', 'Yes PTC':'Published policy'})

Data = pd.merge(Data, CT_PTC_data, how='left', on = ['Tier4PayorID', 'Test'])
Data.loc[Data.CT_PTC.isnull(),'Contracted_Policy_Status1'] = 'No PTC'
Data['Contracted_Policy_Status1'] = Data['Contracted_Policy_Status1'].fillna('Yes PTC')
Data['Contracted_Policy_Status'] = Data['Contracted_Policy_Status1'].map({'Blank PTC':'No policy', 'No PTC': 'No policy', 'Yes PTC':'Published policy'})

Data = pd.merge(Data, PTV_data, how='left', on = ['Tier4PayorID', 'Test'])
Data['PTV_Available'] = 'Yes'
Data.loc[Data.PTV.isnull(),'PTV_Available'] = 'No'

####################################################################
# 
# extract data set to publish to B&R Tableau to support data cleaning effort
# Publish report in the GNAM Monthly Refresh
#
####################################################################

Data_for_Ops = Data[['OrderID', 'OLIID', 'Test', 'TestDeliveredDate', 'OrderStartDate',
               'Tier1PayorID', 'Tier1PayorName', 'Tier1Payor', 'Tier2PayorID',
               'Tier2Payor', 'Tier2PayorName', 'Tier4PayorID', 'Tier4Payor',
               'Tier4PayorName', 'QDXInsPlanCode', 'FinancialCategory',       

               'MP_PTC', 'MP_Policy_Status', 'Medical_Policy_Status', 'Medical_Policy_Status1',
               'CT_PTC', 'CT_Policy_Status', 'Contracted_Policy_Status', 'Contracted_Policy_Status1',
               'PTV'
        ]].copy()

        #########################################
        #   Add the Payor View Set assignment   #
        #########################################
prep_file_name = "Payor-ViewSetAssignment.xlsx"
Payor_view = pd.read_excel(cfg.prep_file_path+prep_file_name, sheet_name = "SetAssignment", usecols="B:D", encoding='utf-8-sig')

for i in Payor_view.Set.unique() :
    #print (i)
    code = Payor_view[Payor_view.Set==i].PayorID
    join_column = Payor_view[Payor_view.Set==i].JoinWith.iloc[0]
    
    Data_for_Ops.loc[Data_for_Ops[join_column].isin(list(code)),i] = '1'

output_file = 'Prostate_OLI+PTC.txt'
Data_for_Ops.to_csv(cfg.output_file_path+output_file, sep='|',index=False)

print("Prostate_OLI+PTC data refresh done")
####################################################################
# Prepare the data for comparison
#
# Current algorithm compare 10 of the 13 comparable clinical criteria
# Not comparing: Gender, Tumor Size, Test Execution
####################################################################
Prostate_compare = {
            'Age_Of_Biopsy' : 'MP_GHI_AgeOfBiopsy__c',
            'SubmittedNCCNRisk' : 'MP_GHI_NCCNRiskCategory__c',
            'HCPProvidedGleasonScore' : 'MP_GHI_HCPProvidedGleasonScore__c',
#               'HCPProvidedPSA' : 'MP_'GHI_HCPProvidedPSANgMl__c',
            'HCPProvidedClinicalStage' : 'MP_GHI_HCPProvidedClinicalStage__c',
#               'HCPProvidedNumberOfPositiveCores' : 'GHI_HCPProvidedNumberOfPositiveCores__c'
#               'NumberOf4Plus3Cores' : 'GHI_NumberOfCores__c',
            'ProcedureType' :  'MP_GHI_ProcedureType__c',
            'IsOrderingHCPCTR' : 'MP_GHI_CTR__c',
            ### Keep MaxPctTumor and MedOnc Order as bottom 2
            'MedOnc_Order' : 'MP_GHI_MedOncOrder__c',  # data issue
            'MaxPctOfTumorInvolvementInAnyCore' : 'MP_GHI_MaxTumorInvolvement__c' # data issue
            }

for i in list(Prostate_compare.keys()):
    Data[i] = Data[i].fillna("Unknown")
    Data[Prostate_compare[i]] = Data[Prostate_compare[i]].fillna("")
    Data[Prostate_compare[i]] = Data[Prostate_compare[i]].apply(lambda x : x.split(sep=";") if x else '')

Data['PreClaim_Failure'] = Data['PreClaim_Failure'].fillna("PA not required") 
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
    if record.Medical_Policy_Status == 'No policy':
        record['MP_InCriteria'] = '..Out'
        for i in list(Prostate_compare.keys()):
            comparing = Prostate_compare[i][7:-3] + '_coverage'
            comparing1 = Prostate_compare[i][7:-3] + '_coverage1'
            record[comparing] = '..Out'
            record[comparing1] = record['Medical_Policy_Status1']
            
        comparing = 'PA_requirement_coverage'
        if record['PreClaim_Failure'] == 'Failure':
            record[comparing] = '..Out'
        else: # blank and non failure
            record[comparing] = '.In'
        
        record['OLI_InCriteria'] = '..Out'
        
        return(record)
    
    MP_InCriteria_list = [] 
    for i in list(Prostate_compare.keys())[:-2]: #exclude MedOncOrder &  until data issue is fixed
        #print ('OLI: ', record[i], ' vs ', record[Prostate_compare[i]])
        comparing = Prostate_compare[i][7:-3] + '_coverage'
        comparing1 = Prostate_compare[i][7:-3] + '_coverage1'
       
        if (record[i] != '') and (record[i] != 'Unknown'):    # Patient clinical criteria is captured in OLI, then compare
            if (type(record[Prostate_compare[i]]) == list):   # PTC clinical criteria is entered
                record[comparing] = '.In' if (record[i] in record[Prostate_compare[i]]) else '..Out'
                record[comparing1] = 'Meet criteria' if (record[i] in record[Prostate_compare[i]]) else '..Out'
                MP_InCriteria_list.append((record[i] in record[Prostate_compare[i]]))
            else:                             # PTC clinical criteria is blank           
                record[comparing] = '.In'
                record[comparing1] = 'Unspecified criteria'
                record[Prostate_compare[i]] = 'Unspecified criteria'  
                # by pass when both patient & ptc have no information
                
        else:   # Patient clinical criteria is unavailable
            if (type(record[Prostate_compare[i]]) == list):   # PTC clinical criteria is entered
                record[comparing] = '..Out'
                record[comparing1] = 'Indeterminable'
                MP_InCriteria_list.append(0)          # Patient clinical criteria is not captured in OLI, set to 'Out' as no information to compare
            else:
                record[comparing] = '.In'
                record[comparing1] = 'Unspecified criteria'
                record[Prostate_compare[i]] = 'Unspecified criteria'
                # by pass when both patient & ptc have no information
    
    for i in list(Prostate_compare.keys())[-2:]:
        comparing = Prostate_compare[i][7:-3] + '_coverage'
        comparing1 = Prostate_compare[i][7:-3] + '_coverage1'
        
        if record[i] != '' and (record[i] != 'Unknown'):        # Patient clinical criteria is captured in OLI
            if (type(record[Prostate_compare[i]] == list)):
                record[comparing] = '.In' if (record[i] in record[Prostate_compare[i]]) else '..Out'
                record[comparing1] = 'Meet criteria' if (record[i] in record[Prostate_compare[i]]) else '..Out'
            else:
                record[comparing] = '.In'
                record[comparing1] = 'Unspecified criteria'
                record[Prostate_compare[i]] = 'Unspecified criteria'
        else:       # Patient clinical criteria is unavailable
            if (type(record[Prostate_compare[i]]) == list):   # PTC clinical criteria is entered
                record[comparing] = '..Out'
                record[comparing1] = 'Indeterminable'
            else:
                record[comparing] = '.In'
                record[comparing1] = 'Unspecified criteria'
                record[Prostate_compare[i]] = 'Unspecified criteria'
    
    # Quadax PreClaim status precede GHI PA Required Flag
    # compare PTV.OSM_PA_Required__c.unique() with PreClaim_Failure
    # for PA_Required = True and PreClaim_Failure = 'Non Failure' then IN
    # PA_Required = True and PreClaim_Failure = 'Failure' or = blank, then OUT
    # for PA_Required = False, then does not matter what is PreClaim_Failure
    comparing = 'PA_requirement_coverage'
    
    AV_InCriteria = []
    if record['PreClaim_Failure'] in (['Failure', 'PA Denied']):
        record[comparing] = '..Out'
        AV_InCriteria.append(0)
    else: # blank and non failure
        record[comparing] = '.In'
        AV_InCriteria.append(1)
       
    
    # len(InCriteria_1_temp) is 0 when the Plan has no PTC
    # 0 in InCriteria_1_temp)) when at least 1 of the criteria is out
    record['MP_Criteria_considered'] = len(MP_InCriteria_list)
    
    if ((len(MP_InCriteria_list) == 0) or (0 in MP_InCriteria_list)):
        record['MP_InCriteria'] = '..Out'
    else:
        record['MP_InCriteria'] = '.In'

    OLI_InCriteria_list = MP_InCriteria_list + AV_InCriteria
    if ((len(OLI_InCriteria_list) == 0) or (0 in OLI_InCriteria_list)):
        record['OLI_InCriteria'] = '..Out'
    else:
        record['OLI_InCriteria'] = '.In'
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
# An OLI is OUT for all PreClaim Failure
# OLI is IN if it is not a PreClaim Failure or PreClaim status is null
####################################################################

def In_or_Out_2 (record):

    # kick it to 'OUT' if PTC is not available, either no PTC record or PTC is blank
    if record.MP_PTC_Available != 'Yes':
        record['MP_InCriteria_2'] = '.Out'
        for i in list(Prostate_compare.keys()):
            comparing = Prostate_compare[i][:-3] + '_coverage_2'
            record[comparing] = '.Out'

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
                record[comparing] = 'Patient & Criteria NA'
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
        record['MP_InCriteria_2'] = '..Out'
    else:
        record['MP_InCriteria_2'] = '..In'
 
    return(record)

#Data = Data.apply(lambda x: In_or_Out_2(x), axis=1)


###################################################################
# Write the output
####################################################################
for i in Payor_view.Set.unique() :
    #print (i)
    code = Payor_view[Payor_view.Set==i].PayorID
    join_column = Payor_view[Payor_view.Set==i].JoinWith.iloc[0]
    
    Data.loc[Data_for_Ops[join_column].isin(list(code)),i] = '1'

output_file = 'In_or_Out_Prostate.txt'
Data.to_csv(cfg.output_file_path+output_file, sep='|',index=False)

print("Payment Assessment Data Prep Done Done")

