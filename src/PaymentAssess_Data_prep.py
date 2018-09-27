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

target = 'Wide_' + 'IBC' +'_PTC.txt'
PTC = pd.read_csv(cfg.input_file_path+target, sep="|", encoding="ISO-8859-1")
PTC_criteria_enum = pd.read_excel(cfg.prep_file_path+'Enum.xlsx', sheet_name = "Criteria_ENUM", encoding='utf-8-sig', usecols="A:C,E:F")

target = 'Wide_' + 'IBC' +'_PTV.txt'
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
 
 'Specialty',
 'NodalStatus',
 'ProcedureType',
 'PatientAgeAtDiagnosis',
 'HCPProvidedClinicalStage',
 'SubmittedER',
 'SubmittedHER2',
 'SubmittedPR',
 'MultiplePrimaries',
 'IBC_TumorSizeCentimeters',
 'SubmittedNCCNRisk', 'SFDCSubmittedNCCNRisk',
 ]

OLI_data = Claim2Rev[(Claim2Rev.Test=='IBC') &\
                     ~(Claim2Rev.CurrentQDXTicketNumber.isnull()) &\
                     (Claim2Rev.TestDeliveredDate >=' 2018-04-01') & (Claim2Rev.OrderStartDate <= '2018-08-31')][select_columns].copy()

OLI_data.loc[OLI_data.NodalStatus == 'Node Negative','NodalStatus'] = 'Node Negative (pN0)'
OLI_data.loc[OLI_data.NodalStatus == 'Micromets (pN1mi: 0.2 - 2.0mm)','NodalStatus'] = 'Micromets (pN1mi: 0.2 2.0mm)'
OLI_data.loc[OLI_data.ProcedureType == 'Non-Biopsy','ProcedureType'] = 'Non Biopsy'

OLI_data.loc[(OLI_data.SubmittedER == 'Positive') & (OLI_data.SubmittedPR == 'PR Positive'),'HormoneReceptorHR']\
     = 'ER Positive and PR Positive (HR positive)'

OLI_data['MultiplePrimaries'] = OLI_data['MultiplePrimaries'].map({0:'No',1:'Yes'})
OLI_data['PatientAgeAtDiagnosis_int'] = OLI_data['PatientAgeAtDiagnosis']
OLI_data['PatientAgeAtDiagnosis'] = (OLI_data.PatientAgeAtDiagnosis >= 50).map({True:'>= 50', False: '< 50'})

'''
Example: OR001072184 : Multi Tumor order
OL001094259 has a claim
OL001094412 is test delivered and does not have a claim

Example: OR001074801 : Multi Tumor order
OL001097109 has a claim
OL001097337 is test delivered and does not have a claim
'''

########################################################
# Read and prepare the Plans' PTC information
########################################################

select_columns = ['Name',
# 'Policy',
 'Test',
# 'Tier2PayorName', 'Tier2PayorID',
# 'Tier4PayorName',
 'Tier4PayorID',
# 'QDX_InsPlan_Code', 'Financial_Category', 'Line_of_Business',
# 'GHI_RecentlyNewlyDiagnosis__c',
 'GHI_AgeAtDiagnosisRange__c',
# 'GHI_PatientHadAPriorOncotypeTest__c',
 'GHI_PatientGender__c',
 'GHI_MultiTumor__c',
 'GHI_Stage__c',
 'GHI_NodeStatus__c',
 'GHI_ERStatus__c',
 'GHI_PRStatus__c',
 'GHI_HormoneReceptorHR__c',
 'GHI_HER2Status__c',
# 'GHI_HER2mode__c',
 'GHI_TumorSize__c',
# 'GHI_TumorHistology__c',
 'GHI_ProcedureType__c',
# 'GHI_PostMenopausalWomen__c',
 'GHI_MedOncOrder__c',
# 'GHI_EvidenceOfDistantMetastaticBC__c',
 'GHI_MultiTumorTestExecution__c',
]
'''
#select_columns_b=['Name','Test','Tier4PayorID']
test = ['IBC']
test_criteria = PTC_criteria_enum[PTC_criteria_enum.Test==test[0]].SFDC_API_Name.unique()
for i in test_criteria:
    test_criteria_selection = PTC_criteria_enum[(PTC_criteria_enum.Test==test[0]) & (PTC_criteria_enum.SFDC_API_Name==i)].Criteria_Enum.unique()
    for j in test_criteria_selection:
        select_columns.append(i + '=' + j)
'''
MP_PTC_data = PTC[~(PTC.Tier4PayorID.isnull()) & (PTC.Policy=='MP')][select_columns]
new_names = {}
for i in select_columns[3:]:
    new_name = 'MP_' + i
    new_names[i]=new_name
MP_PTC_data.rename(columns={'Name':'MP_PTC'}, inplace=True)
MP_PTC_data.rename(columns=new_names, inplace=True)
    
CT_PTC_data = PTC[~(PTC.Tier4PayorID.isnull()) & (PTC.Policy=='CT')][select_columns]
new_names = {}
for i in select_columns[3:]:
    new_name = 'CT_' + i
    new_names[i]=new_name
CT_PTC_data.rename(columns={'Name':'CT_PTC'}, inplace=True)
CT_PTC_data.rename(columns=new_names, inplace=True)

# PTC Multi Tumor = Yes; No
# Select 'Yes' when payor covers multi tumor; 'No' is selected with payor does not cover multi tumor
# is blank when we do not know whether payor covers multi tumor
# Insert to PTC Multiple Primaries = No
### still issue here
#a = (MP_PTC_data.MP_GHI_MultiTumor__c.isna())
#PTC_data.loc[~a,'GHI_MultiTumor__c'].apply(lambda x : x.append('No'))
#PTC_data.loc[~a,'GHI_MultiTumor__c']


########################################################
# Read and prepare the Plans' PTV information
########################################################

select_columns = ['Name','Test','Tier4PayorID','OSM_PA_Required__c', 'SOMN']
PTV_data = PTV[~(PTV.Tier4PayorID.isnull())][select_columns]
PTV_data.rename(columns = {'Name':'PTV'}, inplace=True)

########################################################
# Connect the OLI record with the Plan PTC record 
########################################################

Data = pd.merge(OLI_data, MP_PTC_data, how='left', on = ['Tier4PayorID', 'Test'])
Data['MP_PTC_Available'] = 'Yes'
Data.loc[Data.MP_PTC.isnull(),'MP_PTC_Available'] = 'No'

Data = pd.merge(Data, CT_PTC_data, how='left', on = ['Tier4PayorID', 'Test'])
Data['CT_PTC_Available'] = 'Yes'
Data.loc[Data.CT_PTC.isnull(),'CT_PTC_Available'] = 'No'

Data = pd.merge(Data, PTV_data, how='left', on = ['Tier4PayorID', 'Test'])
Data['PTV_Available'] = 'Yes'
Data.loc[Data.PTV.isnull(),'PTV_Available'] = 'No'

####################################################################
# Calculate IN/OUT criteria            
# Algorithm 1:
# If Plan does not have a PTC, set to 'Out'
# If PTC Clinical Value is null, then bypass the comparison
#    Else if PTC Clinical value is not null
#          If OLI has the value, then compare
#          Else if OLI does not have the value, OUT
####################################################################
IBC_compare = { 'NodalStatus' : 'MP_GHI_NodeStatus__c',
                'HCPProvidedClinicalStage' : 'MP_GHI_Stage__c',
                'SubmittedER' : 'MP_GHI_ERStatus__c',
                'SubmittedPR' : 'MP_GHI_PRStatus__c',
                'SubmittedHER2' : 'MP_GHI_HER2Status__c',
                'HormoneReceptorHR' : 'MP_GHI_HormoneReceptorHR__c',
                'PatientAgeAtDiagnosis' : 'MP_GHI_AgeAtDiagnosisRange__c',
                'ProcedureType' :  'MP_GHI_ProcedureType__c',
                'MultiplePrimaries' : 'MP_GHI_MultiTumor__c'
                }

for i in list(IBC_compare.keys()):
    Data[i] = Data[i].fillna("Unknown")
    Data[IBC_compare[i]] = Data[IBC_compare[i]].fillna("")
    Data[IBC_compare[i]] = Data[IBC_compare[i]].apply(lambda x : x.split(sep=";") if x else '')

def In_or_Out_1 (record):
    InCriteria_1_temp = [] 
    for i in list(IBC_compare.keys())[:-1]:
        #print ('OLI: ', record[i], ' vs ', record[IBC_compare[i]])
        comparing = IBC_compare[i][:-3] + '_coverage'
        if (record[i] != '') and (record[i] != 'Unknown'):  # Patient clinical criteria is captured in OLI, then compare
            if (type(record[IBC_compare[i]]) == list):   # PTC clinical criteria is entered
                InCriteria_1_temp.append((record[i] in record[IBC_compare[i]]))
                record[comparing] = 'Yes' if (record[i] in record[IBC_compare[i]]) else 'No'
            else:
                record[comparing] = '.PTC unknown'
                # by pass when PTC clinical criteria is not entered
        else:
            InCriteria_1_temp.append(0)          # Patient clinical criteria is not captured in OLI, set to 'Out' as no information to compare
            record[comparing] = '.Patient unknown'

    # evaluate multi tumor coverage, only evaluate for multi tumor OLI
    comparing = 'MP_GHI_MultiTumor_coverage'
    if record['MultiplePrimaries'] == 'Yes':
        if (type(record[IBC_compare[i]]) == list):
            record[comparing] = 'Yes' if (record['MultiplePrimaries'] in record['MP_GHI_MultiTumor__c']) else 'No'
            InCriteria_1_temp.append((record['MultiplePrimaries'] in record['MP_GHI_MultiTumor__c']))
        else:
            record[comparing] = '.PTC unknown'
    
    # len(InCriteria_1_temp) is 0 when the Plan has no PTC
    # 0 in InCriteria_1_temp)) when at least 1 of the criteria is out
    if ((len(InCriteria_1_temp) == 0) or (0 in InCriteria_1_temp)):
        record['MP_InCriteria_1'] = 'Out'
    else:
        record['MP_InCriteria_1'] = 'In'
 
    return(record)

Data = Data.apply(lambda x: In_or_Out_1(x), axis=1)


####################################################################
# Write the output
####################################################################
output_file = 'In_or_Out.txt'
Data.to_csv(cfg.output_file_path+output_file, sep='|',index=False)

print("Payment Assessment Data Prep Done Done")
