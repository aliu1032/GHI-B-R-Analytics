import pandas as pd
from datetime import datetime

from data import GetGHIData as GData
import project_io_config as cfg


pd.options.display.max_columns=999
print ('PayorTestValidation :: start :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

PTV = GData.getPTV('', cfg.input_file_path, cfg.refresh)

PA_enum = pd.read_excel(cfg.prep_file_path+'Enum.xlsx', sheet_name = "PA_ENUM", encoding='utf-8-sig', usecols="A:C,E:F")

PTV['OSM_Prior_Authorization__c'].fillna('', inplace=True)
PTV['PA_list'] = PTV['OSM_Prior_Authorization__c'].apply(lambda x: x.split(';') if x else '')

for i in PA_enum[PA_enum.Field=='PriorAuthorization']['Enum'].unique():
    PTV[i] = PTV['PA_list'].apply(lambda x : '1' if i in x else ('' if x=='' else '0'))

Long_PTV = pd.DataFrame([])
build = ['IBC','Prostate', 'DCIS','Colon']
for i in build:
    print ('PayorTestValidation :: start :: ', i ,'   ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    select_columns = ['Name', 'LastModifiedDate',
                       'Tier2PayorID', 'Tier2Payor', 'Line_of_Business',
                       'Tier4PayorID', 'Tier4Payor', 'QDX_InsPlan_Code', 'Line_of_Benefits',
                       'Test', 'OSM_Effective_Start_Date__c', 'OSM_Effective_End_Date__c', 'Billing_Modifier',
                       'OSM_Prior_Authorization__c', 'GHI_PayorSOMNTemplate__c',
                       'OSM_PA_Required__c', 'GHI_PhysicianAlertRequired__c',
                       'No CVP (no PA)', 'Pre-DOS', 'Post-DOS','Must obtain PA before releasing report',
                       'Ordering Physician initiates PA', 'GHI initiates PA',
                       'Payor specific PA form','Payor specific SOMN Form','SOMN','Signature required on SOMN',
                       'Online or Telephonic',
                       'Supporting documents are needed',
                       'Ordering physician signed attestation statement to include Adjuvant Therapy']

    output_file = 'Wide_' + i + '_PTV.txt'
    PTV[PTV.Test == i][select_columns].to_csv(cfg.input_file_path+output_file, sep='|',index=False)
    
    PTV_header = ['Name', 'LastModifiedDate','Tier2PayorID', 'Tier2Payor', 'Line_of_Business',
                  'Tier4PayorID', 'Tier4Payor', 'QDX_InsPlan_Code', 'Line_of_Benefits',
                  'Test', 'OSM_Effective_Start_Date__c', 'OSM_Effective_End_Date__c', 'Billing_Modifier']
    
    melt_PA = ['No CVP (no PA)', 'Pre-DOS', 'Post-DOS','Must obtain PA before releasing report',
               'Ordering Physician initiates PA', 'GHI initiates PA',
               'Payor specific PA form','Payor specific SOMN Form','SOMN','Signature required on SOMN',
               'Online or Telephonic',
               'Supporting documents are needed',
               'Ordering physician signed attestation statement to include Adjuvant Therapy']
    
    Test_Long_PTV = pd.melt(PTV, id_vars=PTV_header, value_vars=melt_PA, var_name='PA Requirement', value_name='Requirement')
    Long_PTV = Long_PTV.append(Test_Long_PTV, ignore_index = True)
    
output_file = 'Long_PTV.txt'
Long_PTV.to_csv(cfg.output_file_path+output_file, sep='|',index=False)

print ('PayorTestValidation :: done :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
