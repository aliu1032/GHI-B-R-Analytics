'''
Created on Jun 21, 2018

@author: aliu
'''
import pandas as pd
from data import GetGHIData as GData

import project_io_config as cfg

pd.options.display.max_columns=999

PTC = GData.getPTC('', cfg.input_file_path, cfg.refresh)

criteria_enum = pd.read_excel(cfg.prep_file_path+'Enum.xlsx', sheet_name = "Criteria_ENUM", encoding='utf-8-sig', usecols="A:C,E:F")
melt_criteria = criteria_enum.SFDC_API_Name.unique()

PTC_Header = ['Name','Policy','Test','Tier2PayorName','Tier2PayorId','Tier4PayorName','Tier4PayorId','QDX_InsPlan_Code','Financial_Category','Line_of_Business']

temp = pd.melt(PTC, id_vars=PTC_Header, value_vars=melt_criteria, var_name='Criteria', value_name='Condition_list')
temp = temp[temp.Condition_list.notnull()]

#replace the criteria api name with criteria label
for i in criteria_enum.SFDC_API_Name.unique():
    temp.loc[temp.Criteria == i,'Criteria'] = list(criteria_enum[criteria_enum.SFDC_API_Name == i]['SFDC_Field_Label'])[0]

temp['Condition_list'] = temp.Condition_list.apply(lambda x : x.split(';') if x else '')
#temp.loc[34166:34167]
#temp.loc[28478]

build = ['IBC', 'Prostate']
PTC_Header.append('Criteria')

for i in build:
    long_PTC = pd.DataFrame([])
    test_temp = temp[temp.Test==i].copy()
    test_criteria = criteria_enum[criteria_enum.Test==i].SFDC_Field_Label.unique()
    criteria_condition = criteria_enum[criteria_enum.Test==i].Criteria_Enum.unique()
    
    for j in criteria_condition:
        test_temp[j] = test_temp['Condition_list'].apply(lambda x : '1' if j in x else '0' )

    for k in test_criteria:
        melt_condition = criteria_enum[(criteria_enum.Test==i) & (criteria_enum.SFDC_Field_Label==k)].Criteria_Enum
        long_PTC = long_PTC.append(pd.melt(test_temp[test_temp.Criteria==k], id_vars=PTC_Header, value_vars = melt_condition, var_name='Condition', value_name='Coverage'), ignore_index=True)
    
    output_file = 'Long_' + i +'_PTC.txt'
    long_PTC.to_csv(cfg.output_file_path+output_file, sep='|',index=False)   





