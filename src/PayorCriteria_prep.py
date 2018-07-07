'''
Created on Jun 21, 2018

@author: aliu
'''
import pandas as pd
from datetime import datetime

from data import GetGHIData as GData
import project_io_config as cfg

pd.options.display.max_columns=999
print ('PayorCriteria :: start :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

PTC = GData.getPTC('', cfg.input_file_path, cfg.refresh)

criteria_enum = pd.read_excel(cfg.prep_file_path+'Enum.xlsx', sheet_name = "Criteria_ENUM", encoding='utf-8-sig', usecols="A:C,E:F")

# Create PTC dump for IBC and Prostate
Long_PTC = pd.DataFrame([])
build = ['IBC','Prostate','DCIS','Colon']
for i in build:
    
    print ('PayorCrieria :: start :: ', i ,'   ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    PTC_Header = ['Name','Policy','Test','Tier2PayorName','Tier2PayorId','Tier4PayorName','Tier4PayorId','QDX_InsPlan_Code','Financial_Category','Line_of_Business']
    test_criteria = criteria_enum[criteria_enum.Test==i].SFDC_API_Name.unique()  # test_criteria is a array
    #criteria_condition = criteria_enum[criteria_enum.Test==i].Criteria_Enum.unique()
    Wide_PTC = PTC[PTC.Test == i][PTC_Header + list(test_criteria)]
    
    for j in test_criteria:
        Wide_PTC[j] = Wide_PTC[j].fillna('')
        Wide_PTC[j+'_list'] = Wide_PTC[j].apply(lambda x : x.split(';') if x else '')

        for k in criteria_enum[(criteria_enum.Test==i) & (criteria_enum.SFDC_API_Name==j)]['Criteria_Enum'].unique():
        #create label:
            new_label = j + '=' + k        
            Wide_PTC[new_label] = Wide_PTC[j+'_list'].apply(lambda x : '1' if k in x else ('' if x == '' else '0')) 

    drop_columns = list(Wide_PTC.columns[Wide_PTC.columns.str.contains('_list')])
    select_columns = [ i for i in Wide_PTC.columns if i not in drop_columns]
    output_file = 'Wide_' + i +'_PTC.txt'
    Wide_PTC[select_columns].to_csv(cfg.output_file_path+output_file, sep='|',index=False)
    
    print ('PayorCrieria :: done writing wide :: ', i ,'   ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    # Create a PTC dump for Tableau visualization
    melt_label = []
    for j in test_criteria:
        for k in criteria_enum[(criteria_enum.Test==i) & (criteria_enum.SFDC_API_Name==j)]['Criteria_Enum'].unique():
            melt_label.append(j +'='+ k)

    Test_Long_PTC = pd.melt(Wide_PTC, id_vars = PTC_Header, value_vars=melt_label, var_name='Criteria_Condition', value_name='Coverage')
    Test_Long_PTC['Criteria_SFDC_API'], Test_Long_PTC['Condition'] = Test_Long_PTC['Criteria_Condition'].str.split('=',1).str

    criteria_label = dict(zip(criteria_enum[criteria_enum.Test==i].SFDC_API_Name, criteria_enum[criteria_enum.Test==i].SFDC_Field_Label))
    Test_Long_PTC['Criteria'] = Test_Long_PTC['Criteria_SFDC_API'].apply(lambda x : criteria_label[x])
    
    Long_PTC = Long_PTC.append(Test_Long_PTC)

drop_columns = ['Criteria_Condition', 'Criteria_SFDC_API']
select_columns = [ i for i in Long_PTC.columns if i not in drop_columns]
output_file = 'Long_PTC.txt'
Long_PTC[select_columns].to_csv(cfg.output_file_path+output_file, sep='|',index=False)
    
print ('PayorCrieria :: done writing long PTC :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))



print ('PayorCrieria :: done :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
