'''
Created on Dec 6, 2017

@author: aliu
'''

import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime
from openpyxl.styles.builtins import output
folder="C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Clean Claim Misc\\"

server = 'EDWStage'
database = 'StagingDB'
target = server + '_' + database + '_' + 'test.rpt'


tsql = """
        Select ORD.*
            , C.Id Case_SysId
            , C.CaseNumber
            , E.Name Case_Owner
            , C.OSM_Case_Record_Type__c
            , C.Type
            , C.Status
            , C.CreatedDate Case_CreatedDate
            , C.ClosedDate Case_CloseDate
            , C.OSM_Case_Age__c
            , C.Subject
            , C.Description
            , C.Status Case_Status
            , C.OSM_Contact_Specialty__c
            , C.OSM_BI_Sent_Date__c
            , C.OSM_BI_Complete__c
            , C.OSM_BI_Status__c
            , C.OSM_PAS_Notes__c
            , C.OSM_PA_Status__c
            , C.OSM_PA_Number__c
            , C.OSM_Additional_Paperwork__c
            , C.OSM_SOMN_Status__c
            , C.OSM_ABN_Required__c
            , C.OSM_ABN_Status__c
            , C.OSM_Self_Pay_Status__c
        from (
                Select B.Id Order_SysId
                     , A.Id OLI_SysId
                     , B.OrderNumber
                     , B.EffectiveDate Order_Start_Date
        --             , B.OSM_Ordering_HCO__c
                     , A.OSM_Order_Line_Item_ID__c
                     , A.OSM_OLI_Start_Date__c
                     , A.OSM_Test_Name__c Test
                     , A.OSM_Order_Status__c
                     , A.OSM_Order_Line_Item_Status__c
                     , A.OSM_KPI_Test_Delivered_Date__c
                     , A.OSM_Test_Delivered_Date__c
                     , D.OSM_Account_Number__c
                     , D.Name Ordering_HCO_Name
                     , D.OSM_Standing_Order_Account__c
                     , D.IsCustomerPortal
                     , D.OSM_Outreach_Rules__c
                from StagingDB.ODS.StgOrderItem A
                     , StagingDB.ODS.stgOrder B
                     left join StagingDB.ODS.StgAccount D on B.OSM_Ordering_HCO__c = D.Id
                where
                    B.EffectiveDate >= '2017-01-07' and B.EffectiveDate <= '2017-04-10'
                    and    A.OrderId = B.Id
            ) ORD
        left join StagingDB.ODS.stgCase C on ORD.Order_SysId = C.OSM_Primary_Order__c
        left join StagingDB.ODS.stgGroup E on C.OwnerId = E.Id
        where C.Type in ('Pre-Billing', 'Missing Data')
            or C.Type is null
        order by ORD.OSM_Order_Line_Item_ID__c
        """
### Keep research Case - Task - Activities History
cnxn = pyodbc.connect('Trusted_Connection=yes',DRIVER='{ODBC Driver 13 for SQL Server}', SERVER=server, DATABASE=database)    
output = pd.read_sql(tsql, cnxn)


output_file = 'PreBilling_MissingData_CasesReesarch.xlsx'
writer = pd.ExcelWriter(folder+output_file, engine='openpyxl', date_format='yyyy/mm/dd')
output.to_excel(writer, sheet_name='Sheet1', index = False)
writer.save()
writer.close()

print("Done, Hurray!")


#select distinct Subject
#from StagingDB.ODS.stgCase
#where Type = 'Missing Data'
#nd Subject like ('%SOMN%')
