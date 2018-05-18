'''
Created on Dec 4, 2017

@author: aliu
'''
import pandas as pd
#import numpy as np
from datetime import datetime

QDX_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\QDX USD-Jul12\\"
GHI_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\QDX USD-Jul12\\"
GHI_outfile_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Nov21\\"


stmnt_file = "stmntInfo.txt"
txStmnt_file = "txStmntInfo.txt"

stmnt_data = pd.read_csv(QDX_file_path+stmnt_file, sep="|")
stmnt_data.stmntNum = stmnt_data.stmntNum.astype('str')
convert = ['stmntDt','stmntApproveDt']
for a in convert:
    stmnt_data[a] = pd.to_datetime(stmnt_data[a],format='%Y%m%d', errors='coerce')
    
stmnt_data.rename(columns = {'stmntStatus':'stmntStatusCode', 'stmntType':'stmntTypeCode'}, inplace=True)

stmntStatus_dict = {'V':'Reviewed', 'L':'Reviewed(sent to printer)',
                    'P':'Pulled', 'A':'Automatically Approved',
                    'Other': 'Pending'}
for a in list(stmntStatus_dict.keys()):
    stmnt_data.loc[stmnt_data.stmntStatusCode==a,'stmntStatus'] = stmntStatus_dict.get(a)

stmntType_dict = {'S':'Statement','DS':'Demand Statement',
                  'L1': 'First Collection Letter', 'L2':'Second Collection Letter'}
                # S + Roster/Client Statement not translated yet
for a in list(stmntType_dict.keys()):
    stmnt_data.loc[stmnt_data.stmntTypeCode==a,'stmntType'] = stmntType_dict.get(a)


txStmnt_data=pd.read_csv(QDX_file_path+txStmnt_file, sep="|")
txStmnt_data.txStmntNum = txStmnt_data.txStmntNum.astype('str')

#stmnt_data[stmnt_data.stmntAcctNum==" PT000921816"]
#txStmnt_data[txStmnt_data.txStmntAcctNum==" PT000921816"]

#stmnt_data.stmntType.unique()
# stmntType “S” – Statement, “DS” – Demand Statement, 
#            L1 – First Collection Letter, L2 –   Second Collection Letter, 
#           “S” + Roster/Client Statement Type (M, S, A, H)
# stmntStatus - V – Reviewed, L – Reviewed (sent to system printer), P – Pulled, A – Automatically approved or before approval system, Other – Pending
# Question to QDX: what status means we delivered the statement to Patient

# txStmntInfo
# txStmntDue P – Ticket Due From Patient, I – Ticket NOT Due From Patient
# txStmntAmt - ticket balance


# 1. extract the Patient Payment Due Statment, txStmntDue : P – Ticket Due From Patient, I – Ticket NOT Due From Patient
Pt_stmnt = txStmnt_data[(txStmnt_data.txStmntDue=='P') & (txStmnt_data.txStmntType=='D')][['txStmntAcctNum', 'txStmntTickNum','txStmntNum','txStmntDue', 'txStmntType', 'txStmntAmt']]
'''
#temp = Pt_stmnt.pivot_table(index= ['txStmntAcctNum','txStmntTickNum','txStmntNum'], values='txStmntDue',aggfunc='count')

temp = stmnt_data.pivot_table(index = ['stmntAcctNum','stmntNum'], columns='stmntType', values='stmntDt', aggfunc='count')
temp[temp.L1>=1]
a = list(temp[temp.L2>=1].index.get_level_values('stmntAcctNum'))
temp[temp.DS>=1]
'''

# 2. find the type, date of the statement
Pt_stmnt = pd.merge(Pt_stmnt, stmnt_data[['stmntAcctNum', 'stmntTypeCode', 'stmntType','stmntDt', 'stmntNum','stmntStatusCode','stmntStatus','stmntApproveDt','stmntAddrType','stmntDelivery']],
                    how='left', left_on=['txStmntAcctNum','txStmntNum'], right_on=['stmntAcctNum','stmntNum']).sort_values(['stmntAcctNum','stmntDt'])

Pt_stmnt[Pt_stmnt.txStmntAcctNum==' PT000878499'] # w/ L1 letter
Pt_stmnt[Pt_stmnt.txStmntAcctNum==' PT000963905']
Pt_stmnt[Pt_stmnt.txStmntAcctNum==' PT000709149'] # w/ L2 letter
Pt_stmnt[Pt_stmnt.txStmntAcctNum==' PT000295496']
Pt_stmnt[Pt_stmnt.txStmntAcctNum==' PT000451180']
Pt_stmnt[Pt_stmnt.txStmntAcctNum==' PT001023896'] # w/ Demand Statement
Pt_stmnt[Pt_stmnt.txStmntAcctNum==' PT001008784']
Pt_stmnt[Pt_stmnt.txStmntAcctNum==' PT001014122'] # some of the DS letter are I: Ticket not due from Patient
#stmnt_data[stmnt_data.stmntAcctNum==' PT001014122']
#txStmnt_data[txStmnt_data.txStmntAcctNum==' PT001014122']

Pt_stmnt[Pt_stmnt.txStmntAcctNum==' PT001011740']
#txStmnt_data[txStmnt_data.txStmntAcctNum==' PT001011740']

a = [' PT000878499',' PT000963905',' PT000709149',' PT000295496',' PT000451180',' PT001023896',' PT001008784',' PT001014122',' PT001011740']
Pt_stmnt_research = Pt_stmnt[Pt_stmnt.txStmntAcctNum.isin(a)]

output_file = 'Patient_Payment_Research.xlsx'
writer = pd.ExcelWriter(GHI_outfile_path+output_file, engine='openpyxl')
Pt_stmnt_research.to_excel(writer, sheet_name='Sheet1', index = False)
writer.save()
writer.close()

# 3. select the claim bill and payment from these tickets
