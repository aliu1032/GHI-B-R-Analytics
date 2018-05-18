'''
Created on Sep 7, 2017

@author: aliu

Purpose of this script is to create an Appeal data set with the OLI data
This is to work with data.Appeal_data_prep that return a dictionary of 2 dataframes: appeal, ClimTicket_appeal_wide

ClaimTicket_appeal_wide flatten the appeal level per Ticket
'''

import pandas as pd
from datetime import datetime

from data import GetGHIData as GData
#from data import GetQDXData as QData
file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Dec15\\"

print("Appeal Rpt :: start :: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
#################################################
#        Read the QDX Appeal data files         #
#################################################

print("Appeal Rpt :: get appeal data :: start :: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

from data import Appeal_data_prep
appeal_data = Appeal_data_prep.make_appeal_data(file_path, refresh=0)
appeal_journal = appeal_data['appeal']
ClaimTicket_appeal_wide = appeal_data['ClaimTicket_appeal_wide']

#################################################
#  Reading Order Line Item Data                 #
#################################################
print("Appeal Rpt :: get OLI data :: start :: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

Revenue_data = GData.revenue_data('ClaimTicket', file_path, 0)
OLI_data = GData.OLI_detail('ClaimTicket',file_path, 0)

#########################################################################################
#  Generate appeal files                                                                #
#  - Left Join Appeal Status + OLI, for appeal productivity                             #
#  - Left Join OLI + Appeal History + complete appeal result                            #
#    for claim appeal analytics                                                         #
#                                                                                       #
#########################################################################################

# append payor information to the appeal status.
# first look for the Payor on Ticket from Revenue file
# if not available, patch it with the Payor of the current ticket from the OLI data
temp = Revenue_data.groupby(['OLIID','TicketNumber']).agg({'AccountingPeriodDate':'idxmax'})
pull_rows = temp.AccountingPeriodDate

Ticket_Payor = Revenue_data.loc[pull_rows][['OLIID','TicketNumber',
#                                           'Currency',
#                                           'TestDeliveredDate','Test',
                                           'Tier1Payor', 'Tier1PayorID', 'Tier1PayorName',
                                           'Tier2Payor', 'Tier2PayorID','Tier2PayorName',
                                           'Tier4Payor', 'Tier4PayorID', 'Tier4PayorName',
                                           'QDXInsPlanCode', 'QDXInsFC','LineOfBenefit', 'FinancialCategory',
                                           'ClaimPayorBusinessUnit', 'ClaimPayorInternationalArea', 'ClaimPayorDivision', 'ClaimPayorCountry']]

CurrentTicket_payor = OLI_data[['OLIID','CurrentTicketNumber',
                                'Tier1Payor','Tier1PayorID','Tier1PayorName',
                                'Tier2Payor','Tier2PayorID','Tier2PayorName',
                                'Tier4Payor','Tier4PayorID','Tier4PayorName',
                                'QDXInsPlanCode', 'QDXInsFC',
                                'LineOfBenefit', 'FinancialCategory'                             
                    ]]

CurrentTicket_status = OLI_data[['OLIID',
                                'Test','TestDeliveredDate',
                                'BillingCaseStatusSummary2','BillingCaseStatusCode','BillingCaseStatus'
                    ]]

OLI_ClaimTicketHeader = pd.merge(Ticket_Payor, CurrentTicket_payor, how='outer', left_on=['OLIID','TicketNumber'], right_on=['OLIID','CurrentTicketNumber'])

# X is from Revenue file, Y is from OLI file
# if the payor information is not available from the Revenue file, get the ticket payor from the OLI file

a = OLI_ClaimTicketHeader.TicketNumber.isnull()
temp = ['Tier1Payor','Tier1PayorID','Tier1PayorName',
                                'Tier2Payor','Tier2PayorID','Tier2PayorName',
                                'Tier4Payor','Tier4PayorID','Tier4PayorName',
                                'QDXInsPlanCode','QDXInsFC','LineOfBenefit', 'FinancialCategory']
OLI_ClaimTicketHeader.loc[a,'TicketNumber'] = OLI_ClaimTicketHeader.loc[a,'CurrentTicketNumber'] 
for u in temp:
    target = u + '_x'
    source = u + '_y'
    OLI_ClaimTicketHeader.loc[a,target] = OLI_ClaimTicketHeader.loc[a,source]
    OLI_ClaimTicketHeader.rename(columns = {target : u}, inplace=True)
    OLI_ClaimTicketHeader.drop(source,axis=1, inplace=True)
    
OLI_ClaimTicketHeader = pd.merge(OLI_ClaimTicketHeader, CurrentTicket_status, how='left', on=['OLIID'])



QDX_complete_appeal_status = ['Completed','Final Review','Due from Patient']


appeal = pd.merge(appeal_journal, OLI_ClaimTicketHeader, how='left', left_on=['appealAccession','appealTickNum'], right_on=['OLIID','TicketNumber'])

## still need to look into why appeal cannot map to the OLIs
# OLI_data have OLI with order start date since 2016. if caseAccession is not mapped to an OLI, they are not in the analysis scope
# not all Order Line Detail are extracted into the table, thus some of the appeal status does not find the OLI
# a. because the Order Line Detail does not order start date < 2015-12-31
# b. the OLI has multiple tickets/cases, the current ticket appealed
# if the OLI is appealed with an old ticket and current ticket has not been appeal, is it still a denial claim?
# see Multi-cases appeal.sql
# check the QDXInsPlanCode is from the current ticket or the appeal case
appeal = appeal[~(appeal.OLIID.isnull())]
## there are appeal oli without test delivered date, need to look into them

# add a flag to indicate the current ticket is the appealed ticket
appeal.loc[(appeal.CurrentTicketNumber == appeal.appealTickNum),'CurrentIsAppealTick'] = str(1)
a = (~(appeal.CurrentTicketNumber.isnull()) & (appeal.CurrentTicketNumber != appeal.appealTickNum))
appeal.loc[a,'CurrentIsAppealTick'] = str(0)

appeal.loc[(appeal.appealSuccess == '1'), 'appealResult'] = 'Success'
appeal.loc[(appeal.appealSuccess == '0'), 'appealResult'] = 'Failed'
appeal.loc[(appeal.BillingCaseStatusSummary2.isin(QDX_complete_appeal_status)) &\
           (appeal.appealSuccess.isnull()), 'appealResult'] = 'Removed'
appeal.loc[~(appeal.BillingCaseStatusSummary2.isnull()) &\
           ~(appeal.BillingCaseStatusSummary2.isin(QDX_complete_appeal_status)) &\
           (appeal.appealSuccess.isnull()), 'appealResult'] = 'In process'

# also need to update billing status if appealresult is success & failed

## Add to OLI_data, append the ClaimTicket_appeal_wide. The ClaimTicket_appeal_wide have complete appeal information merged 
## OLI_data include test not delivered or not claimed, therefore, not all rows have Current Ticket
## not all OLI_data has appeal, thus not all rows have caseAccession
## and an OLI may have multiple appeal tickets ----------------------------------------------------------------------------------------------------
OLI_appealdetail_left = pd.merge(OLI_ClaimTicketHeader, ClaimTicket_appeal_wide, how='left', left_on=['OLIID','TicketNumber'], right_on=['appealAccession','appealTickNum'])
## verify if the success/fail ticket is the current ticket
OLI_appealdetail = pd.merge(OLI_ClaimTicketHeader, ClaimTicket_appeal_wide, how='right', left_on=['OLIID','TicketNumber'], right_on=['appealAccession','appealTickNum'])

# compare the ticketnumber with the OLI current ticket number
# sometimes, the OLI was appealed with a previous ticket and not the current
a = OLI_appealdetail[~(OLI_appealdetail.CurrentTicketNumber.isnull()) & \
                       ~(OLI_appealdetail.appealTickNum.isnull()) &
                       (OLI_appealdetail.CurrentTicketNumber != OLI_appealdetail.appealTickNum)].index
OLI_appealdetail.loc[a,'CurrentIsAppealTick'] = str(0)
a = OLI_appealdetail[~(OLI_appealdetail.CurrentTicketNumber.isnull()) & \
                       ~(OLI_appealdetail.appealTickNum.isnull()) &
                       (OLI_appealdetail.CurrentTicketNumber == OLI_appealdetail.appealTickNum)].index
OLI_appealdetail.loc[a,'CurrentIsAppealTick'] = str(1)

OLI_appealdetail.loc[(OLI_appealdetail['appealSuccess']== '1'), 'appealResult'] = 'Success'
OLI_appealdetail.loc[(OLI_appealdetail['appealSuccess']== '0'), 'appealResult'] = 'Failed'
OLI_appealdetail.loc[(OLI_appealdetail.BillingCaseStatusSummary2.isin(QDX_complete_appeal_status)) &\
                        ~(OLI_appealdetail.appealCaseNum.isnull()) &\
                        (OLI_appealdetail.appealSuccess.isnull()),
                        'appealResult'] = 'Removed'
OLI_appealdetail.loc[~(OLI_appealdetail.BillingCaseStatusSummary2.isin(QDX_complete_appeal_status)) &\
                        ~(OLI_appealdetail.appealCaseNum.isnull()) &\
                        (OLI_appealdetail.appealSuccess.isnull()),
                        'appealResult'] = 'In Process'
# also need to update billing status if appealresult is success & failed
# Rearranging columns
OLI_appealdetail = OLI_appealdetail[[
                        'OLIID', 'TicketNumber','CurrentTicketNumber', 'Test', 'TestDeliveredDate',
                        'Tier1Payor','Tier1PayorID', 'Tier1PayorName','Tier2Payor','Tier2PayorID', 
                        'Tier2PayorName', 'Tier4Payor', 'Tier4PayorID',  'Tier4PayorName',
                        'FinancialCategory', 'QDXInsPlanCode', 'QDXInsFC',
                        'BillingCaseStatusSummary2','BillingCaseStatusCode','BillingCaseStatus',
#                        'OLIappealLvlCnt',
                        'appealCaseNum', 'appealTickNum','CaseappealLvlCnt', 
#                            'caseEntryYrMth',
                        'A1', 'A2', 'A3', 'A4', 'A5', 'ER', 'L1', 'L2', 'L3',
#                            'A1_EntryDt','A2_EntryDt', 'A3_EntryDt', 'A4_EntryDt', 'A5_EntryDt', 'ER_EntryDt',
#                            'L1_EntryDt', 'L2_EntryDt', 'L3_EntryDt',
#                            'A1_DenLtDt', 'A2_DenLtDt', 'A3_DenLtDt', 'A4_DenLtDt', 'A5_DenLtDt', 'ER_DenLtDt',
#                            'L1_DenLtDt','L2_DenLtDt', 'L3_DenLtDt',
#                            'A1_InsCode', 'A2_InsCode', 'A3_InsCode', 'A4_InsCode', 'A5_InsCode', 'ER_InsCode',
#                            'L1_InsCode', 'L2_InsCode', 'L3_InsCode',
#                            'A1_DenReason', 'A2_DenReason', 'A3_DenReason', 'A4_DenReason', 'A5_DenReason', 'ER_DenReason',
#                            'L1_DenReason','L2_DenReason', 'L3_DenReason'
#                        'latestappealDenReason','latestDenialInsCode', 'latestappealDenialLetterDt',
                        'CurrentIsAppealTick','appealSuccess','appealResult',
                        'appealReqNum','appealDOS', 'appealInsCode', 'appealInsFC', 'appealDenReason','appealDenReasonDesc',
                        'appealCurrency','appealAmtChg', 'appealAmtChgExp','appealAmtAllow', 'appealAmtClmRec', 'appealAmt', 'appealAmtAplRec',
                        'appealRptDt'
                        ]]

# make a set with only appeal OLI
##to-do, this one need the OLI HCP, criteria information too
Appeal_OLI = OLI_appealdetail[~(OLI_appealdetail.appealCaseNum.isnull())]


''' OLI with multiple tickets and mulitple apppeal cases
['OL000620641', 'OL000658840', 'OL000705736', 'OL000735309',
       'OL000772580', 'OL000794719', 'OL000854621', 'OL000988463',
       'OL000989517']
'''
####Nov22: check the billing status for the OLI appear in the SuccessAppeal.txt
##temp = OLI_appealdetail[OLI_appealdetail.Test=='IBC'].pivot_table(index='BillingCaseStatusSummary2',columns='appealResult', values='OLIID',aggfunc='count',margins=True)

#Nov 9: Add the Payor View Set assignment

prep_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Scripting\\"
prep_file_name = "Payor-ViewSetAssignment.xlsx"

Payor_view = pd.read_excel(prep_file_path+prep_file_name, sheetname = "Data Table", parse_cols="C,H,I", encoding='utf-8-sig')
Payor_view = Payor_view[~(Payor_view['Top Payors'].isnull() & Payor_view['BCBS'].isnull())].drop_duplicates()

Appeal_OLI = pd.merge(Appeal_OLI, Payor_view, how = 'left', left_on='Tier2PayorID', right_on='Tier2PayorID')
Appeal_OLI[['Top Payors', 'BCBS']] = Appeal_OLI[['Top Payors', 'BCBS']].fillna(0)
Appeal_OLI[['Top Payors', 'BCBS']] = Appeal_OLI[['Top Payors', 'BCBS']].astype('int')

appeal = pd.merge(appeal, Payor_view, how = 'left', left_on='Tier2PayorID', right_on='Tier2PayorID')
appeal[['Top Payors', 'BCBS']] = appeal[['Top Payors', 'BCBS']].fillna(0)
appeal[['Top Payors', 'BCBS']] = appeal[['Top Payors', 'BCBS']].astype('int')

###############################################
#     Write the columns into a excel file     #
###############################################

print("Appeal Rpt :: write Appeal OLI ", len(Appeal_OLI), " rows\n:: start :: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#output_file = 'OLI_appeal_history.xlsx'
output_file = 'Appeal_OLI.xlsx'
writer = pd.ExcelWriter(file_path+output_file, engine = 'openpyxl')
Appeal_OLI.to_excel(writer, sheet_name='Appeal_OLI', index=False, encoding='utf-8')
writer.save()
writer.close()

print("Appeal Rpt :: write Appeal cases ", len(appeal), " rows\n:: start :: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

output_file = 'Appeal_cases.xlsx'
writer = pd.ExcelWriter(file_path+output_file, engine = 'openpyxl')
appeal.to_excel(writer, sheet_name='Appeal_cases', index=False, encoding='utf-8')
writer.save()
writer.close()

print("Appeal Rpt :: write OLI_appealdetail ", len(OLI_appealdetail), " rows\n:: start :: ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

# easier for user to have a full view of appeal for OLI
# need to rearrange the columns, remove the row with blank Ticket Number
output_file = 'OLI_appealdetail.xlsx'
writer = pd.ExcelWriter(file_path+output_file, engine = 'openpyxl')
OLI_appealdetail.to_excel(writer, sheet_name='OLI_Appeal_case_status', index=False, encoding='utf-8')
writer.save()
writer.close()

print("Appeal Rpt :: Done Done Done", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

