'''
@author: aliu
Generate the Claim2Rev report.
Purpose of the report is to track for Test Delivered OLI, the billed amount, payment, adjustment, current outstanding and the revenue recognized for the OLI.
Using the QDX data for Payment and Adjustment, Current Ticket and Current Case.
Data sources: 
Revenue: GHI Revenue data: calculate the sum revenue per OLI
Order Line Detail: Get Test Delivered Date, HCP information from OrderLineDetail
Bills & Receipt: Get per OLI Claim and Payment summary using QDX stdClaim and stdPayment file
Modified Total Charges and Payment:
One OLI .. One:Many Tickets
Thus to calculate the OLI charges need to take off the charges error
OLI Payment need to take off the refund
       
Constraints:
1. NS has missing transaction from stdClaim + stdPayment file (see OLI_Payment_Revenue; NS_QDX Compare)
   Thus getting the bills and receipt data from stgBills, stgPayment, stgAdjust do not match QDX numbers
   And QDX file have the CIE and Refund code. (stdAdjustment has the GHI adjustment code too)
2. fctOrderLineDetail only has the current ticket charge, payment, adjustment and balance
3. fctRevenue has revenue for OLI + Ticket, there are revenue recognized on the non-current ticket
   therefore, need to group by OLI on the fctRevenue to get the total revenue recognized, and cannot only take the 'current ticket'
4. NetSuite export monthly data to EDW. If there are multiple adjustments for an OLI happened in the same month, NS sum the monthly number and assign 
   it with the adjustment code of the last TXN line.
5. IT provides access to Stage StagingDB which is a few days older than the Quadax data feed.
   Thus use Quadax data for Claim, Payment, Adjustment, Ticket, Case for a more update data.
   For OLI data, use EDWDB in StagingDB. Only taking OLI and patient clinical data from EDWDB 
   
'''

import pandas as pd
from datetime import datetime
import time

import project_io_config as cfg
refresh = cfg.refresh

print ('Claim2Rev_QDX_GHI :: start :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

###############################################################
#   Read QDX Claim and Payment (Receipt) data                 #
###############################################################

from data import GetQDXData as QData
Claim_bill = QData.stdClaim('Claim2Rev', cfg.input_file_path, refresh)
Claim_pymnt = QData.stdPayment('Claim2Rev', cfg.input_file_path, refresh)
Claim_case = QData.claim_case_status('QDXClaim_CaseStatus', cfg.input_file_path, refresh)
priorAuth = QData.priorAuth('Claim2Rev', cfg.input_file_path, refresh)

###############################################################
#   Read GHI Revenue Data and OrderLineDetail                 #
#   also get the current SFDC Payor Hierarchy                 #
###############################################################

from data import GetGHIData as GData
Revenue_data = GData.revenue_data('Claim2Rev', cfg.input_file_path, refresh)
OLI_data = GData.OLI_detail('Claim2Rev', cfg.input_file_path, refresh)
SFDC_Payors = GData.getPayors('Claim2Rev', cfg.input_file_path, refresh)
OLI_Result_Specimen = GData.getOLIResult_Specimen('', cfg.input_file_path, refresh)
OLI_SOMN_Status = GData.getSOMN_Status('', cfg.input_file_path, refresh)

###############################################################
#   Read QDX Appeal Data                                      #
###############################################################

from data import Appeal_data_prep
appeal_data = Appeal_data_prep.make_appeal_data(cfg.input_file_path, refresh)
appeal_journal = appeal_data['appeal']
ClaimTicket_appeal_wide = appeal_data['ClaimTicket_appeal_wide']

###############################################################################
#   Derive the Current Ticket Number and Current QDX Case Number for an OLI   #
#   Ideally, 1 OLI is 1 QDX Case Number, 1 OLI has 0..N Tickets               #
#   However, in the system                                                    #
#   OLIID : Ticket Number : Case Number relationship is N : N : N             #
#   In the QDX Case file,                                                     #
#   the max Ticket Number is not always mapped to the                         #
#   max Case Number, and vice versa                                           #
#                                                                             #
#   Examples:                                                                 #
#   OL001099336  938752          1152226
#   Claim_bill[Claim_bill.OLIID=='OL000605423'][['OLIID','TicketNumber','CaseNumber']]
#   OLIID TicketNumber CaseNumber
#   301659  OL000605423       627308     811520
#   301660  OL000605423       627308     938592
#   301964  OL000605423       627400     817204
#                                                                             #
#   Take max. Ticket Number over max. case number as current                  #
#   Step 1. Group by OLI, retrieve the max Ticket Number                      #
#   Step 2. Use the mapped case number as the Current Case Number             #
#           If there are multiple case number, then use the max Case Number   #
#           of this ticket number group                                       #
#   Step 3. If Ticket Number is not available, then use the max Case Number   #
#           as the Current Case Number                                        #
#                                                                             #
#   Get the QDX Billing case status from QDX file                             #
############################################################################### 
# rename OLI_data.CurrentQDXTicketNumber
OLI_data.rename(columns = {'CurrentTicketNumber':'BI_CurrentTicketNumber'}, inplace=True)

temp3 = Claim_bill.groupby(['OLIID','TicketNumber'])['CaseNumber']  # this return a SeriesGroupBy with CaseNumber
d = temp3.apply(lambda x : x.keys()[x.values.argmax()]) # find the latest Case Number for a Ticket

temp1 = Claim_bill.loc[d][['OLIID','TicketNumber','CaseNumber','TXNDate']].copy() 
temp2 = temp1.groupby(['OLIID']).agg({'TXNDate':'idxmax'}).TXNDate   # find the latest Ticket for an OLI

Current_ticket = Claim_bill.loc[temp2][['OLIID','TicketNumber', 'CaseNumber',
                                        'BillingCaseStatusSummary1', 'BillingCaseStatusSummary2',
                                        'BillingCaseStatusCode', 'BillingCaseStatus',
                                        'TicketInsPlan_QDXCode','TicketInsPlan_GHICode',
                                        'PrimaryInsPlan_QDXCode', 'PrimaryInsPlan_GHICode',
                                        'OLIDOS', 'TXNDate',
                                        'Days_toInitPymnt','Days_toLastPymnt']].copy()

Current_ticket.rename(columns = {'TicketNumber': 'CurrentQDXTicketNumber', 'OLIDOS':'QDX_DOS', 'TXNDate':'ClaimEntryDate'}, inplace=True)
TickCnt = (pd.pivot_table(Claim_bill, index=['OLIID'], values = 'TicketNumber',\
                          aggfunc = lambda TicketNumber: len(TicketNumber.unique()))).rename(columns = {'TicketNumber': 'QDXTickCnt'})
Current_ticket = pd.merge(Current_ticket, TickCnt, how='left', left_on='OLIID', right_index=True)

# Retrieve the max Case Number and information with it
Current_case_index = Claim_case.groupby(['caseAccession']).agg({'caseAddedDateTime':'idxmax'}).caseAddedDateTime
Current_case = Claim_case.loc[Current_case_index][['caseAccession','caseCaseNum']]
Current_case.rename(columns = {'caseAccession' : 'OLIID', 'caseCaseNum':'maxCaseNum'}, inplace=True)
CaseCnt = (pd.pivot_table(Claim_case, index=['caseAccession'], values = 'caseCaseNum',\
                          aggfunc = lambda caseCaseNum: len(caseCaseNum.unique()))).rename(columns = {'caseCaseNum': 'QDXCaseCnt'})
Current_case = pd.merge(Current_case, CaseCnt, how='left', left_on = 'OLIID', right_index=True)

# Derive the Current Ticket Number and Current Case Number for an OLI
Current_reference = pd.merge(Current_case, Current_ticket, how='left', on='OLIID')
Current_reference['CurrentQDXCaseNumber'] = Current_reference['CaseNumber']
a = Current_reference.CurrentQDXCaseNumber.isnull()
Current_reference.loc[a,'CurrentQDXCaseNumber'] = Current_reference.loc[a,'maxCaseNum']
Current_reference = Current_reference[['OLIID','CurrentQDXTicketNumber','QDXTickCnt','CurrentQDXCaseNumber','QDXCaseCnt',
                                       'BillingCaseStatusSummary1', 'BillingCaseStatusSummary2',
                                       'BillingCaseStatusCode', 'BillingCaseStatus',
                                       'TicketInsPlan_QDXCode','TicketInsPlan_GHICode',
                                       'PrimaryInsPlan_QDXCode', 'PrimaryInsPlan_GHICode',
                                       'QDX_DOS', 'ClaimEntryDate',
                                       'Days_toInitPymnt','Days_toLastPymnt']]

OLI_data = pd.merge(OLI_data, Current_reference, how='left', on='OLIID')

## IsClaim is whether a TicketNumber is present
OLI_data['IsClaim'] = 1

a = (OLI_data.CurrentQDXTicketNumber.isnull())
OLI_data.loc[a,'IsClaim'] = 0

## IsFullyAdjudicated is 1 for BillingCaseStatusSymmary2 = 'Completed'
## EDW source the flag from NetSuite, don't know where is the bug. QDX billing case status and EDW billing case status inconsistent.
## According to QDX, OLI has an outstanding amount and there claim is being appealed, EDW has completed status and IsFullyAdjudicated = 1 on these opened claims.
## Update the IsFullyAdjuducated flag according to the Current Ticket/Case BillingCaseStatusSummary2
OLI_data.loc[OLI_data.BillingCaseStatusSummary2 == 'Completed', 'IsFullyAdjudicated'] = 1
OLI_data.loc[(OLI_data.BillingCaseStatusSummary2 != 'Completed') & (~OLI_data.BillingCaseStatusSummary2.isna()), 'IsFullyAdjudicated'] = 0
OLI_data.loc[OLI_data.BillingCaseStatusSummary2.isna(), 'IsFullyAdjudicated'] = ''

# if DateOfService is null, update it using Quadax DOS collected with claim
a = (OLI_data.DateOfService.isnull())
OLI_data.loc[a, 'DateOfService'] = OLI_data.loc[a,'QDX_DOS']

## Managed Care needs the current Payer Hierarchy instead of the Payer Hierarchy as when a claim is processed.
## therefore not getting the Payer Hierarchy from EDW; join the current SFDC Payer Hierarchy on Tier4PayorID

# Quadax Insurance Plan 6775 is mapped into 2 Tier4PayorID: PL0006511, PL0001459
# PL0001459 is deleted in SFDC, thus cannot find the current Payer Hierarchy 
# reassign the OLI with Tier4PayorID = PL0001459 to the new ID PL0006511
OLI_data.loc[OLI_data.TicketInsPlan_GHICode=='PL0001459','TicketInsPlan_GHICode'] = 'PL0006511'
OLI_data.loc[OLI_data.PrimaryInsPlan_GHICode=='PL0001459','PrimaryInsPlan_GHICode'] = 'PL0006511'

select_columns = ['Tier1Payor', 'Tier1PayorName', 'Tier1PayorID',
                   'Tier2Payor', 'Tier2PayorName', 'Tier2PayorID',
                   'Tier4Payor', 'Tier4PayorName', 'Tier4PayorID',
                   'FinancialCategory','LineOfBenefit']
OLI_data = pd.merge(OLI_data, SFDC_Payors[select_columns], how='left', left_on='TicketInsPlan_GHICode', right_on='Tier4PayorID')
OLI_data = pd.merge(OLI_data, SFDC_Payors[select_columns], how='left', left_on='PrimaryInsPlan_GHICode', right_on='Tier4PayorID')

OLI_data.rename(columns = {'Tier1Payor_x':'Tier1Payor', 'Tier1PayorName_x':'Tier1PayorName', 'Tier1PayorID_x':'Tier1PayorID',
                           'Tier2Payor_x':'Tier2Payor', 'Tier2PayorName_x':'Tier2PayorName', 'Tier2PayorID_x':'Tier2PayorID',
                           'Tier4Payor_x':'Tier4Payor', 'Tier4PayorName_x':'Tier4PayorName', 'Tier4PayorID_x':'Tier4PayorID',
                           'FinancialCategory_x':'FinancialCategory',
                           'LineOfBenefit_x':'LineOfBenefit',
                           'TicketInsPlan_QDXCode':'QDXInsPlanCode',
                           'Tier1Payor_y':'PrimaryInsTier1Payor', 'Tier1PayorName_y':'PrimaryInsTier1PayorName', 'Tier1PayorID_y':'PrimaryInsTier1PayorID',
                           'Tier2Payor_y':'PrimaryInsTier2Payor', 'Tier2PayorName_y':'PrimaryInsTier2PayorName', 'Tier2PayorID_y':'PrimaryInsTier2PayorID',
                           'Tier4Payor_y':'PrimaryInsTier4Payor', 'Tier4PayorName_y':'PrimaryInsTier4PayorName', 'Tier4PayorID_y':'PrimaryInsTier4PayorID',
                           'FinancialCategory_y':'PrimaryInsFinancialCategory',
                           'LineOfBenefit_y':'PrimaryInsLineOfBenefit'}, inplace=True)

OLI_data['Rerouted_Ticket'] = (OLI_data['Tier4PayorID'] == OLI_data['PrimaryInsTier4PayorID']).map({False:'Yes',True:'No'})
OLI_data.loc[(OLI_data.Tier4PayorID.isnull() & OLI_data.PrimaryInsTier4PayorID.isnull()),'Rerouted_Ticket'] = 'No'  # workaround NaN is not equal to NaN
OLI_data['Reroute_ChangedFC'] = (OLI_data['FinancialCategory'] == OLI_data['PrimaryInsFinancialCategory']).map({False:'Yes',True:'No'})  
OLI_data.loc[(OLI_data.FinancialCategory.isnull() & OLI_data.PrimaryInsFinancialCategory.isnull()),'Reroute_ChangedFC'] = 'No'  # NaN is not equal to NaN

'''
## section to check the Current Case and CurrentTicket number mapping
after reviewing the Current case status, Current Ticket number and the Billing Case status::
conclude to take the billing case status from stdClaim file to get the status with the current ticket
temp_QDX_numbers = Claim_case[['caseAccession','caseCaseNum','caseTicketNum', 'caseEntryYrMth']]
temp_QDX_numbers = temp_QDX_numbers[ ~(temp_QDX_numbers.caseAccession.isnull()) & ~(temp_QDX_numbers.caseAccession == 'NONE')]
#(temp_QDX_numbers.caseEntryYrMth >= '2016-01-01') &
temp_QDX_numbers = pd.merge(temp_QDX_numbers, Current_case, how='left', left_on='caseAccession', right_on='OLIID')
temp_QDX_numbers = pd.merge(temp_QDX_numbers, Current_ticket, how='left', left_on='caseAccession', right_on='OLIID')
a = temp_QDX_numbers[~temp_QDX_numbers.CurrentQDXTicketNumber.isnull()]
b = (a.caseCaseNum == a.CurrentQDXCaseNum) & (a.caseTicketNum != a.CurrentQDXTicketNumber)
c = a[b]['caseAccession']
d = temp_QDX_numbers[temp_QDX_numbers.caseAccession.isin(c)][['caseAccession','caseCaseNum','caseTicketNum','CurrentQDXCaseNum','CurrentQDXTicketNumber'
                                                              , 'caseEntryYrMth','BillingCaseStatusSummary1','BillingCaseStatusSummary2','BillingCaseStatus']].sort_values(by='caseAccession')
output_file = 'Case_ticket_mismatch.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl', date_format='yyyy/mm/dd')
d.to_excel(writer, sheet_name='Case_ticket_mismatch', index = False)
writer.save()
writer.close()

a = (OLI_data.IsClaim==1) & (OLI_data.BusinessUnit=='Domestic') & (OLI_data.OrderStartDate >= '09-01-2017') & (OLI_data.OrderStartDate < '04-01-2018')
# & (OLI_data.Tier2Payor.isnull())
temp = OLI_data[a][['OLIID','OrderStartDate','TestDeliveredDate','IsClaim','TestDelivered','Tier1Payor','Tier2Payor','Tier4Payor','Tier4PayorID','QDXInsPlanCode',
                     'BI_CurrentTicketNumber','CurrentQDXTicketNumber','CurrentQDXCaseNumber','BillingCaseStatusSummary1','BillingCaseStatusSummary2',
                     'BillingCaseStatusCode','BillingCaseStatus','TicketInsPlan_QDXCode','TicketInsPlan_GHICode']]
output_file = 'Claim_missing_Payor_in_EDW.xlsx'
writer = pd.ExcelWriter(cfg.output_file_path+output_file)
temp.to_excel(writer, sheet_name='Claim_missing_Payor', index = False)
writer.save()
writer.close()
'''

############################################################### 
#  Filter the scenario that need checking and                 #
#  not working for the OLI grain                              #
###############################################################

print ('Claim2Rev_QDX_GHI :: Clean unused, dirty data')
# Ticket without OLI
Claim_pymnt_wo_OLI = Claim_pymnt[((Claim_pymnt.OLIID.isnull()) | (Claim_pymnt.OLIID == 'NONE'))]
Claim_pymnt = Claim_pymnt[(~(Claim_pymnt.OLIID.isnull()) & (Claim_pymnt.OLIID != 'NONE'))]

Claim_wo_OLI = Claim_bill[((Claim_bill.OLIID.isnull()) | (Claim_bill.OLIID == 'NONE'))]
Claim_bill = Claim_bill[((~Claim_bill.OLIID.isnull()) & (Claim_bill.OLIID != 'NONE'))].copy()

## Select and drop the OrderLineItem that has multiple Test values, ignore the one with Unknown. Export the value for reporting bug
## pivot with OLIID, count the number of unique Test values
a = pd.pivot_table(Revenue_data, index=['OLIID'], values=['Test'], aggfunc = lambda Test: len(set(Test.unique()) - set(['Unknown'])))
b = list(a[(a.Test > 1)].index) # find the OrderLineItemID with inconsistent test value


## Select and drop the OrderLineItem that has multiple Currency Code, ignore the null CurrencyCode. Export the value for reporting bug
a = pd.pivot_table(Revenue_data, index=['OLIID'], values=['Currency'], aggfunc = lambda Currency: len(Currency.unique())-sum(pd.isnull(Currency)))
b = b + list(a[(a.Currency> 1)].index) # find the OrderLineItemID with inconsistent currency value

error_Revenue_data = Revenue_data[(Revenue_data['OLIID'].isin(b))]

# drop the Top Side adjustment lines : IsRevenueReconciliationAdjustment == '0'
# process changed in 2017 and thus the flag is not applicable since 2017
Revenue_data = Revenue_data[(Revenue_data.IsRevenueReconciliationAdjustment == '0')]
#Revenue_data[(Revenue_data.IsRevenueReconciliationAdjustment == '1')]['AccountingPeriod'].unique()

########################################################################################
#   Aggregate Revenue numbers to the OLI level                                         #
#   Count the number of Ticket captured in Revenue Table per an OLI                    #
#   Group by OLI to get the Revenue collected from a OLI/Assay                         #
#   The total payment, adjustment received up to the data extraction date              #
########################################################################################
print ('Claim2Rev_QDX_GHI :: Aggregate Revenue numbers per OLI')

aggregations = {
    'TotalRevenue':'sum',
    'TotalAccrualRevenue':'sum',
    'TotalCashRevenue':'sum',
    'TotalUSDRevenue':'sum',
    'TotalUSDAccrualRevenue' : 'sum',
    'TotalUSDCashRevenue' :'sum',
    'AccountingPeriodDate' : ['count','min','max']
    }

### Calculate the revenue number per group: OLIID, the index will become OLIID
Summarized_Revenue = Revenue_data.groupby(['OLIID']).agg(aggregations)
Summarized_Revenue.columns = columns = ['_'.join(col).strip() for col in Summarized_Revenue.columns.values] # flatten the multilevel column name
Summarized_Revenue.columns = ['Revenue','AccrualRevenue','CashRevenue',
                                 'USDRevenue', 'USDAccrualRevenue', 'USDCashRevenue',
                                 'AccountingPeriodCnt', 'AccountingPeriodDate_init','AccountingPeriodDate_last']
Summarized_Revenue.reset_index(inplace=True)

# get the OLI detail from the last show accounting period record 
temp = Revenue_data.groupby(['OLIID']).agg({'AccountingPeriodDate':'idxmax'})
pull_rows = temp.AccountingPeriodDate

Summarized_Rev_key = Revenue_data.loc[pull_rows][['OLIID','Currency']].copy()

# Get the OLIID and Currency; will get the Test and Test Delivered Date from OLI by merging the table
# At OLI level, use the Payor of the current ticket
OLI_rev = pd.merge(Summarized_Rev_key, Summarized_Revenue, how='left', left_on=['OLIID'], right_on=['OLIID'])

########################################################################################
#   Roll up the QDX stdclaim information to OLI level                                  #
#   Calculate the Total Charge Amount                                                  #
########################################################################################
print ('Claim2Rev_QDX_GHI :: Aggregate Bill, Payment, Adjustment numbers per OLI')

## Reverse the Amt Received and Amt Adjust sign
a = Claim_bill['ClmAmtRec'] != 0.0
Claim_bill.loc[a,'ClmAmtRec'] = Claim_bill.loc[a,'ClmAmtRec'] *-1
a = Claim_bill['ClmAmtAdj'] != 0.0
Claim_bill.loc[a,'ClmAmtAdj'] = Claim_bill.loc[a,'ClmAmtAdj'] *-1
    
aggregations = {
    'TXNAmount': 'sum',
    'ClmAmtRec': 'sum',
    'ClmAmtAdj': 'sum',
    'ClmTickBal' :'sum'
    }

Summarized_Claim = Claim_bill.groupby(['OLIID']).agg(aggregations)
Summarized_Claim.columns = ['Charge','ClmAmtRec','ClmAmtAdj','Total Outstanding']

########################################################################################
#   Roll up the QDX stdPayment information to OLI level                                #
#   Calculate the Total Adjustment, Payment                                            #
#             the Total CIE amount, Refund using the adjustment code                   #
#   Calculate the net Payer payment = Insurance Paid - Refund                          #
#   The total payment, adjustment received up to the data extraction date              #
########################################################################################
## Reverse the sign of TXNAmt for Adjustment and not CIE
a = ((Claim_pymnt.TXNAmount != 0.0) &
     (Claim_pymnt.TXNType.isin(['AC','AD'])) &
     ~(Claim_pymnt.QDXAdjustmentCode.isin(['C6','CCD']))
    )
Claim_pymnt.loc[a, 'TXNAmount'] = Claim_pymnt.loc[a, 'TXNAmount'] * -1


## Reverse the sign of TXNAmt for Payment
a = ((Claim_pymnt.TXNAmount != 0.0) & (Claim_pymnt.TXNType.isin(['RI','RP'])))
Claim_pymnt.loc[a, 'TXNAmount'] = Claim_pymnt.loc[a, 'TXNAmount'] * -1

aggregations = {
    'TXNAmount' : 'sum',
    }

''' Calculate the Payor Amount '''
Summarized_PayorPaid = Claim_pymnt[(Claim_pymnt.TXNType=='RI')].groupby(['OLIID']).agg(aggregations)
Summarized_PayorPaid.columns = columns = ['_'.join(col).strip() for col in Summarized_PayorPaid.columns.values] # flatten the multilevel column name
Summarized_PayorPaid.columns = ['PayorPaid']

''' Calculate the Patient Paid Amount '''  
Summarized_PtPaid = Claim_pymnt[(Claim_pymnt.TXNType=='RP')].groupby(['OLIID']).agg({'TXNAmount' :'sum'})
Summarized_PtPaid.columns = columns = ['_'.join(col).strip() for col in Summarized_PtPaid.columns.values] 
Summarized_PtPaid.columns = ['PatientPaid']

''' Calculate the total adjustment per OLI '''
Summarized_Adjust = Claim_pymnt[(Claim_pymnt.TXNType.isin(['AC','AD']))].groupby(['OLIID']).agg({'TXNAmount':'sum'})
Summarized_Adjust.columns = ['stdP_ClmAmtAdj']

''' Calculate the Refund, Charge Error, and break the adjustment into BR_Categories '''
print ('Claim2Rev_QDX_GHI :: group adjustment numbers into Billing & Reimbursement analysis categories')

prep_file_name = "QDX_ClaimDataPrep.xlsx"

Adj_code = pd.read_excel(cfg.prep_file_path+prep_file_name, sheet_name = "AdjustmentCode", usecols="A,C:E,G", encoding='utf-8-sig')
Adj_code.columns = [cell.strip() for cell in Adj_code.columns]

category = 'AdjustmentGroup'
temp = Adj_code.groupby([category])

Adjust_Category = pd.DataFrame()
for a in temp.groups.keys():
    #print (a, list(Adj_code[(Adj_code.AdjustmentGroup==a)]['Code']))
    temp_codes = list(Adj_code[(Adj_code[category]==a)]['Code'])
    temp_sum = Claim_pymnt[((Claim_pymnt.TXNType.isin(['AC','AD'])) \
                            & (Claim_pymnt.QDXAdjustmentCode.isin(temp_codes)))].groupby(['OLIID']).agg({'TXNAmount' :'sum'})
    temp_sum.columns = [a]
    Adjust_Category = pd.concat([Adjust_Category,temp_sum], axis=1)  

##also lay out the summarized amount of CategoryDesc
category = 'Category'
temp = Adj_code.groupby([category])
GHIAdjust_Category = pd.DataFrame()
for a in temp.groups.keys():
    temp_codes = list(Adj_code[(Adj_code[category]==a)]['Code'])
    temp_sum = Claim_pymnt[((Claim_pymnt.TXNType.isin(['AC','AD'])) \
                            & (Claim_pymnt.QDXAdjustmentCode.isin(temp_codes)))].groupby(['OLIID']).agg({'TXNAmount' :'sum'})
    temp_sum.columns = [a]
    GHIAdjust_Category = pd.concat([GHIAdjust_Category,temp_sum], axis=1)

'''
  Concatenate the OLI Charges, Payment, Adjustment
  OLIID is the data frame index
'''
print ('Claim2Rev_QDX_GHI :: Update bill amount and payment amount by removing the charge error and refund')

QDX_OLI_Receipt = pd.concat([Summarized_Claim, Summarized_PayorPaid, Summarized_PtPaid, Summarized_Adjust, Adjust_Category, GHIAdjust_Category], axis=1)
QDX_OLI_Receipt = QDX_OLI_Receipt.fillna(0.0)

# Calculate the OLI Test Charge and Payment received
QDX_OLI_Receipt['Total Billed'] = QDX_OLI_Receipt.Charge + QDX_OLI_Receipt['Charged in Error']
QDX_OLI_Receipt['Total Payment'] = QDX_OLI_Receipt.ClmAmtRec + QDX_OLI_Receipt['Refund & Refund Reversal']
QDX_OLI_Receipt['Total Adjustment'] = round(QDX_OLI_Receipt.stdP_ClmAmtAdj - QDX_OLI_Receipt['Charged in Error'] - QDX_OLI_Receipt['Refund & Refund Reversal'],2)

############################################################
# Fill in Product List Price in OLI data if blank          #
# Find the product list price from OLI data                # 
############################################################ 
# temp_price = OLI_data[~(OLI_data.ListPrice.isnull())].groupby(['Test','BilledCurrency','ListPrice'])
# list_price_df = pd.DataFrame(list(temp_price.groups.keys()), columns=['Test','Currency','ListPrice'])
# why there are multiple list prices, in OLI_table and stgBill table

print ('Claim2Rev_QDX_GHI :: fill missing list price')

#read the standard price book price
list_price_df = GData.getProductListPrice()
temp = pd.merge(OLI_data[['Test','BilledCurrency','ListPrice']], list_price_df[['Test','BilledCurrency','Std_ListPrice']],
                how='left', left_on=['Test','BilledCurrency'], right_on=['Test','BilledCurrency'])
a = (~OLI_data.Test.isnull() & ~OLI_data.BilledCurrency.isnull() & OLI_data.ListPrice.isnull())
OLI_data.loc[a,'ListPrice'] = temp.loc[a,'Std_ListPrice']

##########################################################################################
#  Gather and clean data for test status and claim status                                #
#                                                                                        #
#  Compute Status to reflect the end to end Order to Cash status                         #
#  The SFDC Order Status [Canceled, Closed, Order Intake, Processing]                    #
#           Customer Status [Canceled, Closed, In-Lab, Processing, Submitting]           #
#  is insufficient to tell if an order is closed because delivered, canceled or failed   #
##########################################################################################
# keep the Original_OLI_TestDelivered, 
# if there is current Ticket Number, correct the TestDelivered = 0 to 1
OLI_data['Original_OLI_TestDelivered'] = OLI_data['TestDelivered']
a = (OLI_data.TestDelivered == 0) & ~(OLI_data.CurrentQDXTicketNumber.isnull())
OLI_data.loc[a,'TestDelivered'] = 1

# Update TestDeliveredDate using DateofService if TestDeliveredDate is not available
a = (OLI_data.TestDelivered == 1 & OLI_data.TestDeliveredDate.isnull())
OLI_data.loc[a, 'TestDeliveredDate'] = OLI_data.loc[a,'DateOfService']

## Combining Order & OLI Cancellation reason Get the cancel reason from order/oli cancellation reason and vice versa
a = OLI_data.OrderLineItemCancellationReason.isnull() & ~(OLI_data.OrderCancellationReason.isnull())
OLI_data.loc[a,'OrderLineItemCancellationReason'] = OLI_data.loc[a,'OrderCancellationReason']

a = ~OLI_data.OrderLineItemCancellationReason.isnull() & (OLI_data.OrderCancellationReason.isnull())
OLI_data.loc[a,'OrderCancellationReason'] = OLI_data.loc[a,'OrderLineItemCancellationReason']


# Derive the Test Order Status using the Customer Status, Order & OLI cancellation reason, Failure Code, Test Delivered Flag
# Scenario: Test Delivered with no cancellation and no failure code
a = (OLI_data.TestDelivered==1)
OLI_data.loc[a,'Status'] = 'Delivered'
OLI_data.loc[a,'Status Notes'] = "Test Delivered = " + OLI_data.loc[a,'TestDelivered'].astype(str)

### Once cancellation reason is stamped, it is not removed even the cancellation decision is reverted
# Scenario: Test Delivered with either cancellation or failure code
# TestDelivered == 1, cancel reason and/or failure reason is not null, still is a TestDelivered
b = ~(OLI_data.OrderLineItemCancellationReason.isnull())
c = ~(OLI_data.FailureMessage.isnull())
x = a & (b | c)
OLI_data.loc[x, 'Status Notes'] = "Test Delivered = " + OLI_data.loc[a,'TestDelivered'].astype(str) + "*"

# Scenarios: Test Not Delivered
# Assume the Test Order is Active
a = (OLI_data.TestDelivered==0)
OLI_data.loc[a,'Status'] = 'Active'

# check the cancellation reason, failure code and update test order status accordingly

b = OLI_data.OrderCancellationReason.isnull() & OLI_data.OrderLineItemCancellationReason.isnull()  # Cancellation reason is null
c = OLI_data.FailureMessage.isnull()                                                               # Failure code is null

# Scenario: Test not delivered, either order or OLI cancellation reason present and failure message is null
x = a & ~b & c
OLI_data.loc[x,'Status'] = "Canceled"

y = ~(OLI_data.OrderLineItemCancellationReason.isnull()) & ~(OLI_data.OrderCancellationReason.isnull()) &\
    (OLI_data.OrderLineItemCancellationReason != OLI_data.OrderCancellationReason)
z = x & y    
OLI_data.loc[z,'Status Notes'] =  OLI_data.loc[z,'OrderCancellationReason'] + " , " + OLI_data.loc[z, 'OrderLineItemCancellationReason']

y = ~(OLI_data.OrderLineItemCancellationReason.isnull()) & ~(OLI_data.OrderCancellationReason.isnull()) &\
    (OLI_data.OrderLineItemCancellationReason == OLI_data.OrderCancellationReason)
z = x & y
OLI_data.loc[z,'Status Notes'] = OLI_data.loc[z, 'OrderLineItemCancellationReason']

# Scenario: Test not delivered, failure code present
x= a & b & ~c
OLI_data.loc[x,'Status'] = "Failed"
OLI_data.loc[x,'Status Notes'] = OLI_data.loc[x,'FailureMessage']

# Scenario: Test not delivered, has both cancellation reason and failure code
x = a & ~b & ~c
OLI_data.loc[x,'Status'] = "Failed"
y = ~(OLI_data.OrderLineItemCancellationReason.isnull()) & ~(OLI_data.OrderCancellationReason.isnull()) &\
    (OLI_data.OrderLineItemCancellationReason != OLI_data.OrderCancellationReason)
z = x & y  
OLI_data.loc[z,'Status Notes'] = OLI_data.loc[z, 'FailureMessage']

y = ~(OLI_data.OrderLineItemCancellationReason.isnull()) & ~(OLI_data.OrderCancellationReason.isnull()) &\
    (OLI_data.OrderLineItemCancellationReason == OLI_data.OrderCancellationReason)
z = x & y
OLI_data.loc[z,'Status Notes'] = OLI_data.loc[z, 'FailureMessage']


# Scenario: Test not delivered because it is in process
a = (OLI_data.TestDelivered==0) & (OLI_data.Status =='Active')
OLI_data.loc[a,'Status'] = OLI_data.loc[a,'CustomerStatus']

x = pd.Series(OLI_data.CustomerStatus.unique())
a = (OLI_data.TestDelivered==0) & (OLI_data.Status.isin(x[~x.isin(['Closed','Canceled'])]))
OLI_data.loc[a,'Status Notes'] = OLI_data.loc[a,'DataEntryStatus']

############################################################### 
# Create the Claim2Rev report                                 # 
# Merging the GHI Revenue data to the QDX Claim record line   #
# Claim2Rev is at the OLI level                               #
############################################################### 
print ('Claim2Rev_QDX_GHI :: Create Claim2Rev report')

Claim2Rev = pd.merge(QDX_OLI_Receipt, OLI_rev,
                     how = 'left', left_index=True, right_on=['OLIID'])

Claim2Rev = pd.merge(OLI_data, Claim2Rev, how='left', left_on=['OLIID'], right_on=['OLIID'])

########################################################
# Reading and adding appeal information                #
# Override the out of sync (billing) case status       #
# based on the appeal result                           #
########################################################
Appeal_ClaimTicket = ClaimTicket_appeal_wide[['appealCaseNum','CaseappealLvlCnt','appealAccession', 'appealTickNum',
                                              'A1_Status', 'A2_Status', 'A3_Status', 'A4_Status', 'A5_Status',
                                              'ER_Status', 'L1_Status', 'L2_Status', 'L3_Status',
                                              
                                              'A1', 'A2', 'A3', 'A4', 'A5', 'ER', 'L1', 'L2', 'L3',
                                              
                                              'OLIappealLvlCnt',
                                              'Last Appeal level',
                                              'firstappealEntryDt',
                                              'lastappealEntryDt',
                                              #'lastappealDenReason',
                                              #'lastappealDenialLetterDt',
                                              'appealDenReason',
                                              'appealDenReasonDesc', 'appealAmtChg', 'appealAmtChgExp',
                                              'appealAmtAllow', 'appealAmtClmRec', 'appealAmt', 'appealAmtAplRec',
                                              'appealRptDt', 'appealSuccess', 'appealCurrency','appealCampaignCode'
       ]].copy()
#appealRptDt is from completed appeal

# rename columns for merging
Appeal_ClaimTicket.rename(columns = {'appealAccession':'OLIID', 'appealTickNum':'CurrentQDXTicketNumber'}, inplace=True)
Claim2Rev = pd.merge(Claim2Rev, Appeal_ClaimTicket, how='left', on=['OLIID','CurrentQDXTicketNumber'])

'''
temp_check = Claim2Rev[['OLIID','Test','CurrentQDXTicketNumber','appealCaseNum','CurrentQDXCaseNumber',
                        'TestDeliveredDate', 'DateOfService',
                        'Tier1Payor', 'Tier2Payor', 'Tier4Payor']]
a = temp_check[~(temp_check.CurrentQDXTicketNumber.isnull()) & ~(temp_check.appealCaseNum.isnull()) & (temp_check.appealCaseNum != temp_check.CurrentQDXCaseNumber)]
b = Claim_case[Claim_case.caseAccession.isin(a.OLIID)].sort_values(by='caseAccession')
output_file = 'Appeal_Current_Case_ticket_mismatch.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl', date_format='yyyy/mm/dd')
b.to_excel(writer, index = False)
writer.save()
writer.close()
'''

#############################################################################################
## Quadax provides the appeal result = success or failed for completed appeal.             ##
## Identify the In process and Removed appeal using the billing case status                ##
#############################################################################################

### insert flags for tableau reporting use ###
Claim2Rev['Failed'], Claim2Rev['Succeed'], Claim2Rev['In Process'], Claim2Rev['Removed'], Claim2Rev['IsAppeal'], Claim2Rev['CompletedAppeal'] = 0,0,0,0,0,0

Claim2Rev.loc[~Claim2Rev.appealDenReason.isnull(), 'IsAppeal'] = 1

Claim2Rev.loc[Claim2Rev.appealSuccess=='1','appealResult'] = 'Success'
Claim2Rev.loc[Claim2Rev.appealSuccess=='1','Succeed'] = 1
Claim2Rev.loc[Claim2Rev.appealSuccess=='1','CompletedAppeal'] = 1

Claim2Rev.loc[Claim2Rev.appealSuccess=='0','appealResult'] = 'Failed'
Claim2Rev.loc[Claim2Rev.appealSuccess=='0','Failed'] = 1
Claim2Rev.loc[Claim2Rev.appealSuccess=='0','CompletedAppeal'] = 1

Claim2Rev.loc[(Claim2Rev.appealSuccess.isnull()) & ~(Claim2Rev.appealDenReason.isnull()), 'appealResult'] = 'In Process'
Claim2Rev.loc[(Claim2Rev.appealSuccess.isnull()) & ~(Claim2Rev.appealDenReason.isnull()), 'In Process'] = 1

# Scenario Billing Status is in 'Completed, Due from Patient, Final Review', the Appeal tickets are abandoned
QDX_complete_appeal_status = ['Completed','Final Review','Due from Patient']
a = Claim2Rev.appealResult == 'In Process'
b = Claim2Rev.BillingCaseStatusSummary2.isin(QDX_complete_appeal_status)
Claim2Rev.loc[a & b, 'appealResult'] = 'Removed'
Claim2Rev.loc[a & b, 'Removed'] = 1
Claim2Rev.loc[a & b, 'In Process'] = 0

# Scenario appeal case is open with the current ticket for the OLI, BillingCaseStatusSummary2 is not 'Appeal' and not in the completed status
c = Claim2Rev.BillingCaseStatusSummary2 == 'Appeals'
Claim2Rev.loc[a & ~b & ~c, 'BillingCaseStatusSummary2'] = 'Appeals'

# Scenario appealResult is either success or fail, Billing Status is not in the 'Completed, Due from Patient, Final Review'
# this could be due to the Appeal status is a copy of Quadax data as of extract date-1, and Billing Case status is from OLI which is EDW data, i.e Quadax data as of extract date-2
a = Claim2Rev.appealResult.isin(['Failed','Success'])
Claim2Rev.loc[a & ~b, 'BillingCaseStatusSummary2'] = 'Final Review'
Claim2Rev.loc[a & ~b, 'BillingCaseStatus'] = 'Final Review'

#OL000994897 : BillingCaseStatusSummary2 = appeal, and there is no appeal case
# Scenario Billing Status is 'Appeal' and there is no appeal case information
a = Claim2Rev.appealResult.isnull()
Claim2Rev.loc[a & c, 'BillingCaseStatusSummary2'] = 'Claim in Process'

### adding flag for Tableau reporting use
#IsContracted
a = Claim2Rev.ContractedPrice.isna()
Claim2Rev['IsContract'] = 1
Claim2Rev.loc[a,'IsContract'] = 0

#IsAccrual
a = Claim2Rev.RevenueStatus == 'Accrual'
Claim2Rev['IsAccrual'] = 0
Claim2Rev.loc[a,'IsAccrual'] = 1


#### calculate Fully Paid, Partial Paid
# QDX set BillingCaseStatusSummary1 to 'Paid!' regardless the received payment amount; and 'Written Off' for claims that are fully written off.
# Added a flag to differential the Fully Paid and Partial Paid

Claim2Rev['Orig_BillingCaseStatus'] = Claim2Rev['BillingCaseStatus']
a = ((Claim2Rev.BillingCaseStatus =='Paid!') & (Claim2Rev['Revenue Impact Adjustment'] > 0))
Claim2Rev.loc[a,'BillingCaseStatus'] = 'Partial Paid'

#Total Payment, PayorPaid, PatientPaid, Insurance Adjustment
# if Total Payment >= Total Billed - Insurance Adjustment, then Fully Paid
# if Total Payment < Total Billed - Insurance Adjustment, or (Total Payment > 0 and Revenue Impact Adjustment > 0) then Partial Paid

############################################################################
# Reading and adding QDX priorAuth case status                             #
# aka pre claim status                                                     #
# We would like to provide the PA case status as on report generation date #
# to GHI teams                                                             #
# 1. By providing the information to Sales, hopefully, they can follow up  #
#    and press for PA                                                      #
# 2. To analyze and size pre claim status impact to                        #
#    revenue                                                               #
############################################################################

Claim2Rev= pd.merge(Claim2Rev, priorAuth[['priorAuthCaseNum','priorAuthEnteredDt','priorAuthEnteredTime',
                                          'priorAuthDate',
                                          'priorAuthResult','priorAuthReqDesc','priorAuthNumber',
                                          'priorAuthResult_Category', 'PreClaim_Failure']]
                    , how='left', left_on='CurrentQDXCaseNumber', right_on='priorAuthCaseNum')


############################################################################
# Adding Resulted Specimen Procedure Type                                  #
# all the EOB issued with payment to the OLI                               #
############################################################################

Claim2Rev = pd.merge(Claim2Rev, OLI_Result_Specimen[['OLIID','ProcedureType','Age_Of_Specimen','IBC_Candidate_for_Adj_Chemo']], how='left', on ='OLIID')

############################################################################
# Source the OLI allowable amount from Quadax                              #
# Payor provides the allowable amount with the EOB                         #
# A Claimed OLI has 1 or multiple Tickets                                  # 
# A Ticket has 1 or multiple EOB                                           #
# The allowable amount of an OLI is the max. allowable amount of           #
# all the EOB issued with payment to the OLI                               #
############################################################################

allowable_amt = Claim_pymnt[(Claim_pymnt.TXNType=='RI') & (Claim_pymnt.TXNAmount > 0)][['TicketNumber','OLIID','Test','OLIDOS',
                                                                                #'PrimaryInsPlan_GHICode','TicketInsPlan_GHICode','PymntInsPlan_GHICode',
                                                                                 'TXNLineNumber','TXNType','TXNCurrency','TXNAmount',
                                                                                 'stdPymntAllowedAmt','stdPymntDeductibleAmt','stdPymntCoinsAmt']]

OLI_allowable = allowable_amt.groupby(['OLIID']).agg({'stdPymntAllowedAmt':'max'})
OLI_allowable.columns = ['AllowedAmt']

Claim2Rev = pd.merge(Claim2Rev, OLI_allowable, how='left', left_on = 'OLIID', right_index=True)

Claim2Rev = pd.merge(Claim2Rev, OLI_SOMN_Status[['OLIID','SOMN_Status']], how='left', left_on = 'OLIID', right_on = 'OLIID')

############################################################
#   Create a Transpose of the Bill, Adjustment, Revenue    #
#   This is used for the Adjustment detail view            #
############################################################
print ('Claim2Rev_QDX_GHI :: Create a transpose of the Claim2Rev data')

Claim2Rev_tp = Claim2Rev[(~(Claim2Rev.CurrentQDXTicketNumber.isnull()))].copy()

pull_values = ['AccrualRevenue','CashRevenue','USDAccrualRevenue', 'USDCashRevenue',
               'Total Outstanding', 
               'PayorPaid', 'PatientPaid',
               'Charge']

TXN_Detail = pd.pivot_table(Claim2Rev_tp, index = ['OLIID','BilledCurrency'], 
                            values = pull_values, fill_value=0)
TXN_Detail = TXN_Detail.stack().reset_index()
TXN_Detail.columns = ['OLIID','Currency','TXNType','TXNAmount']


### read all adjustment transaction from stdPymnt
temp_adjust = Claim_pymnt[(Claim_pymnt.TXNType.isin(['AC','AD']))] \
                         [['OLIID', 'TicketNumber', 'Test','TXNAcctPeriod'
                          ,'TXNCurrency', 'TXNAmount', 'TXNType','TXNLineNumber'
                          ,'QDXAdjustmentCode', 'Description'
                          ,'GHIAdjustmentCode','CategoryDesc','AdjustmentGroup']].copy()

Summarized_adj = temp_adjust.groupby(['OLIID','TXNCurrency'
                                      ,'QDXAdjustmentCode', 'Description'
                                      ,'GHIAdjustmentCode','CategoryDesc','AdjustmentGroup']).agg({'TXNAmount' :'sum'})
Summarized_adj = Summarized_adj.reset_index()

Summarized_adj.rename(columns = {'TXNCurrency':'Currency'}, inplace=True)
Summarized_adj['TXNType'] = Summarized_adj['GHIAdjustmentCode'] + ':' + Summarized_adj['CategoryDesc']

#######
TXN_Detail = pd.concat([TXN_Detail, Summarized_adj])
TXN_Detail = TXN_Detail[~(TXN_Detail.TXNAmount == 0)].sort_values(by=['OLIID'])


## add TXN Category
TXN_Detail['TXNCategory'] = 'Receipts'

# overwrite for Revenue & Billing
TXNCategory_dict = {'USDAccrualRevenue':'USDAccrual', 'USDCashRevenue':'USDCash'}
for a in list(TXNCategory_dict.keys()):
    TXN_Detail.loc[(TXN_Detail.TXNType==a),'TXNCategory'] = 'USDRevenue'
    
TXNCategory_dict = {'AccrualRevenue':'Accrual', 'CashRevenue':'Cash'}
for a in list(TXNCategory_dict.keys()):
    TXN_Detail.loc[(TXN_Detail.TXNType==a),'TXNCategory'] = 'Revenue'

TXNCategory_dict = {'Charge':'Charge', 'GH04:Charged in Error':'Charged in Error'}
for a in list(TXNCategory_dict.keys()):
    TXN_Detail.loc[TXN_Detail['TXNType'] == a,'TXNCategory'] = 'Billing'

TXN_Detail.loc[TXN_Detail['TXNType'].isin(['Total Outstanding']),'TXNType'] = 'Outstanding'

## Adding the OLI detail, Keeping the Tier2PayorID for merging with the GNAM sets assignment
## may need to add the PrimaryIns info - sep 20
OLI_detail = OLI_data[['OLIID','Test','TestDeliveredDate'
                        ,'Tier1Payor','Tier1PayorID','Tier2Payor','Tier2PayorID','Tier4Payor','FinancialCategory','ReportingGroup'
                        ,'PrimaryInsTier1Payor', 'PrimaryInsTier1PayorID'
                        ,'PrimaryInsTier2Payor', 'PrimaryInsTier2PayorID'
                        ,'PrimaryInsTier4Payor', 'PrimaryInsFinancialCategory'
                        ,'TerritoryRegion', 'OrderingHCPState']]
#'LineOfBenefit'

TXN_Detail = pd.merge(TXN_Detail,OLI_detail,
                       how='left', left_on='OLIID', right_on='OLIID')

#########################################
# Extract Data sets for end users       #
#########################################

# Export a set for Tableau reports
Claim2Rev_tableau = Claim2Rev[['OrderID', 'OLIID', 'Test', 
        'OrderStartDate', 'TestDeliveredDate', 'CurrentQDXTicketNumber',
        'BillingCaseStatusSummary2', 'BillingCaseStatusCode', 'BillingCaseStatus',
        'BilledCurrency', 'ListPrice', 'ContractedPrice', 'Total Outstanding',
        'Total Billed', 'Charge',

        'Total Payment',
        'PayorPaid','PatientPaid',

        'Total Adjustment'
        ] + list(Adj_code.groupby('AdjustmentGroup').groups.keys()) + [
 
        'AllowedAmt',
        
        'Revenue', 'AccrualRevenue', 'CashRevenue',
        'USDRevenue', 'USDAccrualRevenue', 'USDCashRevenue', 
        
        'Tier1Payor','Tier1PayorID', 'Tier1PayorName', 
        'Tier2Payor','Tier2PayorID', 'Tier2PayorName',
        'Tier4Payor','Tier4PayorID', 'Tier4PayorName', 
        'FinancialCategory', 'QDXInsPlanCode',
        
        # PrimaryIns for Appeal Report
        'PrimaryInsTier1Payor', 'PrimaryInsTier1PayorID', 'PrimaryInsTier1PayorName',
        'PrimaryInsTier2Payor', 'PrimaryInsTier2PayorID', 'PrimaryInsTier2PayorName', 
        'PrimaryInsTier4Payor', 'PrimaryInsTier4PayorID', 'PrimaryInsTier4PayorName',  'PrimaryInsFinancialCategory','PrimaryInsPlan_QDXCode',
            
        'Reportable', 'IsCharge', 'TestDelivered',
        'IsClaim', 'IsFullyAdjudicated',
        'IsContract', 'IsAccrual', 'NSInCriteria',
       
        'Status',

        'TerritoryRegion', 'OrderingHCPState',

        'CurrentQDXCaseNumber',
        'A1_Status', 'A2_Status', 'A3_Status', 'A4_Status', 'A5_Status',
        'ER_Status', 'L1_Status', 'L2_Status', 'L3_Status',
        
        'Last Appeal level', 'firstappealEntryDt','appealRptDt',
        'appealDenReason','appealDenReasonDesc',
        'appealAmt', 'appealAmtAplRec',
        'appealResult',
        
        'IsAppeal','CompletedAppeal','Failed','Succeed','In Process','Removed',
        
        'ReportingGroup','RecurrenceScore', 'Specialty',
        'RiskGroup', 'NodalStatus','EstimatedNCCNRisk', 'SubmittedNCCNRisk'
        
        , 'Rerouted_Ticket','Reroute_ChangedFC'
        ]].copy()
                
#Export a dataset in excel for Jodi, includes Domestic Orders
Claim2Rev_USD_excel = Claim2Rev[Claim2Rev.BusinessUnit == 'Domestic'][['OrderID',
        'OLIID', 'Test','OrderStartDate', 'TestDeliveredDate', 'CurrentQDXTicketNumber',
        'BillingCaseStatusSummary2', 'BillingCaseStatusCode', 'BillingCaseStatus', 
        'BilledCurrency', 
        'ListPrice', 'ContractedPrice', 'Total Outstanding',
        'Total Billed', 'Charge', 'Charged in Error',

        'Total Payment',
        'PayorPaid','PatientPaid','Refund & Refund Reversal',

        'Total Adjustment',
        'Revenue Impact Adjustment', 'Insurance Adjustment',
        'GHI Adjustment', 'All other Adjustment',

        'AllowedAmt',
        
        'Revenue', 'AccrualRevenue', 'CashRevenue',
        'USDRevenue', 'USDAccrualRevenue', 'USDCashRevenue', 
                
        'Tier1PayorID','Tier1PayorName', 'Tier1Payor',
        'Tier2PayorID', 'Tier2Payor','Tier2PayorName',  
        'Tier4PayorID', 'Tier4Payor','Tier4PayorName',
        'QDXInsPlanCode',  'FinancialCategory',
        #'LineOfBenefit',
        'PrimaryInsTier1Payor', 'PrimaryInsTier1PayorID', 'PrimaryInsTier1PayorName',
        'PrimaryInsTier2Payor', 'PrimaryInsTier2PayorID', 'PrimaryInsTier2PayorName', 
        'PrimaryInsTier4Payor', 'PrimaryInsTier4PayorID', 'PrimaryInsTier4PayorName',  'PrimaryInsFinancialCategory',
                     
        'Status', #'Status Notes',

        'BusinessUnit', 'InternationalArea', 'Division', 'Country', 'OrderingHCPName',
        'Territory', 'TerritoryRegion', 'TerritoryArea',
        'OrderingHCPCity', 'OrderingHCPState', 'OrderingHCPCountry',
        'IsOrderingHCPCTR', 'IsOrderingHCPPECOS', 
        
        'ReportingGroup', 'RecurrenceScore', 'Specialty','RiskGroup', 
        'NodalStatus','EstimatedNCCNRisk',

        'CurrentQDXCaseNumber', 'appealCampaignCode',
        'A1', 'A2', 'A3', 'A4', 'A5', 'ER', 'L1', 'L2', 'L3',

        'Last Appeal level', 'firstappealEntryDt',
        'appealDenReason','appealDenReasonDesc',
        'appealAmtChg', 'appealAmtChgExp',
        'appealAmtAllow', 'appealAmtClmRec', 'appealAmt', 'appealAmtAplRec',
        'appealRptDt', 'appealSuccess', 'appealCurrency', 'appealResult',
        'Specialty','NodalStatus','EstimatedNCCNRisk','SubmittedNCCNRisk',

        'priorAuthCaseNum','priorAuthDate',
        'priorAuthEnteredDt','priorAuthEnteredTime',
        'priorAuthResult', 'priorAuthResult_Category'
        ]]

OLI_PTx = Claim2Rev[Claim2Rev.BusinessUnit == 'Domestic']\
        [['OrderID', 'OLIID', 'Test', 
        'TestDeliveredDate',
        'ClaimEntryDate','Days_toInitPymnt','Days_toLastPymnt',
        'CurrentQDXTicketNumber','QDXTickCnt','QDXCaseCnt',
                
        'BillingCaseStatusSummary2', 'BillingCaseStatusCode', 'BillingCaseStatus', 'Orig_BillingCaseStatus',
#        'BilledCurrency', 'ListPrice', 'ContractedPrice', 'Total Outstanding',
        'Total Billed', 'Charge',

#        'ClmAmtRec', 
        'Total Payment',
        'PayorPaid','PatientPaid',
        'AllowedAmt',

#        'ClmAmtAdj', 'stdP_ClmAmtAdj',
#        'Total Adjustment'
        ] + list(Adj_code.groupby('AdjustmentGroup').groups.keys()) + [
 
 #       'AllowedAmt',
                
        'Tier1PayorID','Tier1PayorName', 'Tier1Payor',
        'Tier2PayorID', 'Tier2Payor','Tier2PayorName',  
        'Tier4PayorID', 'Tier4Payor','Tier4PayorName',
        'QDXInsPlanCode',  'FinancialCategory',
#'LineOfBenefit',
#        'PrimaryInsTier1Payor', 'PrimaryInsTier1PayorID', 'PrimaryInsTier1PayorName',
#        'PrimaryInsTier2Payor', 'PrimaryInsTier2PayorID', 'PrimaryInsTier2PayorName', 
#        'PrimaryInsTier4Payor', 'PrimaryInsTier4PayorID', 'PrimaryInsTier4PayorName',  'PrimaryInsFinancialCategory',
            
#        'Reportable', 'IsCharge',
        'TestDelivered', 'IsClaim', 'IsFullyAdjudicated', 'Rerouted_Ticket',
#        'IsContract', 'IsAccrual',
#        'RevenueStatus', 'NSInCriteria',
        
        'Status',
#         'FailureMessage',#'Status Notes',

        'BusinessUnit', 'InternationalArea', 'Division', 'Country', 'OrderingHCPName',
        'Territory', 'TerritoryRegion', 'TerritoryArea',
        'OrderingHCPCity', 'OrderingHCPState', 'OrderingHCPCountry',
        'IsOrderingHCPCTR', 'IsOrderingHCPPECOS', 'OrderStartDate',
        
        'Specialty','ProcedureType','Age_Of_Specimen',
        'NodalStatus','PatientAgeAtDiagnosis',
        'SubmittingDiagnosis','HCPProvidedClinicalStage',
        'SubmittedER', 'SubmittedHER2','SubmittedPR','MultiplePrimaries', 'IBC_TumorSizeCentimeters','DCISTumorSize',
#        'RecurrenceScore','HER2GeneScore','ERGeneScore','PRGeneScore','DCISScore',
        'HCPProvidedGleasonScore','HCPProvidedPSA',
        'EstimatedNCCNRisk', 'SubmittedNCCNRisk',
#        'SFDCSubmittedNCCNRisk','FavorablePathologyComparison',
#        'RiskGroup','ReportingGroup',
        
        'ProstateVolume','PSADensity','NumberOfCoresCollected','HCPProvidedNumberOfPositiveCores','MaxPctOfTumorInvolvementInAnyCore',
        'NumberOf4Plus3Cores','PreGPSManagementRecommendation','OtherPreGPSManagementRecommendation',
        
        'IBC_Candidate_for_Adj_Chemo', 'SOMN_Status',
        
        'appealDenReason','appealDenReasonDesc','appealSuccess', 'appealResult',
               
        'priorAuthResult','priorAuthResult_Category','priorAuthNumber', 'PreClaim_Failure'
        ]]

#Extract data set of IBC Appeals Detail 
Cond = (Claim2Rev.BusinessUnit == 'Domestic') & \
       (Claim2Rev.Test == 'IBC') & (~Claim2Rev.appealResult.isnull())
       
IBC_Appeals_Detail = Claim2Rev[Cond][['Tier1PayorID','Tier1PayorName',
                                             'Tier2PayorID', 'Tier2PayorName',
                                             'Tier4PayorID', 'Tier4PayorName',
                                             'FinancialCategory',
                                             'appealResult', 'NodalStatus','appealDenReasonDesc','TestDeliveredDate',
                                             'BillingCaseStatusSummary2','CurrentQDXCaseNumber','OLIID',
                                             'A1', 'A2', 'A3', 'A4', 'A5', 'ER', 'L1', 'L2', 'L3',
                                             'appealAmt','appealAmtAplRec']]
#Rename Columns
IBC_Appeals_Detail.columns =  [['Tier1PayorID','Tier1PayorName','Tier2PayorID', 'Tier2PayorName',
                                'Tier4PayorID', 'Tier4PayorName','FinancialCategory',
                                'Appeal Result', 'NodalStatus','Appeal Den Reason Desc','TestDeliveredDate',
                                'Billing Case','QDX Case Number','Oliid',
                                'A1', 'A2', 'A3', 'A4', 'A5', 'ER', 'L1', 'L2', 'L3',
                                'Appeal Amt','Appeal Amt Apl Rec']]      

#Extract a data set of Prostate Appeals Detail       
Cond = (Claim2Rev.BusinessUnit == 'Domestic') & \
       (Claim2Rev.Test == 'Prostate') & (~Claim2Rev.appealResult.isnull())
       
Prostate_Appeals_Detail = Claim2Rev[Cond][['Tier1PayorID','Tier1PayorName',
                                             'Tier2PayorID', 'Tier2PayorName',
                                             'Tier4PayorID', 'Tier4PayorName',
                                             'FinancialCategory',
                                             'appealResult', 'EstimatedNCCNRisk','appealDenReasonDesc','TestDeliveredDate',
                                             'BillingCaseStatusSummary2','CurrentQDXCaseNumber','OLIID',
                                             'A1', 'A2', 'A3', 'A4', 'A5', 'ER', 'L1', 'L2', 'L3',
                                             'appealAmt','appealAmtAplRec']]
#Rename columns
Prostate_Appeals_Detail.columns = [['Tier1PayorID','Tier1PayorName','Tier2PayorID', 'Tier2PayorName',
                                'Tier4PayorID', 'Tier4PayorName','FinancialCategory',
                                'Appeal Result', 'EstimatedNCCNRisk','Appeal Den Reason Desc','TestDeliveredDate',
                                'Billing Case','QDX Case Number','Oliid',
                                'A1', 'A2', 'A3', 'A4', 'A5', 'ER', 'L1', 'L2', 'L3',
                                'Appeal Amt','Appeal Amt Apl Rec']]  

#########################################
#   Add the Payor View Set assignment   #
#########################################
prep_file_name = "Payor-ViewSetAssignment.xlsx"
Payor_view = pd.read_excel(cfg.prep_file_path+prep_file_name, sheet_name = "SetAssignment", usecols="B:D", encoding='utf-8-sig')

for i in Payor_view.Set.unique() :
    #print (i)
    code = Payor_view[Payor_view.Set==i].PayorID
    join_column = Payor_view[Payor_view.Set==i].JoinWith.iloc[0]
    
    Claim2Rev_tableau.loc[Claim2Rev_tableau[join_column].isin(list(code)),i] = '1'
    Claim2Rev.loc[Claim2Rev_tableau[join_column].isin(list(code)),i] = '1'
    TXN_Detail.loc[TXN_Detail[join_column].isin(list(code)),i] = '1'
    
### need to create PrimaryInsSet
for i in Payor_view.Set.unique():
    code = Payor_view[Payor_view.Set==i].PayorID
    join_column = Payor_view[Payor_view.Set==i].JoinWith.iloc[0] ## JoinWith PrimaryInsTier2PayorID, ##PrimaryInsTier1PayorID, PrimaryInsFinancialCategory
    
    mapping = {'Tier2PayorID':'PrimaryInsTier2PayorID', 'Tier1PayorID':'PrimaryInsTier1PayorID', 'FinancialCategory':'PrimaryInsFinancialCategory'}
    primaryins_set = 'PrimaryIns' + '_' + i
    
    Claim2Rev_tableau.loc[Claim2Rev_tableau[mapping[join_column]].isin(list(code)),primaryins_set] = '1'
    TXN_Detail.loc[TXN_Detail[mapping[join_column]].isin(list(code)),primaryins_set] = '1'

#Claim2Rev_tableau.drop(['Tier2PayorID', 'Tier1PayorID'], axis=1, inplace=True)
# repeat columns for Tableau color purpose
dup = ['USDAccrualRevenue', 'USDCashRevenue',
        'Total Payment', 'Total Outstanding', 'Total Adjustment',
        'PayorPaid','PatientPaid']
for i in dup:
    add_column = i + '_'
    Claim2Rev_tableau[add_column] = Claim2Rev_tableau[i]
      
###############################################
#     Write the columns into a excel file     #
###############################################

print ('Claim2Rev_QDX_GHI :: write OLITXT_Detail', len(TXN_Detail), 'rows :: start ::', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
output_file = 'OLI_TXN_Detail.txt'
TXN_Detail.to_csv(cfg.output_file_path+output_file, sep='|',index=False)

print ('Claim2Rev_QDX_GHI :: write Claim2Rev_4_Tableau report ', len(Claim2Rev_tableau), 'rows :: start ::', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
output_file = 'Claim2Rev_4_Tableau.txt'
Claim2Rev_tableau.to_csv(cfg.output_file_path+output_file, sep='|',index=False)

print ('Claim2Rev_QDX_GHI :: write Claim2Rev report ', len(Claim2Rev), 'rows :: start ::', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
output_file = 'Claim2Rev.txt'
Claim2Rev.to_csv(cfg.output_file_path+output_file, sep='|',index=False)

print ('Claim2Rev_QDX_GHI :: write Claim2Rev output for Payment Assessment ', len(OLI_PTx), 'rows :: start ::', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
output_file = 'OLI_PTx.txt'
OLI_PTx.to_csv(cfg.output_file_path+output_file, sep='|',index=False)


print ('Claim2Rev_QDX_GHI :: write Claim2Rev USD xlsx report ', len(Claim2Rev_USD_excel), 'rows :: start ::', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
output_file = 'Claim2Rev_USD.xlsx'
writer = pd.ExcelWriter(cfg.output_file_path+output_file, engine='openpyxl', date_format='yyyy/mm/dd')
Claim2Rev_USD_excel.to_excel(writer, sheet_name='Claim2Rev', index = False)
writer.save()
writer.close()


print ('Claim2Rev_QDX_GHI :: write IBC Appeals xlsx report ', len(IBC_Appeals_Detail), 'rows :: start ::', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
output_file = 'IBC_Appeals_Detail.xlsx'
writer = pd.ExcelWriter(cfg.output_file_path+output_file, engine='openpyxl', date_format='yyyy/mm/dd')
IBC_Appeals_Detail.to_excel(writer, index = False)
writer.save()
writer.close()

print ('Claim2Rev_QDX_GHI :: write Prostate Appeals xlsx report ', len(Prostate_Appeals_Detail), 'rows :: start ::', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
output_file = 'Prostate_Appeals_Detail.xlsx'
writer = pd.ExcelWriter(cfg.output_file_path+output_file, engine='openpyxl', date_format='yyyy/mm/dd')
Prostate_Appeals_Detail.to_excel(writer, index = False)
writer.save()
writer.close()

#PreClaim Status for Ron's
Cond = (Claim2Rev.BusinessUnit == 'Domestic') & \
        (Claim2Rev.TestDeliveredDate >= '2017-01-01') & \
        ((Claim2Rev.Test == 'IBC') | (Claim2Rev.Test == 'Prostate'))
       
PreClaim_Status_SalesOps = Claim2Rev[Cond][['OrderID','OLIID', 'Test','priorAuthResult']]

output_file = 'PreClaim_Status_SalesOps.xlsx'
writer = pd.ExcelWriter(cfg.output_file_path+output_file, engine='openpyxl', date_format='yyyy/mm/dd')
PreClaim_Status_SalesOps.to_excel(writer, index = False)
writer.save()
writer.close()

###############################################
#    Writing a Data refresh log into Excel    #
###############################################

Dashboard_Dataset = [['Front-End Charts', 'Claim2Rev', 'Managed Care Analytics']
                    ,['Payor Summary', 'Claim2Rev','Managed Care Analytics']
                    ,['Adjustment-Detail','OLI_TXN_Detail','Managed Care Analytics']
                    ,['IBC-QBR1','Claim2Rev','Managed Care Analytics']
                    ,['IBC-QBR2','Claim2Rev','Managed Care Analytics']
#                    ,['Appeals-Summary','Claim2Rev','Managed Care Analytics']
#                    ,['Appeals - Details', 'Claim2Rev','Managed Care Analytics']
#                    ,['IBC Appeals Detail','Claim2Rev','Managed Care Analytics']
#                    ,['Prostate Appeals Detail', 'Claim2Rev','Managed Care Analytics']
                    ,['All','Claim2Rev','Managed Care Appeal Reports']
                    ,['Payor Test Criteria Summary','Long PTC','Managed Care PTC-PTV Report']
                    ,['Payor Test Validation Summary','Long PTV','Managed Care PTC-PTV Report']
                    ,['Missing IBC Medical Policy','In_or_Out_IBC','Managed Care PTC-PTV Report']
                    ,['No IBC coverage contract','In_or_Out_IBC','Managed Care PTC-PTV Report']
                    ,['Missing Prostate Medical Policy','In_or_Out_Prostate','Managed Care PTC-PTV Report']
                    ,['No Prostate coverage contract','In_or_Out_Prostate','Managed Care PTC-PTV Report']
                    ,['All','Payor_Monthly_Report', 'Payor Monthly Report']
                    ]

data_refresh = pd.DataFrame(data=Dashboard_Dataset, columns=['Dashboard','Dataset','Tableau File'])
data_refresh['Data Refresh'] = time.strftime('%b-%d, %Y, %H:%M:%S %Z')

output_file = 'Data_Refresh_log.xlsx'
writer = pd.ExcelWriter(cfg.output_file_path+output_file)
data_refresh.to_excel(writer, index=False)
writer.save()
writer.close()
'''
a = Claim2Rev_output.Tier1PayorID.isin(['CR085599','CR085614'])
b = Claim2Rev_output.Test.isin(['IBC','Prostate'])
Sankey_POC1 = Claim2Rev[a & b]
Sankey_POC1['VizSide'] = 'A'

Sankey_POC2 = Claim2Rev[a & b]
Sankey_POC2['VizSide'] = 'B'

Sankey_POC = pd.concat([Sankey_POC1, Sankey_POC2])
output_file = 'Sankey_POC.txt'
Sankey_POC.to_csv(cfg.output_file_path+output_file, sep='|',index=False)
'''

print ('Hurray !!! Done Done Done',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

'''   
test code on writing into BOX
CLIENT_ID='7vg4f7ul7vgton8722yd6s85ildt7z0'
CLIENT_SECRET = 'yYBc525Gdi73DCgG8KTStWsjkzXgLmEk'
ACCESS_TOKEN = 'IPe1uhPH1yGYY2UdtZqp6z3QYjUxksnY' ##developer token which is expired in a short time
oauth = OAuth2(client_id = CLIENT_ID, client_secret=CLIENT_SECRET, access_token=ACCESS_TOKEN) # use to authenticate with the temporary developer token
client = Client(oauth)
me = client.user(user_id='me').get()
print ('user_login: ' + me['login'])
'''
