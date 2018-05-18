'''
@author: aliu

Generate the Claim2Rev report.
Purpose of the report is to track for Test Delivered OLI, the billed amount, payment, adjustment, current outstanding and the revenue recognized for the OLI.
Using the QDX data for Payment and Adjustment due to the issues with GHI data listed in Constraints

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
   
Oct 18: 
Switch Revenue data source from StagingDB.Analytics.mvwRevenue to EDWDB.dbo.fctRevenue to get the IsRevenueReconciliationAdjustment flag
Exclude the IsRevenueReconciliationAdjustment from Claim2Rev analysis

Nov 27: adding appeal case, history & result to OLI

Dec 15: taking Billing Case status from QDX file, because the StagingDB is a few days older, make it harder to reconcile with the appeal status 
to determine the billing case status

Jan 22: update the calculation reconciliation logic

Jan 22 :: Workaround BI does not always have the right current ticket number;
          Derive the current ticket from stdClaim. This table has all the tickets issued by Quadax. Group rows by caseAccession ,
          the current ticket number is the one with the latest TXNDate
Jan 24 :: A known QDX bug with Roster billing, it creates multiple cases for an OLI
       :: workaround by derive the current case number from QDX Case, group by caseAccession and pick the case with the latest caseAddedDateTime
       :: and get the QDX billing case status from the QDX Case file
       :: reason for getting from QDX because the EDW Billing Case Status is from StageDB which is a few days old. The stale data cause issue with compare
       :: QDX appeal status
'''
import pandas as pd
#import numpy as np
from datetime import datetime


input_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\May092018\\"
output_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\May092018\\"
prep_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\workspace\\Supplement\\"

print ('Claim2Rev_QDX_GHI :: start :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
refresh = 0

###############################################################
#   Read QDX Claim and Payment (Receipt) data                 #
###############################################################

from data import GetQDXData as QData
Claim_bill = QData.stdClaim('Claim2Rev', input_file_path, refresh)
Claim_pymnt = QData.stdPayment('Claim2Rev', input_file_path, refresh)
Claim_case = QData.claim_case_status('QDXClaim_CaseStatus', input_file_path, refresh)
priorAuth = QData.priorAuth('Claim2Rev', input_file_path, refresh)


###############################################################
#   Read GHI Revenue Data and OrderLineDetail and Appeal Data #
############################################################### 

from data import GetGHIData as GData
Revenue_data = GData.revenue_data('Claim2Rev', input_file_path, refresh)
OLI_data = GData.OLI_detail('Claim2Rev', input_file_path, refresh)
#OLI_utilization = GData.OLI_detail('Utilization', input_file_path, 0)

###############################################################
#   Read QDX Appeal Data                                      #
###############################################################

from data import Appeal_data_prep
appeal_data = Appeal_data_prep.make_appeal_data(input_file_path, refresh)
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
#   Max. Ticket Number precede max. case number as current                    #
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

# Retrieve the max Ticket Number and information with it
# first group by OLIID + Ticket Number and get the max case number
# then group by OLIID to get the max Ticket Number

temp = Claim_bill.groupby(['OLIID','TicketNumber']).agg({'CaseNumber':'idxmax'}).CaseNumber
temp1 = Claim_bill.loc[temp][['OLIID','TicketNumber','CaseNumber','TXNDate']]

temp2 = temp1.groupby(['OLIID']).agg({'TXNDate':'idxmax'}).TXNDate
Current_ticket = Claim_bill.loc[temp2][['OLIID','TicketNumber', 'CaseNumber',
                                                   'BillingCaseStatusSummary1', 'BillingCaseStatusSummary2',
                                                   'BillingCaseStatusCode', 'BillingCaseStatus']]

Current_ticket.rename(columns = {'TicketNumber': 'CurrentQDXTicketNumber'}, inplace=True)
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
                                       'BillingCaseStatusCode', 'BillingCaseStatus']]

OLI_data = pd.merge(OLI_data, Current_reference, how='left', on='OLIID')

'''
## section to check the Current Case and CurrentTicket number mapping
## after reviewing the Current case status, Current Ticket number and the Billing Case status:: conclude to take the billing case status from stdClaim file to get the status with the current ticket
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
Claim_bill = Claim_bill[((~Claim_bill.OLIID.isnull()) & (Claim_bill.OLIID != 'NONE'))]

## Select and drop the OrderLineItem that has multiple Test values, ignore the one with Unknown. Export the value for reporting bug
## pivot with OLIID, count the number of unique Test values
a = pd.pivot_table(Revenue_data, index=['OLIID'], values=['Test'], aggfunc = lambda Test: len(set(Test.unique()) - set(['Unknown'])))
b = list(a[(a.Test > 1)].index) # find the OrderLineItemID with inconsistent test value


## Select and drop the OrderLineItem that has multiple Currency Code, ignore the null CurrencyCode. Export the value for reporting bug
a = pd.pivot_table(Revenue_data, index=['OLIID'], values=['Currency'], aggfunc = lambda Currency: len(Currency.unique())-sum(pd.isnull(Currency)))
b = b + list(a[(a.Currency> 1)].index) # find the OrderLineItemID with inconsistent currency value

error_Revenue_data = Revenue_data[(Revenue_data['OLIID'].isin(b))]

# Oct 18: drop the Top Side adjustment lines : IsRevenueReconciliationAdjustment == '0'
# Nov7 : include all revenue rows
# Nov14: remove the adjustment rows. Need to diff the file and identify the adjustment row in 2017
Revenue_data = Revenue_data[(Revenue_data.IsRevenueReconciliationAdjustment == '0')]
#Revenue_data[(Revenue_data.IsRevenueReconciliationAdjustment == '1')]['AccountingPeriod'].unique()
## the flag looks stopped since 2017

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
    'ClaimPeriodDate' : 'max',
    'AccountingPeriodDate' : ['count','min','max']
    }

### Calculate the revenue number per group: OLIID, the index will become OLIID
Summarized_Revenue = Revenue_data.groupby(['OLIID']).agg(aggregations)
Summarized_Revenue.columns = columns = ['_'.join(col).strip() for col in Summarized_Revenue.columns.values] # flatten the multilevel column name
Summarized_Revenue.columns = [['Revenue','AccrualRevenue','CashRevenue',
                                 'USDRevenue', 'USDAccrualRevenue', 'USDCashRevenue',
                                 'ClaimPeriodDate',
                                 'AccountingPeriodCnt', 'AccountingPeriodDate_init','AccountingPeriodDate_last']]
Summarized_Revenue = Summarized_Revenue.reset_index()

# get the OLI detail from the last show accounting period record 
temp = Revenue_data.groupby(['OLIID']).agg({'AccountingPeriodDate':'idxmax'})
pull_rows = temp.AccountingPeriodDate

Summarized_Rev_key = Revenue_data.loc[pull_rows][['OLIID','Currency']].copy()

# Nov 05: only get the OLIID and Currency; will get the Test and Test Delivered Date from OLI by merging the table
# At OLI level, use the Payor of the current ticket
OLI_rev = pd.merge(Summarized_Rev_key, Summarized_Revenue, how='left', left_on=['OLIID'], right_on=['OLIID'])

########################################################################################
#   Roll up the QDX stdclaim information to OLI level                                  #
#   Calculate the Total Billed Amount                                                  #
########################################################################################
print ('Claim2Rev_QDX_GHI :: Aggregate Bill, Payment, Adjustment numbers per OLI')

## Reverse the Amt Received and Amt Adjust sign
temp = ['ClmAmtRec','ClmAmtAdj']
for a in temp:
    Claim_bill.loc[(Claim_bill[a] != 0.0),a] = Claim_bill.loc[(Claim_bill[a] != 0.0),a] * -1

aggregations = {
    'TXNAmount': 'sum',
    'ClmAmtRec': 'sum',
    'ClmAmtAdj': 'sum',
    'ClmTickBal' :'sum'
    }

Summarized_Claim = Claim_bill.groupby(['OLIID']).agg(aggregations)
Summarized_Claim.columns = [['Charge','ClmAmtRec','ClmAmtAdj','Total Outstanding']]

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
#    'stdPymntAllowedAmt': 'sum',
#    'stdPymntDeductibleAmt': 'sum',
#    'stdPymntCoinsAmt':'sum'
    }

''' Calculate the Payor Amount '''
Summarized_PADC = Claim_pymnt[(Claim_pymnt.TXNType=='RI')].groupby(['OLIID']).agg(aggregations)
Summarized_PADC.columns = columns = ['_'.join(col).strip() for col in Summarized_PADC.columns.values] # flatten the multilevel column name
Summarized_PADC.columns = [['PayorPaid']]
#Summarized_PADC.columns = [['PayorPaid','AllowedAmt','DeductibleAmt','CoinsAmt']]

''' Calculate the Patient Paid Amount '''  
Summarized_PtPaid = Claim_pymnt[(Claim_pymnt.TXNType=='RP')].groupby(['OLIID']).agg({'TXNAmount' :'sum'})
Summarized_PtPaid.columns = columns = ['_'.join(col).strip() for col in Summarized_PtPaid.columns.values] 
Summarized_PtPaid.columns = [['PatientPaid']]

''' Calculate the total adjustment per OLI '''
Summarized_Adjust = Claim_pymnt[(Claim_pymnt.TXNType.isin(['AC','AD']))].groupby(['OLIID']).agg({'TXNAmount':'sum'})
Summarized_Adjust.columns = [['stdP_ClmAmtAdj']]

'''
  Calculate the Refund, Charge Error, and break the adjustment into BR_Categories
  # Oct 31: Break adjustment into Finance GHI codes
  # Nov 7: Break adjustment into Analytics Category
'''
print ('Claim2Rev_QDX_GHI :: group adjustment numbers into Billing & Reimbursement analysis categories')

prep_file_name = "QDX_ClaimDataPrep.xlsx"

Adj_code = pd.read_excel(prep_file_path+prep_file_name, sheetname = "AdjustmentCode", parse_cols="A,C:E,G", encoding='utf-8-sig')
Adj_code.columns = [cell.strip() for cell in Adj_code.columns]

category = 'AdjustmentGroup'
#category = 'CategoryDesc'
temp = Adj_code.groupby([category])

Adjust_Category = pd.DataFrame()
for a in temp.groups.keys():
    #print (a, list(Adj_code[(Adj_code.GHI_BR_Category==a)]['Code']))
    temp_codes = list(Adj_code[(Adj_code[category]==a)]['Code'])
    temp_sum = Claim_pymnt[((Claim_pymnt.TXNType.isin(['AC','AD'])) \
                            & (Claim_pymnt.QDXAdjustmentCode.isin(temp_codes)))].groupby(['OLIID']).agg({'TXNAmount' :'sum'})
    temp_sum.columns = [a]
    Adjust_Category = pd.concat([Adjust_Category,temp_sum], axis=1)  

'''
  Concatenate the OLI Charges, Payment, Adjustment
  OLIID is the data frame index
'''
print ('Claim2Rev_QDX_GHI :: Update bill amount and payment amount by removing the charge error and refund')

QDX_OLI_Receipt = pd.concat([Summarized_Claim, Summarized_PADC, Summarized_PtPaid, Summarized_Adjust, Adjust_Category], axis=1)
QDX_OLI_Receipt = QDX_OLI_Receipt.fillna(0.0)

# Calculate the OLI Test Charge and Payment received
QDX_OLI_Receipt['Total Billed'] = QDX_OLI_Receipt.Charge + QDX_OLI_Receipt['Charged in Error']
QDX_OLI_Receipt['Total Payment'] = QDX_OLI_Receipt.ClmAmtRec + QDX_OLI_Receipt['Refund & Refund Reversal']
QDX_OLI_Receipt['Total Adjustment'] = round(QDX_OLI_Receipt.stdP_ClmAmtAdj - QDX_OLI_Receipt['Charged in Error'] - QDX_OLI_Receipt['Refund & Refund Reversal'],2)

###check sum ###
QDX_OLI_Receipt['Recon_Adjustment'] = round((QDX_OLI_Receipt.ClmAmtAdj - QDX_OLI_Receipt.stdP_ClmAmtAdj),2)
QDX_OLI_Receipt['Sum_AdjBreakout'] = round(QDX_OLI_Receipt.stdP_ClmAmtAdj
                                           + QDX_OLI_Receipt['Charged in Error']
                                           - QDX_OLI_Receipt['Refund & Refund Reversal']
                                           - QDX_OLI_Receipt['Total Adjustment'],2)
QDX_OLI_Receipt['Receipt_Checksum'] = round(QDX_OLI_Receipt['Total Billed'] - QDX_OLI_Receipt[['Total Adjustment', 'Total Payment', 'Total Outstanding']].sum(axis=1),2)

#Jan 22: adding the check to payment
QDX_OLI_Receipt['Recon_Pymnt'] = round((QDX_OLI_Receipt['ClmAmtRec'] - QDX_OLI_Receipt['Total Payment']),2)
# need to insert the blank List Price -
# check why OLI has current Ticket number, but the Ticket number is not in the stdClaim and stdPayment file, and the Revenue file

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
temp = pd.merge(OLI_data[['Test','BilledCurrency','ListPrice']], list_price_df[['Test','BilledCurrency','Std_ListPrice']], how='left', left_on=['Test','BilledCurrency'], right_on=['Test','BilledCurrency'])
a = (~OLI_data.Test.isnull() & ~OLI_data.BilledCurrency.isnull() & OLI_data.ListPrice.isnull())
OLI_data.loc[a,'ListPrice'] = temp.loc[a,'Std_ListPrice']

##########################################################################################
#  Gather and clean data for test status and claim status                                #
#                                                                                        #
#  Compute Status to reflect the full Order to Cash status                               #
#  The SFDC Order Status [Canceled, Closed, Order Intake, Processing]                    #
#           Customer Status [Canceled, Closed, In-Lab, Processing, Submitting]           #
#  is insufficient to tell if an order is closed because delivered, canceled or failed   #
##########################################################################################
# keep the Original_OLI_TestDelivered, overwrite TestDelivered = 0 to 1 and Overrider IsClaim Statust
# if there is a current Ticket Number
OLI_data['Original_OLI_TestDelivered'] = OLI_data['TestDelivered']
a = (OLI_data.TestDelivered == 0) & ~(OLI_data.CurrentQDXTicketNumber.isnull())
OLI_data.loc[a,'TestDelivered'] = 1

## Jan 9: update TestDeliveredDate using DateofService if it is null
## Jan 9, need to update into somewhere for OLI_TXN - it should be include as it is reading the Test Delivered Date from OLI_data after this change.
OLI_data.loc[a, 'TestDeliveredDate'] = OLI_data.loc[a,'DateOfService']

# change 'In' to 'in', make the data value consistent
OLI_data.loc[OLI_data.BillingCaseStatusSummary2 == 'Claim In Process','BillingCaseStatusSummary2'] = 'Claim in Process'
'''
select OrderLineItemId, CaseStatusSummaryLevel2, BillingCaseStatusCode 
from StagingDB.Analytics.stgOrderDetail
where OrderLineItemID in ('OL000940965', 'OL001007665')
'''

''' data clean up for Test Delivered -- analysis
summary = OLI_data.pivot_table(index = ['BillingCaseStatusSummary2'], columns = 'Override_TestDelivered', values='OLIID', aggfunc='count', margins=True)
summary = OLI_data.pivot_table(index = 'Override_TestDelivered', values='OLIID', aggfunc='count', margins=True)
summary = OLI_data.pivot_table(index = ['BillingCaseStatusSummary2'], columns = 'IsClaim', values='OLIID', aggfunc='count', margins=True)

summary = OLI_data.pivot_table(index = ['BillingCaseStatusSummary2'], values='IsFullyAdjudicated', aggfunc='sum', margins=True)
summary = OLI_data.pivot_table(index = ['BillingCaseStatusSummary2'], columns = 'IsFullyAdjudicated', values='OLIID', aggfunc='count', margins=True)
'''

## Combining Order & OLI Cancellation reason Get the cancel reason from order/oli cancellation reason and vice versa
a = OLI_data.OrderLineItemCancellationReason.isnull() & ~(OLI_data.OrderCancellationReason.isnull())
OLI_data.loc[a,'OrderLineItemCancellationReason'] = OLI_data.loc[a,'OrderCancellationReason']

a = ~OLI_data.OrderLineItemCancellationReason.isnull() & (OLI_data.OrderCancellationReason.isnull())
OLI_data.loc[a,'OrderCancellationReason'] = OLI_data.loc[a,'OrderLineItemCancellationReason']

#a = OLI_data.FailureCode == ' '
#OLI_data.loc[a,'FailureCode'] = np.NaN
#temp = OLI_data.pivot_table(index = 'FailureCode', values='OLIID', aggfunc='count')

# Derive the Test Order Status using the Customer Status, Order & OLI cancellation reason, Failure Code, Test Delivered Flag
# Scenario: Test Delivered with no cancellation and no failure code
a = (OLI_data.TestDelivered==1)
OLI_data.loc[a,'Status'] = 'Delivered'
OLI_data.loc[a,'Status Notes'] = "Test Delivered = " + OLI_data.loc[a,'TestDelivered'].astype(str)

# Scenario: Test Delivered with either cancellation or failure code
## if TestDelivered == 1, if cancel reason and/or failure reason is not null, still is a TestDelivered
b = ~(OLI_data.OrderLineItemCancellationReason.isnull())
c = ~(OLI_data.FailureMessage.isnull())
x = a & (b | c)
OLI_data.loc[x, 'Status Notes'] = "Test Delivered = " + OLI_data.loc[a,'TestDelivered'].astype(str) + "*"

#output the information for analysis & checking
Delivered_w_issues = OLI_data[x]
output_file = 'Delivered_w_issues.txt'
Delivered_w_issues.to_csv(output_file_path+output_file, sep='|',index=False)

#Scenarios: Test Not Delivered
a = (OLI_data.TestDelivered==0)
OLI_data.loc[a,'Status'] = 'Active'

b = OLI_data.OrderCancellationReason.isnull() & OLI_data.OrderLineItemCancellationReason.isnull()  # Cancellation reason is null
c = OLI_data.FailureMessage.isnull()                                                                  # Failure code is null

# Scenario: Test not delivered, with cancellation reason only
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

# Scenario: Test not delivered, with failure code only
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

# Scenario: Test not delivered because it is in the work
a = (OLI_data.TestDelivered==0) & (OLI_data.Status =='Active')
OLI_data.loc[a,'Status'] = OLI_data.loc[a,'CustomerStatus']

x = pd.Series(OLI_data.CustomerStatus.unique())
a = (OLI_data.TestDelivered==0) & (OLI_data.Status.isin(x[~x.isin(['Closed','Canceled'])]))
OLI_data.loc[a,'Status Notes'] = OLI_data.loc[a,'DataEntryStatus']

## if delivered with cancel and failure notes, should the cancellation and failure notes show?
## check the submit

############################################################ 
# Create the Claim2Rev report                              # 
# Merging the GHI Revenue data to the QDX Claim record line
# Claim2Rev is at the OLI level
############################################################ 
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
                                              'lastappealEntryDt',
                                              #'lastappealDenReason',
                                              #'lastappealDenialLetterDt',
                                              'appealDenReason',
                                              'appealDenReasonDesc', 'appealAmtChg', 'appealAmtChgExp',
                                              'appealAmtAllow', 'appealAmtClmRec', 'appealAmt', 'appealAmtAplRec',
                                              'appealRptDt', 'appealSuccess', 'appealCurrency'
       ]].copy()

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

Claim2Rev.loc[Claim2Rev.appealSuccess=='1','appealResult'] = 'Success'
Claim2Rev.loc[Claim2Rev.appealSuccess=='0','appealResult'] = 'Failed'
Claim2Rev.loc[(Claim2Rev.appealSuccess.isnull()) & ~(Claim2Rev.appealDenReason.isnull()), 'appealResult'] = 'In Process'

QDX_complete_appeal_status = ['Completed','Final Review','Due from Patient']

# Scenario Billing Status is in 'Completed, Due from Patient, Final Review', the Appeal tickets are aborted (removed)
a = Claim2Rev.appealResult == 'In Process'
b = Claim2Rev.BillingCaseStatusSummary2.isin(QDX_complete_appeal_status)
Claim2Rev.loc[a & b, 'appealResult'] = 'Removed'

# Scenario appeal case is open with the current ticket for the OLI, BillingCaseStatusSummary2 is not 'Appeal'
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

############################################################################
#      Adding Test Clinical Criteria to Claim2Rev                          #
############################################################################

#print ('Claim2Rev_QDX_GHI :: adding OLI Utilization :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#just update the Micromets ReportingGroup

#ClinicalCriteria = OLI_data[['OLIID', 'NodalStatus', 'RiskGroup', 'ReportingGroup', 'ClinicalStage',
#                             'EstimatedNCCNRisk', 'SubmittedNCCNRisk', 'FavorablePathologyComparison']].copy()
a = Claim2Rev.ReportingGroup == 'Node Positive (Micromet)'
Claim2Rev.loc[a,'ReportingGroup'] = Claim2Rev.loc[a,'NodalStatus'] 

#Claim2Rev = pd.merge(Claim2Rev, ClinicalCriteria, how='left', on='OLIID')

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
                                          'priorAuthResult','priorAuthReqDesc','priorAuthDate','priorAuthNumber',
                                          'priorAuthResult_Category']]
                    , how='left', left_on='CurrentQDXCaseNumber', right_on='priorAuthCaseNum')

#############################################################################
#Rearranging Columns
Claim2Rev_output = Claim2Rev[['OrderID',
        'OLIID', 'Test', 'TestDeliveredDate', 'CurrentQDXTicketNumber','QDXTickCnt','QDXCaseCnt',
        'BillingCaseStatusSummary2', 'BillingCaseStatusCode', 'BillingCaseStatus',
        'BilledCurrency', 'ListPrice', 'ContractedPrice', 'Total Outstanding',
        'Total Billed',
        'Charge',

#        'ClmAmtRec', 
        'Total Payment',
        'PayorPaid','PatientPaid',

#        'ClmAmtAdj', 'stdP_ClmAmtAdj',
        'Total Adjustment'
        ] + list(Adj_code.groupby([category]).groups.keys()) + [
#        'Recon_Adjustment','Sum_AdjBreakout', 'Receipt_Checksum',
        
#        'Currency', 
        'Revenue', 'AccrualRevenue', 'CashRevenue',
        'USDRevenue', 'USDAccrualRevenue', 'USDCashRevenue', 
        
        'Tier1PayorID','Tier1PayorName', 'Tier1Payor',
        'Tier2PayorID', 'Tier2Payor','Tier2PayorName',  
        'Tier4PayorID', 'Tier4Payor','Tier4PayorName',
        'QDXInsPlanCode', 'LineOfBenefit', 'QDXInsFC', 'FinancialCategory',
            
        'Reportable', 'IsCharge',
        'RevenueStatus',  'TestDelivered', 'IsClaim', 'IsFullyAdjudicated', 'NSInCriteria',
        
        'Status', 'FailureMessage',#'Status Notes',

        'BusinessUnit', 'InternationalArea', 'Division', 'Country', 'OrderingHCPName',
        'Territory', 'TerritoryRegion', 'TerritoryArea',
        'OrderingHCPCity', 'OrderingHCPState', 'OrderingHCPCountry',
        'IsOrderingHCPCTR', 'IsOrderingHCPPECOS', 'OrderStartDate',

#        'OLIStartDate', 'DateOfService',
#        'TicketCnt',
        'ClaimPeriodDate', 'AccountingPeriodCnt',
#        'AccountingPeriodDate_init', 'AccountingPeriodDate_last',
#        'AllowedAmt_Outliner', 'AllowedAmt', 'DeductibleAmt', 'CoinsAmt'

        'appealCaseNum','CurrentQDXCaseNumber',
        'A1_Status', 'A2_Status', 'A3_Status', 'A4_Status', 'A5_Status',
        'ER_Status', 'L1_Status', 'L2_Status', 'L3_Status',
        
        'A1', 'A2', 'A3', 'A4', 'A5', 'ER', 'L1', 'L2', 'L3',
        'Last Appeal level', 'lastappealEntryDt',
        'appealDenReason','appealDenReasonDesc',
        'appealAmtChg', 'appealAmtChgExp',
        'appealAmtAllow', 'appealAmtClmRec', 'appealAmt', 'appealAmtAplRec',
        'appealRptDt', 'appealSuccess', 'appealCurrency', 'appealResult',
        
        'Specialty','NodalStatus','RiskGroup','ReportingGroup','ClinicalStage',
        'EstimatedNCCNRisk', 'SubmittedNCCNRisk','FavorablePathologyComparison',
        
        'priorAuthCaseNum','priorAuthEnteredDt','priorAuthEnteredTime', 'priorAuthDate',
        'priorAuthResult','priorAuthReqDesc','priorAuthDate','priorAuthNumber',
        'priorAuthResult_Category'       
        ]].copy()
        
#Jan 18: extract a dataset in excel for Jodi
#Feb 15: after reviewed with Jodi, change to filter by BilledCurrency instead of Currency 
#  & Claim2Rev.Tier1Payor != 'Unknown' -- hold on to unknown 
# check Order number
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

#        'Currency',
        'Revenue', 'AccrualRevenue', 'CashRevenue',
        'USDRevenue', 'USDAccrualRevenue', 'USDCashRevenue', 
        
        'Tier1PayorID','Tier1PayorName', 'Tier1Payor',
        'Tier2PayorID', 'Tier2Payor','Tier2PayorName',  
        'Tier4PayorID', 'Tier4Payor','Tier4PayorName',
        'QDXInsPlanCode', 'LineOfBenefit', 'QDXInsFC', 'FinancialCategory',
                     
        'Status', #'Status Notes',

        'BusinessUnit', 'InternationalArea', 'Division', 'Country', 'OrderingHCPName',
        'Territory', 'TerritoryRegion', 'TerritoryArea',
        'OrderingHCPCity', 'OrderingHCPState', 'OrderingHCPCountry',
        'IsOrderingHCPCTR', 'IsOrderingHCPPECOS', 

        'appealCaseNum','CurrentQDXCaseNumber',
        'A1', 'A2', 'A3', 'A4', 'A5', 'ER', 'L1', 'L2', 'L3',

#        'A1_Status', 'A2_Status', 'A3_Status', 'A4_Status', 'A5_Status',
#        'ER_Status', 'L1_Status', 'L2_Status', 'L3_Status',

        'Last Appeal level', 'lastappealEntryDt',
        'appealDenReason','appealDenReasonDesc',
        'appealAmtChg', 'appealAmtChgExp',
        'appealAmtAllow', 'appealAmtClmRec', 'appealAmt', 'appealAmtAplRec',
        'appealRptDt', 'appealSuccess', 'appealCurrency', 'appealResult',
        'Specialty','NodalStatus','EstimatedNCCNRisk',

        'priorAuthCaseNum','priorAuthDate',
        'priorAuthDate',
        'priorAuthEnteredDt','priorAuthEnteredTime',
        'priorAuthResult', 'priorAuthResult_Category'
        ]]



############################################################
#     Create a Transpose of the Bill, Adjustment, Revenue  #
############################################################
print ('Claim2Rev_QDX_GHI :: Create a transpose of the Claim2Rev data')
#'OLIDOS','ClaimPeriodDate',
#### ClaimPeriodDate is null for OL000772580 and then the pivot_table excluded it

Claim2Rev_tp = Claim2Rev[(~(Claim2Rev.CurrentQDXTicketNumber.isnull()))].copy()

pull_values = ['AccrualRevenue','CashRevenue','USDAccrualRevenue', 'USDCashRevenue',
               'Total Outstanding', 
#               'Total Payment', # because this is the sum of Payor Paid + PatientPaid + Refund
               'PayorPaid', 'PatientPaid',
#               'Total Billed', # because this is the sum of Charge + CIE
               'Charge']
# + list(Adj_code.groupby([category]).groups.keys())
#                                      'ListPrice', 'ContractedPrice'

TXN_Detail = pd.pivot_table(Claim2Rev_tp, index = ['OLIID','BilledCurrency'], 
                            values = pull_values, fill_value=0)
TXN_Detail = TXN_Detail.stack().reset_index()
TXN_Detail.columns = [['OLIID','Currency','TXNTypeDesc','TXNAmount']]

### read stdClaim and get the QDXCode, QDXAdjustment
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
Summarized_adj['TXNTypeDesc'] = Summarized_adj['GHIAdjustmentCode'] + ':' + Summarized_adj['CategoryDesc']


TXN_Detail = pd.concat([TXN_Detail,Summarized_adj])
# drop the rows with zeros
TXN_Detail = TXN_Detail[~(TXN_Detail.TXNAmount == 0)].sort_values(by=['OLIID'])


## add TXNCategory and TXNSubCategory
TXN_Detail['TXNCategory'] = 'Receipts'
TXN_Detail['TXNSubCategory'] = 'Adjustment'

# overwrite for Revenue & Billing
TXNSubCategory_dict = {'USDAccrualRevenue':'USDAccrual', 'USDCashRevenue':'USDCash'}
for a in list(TXNSubCategory_dict.keys()):
    TXN_Detail.loc[(TXN_Detail.TXNTypeDesc==a),'TXNCategory'] = 'USDRevenue'
    TXN_Detail.loc[(TXN_Detail.TXNTypeDesc==a),'TXNSubCategory'] = TXNSubCategory_dict.get(a)
    
TXNSubCategory_dict = {'AccrualRevenue':'Accrual', 'CashRevenue':'Cash'}
for a in list(TXNSubCategory_dict.keys()):
    TXN_Detail.loc[(TXN_Detail.TXNTypeDesc==a),'TXNCategory'] = 'Revenue'
    TXN_Detail.loc[(TXN_Detail.TXNTypeDesc==a),'TXNSubCategory'] = TXNSubCategory_dict.get(a)

TXNSubCategory_dict = {'Charge':'Charge', 'GH04:Charged in Error':'Charged in Error'}
for a in list(TXNSubCategory_dict.keys()):
    TXN_Detail.loc[TXN_Detail['TXNTypeDesc'] == a,'TXNCategory'] = 'Billing'
    TXN_Detail.loc[TXN_Detail['TXNTypeDesc'] == a,'TXNSubCategory'] = TXNSubCategory_dict.get(a)
TXN_Detail.loc[TXN_Detail['TXNTypeDesc'] == 'GH04:Charged in Error','TXNTypeDesc'] = 'Charged in Error'

TXN_Detail.loc[TXN_Detail['TXNTypeDesc'].isin(['Total Outstanding']),'TXNSubCategory'] = 'Outstanding'
TXN_Detail.loc[TXN_Detail['TXNTypeDesc'].isin(['Total Outstanding']),'TXNTypeDesc'] = 'Outstanding'

TXN_Detail.loc[TXN_Detail['TXNTypeDesc'].isin(['PayorPaid','PatientPaid','GH09:Refund & Refund Reversal']),'TXNSubCategory'] = 'Payment'
TXN_Detail.loc[TXN_Detail['TXNTypeDesc'] == 'GH09:Refund & Refund Reversal','TXNTypeDesc'] = 'Refund & Refund Reversal'


## Adding the OLI detail
OLI_detail = OLI_data[['OLIID','Test','TestDeliveredDate'
                        ,'Tier1Payor','Tier2Payor','Tier2PayorID','Tier4Payor','FinancialCategory','LineOfBenefit'
                        , 'TerritoryRegion', 'OrderingHCPState']]


TXN_Detail = pd.merge(TXN_Detail,OLI_detail,
                       how='left', left_on='OLIID', right_on='OLIID')

#########################################
#   Add the Payor View Set assignment   #
#########################################
prep_file_name = "Payor-ViewSetAssignment.xlsx"


Payor_view = pd.read_excel(prep_file_path+prep_file_name, sheetname = "SetAssignment", parse_cols="B:C", encoding='utf-8-sig')

for i in Payor_view.Set.unique() :
    #print (i)
    code = Payor_view[Payor_view.Set==i].Tier2PayorID
    Claim2Rev_output.loc[Claim2Rev_output.Tier2PayorID.isin(list(code)),i] = '1'
    
    code = Payor_view[Payor_view.Set==i].Tier2PayorID
    TXN_Detail.loc[TXN_Detail.Tier2PayorID.isin(list(code)),i] = '1'
    

#########################################
#   Special Data set for users          #
#########################################

#PreClaim Status for Ron's
Cond = (Claim2Rev.BusinessUnit == 'Domestic') & \
        (Claim2Rev.TestDeliveredDate >= '2017-01-01') & \
        ((Claim2Rev.Test == 'IBC') | (Claim2Rev.Test == 'Prostate'))
       
PreClaim_Status_SalesOps = Claim2Rev[Cond][['OrderID','OLIID', 'Test','priorAuthResult']]

#IBC Appeals Detail 
Cond = (Claim2Rev.BusinessUnit == 'Domestic') & \
       (Claim2Rev.Test == 'IBC') & (~Claim2Rev.appealResult.isnull())
       
IBC_Appeals_Detail = Claim2Rev_output[Cond][['Tier1PayorID','Tier1PayorName',
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

#Prostate Appeals Detail       
Cond = (Claim2Rev.BusinessUnit == 'Domestic') & \
       (Claim2Rev.Test == 'Prostate') & (~Claim2Rev.appealResult.isnull())
       
Prostate_Appeals_Detail = Claim2Rev_output[Cond][['Tier1PayorID','Tier1PayorName',
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


###############################################
#     Write the columns into a excel file     #
###############################################

print ('Claim2Rev_QDX_GHI :: write OLITXT_Detail', len(TXN_Detail), 'rows :: start ::', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

output_file = 'OLI_TXN_Detail.txt'
TXN_Detail.to_csv(output_file_path+output_file, sep='|',index=False)


print ('Claim2Rev_QDX_GHI :: write Claim2Rev report ', len(Claim2Rev), 'rows :: start ::', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

output_file = 'Claim2Rev.txt'
Claim2Rev_output.to_csv(output_file_path+output_file, sep='|',index=False)

output_file = 'Claim2Rev_USD.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl', date_format='yyyy/mm/dd')
Claim2Rev_USD_excel.to_excel(writer, sheet_name='Claim2Rev', index = False)
writer.save()
writer.close()

'''
output_file = 'PreClaim_Status_SalesOps.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl', date_format='yyyy/mm/dd')
PreClaim_Status_SalesOps.to_excel(writer, index = False)
writer.save()
writer.close()
'''

output_file = 'IBC_Appeals_Detail.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl', date_format='yyyy/mm/dd')
IBC_Appeals_Detail.to_excel(writer, index = False)
writer.save()
writer.close()

output_file = 'Prostate_Appeals_Detail.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl', date_format='yyyy/mm/dd')
Prostate_Appeals_Detail.to_excel(writer, index = False)
writer.save()
writer.close()

'''
output_file = 'Claim2Rev_Check.xlsx'
writer = pd.ExcelWriter(QDX_file_path+output_file, engine='openpyxl', date_format='yyyy/mm/dd')
Claim2Rev.to_excel(writer, sheet_name='Claim2Rev', index = False)
writer.save()
writer.close()
'''

print ('Hurray !!! Done Done Done',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

