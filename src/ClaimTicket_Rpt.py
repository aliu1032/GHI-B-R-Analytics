'''
@author: aliu

Create the ClaimTicket Report
Purpose of this report is to combine the Quadax stdClaim and stdPayment file,
and provide a full view of an OLI from initial billing ticket, and payment and adjustment history for the OLI
Also include the GHI revenue recognized for the ticket, and the payor of the current ticket and HCP pulled from OrderLineDetail

Sep 20: Combining QDX claim and payment lines to take a view of a claim cycle
Sep 21: Getting GHI Revenue data for claim cycle
Oct 3: Get Test Delivered Date, HCP information from OrderLineDetail
Nov 3: Using the Payor information from Revenue file for TXN_Detail, because, Revenue at the ticket is recognized to different payors
       For Claim, Payment & Adjustment: Ideal is to use the Payor of the Ticket 
       However, to match the BI OLI reports, need to associate the Current Ticket Payor to all tickets of an OLI.
Nov 10: Include the payment and claim wo OLI from the QDX std files. Thus the amount will show up in transactions long view and be include to the Accounting Period view

Nov 22:do not provide the Reportable, Test Delivered, IsClaim, IsFullyAdjudicated flag with Ticket. These values applicable to OLI. Giving it out with Ticket could cause double counting. 

Dec 5: Create a view that show the OLI Journal using QDX data.
       Taking in the QDX Payment Insurance Company code to trace the payor payment source. This (may be) needed for allowable analysis

Dec 11: Adding Test Criteria information to root cause revenue impacted adjustment
Dec 15: Getting Billing Case Status from QDX Case file

Data source:
Payment & Adjustment : from Quadax stdClaim and stdPayment files,
Revenue: GHI StagingDB.Analytics.mvwRevenue
OrderLineDetail: Test, Payer information : GHI StagingDB.Analytics.stgOrderLineDetail


## run the whole thing in Python to see if I get the Payer info into Revenue

'''
import pandas as pd
from datetime import datetime

import project_io_config as cfg
refresh = cfg.refresh

pd.options.display.max_columns=50

print('ClaimTicket Report :: start :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

###############################################################
#   Read QDX Claim and Payment (Receipt) data                 #
###############################################################
print('ClaimTicket Report :: read QDX data :: start :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

from data import GetQDXData as QData
Claim_bill = QData.stdClaim('ClaimTicket', cfg.input_file_path, refresh)
Claim_pymnt = QData.stdPayment('ClaimTicket', cfg.input_file_path, refresh)
Claim_case = QData.claim_case_status('QDXClaim_CaseStatus', cfg.input_file_path, refresh)

###############################################################
#   Read GHI Revenue Data and OrderLineDetail                 #
############################################################### 

print('ClaimTicket Report :: read GHI data :: start :: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

# when prepare report summarized by OLI+Ticket, get the Payer info from Revenue.
# OrderLineDetail has payer of the current ticket
# Revenue has the payer of the ticket

from data import GetGHIData as GData
Revenue_data = GData.revenue_data('ClaimTicket', cfg.input_file_path, refresh)
OLI_data = GData.OLI_detail('Claim2Rev', cfg.input_file_path, refresh)
SFDC_Payors = GData.getPayors('Claim2Rev', cfg.input_file_path, refresh)

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

temp = Claim_bill.groupby(['OLIID','TicketNumber'])['CaseNumber']  # this return a SeriesGroupBy with CaseNumber
d = temp.apply(lambda x : x.keys()[x.values.argmax()])
temp1 = Claim_bill.loc[d][['OLIID','TicketNumber','CaseNumber','TXNDate']]

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

# Update TestDeliveredDate using DateofService if it is not available
## does it matter whether the TicketNumber is null or not??
# need to update into somewhere for OLI_TXN - it should be include as it is reading the Test Delivered Date from OLI_data after this change.
a = (OLI_data.TestDeliveredDate.isnull()) & ~(OLI_data.CurrentQDXTicketNumber.isnull())
OLI_data.loc[a, 'TestDeliveredDate'] = OLI_data.loc[a,'DateOfService']

###############################################################
#   Read Adjustment Code & Write off analysis buckets         #
############################################################### 

prep_file_name = "QDX_ClaimDataPrep.xlsx"

Adj_code = pd.read_excel(cfg.prep_file_path+prep_file_name, sheet_name = "AdjustmentCode", usecols="A,C:E,G", encoding='utf-8-sig')
Adj_code.columns = [cell.strip() for cell in Adj_code.columns]

# define the value to group QDX adjustment
#category = 'AdjustmentGroup'
category = 'BR_Category'
#category = 'CategoryDesc'

############################################################### 
#  Aggregate Revenue numbers to the Accounting Period         #
#  Filter the scenario that need checking and                 #
###############################################################
'''
## Oct 19: to-do: should include the ticket wo OLI as this script is at the Ticket level
#think of a way to match the OLI with null Ticket in Rev with the Ticket with missing OLI in Receipt
Claim_pymnt_wo_OLI = Claim_pymnt[((Claim_pymnt.OLIID.isnull()) | (Claim_pymnt.OLIID == 'NONE'))]
Claim_pymnt = Claim_pymnt[(~(Claim_pymnt.OLIID.isnull()) & (Claim_pymnt.OLIID != 'NONE'))]

Claim_wo_OLI = Claim_bill[((Claim_bill.OLIID.isnull()) | (Claim_bill.OLIID == 'NONE'))]
Claim_bill = Claim_bill[((~Claim_bill.OLIID.isnull()) & (Claim_bill.OLIID != 'NONE'))]

## Select and drop the OrderLineItem that has multiple Test values, ignore the one with Unknown. Export the value for reporting bug
## pivot with OLIID, count the number of unique Test values
a = pd.pivot_table(Revenue_data, index=['OLIID'], values=['Test'], aggfunc = lambda Test: len(set(Test.unique()) - set(['Unknown'])))
b = list(a[(a.Test > 1)].index) # find the OrderLineItemID with inconsistent test value


## Select and drop the OrderLineItem that has multiple CurrencyCode, ignore the null CurrencyCode. Export the value for reporting bug
a = pd.pivot_table(Revenue_data, index=['OLIID'], values=['CurrencyCode'], aggfunc = lambda CurrencyCode: len(CurrencyCode.unique())-sum(pd.isnull(CurrencyCode)))
b = b + list(a[(a.CurrencyCode> 1)].index) # find the OrderLineItemID with inconsistent currency value

error_Revenue_data = Revenue_data[(Revenue_data['OLIID'].isin(b))]
'''
Revenue_data = Revenue_data[(Revenue_data.IsRevenueReconciliationAdjustment == '0')]

# Oct 16: keep the revenue records though there is inconsistent Test or inconsistent currency values
# when rolling up by OLI, it will have all the contributing revenue numbers. and the Test and currency values are taken from the OrderLineDetail
#Revenue_data = Revenue_data[(~Revenue_data['OLIID'].isin(b))].copy()
#Revenue_data[(Revenue_data.OLIID.str.startswith('OL000974373|836502'))]

# Nov14: remove the adjustment rows. Need to diff the file and identify the adjustment row in 2017
# BI Revenue dash board excluded the ReconciliationAdjustment = 1.
# The Reconciliation Adjustment are called out in 2016, the reconciliation adjustment are not flagged starting in 2017
# Impact: the adjustment could inflate the Payer OLI revenue numbers.
# but the reconciliation should be in the Revenue dash board to match the Finance report.
# Net net requirement: Revenue Reconciliation Adjustment should be include to Revenue dashbaord, and filter out for OLI Claim2Rev

########################################################################################
#   Flip the sign of Payment and Adjustment (exclude CIE)                              #
########################################################################################

# Add the AccountingPeriod columns using the TXNAcctPeriod value
# This is use to append with Revenue data to get a journal view
Claim_pymnt['AccountingPeriod'] = Claim_pymnt.TXNAcctPeriod
Claim_bill['AccountingPeriod'] = Claim_bill.TXNAcctPeriod

## Reverse the sign of TXNAmt for Adjustment, except CIE
a = ((Claim_pymnt.TXNAmount != 0.0) &
     (Claim_pymnt.TXNType.isin(['AC','AD'])) &
     ~(Claim_pymnt.QDXAdjustmentCode.isin(['C6','CCD']))
    )
Claim_pymnt.loc[a, 'TXNAmount'] = Claim_pymnt.loc[a, 'TXNAmount'] * -1

## Reverse the sign of TXNAmt for Payment
a = ((Claim_pymnt.TXNAmount != 0.0) & (Claim_pymnt.TXNType.isin(['RI','RP'])))
Claim_pymnt.loc[a, 'TXNAmount'] = Claim_pymnt.loc[a, 'TXNAmount'] * -1

########################################################################################
#   Identify the Ticket Payer                                                          #
########################################################################################
# Find the index of last AccountingPeriodDate for OLIID+TicketNumber.
# Pull the Payer information from the last AccountingPeriod and assume this is the Payer for this OLIID+Ticket for all Accounting Period
## for some reason, SFDC lost the OLI information and thus not available in EDW OLI detail, although there is Revenue & Payment Transactions
## therefore need to pull the test and test delivered date from revenue file to patch
## To-do: Follow up with Laura, Mary, Jen, OR000722222 (OLIID: OL000730539) the order line item is not there in SFDC while work order, result, specimens exist.
# Pull only the Tier4PayorID from Revenue. For GNAM, users would like the current SFDC Payor Hierarchy instead of the Hierarchy as of when a claim is submitted

temp = Revenue_data.groupby(['OLIID','TicketNumber']).agg({'AccountingPeriodDate':'idxmax'})
pull_rows = temp.AccountingPeriodDate

Ticket_Payor_Test = Revenue_data.loc[pull_rows][['OLIID','TicketNumber',
                                           'Currency','TestDeliveredDate','Test',
#                                           'Tier1Payor', 'Tier1PayorID', 'Tier1PayorName',
#                                           'Tier2Payor', 'Tier2PayorID','Tier2PayorName',
                                           'Tier4Payor', 'Tier4PayorName',
                                           'Tier4PayorID', 
                                           'QDXInsPlanCode','QDXInsFC', 'LineOfBenefit', 'FinancialCategory',
                                           'ClaimPayorBusinessUnit', 'ClaimPayorInternationalArea', 'ClaimPayorDivision', 'ClaimPayorCountry']]

OLI_Payor = OLI_data[['OLIID','CurrentQDXTicketNumber',#'CurrentQDXCaseNumber',
#                     'Tier1Payor','Tier1PayorID','Tier1PayorName',
#                     'Tier2Payor','Tier2PayorID','Tier2PayorName',
                      'Tier4Payor','Tier4PayorName',
                      'Tier4PayorID',
                      'QDXInsPlanCode', 'QDXInsFC','LineOfBenefit', 'FinancialCategory'                             
                    ]]

## example: ticket number = 680557, Quadax has the claim ticket but no reference to the OLI
## GHI has nothing of this ticket in the Revenue nor the OLI table
## In this case, have to get the Currency, Test and Payer information from the QDX file
## [Claim_bill.OLIID=='NONE']
## OR000626537 (OLIID: OL000642948, Ticket 612550) no prebilling, billing case in SFDC.
## There is test delivered date, result. And QDX reports received a payment for it
Claim_Payor_Test = Claim_bill[['OLIID','TicketNumber',
                               'TXNCurrency','OLIDOS','Test',
#                               'TicketInsComp_GHICode',
                               'TicketInsPlan_GHICode',
                               'TicketInsPlan_QDXCode'#,'TicketInsComp_QDXCode','TicketInsPlanName'
                               ]].copy()
                               
Claim_Payor_Test.columns = ['OLIID', 'TicketNumber',
                             'BilledCurrency','TestDeliveredDate','Test',
#                             'Tier2PayorID',
                             'Tier4PayorID',
                             'QDXInsPlanCode']

#OLI_info_1 is a set of OLI data appending the AccountPeriod Report. Basic OLI header detail minus Payor
OLI_Test_Info = OLI_data[['OLIID'
                          , 'BilledCurrency','TestDeliveredDate', 'Test'
                          , 'OrderStartDate','OLIStartDate'
                          ]]

OLI_Territory = OLI_data[['OLIID'
                       , 'Territory','BusinessUnit','InternationalArea','Division','Country']]

OLI_HCPHCO = OLI_data[['OLIID'
                       , 'OrderingHCPName','OrderingHCPCity', 'OrderingHCPState', 'OrderingHCPCountry'
                       , 'IsOrderingHCPCTR', 'IsOrderingHCPPECOS', 'Specialty'
                       , 'OrderingHCO', 'OrderingHCOCity', 'OrderingHCOState', 'OrderingHCOCountry']]

OLI_patientcriteria = OLI_data[['OLIID'
                                , 'NodalStatus','EstimatedNCCNRisk','SubmittedNCCNRisk','ReportingGroup','RiskGroup']]

OLI_status = OLI_data[['OLIID',
                       'OrderStatus','TestDelivered',
                       'OrderCancellationReason','OrderLineItemCancellationReason','FailureMessage']]
                    
########################################################################################
#   AccountPeriod_Rpt: ClaimTicket2Rev Transactions Wide                               #
#   Each row is a OLI + Ticket + Accounting Period                                     #
#   Group by OLI + Ticket Number + Accounting Period                                   #
#   to get the Revenue Payment, Adjustment per Accounting Period                       #
#   Payment, adjustment received per accounting period                                 #
#   up to the data extraction date                                                     #
########################################################################################
''' Roll up Revenue to Accounting Period '''
print('ClaimTicket Transaction :: summarize revenue to ticket :: start :: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

aggregations = {
    'TotalRevenue': 'sum',
    'TotalAccrualRevenue':'sum',
    'TotalCashRevenue': 'sum',
    'TotalUSDRevenue': 'sum',
    'TotalUSDAccrualRevenue' :'sum',
    'TotalUSDCashRevenue' : 'sum',
#    'ClaimPeriodDate' : 'max',
    }
### Calculate the revenue number per group: OLIID and Ticket Number, the index will become (OLIID + TicketNumber)
Summarized_Revenue = Revenue_data.groupby(['OLIID','TicketNumber','AccountingPeriod']).agg(aggregations)
Summarized_Revenue.columns = ['Revenue','AccrualRevenue','CashRevenue',
                              'USDRevenue', 'USDAccrualRevenue', 'USDCashRevenue']
                              #'ClaimPeriodDate'
Summarized_Revenue = Summarized_Revenue.reset_index()

########################################################################################
#   Roll up the QDX stdclaim information to Accounting Period                          #
#   Calculate the Total charge amount                                                  #
########################################################################################
print ('Claim2Rev_QDX_GHI :: Aggregate Charge amount and Outstanding per OLI+Ticket+AccountingPeriod')

aggregations = {
    'TXNAmount': 'sum',
    }

Summarized_Claim = Claim_bill.groupby(['OLIID','TicketNumber','TXNAcctPeriod']).agg(aggregations)
Summarized_Claim.rename(columns = {'TXNAmount':'Charge'}, inplace=True)
            
########################################################################################
#   Group by OLI + Ticket Number + Accounting Period to get                            #
#         - Payor Paid, Patient Paid                                                   #
#   The total payment, adjustment received up to the data extraction date              #
########################################################################################
print('ClaimTicket Report :: summarize charge, payment & adjustment numbers to ticket :: start :: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

''' Calculate the Payer Paid Amount for an OLI + Ticket + AccountingPeriod from QDX Payment Data '''  
aggregations = {
    'TXNAmount' :'sum',
    }

Summarized_PayorPaid = Claim_pymnt[(Claim_pymnt.TXNType=='RI')].groupby(['OLIID','TicketNumber','TXNAcctPeriod']).agg(aggregations)
Summarized_PayorPaid.columns = ['PayorPaid']

''' Calculate the Patient Paid Amount '''  
aggregations = {
    'TXNAmount' : 'sum'
    }
Summarized_PtPaid = Claim_pymnt[(Claim_pymnt.TXNType=='RP')].groupby(['OLIID','TicketNumber','TXNAcctPeriod']).agg(aggregations)
Summarized_PtPaid.columns = ['PatientPaid']

''' Calculate the Adjustment per OLI + Ticket + Accounting Period + AdjustmentGroup  '''  
temp_adjustment = Claim_pymnt[Claim_pymnt.TXNType.isin(['AC','AD'])].copy()
Summarized_Adjust = temp_adjustment.pivot_table(index = ['OLIID','TicketNumber','TXNAcctPeriod'], columns = 'BR_Category', values='TXNAmount', aggfunc='sum')

##think if there needs to group by GHI Category ###
########################################################################################
#  Generate the Claim_AcctPeriod_Rpt, a wide format with various amounts               #
#  Merge the summarized claim, payment, adjustment, adjustment group and revenue       #
#  Each row is OLI# + Ticket # + Accounting Period                                     #
#  Journal Entry for 'human' not for Tableau                                           #
########################################################################################

print('ClaimTicket Report :: AccountingPeriod grain :: start :: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )
# ticket Charge Amt, Payor Paid, Patient Paid, Adjustment Group: CIE, Refund, Revenue Impact, GHI Adj, Insuarnce Adj, All other

AcctPeriod_Rpt = pd.concat([Summarized_Claim, Summarized_PayorPaid, Summarized_PtPaid, Summarized_Adjust], axis=1)
AcctPeriod_Rpt.reset_index(inplace=True)
AcctPeriod_Rpt.rename(columns = {'TXNAcctPeriod' : 'AccountingPeriod'}, inplace=True)
AcctPeriod_Rpt.AccountingPeriod = AcctPeriod_Rpt.AccountingPeriod.str.upper()  ## FEB 2016 vs Feb 2016
AcctPeriod_Rpt.fillna(0.0, inplace=True)  # to avoid calculation with NaN resulting NaN

''' Calculate the Payment, Billed, Adjustment per OLI + TicketNumber + Accounting Period '''
AcctPeriod_Rpt['Payment'] = AcctPeriod_Rpt[['PayorPaid','PatientPaid']].sum(axis=1)+AcctPeriod_Rpt['Refund & Refund Reversal']
AcctPeriod_Rpt['Billed'] = AcctPeriod_Rpt[['Charge','Charged in Error']].sum(axis=1)

''' Total Adjustment = all AC & AD, excluding Charged in Error and Refund '''
temp_adjust_cat = list(Adj_code[category].unique())
temp_adjust_cat.remove('Charged in Error')
temp_adjust_cat.remove('Refund & Refund Reversal')
AcctPeriod_Rpt['Adjustment'] = AcctPeriod_Rpt[temp_adjust_cat].sum(axis=1)

''' Merge the Revenue, Bill, Payment, Adjustment '''
AcctPeriod_Rpt = pd.merge(Summarized_Revenue, AcctPeriod_Rpt, how='outer', on=['OLIID','TicketNumber','AccountingPeriod'])
AcctPeriod_Rpt.fillna(0.0, inplace=True)

AcctPeriod_Rpt['AccountingPeriodDate'] = pd.to_datetime(AcctPeriod_Rpt.AccountingPeriod, format="%b %Y")

########################################################################################
#   Extract the OLI+Ticket Outstanding Amount from QDX stdclaim                        #
#   The outstanding amount is as per the date with stdClaim report is run              #
########################################################################################
aggregations = {
    'ClmTickBal' : 'sum',
    }

Summarized_Outstanding = Claim_bill.groupby(['OLIID','TicketNumber']).agg(aggregations)
Summarized_Outstanding.rename(columns = {'ClmTickBal':'Outstanding'}, inplace=True)
Summarized_Outstanding = Summarized_Outstanding[Summarized_Outstanding.Outstanding != 0]
Summarized_Outstanding.reset_index(inplace=True)
#a = Summarized_Outstanding[Summarized_Outstanding.TicketNumber == 2].index.get_level_values('OLIID').unique()
#b = Claim_bill[(Claim_bill.OLIID.isin(a)) & (Claim_bill.TXNCurrency == 'USD')]['OLIID']

# Set AccountingPeriod to the max() for outstanding rows.
Rpt_AcctPeriod = AcctPeriod_Rpt['AccountingPeriodDate'].max().strftime("%b %Y")
Rpt_AcctPeriodDate = AcctPeriod_Rpt[AcctPeriod_Rpt['AccountingPeriodDate']==Rpt_AcctPeriod]['AccountingPeriodDate'].iloc[0]
Summarized_Outstanding['AccountingPeriod'] = Rpt_AcctPeriod.upper()
Summarized_Outstanding['AccountingPeriodDate'] = Rpt_AcctPeriodDate

''' Append the Outstanding '''
AcctPeriod_Rpt = pd.merge(AcctPeriod_Rpt, Summarized_Outstanding, how='outer', on=['OLIID','TicketNumber','AccountingPeriod','AccountingPeriodDate'])
AcctPeriod_Rpt.fillna(0.0, inplace=True)  

#OL001080997

'''adding Payor of the ticket and OLI info '''
AcctPeriod_Rpt = pd.merge(AcctPeriod_Rpt, Ticket_Payor_Test, how='left', on = ['OLIID','TicketNumber'])
AcctPeriod_Rpt = pd.merge(AcctPeriod_Rpt, OLI_Test_Info, how = 'left', on = ['OLIID'])
AcctPeriod_Rpt = pd.merge(AcctPeriod_Rpt, OLI_Territory, how = 'left', on = ['OLIID'])
AcctPeriod_Rpt = pd.merge(AcctPeriod_Rpt, TickCnt, how='left', left_on=['OLIID'], right_index=True)

# Merge the Test & Test Delivered Date value from Revenue and OLI. Needs to combine since the data are not always available in the files
# *_x is from Revenue Ticket (Ticket_Payor_test)
# *_y is from OLI (OLI_Test_Info)
AcctPeriod_Rpt['Test'] = AcctPeriod_Rpt['Test_x']
AcctPeriod_Rpt.loc[AcctPeriod_Rpt.Test.isnull(), 'Test'] = AcctPeriod_Rpt.loc[AcctPeriod_Rpt.Test.isnull(), 'Test_y']

AcctPeriod_Rpt['TestDeliveredDate'] = AcctPeriod_Rpt['TestDeliveredDate_x']
AcctPeriod_Rpt.loc[AcctPeriod_Rpt.Test.isnull(), 'TestDeliveredDate'] = AcctPeriod_Rpt.loc[AcctPeriod_Rpt.Test.isnull(), 'TestDeliveredDate_y']

AcctPeriod_Rpt.drop(['Test_x','Test_y','TestDeliveredDate_x', 'TestDeliveredDate_y'], axis=1, inplace=True)

AcctPeriod_Rpt = pd.merge(AcctPeriod_Rpt, Claim_Payor_Test, how='left', on =['OLIID','TicketNumber'])

# X values are from Revenue Ticket
# Y values are from QDX Claim
# this is needed for QDX Claims which do not have no reference to OLI#, the information is not available from the GHI Revenue nor OLI file
for a in list(['BilledCurrency','Test','TestDeliveredDate','Tier4PayorID','QDXInsPlanCode']):
    #'Tier2PayorID',
    first = a + '_x'
    backup = a +'_y'
    AcctPeriod_Rpt[a] = AcctPeriod_Rpt[first]
    AcctPeriod_Rpt.loc[AcctPeriod_Rpt[a].isnull(),a] = AcctPeriod_Rpt.loc[AcctPeriod_Rpt[a].isnull(), backup]
    AcctPeriod_Rpt.drop([first, backup], axis=1, inplace=True)

# Merge the current Payor Hierarchy from SFDC for GNAM report 
AcctPeriod_Rpt = pd.merge(AcctPeriod_Rpt, SFDC_Payors, how='left', on='Tier4PayorID')

# Rearranging and sort data for output 
AcctPeriod_Rpt = AcctPeriod_Rpt[[
                 'OLIID', 'TicketNumber','QDXTickCnt'
                 ,'AccountingPeriod','AccountingPeriodDate'
                 ,'OrderStartDate' #,'ClaimPeriodDate'
                 ,'OLIStartDate', 'TestDeliveredDate', 'Currency'
                 , 'Revenue', 'AccrualRevenue', 'CashRevenue'
                 , 'USDRevenue', 'USDAccrualRevenue', 'USDCashRevenue'
                 , 'Billed', 'Charge', 'Charged in Error'
                 , 'Payment', 'PayorPaid', 'PatientPaid', 'Refund & Refund Reversal'
                 , 'Adjustment', 'Insurance Adjustment', 'Revenue Impact Adjustment','GHI Adjustment','All other Adjustment'
                 , 'Outstanding'
                 , 'Tier1Payor', 'Tier1PayorID', 'Tier1PayorName', 'Tier2Payor'
                 , 'Tier2PayorID', 'Tier2PayorName'
                 , 'Tier4Payor', 'Tier4PayorID', 'Tier4PayorName'               
                 , 'QDXInsPlanCode', 'LineOfBenefit'
                 , 'FinancialCategory', 'ClaimPayorBusinessUnit'
                 , 'ClaimPayorInternationalArea', 'ClaimPayorDivision'
                 , 'ClaimPayorCountry'
                 , 'Test', 'BilledCurrency', 'BusinessUnit'
                 , 'InternationalArea', 'Division', 'Country', 
                ]].sort_values(by=['OLIID','TicketNumber','AccountingPeriodDate'])
                
#############################################################################################
# Create a Transaction Journal view                                                         #
#                                                                                           #
# to show transaction activities happen during a selected accounting period                 #
# useful for report and analysis by Accounting period                                       #
# keep the adjustment detail for the ability to drill down different transaction categories #
#                                                                                           #
#############################################################################################
'''Extract the Revenue transactions '''
'''Including the Total for comparison'''
print('ClaimTicket Report :: create a journal of revenue, charge, payment, adjustment for rev vs receipt analysis :: start :: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

temp_rev = pd.pivot_table(Revenue_data, index = ['OLIID','TicketNumber', 'AccountingPeriod'],
                          values = ['TotalAccrualRevenue', 'TotalCashRevenue'])
#'TotalRevenue',
#                                    'TotalUSDRevenue','TotalUSDAccrualRevenue', 'TotalUSDCashRevenue'
# remove the USD since the comparison applicable to TXN currency                                                 
temp_rev.rename(columns = {'TotalRevenue':'Revenue'
                           , 'TotalAccrualRevenue':'AccrualRevenue', 'TotalCashRevenue':'CashRevenue'
                           , 'TotalUSDRevenue':'USDRevenue'
                           ,'TotalUSDAccrualRevenue':'USDAccrualRevenue', 'TotalUSDCashRevenue':'USDCashRevenue'}, inplace=True)
temp_rev = temp_rev.stack().reset_index()
temp_rev.columns = ['OLIID','TicketNumber','TXNAcctPeriod', 'TXNType','TXNAmount']

# drop the rows with zeros
temp_rev = temp_rev[~(temp_rev.TXNAmount == 0.0)].sort_values(by=['OLIID'])
temp_rev.loc[temp_rev.TXNType=='Revenue','TXNLineNumber'] = int(-4)
temp_rev.loc[temp_rev.TXNType!='Revenue','TXNLineNumber'] = int(-5)

#TXNType_dict = {'Revenue':'Total','AccrualRevenue':'Subtotal', 'CashRevenue':'Subtotal'}
#for a in list(TXNType_dict.keys()):
####    temp_rev.loc[(temp_rev.TXNType==a),'TXNType'] = TXNType_dict.get(a)
#    temp_rev.loc[(temp_rev.TXNType==a),'TXN Subcategory'] = TXNType_dict.get(a)
#    temp_rev.loc[(temp_rev.TXNType==a),'TXN Category'] = 'Revenue'
temp_rev['TXN Category'] = 'Revenue'


''' pull the bill amount, total payment from ClaimTicket2Rev, which each row is OLI + Ticket + Accounting Period
    The bill amount and payment amount per each OLI + Ticket + Accounting Period
'''
## this include payment from Ticket without OLI reference
##remove the total bill, total payment, total outstanding, have Tableau calculate the subtotal
'''
temp_bp = pd.pivot_table(AcctPeriod_Rpt, index=['OLIID','TicketNumber','AccountingPeriod','BilledCurrency'],
                         values = ['Billed','Payment'])
temp_bp = temp_bp.stack().reset_index()
temp_bp.columns = ['OLIID','TicketNumber','TXNAcctPeriod','TXNCurrency', 'TXNType','TXNAmount']
temp_bp.loc[(temp_bp.TXNType=='Billed'),'TXN Category'] = 'Billing'
#temp_bp.loc[(temp_bp.TXNType=='Total Billed'),'TXN Subcategory'] = 'Total' ###
temp_bp.loc[(temp_bp.TXNType=='Payment'),'TXN Category'] = 'Payment'
#temp_bp.loc[(temp_bp.TXNType=='Total Payment'),'TXN Subcategory'] = 'Total' ##3
temp_bp.loc[(temp_bp.TXNType=='Total Billed'),'TXNLineNumber'] = int(-1)
temp_bp.loc[(temp_bp.TXNType=='Total Payment'),'TXNLineNumber'] = int(-2)
'''

''' Extract the Claim Amount '''
## Extract the claim amount from stdClaim. This is the charge amount issued with the Ticket at the TXNAcctPeriod
temp_claim = Claim_bill[['OLIID', 'TicketNumber','TXNAcctPeriod'
                         , 'TXNCurrency','TXNAmount', 'TXNLineNumber','TXNType']].copy()
temp_claim['TXN Category'] = 'Billing'
#temp_claim['TXN Subcategory'] = 'Subtotal'
temp_claim['TXNType'] = 'Charge'

## Extract the outstanding amount stdClaim. This is the outstanding amount of the ticket at the date when the file is generated
temp_outstanding = Claim_bill[['OLIID', 'TicketNumber'
                        ,'TXNCurrency', 'ClmTickBal', 'TXNLineNumber']].copy()
temp_outstanding.rename(columns = {'ClmTickBal':'TXNAmount'}, inplace=True)
temp_outstanding['TXNAcctPeriod'] = Rpt_AcctPeriod
temp_outstanding['TXNType'] ='Outstanding'
temp_outstanding['TXN Category'] = 'Outstanding'
#temp_outstanding['TXN Subcategory'] = 'Subtotal'
temp_outstanding['TXNLineNumber'] = int(-3)

''' Extract the Payment, Adjustment (includes Refund and Charge Error) '''
# Extract Payment, Adjustment, Refund, Charge Error from stdPayment
temp_pymnt = Claim_pymnt[['OLIID', 'TicketNumber','TXNAcctPeriod'
                          , 'TXNCurrency'
                          , 'TXNAmount', 'TXNType','TXNLineNumber'
                          , 'QDXAdjustmentCode', 'Description'
                          , 'GHIAdjustmentCode','CategoryDesc'
                          , 'BR_Category'
                          # Dec5: adding pymt insurance info
#                          , 'PymntInsComp_QDXCode', 'PymntInsPlan_QDXCode'
#                          , 'PymntInsComp_GHICode', 'PymntInsPlan_GHICode'
#                          , 'PymntInsFC', 
                          #'IsPrimaryInsPymnt'
                          ]].copy()
temp_pymnt.rename(columns= {'Description' : 'QDXAdjustmentDesc'}, inplace=True)

temp_pymnt.loc[temp_pymnt.TXNType == 'RI','TXN Category'] = 'Payment'
#temp_pymnt.loc[temp_pymnt.TXNType == 'RI','TXN Subcategory'] = 'Subtotal'
temp_pymnt.loc[temp_pymnt.TXNType == 'RI','TXNType'] = 'PayorPaid'

temp_pymnt.loc[temp_pymnt.TXNType == 'RP','TXN Category'] = 'Payment'
#temp_pymnt.loc[temp_pymnt.TXNType == 'RP','TXN Subcategory'] = 'Subtotal'
temp_pymnt.loc[temp_pymnt.TXNType == 'RP','TXNType'] = 'PatientPaid'

a = temp_pymnt.TXNType.isin(['AC','AD'])
temp_pymnt.loc[a,'TXN Category'] = 'Adjustment'
temp_pymnt['temp_str'] = temp_pymnt['GHIAdjustmentCode'] + ":" + temp_pymnt['CategoryDesc']
temp_pymnt.loc[a,'TXNType'] = temp_pymnt.loc[a, 'temp_str']

## move the charge in error to billing category
a = temp_pymnt.QDXAdjustmentCode.isin(['C6','CCD'])
temp_pymnt.loc[a,'TXN Category'] = 'Billing'
#temp_pymnt.loc[a,'TXN Subcategory'] = 'Subtotal'
temp_pymnt.loc[a,'TXNType'] = temp_pymnt['GHIAdjustmentCode'] + ":" + temp_pymnt['CategoryDesc']

## move the refund to payment category
a = temp_pymnt.QDXAdjustmentCode.isin(['D4','CF'])
temp_pymnt.loc[a,'TXN Category'] = 'Payment'
#temp_pymnt.loc[a,'TXN Subcategory'] = 'Subtotal'
temp_pymnt.loc[a,'TXNType'] = temp_pymnt['GHIAdjustmentCode'] + ":" + temp_pymnt['CategoryDesc']

#################################################
#  Compile the Transaction Journal              #
#################################################

# Adding Ticket Payor Information to temp_rev
temp_rev = pd.merge(temp_rev, Ticket_Payor_Test, how='left', on=['OLIID','TicketNumber'])
temp_rev = pd.merge(temp_rev, TickCnt, how='left', left_on=['OLIID'], right_index=True)
temp_rev.rename(columns = {'Currency' : 'TXNCurrency'}, inplace=True)
temp_rev = pd.merge(temp_rev, OLI_Test_Info, how='left',on='OLIID')

# if the OLI/ticket not reach the Revenue stage, there is not entry in the Revenue Table. In this case, patch with current payor information
# why Revenue Table have empty payor
#temp_rev = pd.merge(temp_rev, OLI_Payor)
#temp_rev = pd.merge(temp_rev, OLI_info_2, how='left',on='OLIID')

# need to keep the test & date from Revenue to workaround the issue that OLI information is not available
# X is from Ticket_Payor from Revenue Table
# Y is from OLI - OLI_info_2
##Jun 5: Comment: If OLI_Test_info is missing Test Value, why would it not impact the adjustment, payment, bill and outstanding rows?
temp_rev['Test'] = temp_rev['Test_x']
temp_rev.loc[temp_rev.Test.isnull(), 'Test'] = temp_rev.loc[temp_rev.Test.isnull(), 'Test_y']

temp_rev['TestDeliveredDate'] = temp_rev['TestDeliveredDate_x']
temp_rev.loc[temp_rev.TestDeliveredDate.isnull(), 'TestDeliveredDate'] = temp_rev.loc[temp_rev.TestDeliveredDate.isnull(), 'TestDeliveredDate_y']

temp_rev.drop(['Test_x','Test_y','TestDeliveredDate_x', 'TestDeliveredDate_y'], axis=1, inplace=True)
   
########################################################################################
# add OLI Payor to claim, pymnt and outstanding                                        #
# there are 2 versions                                                                 #
# Version 1: merge the current ticket payor information to claim, payment, adjustment  #
#            This will match the Claim2Rev report that rolls number up to OLI          #
# Version 2: merge the Ticket payor information to claim, payment, adjustment          #
#            This will match the Claim_AccountingPeriod_Rpt                            #
#            that rolls numbers up to OLI + Ticket + Accounting Period                 #
########################################################################################
#, temp_bp
Ticket_TXN_Detail = pd.concat([temp_claim, temp_pymnt, temp_outstanding], sort=False).\
                    sort_values(by = ['OLIID', 'TXNLineNumber','TXNAcctPeriod'])

########### Version 1 ################
#Version 1: Adding the Current Ticket Payor to the Claim, Payment, Adjustment - thus the Claim, Payment, Adjustment matches the Claim2Rev
## consider to use OLI_Payor
#Ticket_TXN_Detail_v1 = pd.merge(Ticket_TXN_Detail, OLI_data, how='left', left_on='OLIID', right_on='OLIID')
Ticket_TXN_Detail_v1 = pd.merge(Ticket_TXN_Detail, OLI_Payor, how='left', left_on='OLIID', right_on='OLIID')
Ticket_TXN_Detail_v1 = pd.merge(Ticket_TXN_Detail_v1, TickCnt, how='left', left_on=['OLIID'], right_index=True)
Ticket_TXN_Detail_v1 = pd.merge(Ticket_TXN_Detail_v1, OLI_Test_Info, how='left', on='OLIID')

Ticket_TXN_Detail_v1 = pd.concat([Ticket_TXN_Detail_v1, temp_rev], ignore_index=True, sort=False)
Ticket_TXN_Detail_v1 = Ticket_TXN_Detail_v1[~(Ticket_TXN_Detail_v1.TXNAmount == 0.0)]
Ticket_TXN_Detail_v1['TXNAcctPeriodDate'] = pd.to_datetime(Ticket_TXN_Detail_v1.TXNAcctPeriod, format="%b %Y")

# rearranging columns and sort rows
Ticket_TXN_Detail_v1 = Ticket_TXN_Detail_v1[['OLIID', 'TicketNumber','Test'
                                       , 'TestDeliveredDate'
                                       # ClaimPeriod, DateOfService ## check these                                       
                                       , 'TXNLineNumber', 'TXNAcctPeriod','TXNAcctPeriodDate', 'TXNCurrency', 'TXNAmount'
                                       , 'TXN Category', 'BR_Category','TXNType'

                                       , 'QDXAdjustmentCode', 'QDXAdjustmentDesc', 'GHIAdjustmentCode'
                                       # 'CategoryDesc',                                         
#                                       , 'Tier1Payor', 'Tier1PayorID', 'Tier1PayorName'
#                                       , 'Tier2Payor', 'Tier2PayorID', 'Tier2PayorName'
                                       , 'Tier4Payor', 'Tier4PayorID', 'Tier4PayorName'                                       
                                       , 'QDXInsPlanCode', 'QDXInsFC', 'LineOfBenefit', 'FinancialCategory'
                                       , 'ClaimPayorBusinessUnit', 'ClaimPayorCountry', 'ClaimPayorDivision','ClaimPayorInternationalArea'
#                                       , 'Territory', 'BusinessUnit', 'InternationalArea', 'Division', 'Country'
                                       , 'CurrentQDXTicketNumber'
#                                       , 'BillingCaseStatusSummary2', 'BillingCaseStatusCode', 'BillingCaseStatus'
#                                       , 'BillType'
#                                       , 'BilledCurrency', 'ListPrice', 'ContractedPrice'        
#                                       , 'OrderingHCPName', 'OrderingHCPCity', 'OrderingHCPState', 'OrderingHCPCountry'
#                                       , 'IsOrderingHCPCTR', 'IsOrderingHCPPECOS', 'Specialty'
#                                       , 'OrderingHCO', 'OrderingHCOCity', 'OrderingHCOCountry', 'OrderingHCOState'
                                       ]].sort_values(by = ['OLIID','TicketNumber','TXNLineNumber','TXNType'])
                                       
########### Version 2 ################
# Version 2: Adding the Ticket Payor to the Claim, Payment, Adjustment - to match the Claim, Payment, Adjustment number to ClaimTicket

Ticket_TXN_Detail_v2 = pd.merge(Ticket_TXN_Detail, Ticket_Payor_Test, how='left', on=['OLIID', 'TicketNumber'])
Ticket_TXN_Detail_v2 = pd.merge(Ticket_TXN_Detail_v2, TickCnt, how='left', left_on=['OLIID'], right_index=True)
Ticket_TXN_Detail_v2 = pd.merge(Ticket_TXN_Detail_v2, OLI_Test_Info, how='left',on='OLIID')

# X is from Revenue Ticket
# Y is from OLI
Ticket_TXN_Detail_v2['Test'] = Ticket_TXN_Detail_v2['Test_x']
Ticket_TXN_Detail_v2.loc[Ticket_TXN_Detail_v2.Test.isnull(), 'Test'] = Ticket_TXN_Detail_v2.loc[Ticket_TXN_Detail_v2.Test.isnull(), 'Test_y']

Ticket_TXN_Detail_v2['TestDeliveredDate'] = Ticket_TXN_Detail_v2['TestDeliveredDate_x']
Ticket_TXN_Detail_v2.loc[Ticket_TXN_Detail_v2.TestDeliveredDate.isnull(), 'TestDeliveredDate'] = Ticket_TXN_Detail_v2.loc[Ticket_TXN_Detail_v2.TestDeliveredDate.isnull(), 'TestDeliveredDate_y']

Ticket_TXN_Detail_v2.drop(['Test_x','Test_y','TestDeliveredDate_x', 'TestDeliveredDate_y'], axis=1, inplace=True)


Ticket_TXN_Detail_v2 = pd.concat([Ticket_TXN_Detail_v2, temp_rev], ignore_index=True, sort=False)
Ticket_TXN_Detail_v2 = Ticket_TXN_Detail_v2[~(Ticket_TXN_Detail_v2.TXNAmount == 0.0)]
Ticket_TXN_Detail_v2['TXNAcctPeriodDate'] = pd.to_datetime(Ticket_TXN_Detail_v2.TXNAcctPeriod, format="%b %Y")

Ticket_TXN_Detail_v2 = pd.merge(Ticket_TXN_Detail_v2, SFDC_Payors, how='left', on='Tier4PayorID')

Ticket_TXN_Detail_v2 = pd.merge(Ticket_TXN_Detail_v2, OLI_Territory, how='left',on='OLIID')
temp = {'BusinessUnit':'ClaimPayorBusinessUnit','InternationalArea':'ClaimPayorInternationalArea',
        'Division' : 'ClaimPayorDivision','Country': 'ClaimPayorCountry'}
a = Ticket_TXN_Detail_v2.BusinessUnit.isnull()
for u in list(temp.keys()):
    Ticket_TXN_Detail_v2.loc[a,u] = Ticket_TXN_Detail_v2.loc[a,temp.get(u)]

#### Jun 5, 18 ::: add appeal, prior auth info, clinical criteria, info
#### when need to explore how to take proactive actions to reduce the Revenue Impacted Adjustment
Ticket_TXN_Detail_v2 = pd.merge(Ticket_TXN_Detail_v2, OLI_patientcriteria, how='left', on='OLIID')
Ticket_TXN_Detail_v2 = pd.merge(Ticket_TXN_Detail_v2, OLI_HCPHCO, how='left', on='OLIID')

Ticket_TXN_Detail_v2 = pd.merge(Ticket_TXN_Detail_v2, Current_reference[['OLIID','CurrentQDXCaseNumber']], how='left', on='OLIID')

priorAuth = QData.priorAuth('Claim2Rev', cfg.input_file_path, refresh)

### there is QDXCurrentCaseNumber
Ticket_TXN_Detail_v2 = pd.merge(Ticket_TXN_Detail_v2, priorAuth[['priorAuthCaseNum','priorAuthEnteredDt','priorAuthEnteredTime',
                                          'priorAuthDate',
                                          'priorAuthResult','priorAuthReqDesc','priorAuthNumber',
                                          'priorAuthResult_Category']]
                    , how='left', left_on='CurrentQDXCaseNumber', right_on='priorAuthCaseNum')

# rearranging columns and sort rows
Ticket_TXN_Detail_v2 = Ticket_TXN_Detail_v2[['OLIID', 'Test', 'TicketNumber', 'QDXTickCnt'
                                       , 'TestDeliveredDate','TXNAcctPeriod','TXNAcctPeriodDate'
                                       # ClaimPeriod, DateOfService ## check these                                       
                                       , 'TXNLineNumber', 'TXNCurrency', 'TXNAmount'
                                       , 'TXN Category','BR_Category', 'TXNType'
 #                                      , 'TXNTypeDesc'
                                       , 'QDXAdjustmentCode', 'QDXAdjustmentDesc', 'GHIAdjustmentCode'
                                       # 'CategoryDesc', 
                                       , 'Tier1Payor', 'Tier1PayorID', 'Tier1PayorName'
                                       , 'Tier2Payor', 'Tier2PayorID', 'Tier2PayorName'
                                       , 'Tier4Payor', 'Tier4PayorID', 'Tier4PayorName'
                                       , 'QDXInsPlanCode', 'QDXInsFC','LineOfBenefit', 'FinancialCategory'
                                       , 'ClaimPayorBusinessUnit', 'ClaimPayorCountry', 'ClaimPayorDivision','ClaimPayorInternationalArea'
                                       , 'Territory'
                                       , 'BusinessUnit', 'InternationalArea', 'Division', 'Country'
#                                       , 'RevenueStatus'
#                                       , 'NSInCriteria'
#                                       , 'CurrentQDXTicketNumber'
#                                       , 'BillingCaseStatusSummary2', 'BillingCaseStatusCode', 'BillingCaseStatus','BillType'
#                                       , 'BilledCurrency', 'ListPrice', 'ContractedPrice'        
#                                       , 'OrderingHCPName', 'OrderingHCPCity', 'OrderingHCPState', 'OrderingHCPCountry'
#                                       , 'IsOrderingHCPCTR', 'IsOrderingHCPPECOS','Specialty'
#                                       , 'OrderingHCO', 'OrderingHCOCity', 'OrderingHCOState', 'OrderingHCOCountry'
#                                       , 'PymntInsComp_QDXCode', 'PymntInsPlan_QDXCode'
#                                       , 'PymntInsComp_GHICode', 'PymntInsPlan_GHICode'
#                                       , 'PymntInsFC'
                                       #, 'IsPrimaryInsPymnt'
                                       
                                       , 'NodalStatus','EstimatedNCCNRisk','SubmittedNCCNRisk','ReportingGroup','RiskGroup'
                                       
                                       , 'OrderingHCPName','OrderingHCPCity', 'OrderingHCPState', 'OrderingHCPCountry'
                                       , 'IsOrderingHCPCTR', 'IsOrderingHCPPECOS', 'Specialty'
                                       , 'OrderingHCO', 'OrderingHCOCity', 'OrderingHCOState', 'OrderingHCOCountry'
                       
#                                       , 'priorAuthEnteredDt','priorAuthEnteredTime'
                                       , 'priorAuthDate'
                                       , 'priorAuthResult','priorAuthReqDesc','priorAuthNumber'
                                       , 'priorAuthResult_Category'
                                       ]].sort_values(by = ['OLIID','TicketNumber','TXNLineNumber'],ascending=[True, True, False])
                                       
###############################################
#     Write the columns into a excel file     #
###############################################
print('to csv:: start :: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

print('writing ClaimTicket_AccountingPeriod_Rpt', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )
# remove $0 for better display in Tableau
update = ['Revenue', 'AccrualRevenue', 'CashRevenue'
          , 'USDRevenue', 'USDAccrualRevenue', 'USDCashRevenue'
          , 'Billed', 'Charge', 'Charged in Error'
          , 'Payment', 'PayorPaid', 'PatientPaid', 'Refund & Refund Reversal'
          , 'Adjustment', 'Insurance Adjustment', 'Revenue Impact Adjustment','GHI Adjustment','All other Adjustment'
          , 'Outstanding'
]
for i in update:
    AcctPeriod_Rpt.loc[AcctPeriod_Rpt[i]==0,i] = ''
output_file = 'ClaimTicket_AccountingPeriod_Rpt.txt'
AcctPeriod_Rpt.to_csv(cfg.output_file_path+output_file, sep='|',index=False)


print('Ticket TXN ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )
output_file = 'Ticket_TXN_Detail.txt'
#a = Ticket_TXN_Detail_v2['TXN Subcategory'] == 'Total'  ##remove the total bill, total payment, total outstanding, have Tableau calculate the subtotal
Ticket_TXN_Detail_v2.to_csv(cfg.output_file_path+output_file, sep='|',index=False)


print('to csv :: done :: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )
print("DONE DONE DONE, Hurray")

'''
a = Ticket_TXN_Detail_v2[(Ticket_TXN_Detail_v2.TestDeliveredDate >='2017-06-01') & (Ticket_TXN_Detail_v2.TestDeliveredDate <='2017-09-30')]
output_file = 'Ticket_TXN_test.xlsx'
writer = pd.ExcelWriter(cfg.output_file_path+output_file, engine='openpyxl', date_format='yyyy/mm/dd')
a.to_excel(writer, sheet_name='Ticket_TXN', index = False)
writer.save()
writer.close()

print('Ticket TXN v1', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )
output_file = 'Ticket_TXN_Detail_Payor_v1.txt'
Ticket_TXN_Detail_v1.to_csv(cfg.output_file_path+output_file, sep='|',index=False)
'''


# sample with multiple tickets OL000764523
# 4 tickets: OL000613935 check
# check and drop deliver date


''' 
notes
# pivotTable
temp_pivot = pd.pivot_table(Revenue_data,index=['TicketNumber'], values=['OLIID'], aggfunc='nunique')
temp_pivot[temp_pivot.OLIID>1].index
Revenue_data[Revenue_data.TicketNumber.isin(temp_pivot[temp_pivot.OLIID>1].index)][['OLIID','TicketNumber','AccountingPeriod','TestDeliveredDate']].sort_values(by='TicketNumber')
## there are ticket map to multiple OLI#


###########################################
#  Read the Write off condition code      #
###########################################

wo_condition = QData.workListRecs('wo_condition', cfg.input_file_path, refresh)

#a = pd.pivot_table(wo_condition, index='workListTicketNum', values=['conditionCode'], aggfunc='count')
#a = wo_condition.groupby(['workListTicketNum']).agg({'workListTicketNum': 'count'})
a = pd.pivot_table(wo_condition, index='workListTicketNum', values=['conditionCode'], aggfunc= lambda conditionCode: len(conditionCode.unique()))
b = a[(a.conditionCode > 1)].index

# not all workListRecs has validTicketNumber, some have ticketNum= 0
# some lines have conditionCode = None and Other e.g. workListTicketNum == '123075'


## why ticket 'corrected' to a different OLI??


130403  OL000739075       658680         FEB 2016        2016-01-20
273178  OL000739076       658680         APR 2016        2016-01-21
272112  OL000739076       658680         OCT 2016        2016-01-21

temp_pivot = pd.pivot_table(Revenue_data,index=['OLIID','TicketNumber','Test'], values=['AccountingPeriod'], aggfunc='nunique')
Revenue_data[Revenue_data.OLIID==temp_pivot[temp_pivot.AccountingPeriod>1].index[10][0]][['OLIID','TicketNumber','AccountingPeriod']]
# groupby is to group the rows into buckets
# to find the number of groups
temp_1 = Revenue_data.groupby(['OLIID','TicketNumber','Test'])
print (temp_1.ngroups)
# to find the size of each group
size = temp_1.size()
# to find the group that is of a given size
size[size>1].index[10]
Revenue_data[Revenue_data.OLIID==size[size>1].index[10][0]][['OLIID','TicketNumber','AccountingPeriod']]

#OL000621765 - an OLI has many tickets in Revenue, but not in std claim, pymnt
# because those are revenue reconciliation adjustment (in 2016, the flag is used, in 2017 the adjustment is not flagged)



#Ticket_TXN_Detail[(Ticket_TXN_Detail.TXN Category=='Revenue') & (Ticket_TXN_Detail.BusinessUnit.isnull())]['OLIID'].unique()
#array(['OL0000UKVAT', 'OL000730539', 'OL000849287', 'R11XOJ6',
#       'TSReclassJE', 'TSRevReduct', 'Unknown()'], dtype=object)
#Ticket_TXN_Detail[Ticket_TXN_Detail.OLIID=='TSReclassJE'][['TXN Category','TXNAmount','BusinessUnit','ClaimPayorBusinessUnit','Tier1Payor','TXNAcctPeriod']]
'''