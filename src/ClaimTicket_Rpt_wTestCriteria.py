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


Data source:
Payment & Adjustment : from Quadax stdClaim and stdPayment files,
Revenue: GHI StagingDB.Analytics.mvwRevenue
OrderLineDetail: Test, Payor information : GHI StagingDB.Analytics.stgOrderLineDetail

'''
import pandas as pd
import numpy as np
from datetime import datetime

QDX_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Nov21\\"
GHI_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Nov21\\"
GHI_outfile_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Nov21\\"

print('ClaimTicket Report :: start :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

###############################################################
#   Read QDX Claim and Payment (Receipt) data                 #
###############################################################
print('ClaimTicket Report :: read QDX data :: start :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

from data import GetQDXData as QData
Claim_bill = QData.stdClaim('ClaimTicket', QDX_file_path, 0)
Claim_pymnt = QData.stdPayment('ClaimTicket', QDX_file_path, 0)
Claim_case = QData.claim_case_status('QDXClaim_CaseStatus', QDX_file_path, 0)

###########################################
#  Read the Write off condition code      #
###########################################

wo_condition = QData.workListRecs('wo_condition', QDX_file_path, 0)

#a = pd.pivot_table(wo_condition, index='workListTicketNum', values=['conditionCode'], aggfunc='count')
#a = wo_condition.groupby(['workListTicketNum']).agg({'workListTicketNum': 'count'})
a = pd.pivot_table(wo_condition, index='workListTicketNum', values=['conditionCode'], aggfunc= lambda conditionCode: len(conditionCode.unique()))
b = a[(a.conditionCode > 1)].index

# not all workListRecs has validTicketNumber, some have ticketNum= 0
# some lines have conditionCode = None and Other e.g. workListTicketNum == '123075'

###############################################################
#   Read GHI Revenue Data and OrderLineDetail                 #
############################################################### 

print('ClaimTicket Report :: read GHI data :: start :: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

# when prepare report summarized by OLI+Ticket, get the Payor info from Revenue.
# OrderLineDetail has payor of the current ticket
# Revenue has the payor of the ticket

from data import GetGHIData as GData
Revenue_data = GData.revenue_data('ClaimTicket', GHI_file_path, 0)
OLI_data = GData.OLI_detail('ClaimTicket_wCriteria', GHI_file_path, 0)

OLI_data.drop(['BillingCaseStatusSummary2', 'BillingCaseStatusCode','BillingCaseStatus'], axis=1, inplace=True)
OLI_data = pd.merge(OLI_data, Claim_case[['caseTicketNum', 'caseAccession', 'BillingCaseStatusSummary1', 'BillingCaseStatusSummary2',
       'BillingCaseStatusCode', 'BillingCaseStatus']], how='left', left_on=['CurrentTicketNumber','OLIID'], right_on=['caseTicketNum','caseAccession'])
OLI_data.drop(['caseTicketNum', 'caseAccession'], axis=1, inplace=True)

###############################################################
#   Read Adjustment Code & Write off analysis buckets         #
############################################################### 

prep_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Scripting\\"
prep_file_name = "QDX_ClaimDataPrep.xlsx"

Adj_code = pd.read_excel(prep_file_path+prep_file_name, sheetname = "AdjustmentCode", parse_cols="A,C:E,G", encoding='utf-8-sig')
Adj_code.columns = [cell.strip() for cell in Adj_code.columns]

# define the value to group QDX adjustment
category = 'AdjustmentGroup'
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
#   Count the number of Ticket captured in Revenue Table per an OLI                    #
########################################################################################

TickCnt = (pd.pivot_table(Claim_bill, index=['OLIID'], values = 'TicketNumber',\
                          aggfunc = lambda TicketNumber: len(TicketNumber.unique()))).rename(columns = {'TicketNumber': 'TickCnt'})

## create a df using the pivot_table output series, the index is OLIID. Add the column to the Revenue_data
## append the TickCnt to Claim_Cycle_claim and Claim_Cycle_pymnt to show the value in those rows
Claim_bill = pd.merge(Claim_bill, TickCnt, how='left', left_on='OLIID', right_index=True)
Claim_pymnt = pd.merge(Claim_pymnt, TickCnt, how='left', left_on='OLIID', right_index=True)

# Add the AccountingPeriod columns using the TXNAcctPeriod value
# This is use to append with Revenue data to get a journal view
Claim_pymnt['AccountingPeriod'] = Claim_pymnt.TXNAcctPeriod
Claim_bill['AccountingPeriod'] = Claim_bill.TXNAcctPeriod

########################################################################################
#   Identify the Ticket Payor                                                          #
########################################################################################
# Find the index of last AccountingPeriodDate for OLIID+TicketNumber.
# Pull the Payer information from the last AccountingPeriod and assume this is the Payer for this OLIID+Ticket for all Accounting Period

temp = Revenue_data.groupby(['OLIID','TicketNumber']).agg({'AccountingPeriodDate':'idxmax'})
pull_rows = temp.AccountingPeriodDate

Ticket_Payor = Revenue_data.loc[pull_rows][['OLIID','TicketNumber',
                                           'Currency','TestDeliveredDate','Test',
                                           'Tier1Payor', 'Tier1PayorID', 'Tier1PayorName',
                                           'Tier2Payor', 'Tier2PayorID','Tier2PayorName',
                                           'Tier4Payor', 'Tier4PayorID', 'Tier4PayorName',
                                           'QDXInsPlanCode','QDXInsFC', 'LineOfBenefit', 'FinancialCategory',
                                           'ClaimPayorBusinessUnit', 'ClaimPayorInternationalArea', 'ClaimPayorDivision', 'ClaimPayorCountry']]

## for some reason, SFDC lost the OLI information and thus not available in EDW OLI detail, although there is Revenue & Payment Transactions
## therefore need to pull the test and test delivered date from revenue file to patch
## To-do: Follow up with Laura, Mary, Jen, OR000722222 (OLIID: OL000730539) the order line item is not there in SFDC while work order, result, specimens exist.

CurrentTicket_payor = OLI_data[['OLIID','CurrentTicketNumber',
                                'Tier1Payor','Tier1PayorID','Tier1PayorName',
                                'Tier2Payor','Tier2PayorID','Tier2PayorName',
                                'Tier4Payor','Tier4PayorID','Tier4PayorName',
                                'QDXInsPlanCode', 'QDXInsFC','LineOfBenefit', 'FinancialCategory'                             
                    ]]


#OLI_ClaimTicketHeader = pd.merge(Ticket_Payor, CurrentTicket_payor, how='outer', left_on=['OLIID','TicketNumber'], right_on=['OLIID','CurrentTicketNumber'])

#OLI_info_1 is a set of OLI data appending the AccountPeriod Report.
OLI_info_1 = OLI_data[['OLIID','Test','BilledCurrency',
                     'BusinessUnit', 'InternationalArea', 'Division', 'Country', 
#                     'OrderingHCPName','OrderingHCPCity', 'OrderingHCPState', 'OrderingHCPCountry',
#                     'IsOrderingHCPCTR', 'IsOrderingHCPPECOS',
                     'OrderStartDate','OLIStartDate', 'TestDeliveredDate', 
                     ]]
                     
OLI_info_2 = OLI_data[['OLIID','Test','TestDeliveredDate','CurrentTicketNumber',
                     'BillingCaseStatusSummary2','BillingCaseStatusCode','BillingCaseStatus','BillType',
                     'Territory','BusinessUnit','InternationalArea','Division','Country',
                     'OrderingHCPName','OrderingHCPCity', 'OrderingHCPState', 'OrderingHCPCountry',
                     'IsOrderingHCPCTR', 'IsOrderingHCPPECOS', 'Specialty',
                     'OrderingHCO', 'OrderingHCOCity', 'OrderingHCOState', 'OrderingHCOCountry'
                    ]]

OLI_info_3 = OLI_data[['MultiplePrimaryConfirmed', 'MultiplePrimaryRequested'
                       , 'Diagnosis', 'SubmittingDiagnosis', 'ClinicalStage'
                       , 'NodalStatus','ERStatus', 'SubmittedERStatus','HER2Status',  'SubmittedHER2'
                       , 'PRStatus','TumorSize'
                       , 'DCISTumorSize', 'DCIS_TumorSizeRange',
                       , 'PatientAgeAtDiagnosisRange','SubmittedNCCNRisk', 'EstimatedNCCNRisk', 'FailureCode'
                     ]]
                    

########################################################################################
#   ClaimTicket2Rev Transactions Wide                                                  #
#                                                                                      #
#   Group by OLI + Ticket Number + Accounting Period                                   #
#   to get the Revenue Payment, Adjustment per Accounting Period                       #
#   The total payment, adjustment received up to the data extraction date              #
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
    'ClaimPeriodDate' : 'max',
    }
### Calculate the revenue number per group: OLIID and Ticket Number, the index will become (OLIID + TicketNumber)
Summarized_Revenue = Revenue_data.groupby(['OLIID','TicketNumber','AccountingPeriod']).agg(aggregations)
Summarized_Revenue.columns = [['Revenue','AccrualRevenue','CashRevenue',
                                 'USDRevenue', 'USDAccrualRevenue', 'USDCashRevenue',
                                 'ClaimPeriodDate']]
Summarized_Revenue = Summarized_Revenue.reset_index()

########################################################################################
#   Roll up the QDX stdclaim information to Accounting Period                          #
#   Calculate the Total Billed Amount                                                  #
########################################################################################
print ('Claim2Rev_QDX_GHI :: Aggregate Bill, Payment, Adjustment numbers per OLI')

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

''' 
  Calculate the Payer Paid Amount for an OLI from QDX Payment Data
  # Allowable, Deductible & Coinsurance 
'''  
#  using data from QDX payment file
#  Read the RI lines, aggregate the number per OLI+Ticket

aggregations = {
    'TXNAmount' :'sum',
    }

Summarized_PADC = Claim_pymnt[(Claim_pymnt.TXNType=='RI')].groupby(['OLIID','TicketNumber','TXNAcctPeriod']).agg(aggregations)
Summarized_PADC.columns = [['PayorPaid']]

''' Calculate the Patient Paid Amount '''  
aggregations = {
    'TXNAmount' : 'sum'
    }
Summarized_PtPaid = Claim_pymnt[(Claim_pymnt.TXNType=='RP')].groupby(['OLIID','TicketNumber','TXNAcctPeriod']).agg(aggregations)
Summarized_PtPaid.columns = [['PatientPaid']]

''' Calculate the Adjustment per AdjustmentGroup + Accounting Period '''  
temp_adjustment = Claim_pymnt[Claim_pymnt.TXNType.isin(['AC','AD'])].copy()

temp_adjustment = pd.merge(temp_adjustment, Adj_code[['Code',category]], how='left', left_on='QDXAdjustmentCode', right_on='Code')

Summarized_Adjust = temp_adjustment.pivot_table(index = ['OLIID','TicketNumber','TXNAcctPeriod'], columns = 'AdjustmentGroup', values='TXNAmount', aggfunc='sum')


## nov 10: the CIE and Refund is not called out, think and review the file to see if the amt need to be called out

########################################################################################
#   Generate the Claim_AcctPeriod_Rpt                                                  #
#  Merge the summarized claim, payment, adjustment and revenue                         #
########################################################################################

print('ClaimTicket Report :: AccountingPeriod grain :: start :: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

AcctPeriod_Rpt = pd.concat([Summarized_Claim, Summarized_PADC, Summarized_PtPaid, Summarized_Adjust], axis=1)
AcctPeriod_Rpt.reset_index(inplace=True)
AcctPeriod_Rpt.rename(columns = {'TXNAcctPeriod' : 'AccountingPeriod'}, inplace=True)
AcctPeriod_Rpt.AccountingPeriod = AcctPeriod_Rpt.AccountingPeriod.str.upper()
AcctPeriod_Rpt.fillna(0.0, inplace=True)  # to avoid calculation with NaN resulting NaN
## FEB 2016 vs Feb 2016
# OL000769769, OL000605838, #OL000605300

''' flip the sign of Payor & Patient payment, Adjustment '''
flip_code = list(Adj_code[category].unique())
flip_code.remove('Charged in Error')
flip_code.append('PayorPaid')
flip_code.append('PatientPaid')    
for a in flip_code:
    change = (~(AcctPeriod_Rpt[a]==0.0))
    AcctPeriod_Rpt.loc[change, a] = AcctPeriod_Rpt.loc[change,a] * -1

''' Calculate the Payment, Billed, Adjustment per OLI + TicketNumber + Accounting Period '''
AcctPeriod_Rpt['Total Payment'] = AcctPeriod_Rpt[['PayorPaid','PatientPaid']].sum(axis=1)+AcctPeriod_Rpt['Refund & Refund Reversal']
AcctPeriod_Rpt['Total Billed'] = AcctPeriod_Rpt[['Charge','Charged in Error']].sum(axis=1)

temp_adjust_cat = list(Adj_code[category].unique())
temp_adjust_cat.remove('Charged in Error')
temp_adjust_cat.remove('Refund & Refund Reversal')
AcctPeriod_Rpt['Total Adjustment'] = AcctPeriod_Rpt[temp_adjust_cat].sum(axis=1)

''' Merge the Revenue, Bill, Payment, Adjustment '''
AcctPeriod_Rpt = pd.merge(Summarized_Revenue, AcctPeriod_Rpt, how='outer', on=['OLIID','TicketNumber','AccountingPeriod'])
AcctPeriod_Rpt.fillna(0.0, inplace=True)

'''adding Payor of the ticket and OLI info '''
AcctPeriod_Rpt = pd.merge(AcctPeriod_Rpt, Ticket_Payor, how='left', on = ['OLIID','TicketNumber'])
AcctPeriod_Rpt = pd.merge(AcctPeriod_Rpt, OLI_info_1, how = 'left', on = ['OLIID'])
AcctPeriod_Rpt = pd.merge(AcctPeriod_Rpt, TickCnt, how='left', left_on=['OLIID'], right_index=True)
AcctPeriod_Rpt['AccountingPeriodDate'] = pd.to_datetime(AcctPeriod_Rpt.AccountingPeriod, format="%b %Y")

# Merge the Test & Test Delivered Date value from Revenue and OLI. Needs to combine since the data are not always available in the files
# X is from Revenue Ticket
# Y is from OLI
AcctPeriod_Rpt['Test'] = AcctPeriod_Rpt['Test_x']
AcctPeriod_Rpt.loc[AcctPeriod_Rpt.Test.isnull(), 'Test'] = AcctPeriod_Rpt.loc[AcctPeriod_Rpt.Test.isnull(), 'Test_y']

AcctPeriod_Rpt['TestDeliveredDate'] = AcctPeriod_Rpt['TestDeliveredDate_x']
AcctPeriod_Rpt.loc[AcctPeriod_Rpt.Test.isnull(), 'TestDeliveredDate'] = AcctPeriod_Rpt.loc[AcctPeriod_Rpt.Test.isnull(), 'TestDeliveredDate_y']

''' Rearranging and sort data for output '''
AcctPeriod_Rpt = AcctPeriod_Rpt[[
                 'OLIID', 'TicketNumber', 'TickCnt','AccountingPeriod','AccountingPeriodDate'
                 ,'OLIStartDate', 'TestDeliveredDate', 'Currency'
                 , 'Revenue', 'AccrualRevenue', 'CashRevenue'
                 , 'USDRevenue', 'USDAccrualRevenue', 'USDCashRevenue'
                 , 'Total Billed', 'Charge', 'Charged in Error'
                 , 'Total Payment', 'PayorPaid', 'PatientPaid', 'Refund & Refund Reversal'
                 , 'Total Adjustment', 'Insurance Adjustment', 'Revenue Impact','GHI Adjustment','All other'
 
                 , 'Tier1Payor', 'Tier1PayorID', 'Tier1PayorName', 'Tier2Payor'
                 , 'Tier2PayorID', 'Tier2PayorName', 'Tier4Payor', 'Tier4PayorID'
                 , 'Tier4PayorName', 'QDXInsPlanCode', 'LineOfBenefit'
                 , 'FinancialCategory', 'ClaimPayorBusinessUnit'
                 , 'ClaimPayorInternationalArea', 'ClaimPayorDivision'
                 , 'ClaimPayorCountry', 'Test', 'BilledCurrency', 'BusinessUnit'
                 , 'InternationalArea', 'Division', 'Country', 'OrderStartDate','ClaimPeriodDate'
                ]].sort_values(by=['OLIID','TicketNumber','AccountingPeriodDate'])
                
#############################################################################################
# Create a Transaction Journal view                                                         #
#                                                                                           #
# for report and analysis by Accounting period                                              #
# keep the adjustment detail for the ability to drill down different transaction categories #
# purpose to show transaction activities happen during the selected accounting period       #
#############################################################################################
'''Extract the Revenue transactions '''

print('ClaimTicket Report :: create a journal of revenue, charge, payment, adjustment for rev vs receipt analysis :: start :: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

temp_rev = pd.pivot_table(Revenue_data, index = ['OLIID','TicketNumber', 'AccountingPeriod'],
                          values = ['TotalAccrualRevenue', 'TotalCashRevenue','TotalUSDAccrualRevenue', 'TotalUSDCashRevenue'])
                                                 
temp_rev.rename(columns = {'TotalAccrualRevenue':'AccrualRevenue', 'TotalCashRevenue':'CashRevenue'
                         ,'TotalUSDAccrualRevenue':'USDAccrualRevenue', 'TotalUSDCashRevenue':'USDCashRevenue'}, inplace=True)
temp_rev = temp_rev.stack().reset_index()
temp_rev.columns = [['OLIID','TicketNumber','TXNAcctPeriod', 'TXNTypeDesc','TXNAmount']]
# drop the rows with zeros
temp_rev = temp_rev[~(temp_rev.TXNAmount == 0.0)].sort_values(by=['OLIID'])
temp_rev['TXNLineNumber'] = int(-1)

TXNType_dict = {'USDAccrualRevenue':'FAU', 'USDCashRevenue':'FCU'}
TXNSubCategory_dict = {'USDAccrualRevenue':'USDAccrual', 'USDCashRevenue':'USDCash'}
for a in list(TXNType_dict.keys()):
    temp_rev.loc[(temp_rev.TXNTypeDesc==a),'TXNType'] = TXNType_dict.get(a)
    temp_rev.loc[(temp_rev.TXNTypeDesc==a),'TXNCategory'] = 'USDRevenue'
    temp_rev.loc[(temp_rev.TXNTypeDesc==a),'TXNSubCategory'] = TXNSubCategory_dict.get(a)
    
TXNType_dict = {'AccrualRevenue':'FA', 'CashRevenue':'FC'}
TXNSubCategory_dict = {'AccrualRevenue':'Accrual', 'CashRevenue':'Cash'}
for a in list(TXNType_dict.keys()):
    temp_rev.loc[(temp_rev.TXNTypeDesc==a),'TXNType'] = TXNType_dict.get(a)
    temp_rev.loc[(temp_rev.TXNTypeDesc==a),'TXNCategory'] = 'Revenue'
    temp_rev.loc[(temp_rev.TXNTypeDesc==a),'TXNSubCategory'] = TXNSubCategory_dict.get(a)


''' Extract the Claim Amount and Outstanding Amount '''
## Extract the claim amount from stdClaim. This is the charge amount issued with the Ticket
temp_claim = Claim_bill[['OLIID', 'TicketNumber','TXNAcctPeriod'
                         , 'TXNAmount', 'TXNLineNumber','TXNType']].copy()
temp_claim['TXNCategory'] = 'Billing'
temp_claim['TXNSubCategory'] = 'Charges'
temp_claim['TXNTypeDesc'] = 'Claim Amount'

## Extract the outstanding amount stdClaim. This is the outstanding amount of the ticket at the TXNAcctPeriod
temp_outstanding = Claim_bill[['OLIID', 'TicketNumber', 'TXNAcctPeriod'
                        , 'ClmTickBal', 'TXNLineNumber']].copy()
temp_outstanding.rename(columns = {'ClmTickBal':'TXNAmount'}, inplace=True)
temp_outstanding['TXNType'] ='RO'
temp_outstanding['TXNCategory'] = 'Receipts'
temp_outstanding['TXNSubCategory'] = 'Outstanding'
temp_outstanding['TXNTypeDesc'] = 'Outstanding'
temp_outstanding['QDXAdjustmentDesc'] = 'Outstanding'


''' Extract the Payment, Adjustment (includes Refund and Charge Error) '''
# Extract Payment, Adjustment, Refund, Charge Error from stdPayment
temp_pymnt = Claim_pymnt[['OLIID', 'TicketNumber','TXNAcctPeriod'
                          , 'TXNAmount', 'TXNType','TXNLineNumber','Description'
                          ,'GHIAdjustmentCode','CategoryDesc', 'QDXAdjustmentCode'
                          # Dec5: adding pymt insurance info
                          , 'PymntInsComp_QDXCode', 'PymntInsPlan_QDXCode'
                          , 'PymntInsComp_GHICode', 'PymntInsPlan_GHICode'
                          , 'PymntInsFC', 'IsPrimaryInsPymnt'
                          ]].copy()
#'Test','TXNCurrency'
temp_pymnt.rename(columns= {'Description' : 'QDXAdjustmentDesc'}, inplace=True)
temp_pymnt['TXNCategory'] = 'Receipts'

temp_pymnt.loc[temp_pymnt.TXNType.isin(['RI','RP']), 'TXNSubCategory'] = 'Payment'
temp_pymnt.loc[temp_pymnt.TXNType == 'RI','TXNTypeDesc'] = 'PayorPaid'
temp_pymnt.loc[temp_pymnt.TXNType == 'RI','QDXAdjustmentDesc'] = 'PayorPaid'
temp_pymnt.loc[temp_pymnt.TXNType == 'RP','TXNTypeDesc'] = 'PatientPaid'
temp_pymnt.loc[temp_pymnt.TXNType == 'RP','QDXAdjustmentDesc'] = 'PatientPaid'

## adding AdjustmentCategory
temp_pymnt = pd.merge(temp_pymnt, Adj_code[['Code','AdjustmentGroup']], how='left',left_on='QDXAdjustmentCode', right_on='Code')
temp_pymnt.drop('Code', axis=1, inplace=True)

a = temp_pymnt.TXNType.isin(['AC','AD'])
temp_pymnt.loc[a,'TXNSubCategory'] = 'Adjustment'
temp_pymnt.loc[a,'TXNType'] = temp_pymnt.loc[a,'GHIAdjustmentCode']
temp_pymnt['temp_str'] = temp_pymnt['GHIAdjustmentCode'] + " : " + temp_pymnt['CategoryDesc']
temp_pymnt.loc[a,'TXNTypeDesc'] = temp_pymnt.loc[a, 'temp_str']

## move the charge in error to billing category
a = temp_pymnt.QDXAdjustmentCode.isin(['C6','CCD'])
temp_pymnt.loc[a,'TXNCategory'] = 'Billing'
temp_pymnt.loc[a,'TXNSubCategory'] = 'Charged in Error'
temp_pymnt.loc[a,'TXNType'] = 'CIE'
temp_pymnt.loc[a,'TXNTypeDesc'] = 'Charged in Error'

## move the refund to payment category
a = temp_pymnt.QDXAdjustmentCode.isin(['D4','CF'])
temp_pymnt.loc[a,'TXNSubCategory'] = 'Payment'
temp_pymnt.loc[a,'TXNType'] = 'RR'
temp_pymnt.loc[a,'TXNTypeDesc'] = 'Refund'

# Reverse the sign for Payment and Adjustment
a = (temp_pymnt.TXNSubCategory == 'Adjustment')
temp_pymnt.loc[a,'TXNAmount'] = temp_pymnt.loc[a,'TXNAmount'] *-1
a = (temp_pymnt.TXNSubCategory == 'Payment')
temp_pymnt.loc[a,'TXNAmount'] = temp_pymnt.loc[a,'TXNAmount'] *-1

#################################################
#  Compile the Transaction Journal              #
#################################################

# Adding Ticket Payor Information to temp_rev
temp_rev = pd.merge(temp_rev, Ticket_Payor, how='left', on=['OLIID','TicketNumber'])
temp_rev.rename(columns = {'Currency' : 'TXNCurrency'}, inplace=True)

temp_rev = pd.merge(temp_rev, OLI_info_2, how='left',on='OLIID')

# need to keep the test & date from Revenue to workaround the issue that OLI information is not available
# X is from Revenue Ticket
# Y is from OLI
temp_rev['Test'] = temp_rev['Test_x']
temp_rev.loc[temp_rev.Test.isnull(), 'Test'] = temp_rev.loc[temp_rev.Test.isnull(), 'Test_y']

temp_rev['TestDeliveredDate'] = temp_rev['TestDeliveredDate_x']
temp_rev.loc[temp_rev.TestDeliveredDate.isnull(), 'TestDeliveredDate'] = temp_rev.loc[temp_rev.TestDeliveredDate.isnull(), 'TestDeliveredDate_y']

temp_rev.drop(['Test_x','Test_y','TestDeliveredDate_x', 'TestDeliveredDate_y'], axis=1, inplace=True)

# whatever reason, if the OLI is not found in OLI_data, then the OLI_data becomes blank.
# pull the BU etc information from ClaimPayor BU
# and BU information is required to Revenue rows to match the BI Revenue Report
temp = {'BusinessUnit':'ClaimPayorBusinessUnit','InternationalArea':'ClaimPayorInternationalArea',
        'Division' : 'ClaimPayorDivision','Country': 'ClaimPayorCountry'}
a = temp_rev.BusinessUnit.isnull()
for u in list(temp.keys()):
    temp_rev.loc[a,u] = temp_rev.loc[a,temp.get(u)]
    
########################################################################################
# add OLI Payor to claim, pymnt and outstanding                                        #
# there are 2 versions                                                                 #
# Version 1: merge the current ticket payor information to claim, payment, adjustment  #
#            This will match the Claim2Rev report that rolls number up to OLI          #
# Version 2: merge the Ticket payor information to claim, payment, adjustment          #
#            This will match the Claim_AccountingPeriod_Rpt                            #
#            that rolls numbers up to OLI + Ticket + Accounting Period                 #
########################################################################################

Ticket_TXN_Detail = pd.concat([temp_claim, temp_pymnt, temp_outstanding]).sort_values(by = ['OLIID', 'TXNLineNumber','TXNAcctPeriod'])

########### Version 1 ################
#Version 1: Adding the Current Ticket Payor to the Claim, Payment, Adjustment - thus the Claim, Payment, Adjustment matches the Claim2Rev
Ticket_TXN_Detail_v1 = pd.merge(Ticket_TXN_Detail, OLI_data, how='left', left_on='OLIID', right_on='OLIID')

Ticket_TXN_Detail_v1 = pd.concat([Ticket_TXN_Detail_v1, temp_rev], ignore_index=True)
Ticket_TXN_Detail_v1 = Ticket_TXN_Detail_v1[~(Ticket_TXN_Detail_v1.TXNAmount == 0.0)]
Ticket_TXN_Detail_v1['TXNAcctPeriodDate'] = pd.to_datetime(Ticket_TXN_Detail_v1.TXNAcctPeriod, format="%b %Y")

# rearranging columns and sort rows
Ticket_TXN_Detail_v1 = Ticket_TXN_Detail_v1[['OLIID', 'TicketNumber','Test'
                                       , 'TestDeliveredDate'
                                       # ClaimPeriod, DateOfService ## check these                                       
                                       , 'TXNLineNumber', 'TXNAcctPeriod','TXNAcctPeriodDate', 'TXNCurrency', 'TXNAmount'
                                       , 'TXNCategory','TXNSubCategory'
                                       , 'TXNType', 'TXNTypeDesc'
                                       , 'QDXAdjustmentCode', 'QDXAdjustmentDesc',
                                       # 'CategoryDesc', 
                                       'GHIAdjustmentCode', 'AdjustmentGroup'
                                       , 'Tier1Payor', 'Tier1PayorID', 'Tier1PayorName'
                                       , 'Tier2Payor', 'Tier2PayorID', 'Tier2PayorName'
                                       , 'Tier4Payor', 'Tier4PayorID', 'Tier4PayorName'
                                       , 'QDXInsPlanCode', 'QDXInsFC', 'LineOfBenefit', 'FinancialCategory'
                                       , 'ClaimPayorBusinessUnit', 'ClaimPayorCountry', 'ClaimPayorDivision','ClaimPayorInternationalArea'
                                       , 'Territory', 'BusinessUnit', 'InternationalArea', 'Division', 'Country'
                                       , 'CurrentTicketNumber', 'BillingCaseStatusSummary2', 'BillingCaseStatusCode', 'BillingCaseStatus', 'BillType'
#                                       , 'BilledCurrency', 'ListPrice', 'ContractedPrice'        
                                       , 'OrderingHCPName', 'OrderingHCPCity', 'OrderingHCPState', 'OrderingHCPCountry'
                                       , 'IsOrderingHCPCTR', 'IsOrderingHCPPECOS', 'Specialty'
                                       , 'OrderingHCO', 'OrderingHCOCity', 'OrderingHCOCountry', 'OrderingHCOState'
                                       ]].sort_values(by = ['OLIID','TicketNumber','TXNLineNumber','TXNType'])
                                       
########### Version 2 ################
# Version 2: Adding the Ticket Payor to the Claim, Payment, Adjustment - to match the Claim, Payment, Adjustment number to ClaimTicket
Ticket_TXN_Detail_v2 = pd.merge(Ticket_TXN_Detail, Ticket_Payor, how='left', on=['OLIID', 'TicketNumber'])
Ticket_TXN_Detail_v2 = pd.merge(Ticket_TXN_Detail_v2, OLI_info_2, how='left',on='OLIID')

# X is from Revenue Ticket
# Y is from OLI
Ticket_TXN_Detail_v2['Test'] = Ticket_TXN_Detail_v2['Test_x']
Ticket_TXN_Detail_v2.loc[Ticket_TXN_Detail_v2.Test.isnull(), 'Test'] = Ticket_TXN_Detail_v2.loc[Ticket_TXN_Detail_v2.Test.isnull(), 'Test_y']

Ticket_TXN_Detail_v2['TestDeliveredDate'] = Ticket_TXN_Detail_v2['TestDeliveredDate_x']
Ticket_TXN_Detail_v2.loc[Ticket_TXN_Detail_v2.TestDeliveredDate.isnull(), 'TestDeliveredDate'] = Ticket_TXN_Detail_v2.loc[Ticket_TXN_Detail_v2.TestDeliveredDate.isnull(), 'TestDeliveredDate_y']

Ticket_TXN_Detail_v2.drop(['Test_x','Test_y','TestDeliveredDate_x', 'TestDeliveredDate_y'], axis=1, inplace=True)

Ticket_TXN_Detail_v2 = pd.concat([Ticket_TXN_Detail_v2, temp_rev], ignore_index=True)
Ticket_TXN_Detail_v2 = Ticket_TXN_Detail_v2[~(Ticket_TXN_Detail_v2.TXNAmount == 0.0)]
Ticket_TXN_Detail_v2['TXNAcctPeriodDate'] = pd.to_datetime(Ticket_TXN_Detail_v2.TXNAcctPeriod, format="%b %Y")

# rearranging columns and sort rows
Ticket_TXN_Detail_v2 = Ticket_TXN_Detail_v2[['OLIID', 'TicketNumber','Test'
                                       , 'TestDeliveredDate'
                                       # ClaimPeriod, DateOfService ## check these                                       
                                       , 'TXNLineNumber', 'TXNAcctPeriod','TXNAcctPeriodDate', 'TXNCurrency', 'TXNAmount'
                                       , 'TXNCategory','TXNSubCategory'
                                       , 'TXNType', 'TXNTypeDesc'
                                       , 'QDXAdjustmentCode', 'QDXAdjustmentDesc',
                                       # 'CategoryDesc', 
                                       'GHIAdjustmentCode', 'AdjustmentGroup'
                                       , 'Tier1Payor', 'Tier1PayorID', 'Tier1PayorName'
                                       , 'Tier2Payor', 'Tier2PayorID', 'Tier2PayorName'
                                       , 'Tier4Payor', 'Tier4PayorID', 'Tier4PayorName'
                                       , 'QDXInsPlanCode', 'QDXInsFC','LineOfBenefit', 'FinancialCategory'
                                       , 'ClaimPayorBusinessUnit', 'ClaimPayorCountry', 'ClaimPayorDivision','ClaimPayorInternationalArea'
                                       , 'Territory', 'BusinessUnit', 'InternationalArea', 'Division', 'Country'
#                                       , 'RevenueStatus'
#                                       , 'NSInCriteria'
                                       , 'CurrentTicketNumber', 'BillingCaseStatusSummary2', 'BillingCaseStatusCode', 'BillingCaseStatus','BillType'
#                                       , 'BilledCurrency', 'ListPrice', 'ContractedPrice'        
                                       , 'OrderingHCPName', 'OrderingHCPCity', 'OrderingHCPState', 'OrderingHCPCountry'
                                       , 'IsOrderingHCPCTR', 'IsOrderingHCPPECOS','Specialty'
                                       , 'OrderingHCO', 'OrderingHCOCity', 'OrderingHCOState', 'OrderingHCOCountry'
                                       , 'PymntInsComp_QDXCode', 'PymntInsPlan_QDXCode'
                                       , 'PymntInsComp_GHICode', 'PymntInsPlan_GHICode'
                                       , 'PymntInsFC', 'IsPrimaryInsPymnt'
                                       ]].sort_values(by = ['OLIID','TicketNumber','TXNLineNumber','TXNType'])


###############################################
#     Write the columns into a excel file     #
###############################################
print('to csv:: start :: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

print('writing Claim Ticket ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

output_file = 'ClaimTicket_AccountingPeriod_Rpt.txt'
AcctPeriod_Rpt.to_csv(GHI_outfile_path+output_file, sep='|',index=False)

print('Ticket TXN ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )
output_file = 'Ticket_TXN_Detail.txt'
Ticket_TXN_Detail_v2.to_csv(GHI_outfile_path+output_file, sep='|',index=False)

print('Ticket TXN v1', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )
output_file = 'Ticket_TXN_Detail_Payor_v1.txt'
Ticket_TXN_Detail_v1.to_csv(GHI_outfile_path+output_file, sep='|',index=False)

print('to csv :: done :: ', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )
print("DONE DONE DONE, Hurray")

# sample with multiple tickets OL000764523
# 4 tickets: OL000613935 check
# check and drop deliver date


''' notes
# pivotTable
temp_pivot = pd.pivot_table(Revenue_data,index=['TicketNumber'], values=['OLIID'], aggfunc='nunique')
temp_pivot[temp_pivot.OLIID>1].index
Revenue_data[Revenue_data.TicketNumber.isin(temp_pivot[temp_pivot.OLIID>1].index)][['OLIID','TicketNumber','AccountingPeriod','TestDeliveredDate']].sort_values(by='TicketNumber')
## there are ticket map to multiple OLI#
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
'''
#OL000621765 - an OLI has many tickets in Revenue, but not in std claim, pymnt
# because those are revenue reconciliation adjustment (in 2016, the flag is used, in 2017 the adjustment is not flagged)



#Ticket_TXN_Detail[(Ticket_TXN_Detail.TXNCategory=='Revenue') & (Ticket_TXN_Detail.BusinessUnit.isnull())]['OLIID'].unique()
#array(['OL0000UKVAT', 'OL000730539', 'OL000849287', 'R11XOJ6',
#       'TSReclassJE', 'TSRevReduct', 'Unknown()'], dtype=object)
#Ticket_TXN_Detail[Ticket_TXN_Detail.OLIID=='TSReclassJE'][['TXNCategory','TXNAmount','BusinessUnit','ClaimPayorBusinessUnit','Tier1Payor','TXNAcctPeriod']]

