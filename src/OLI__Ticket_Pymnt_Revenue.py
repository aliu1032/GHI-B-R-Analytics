'''
Created on Sep 7, 2017

@author: aliu

Sep 28, 2017
- change the payment, adjustment, outstanding source from OLI to mvwRevenue
  because stgOrderLineItem has the payment, adjustment of the current ticket
  source List price and contracted price from stgOrderLineItem

Oct 4, 2017
- change the bill, payment and adjustment source from mvwRevenue
  because mvwRevenue, fctRevenue has missed importing some transaction from NS tables: stgBill, stgPayment, stgAdjustment
  As a result, TotalRevenue, CashRevenue, Accrual Revenue are from mvwRevenue
               Bill amount, Payment amount, Adjustment amount are from NS tables
               Outstanding amount is from OrderLineDetail
#### need to change from mvwRevenue to fctRevenue to get the isRecRevAdjustment flag

Oct 4, 2017
Try grouping and reporting at the OLI+TicketNumber level

Oct 5, 2017
OrderLineItem left join revenue such that to get the information only from the current ticket
Assume (if any) other tickets are charge errors.
It would be good to extract those in a separate file and double check

non Current Ticket has revenue recognized amount
It will over or under revenue if exclude those amount with the non Current Ticket
'''

import pandas as pd
import numpy as np
from data import GetGHIData as GData

output_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Oct11\\"
file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Oct11\\"

#####################################
#  Read the Revenue Data            #
#####################################
#there are a good number of rows in revenue_data do not have Ticket Number and these rows do not have Revenue value
# a = Revenue_data[(Revenue_data.TicketNumber.isnull())].index
# Revenue_data.loc[a]['TotalUSDRevenue'].unique() # is nan

Revenue_data = GData.revenue_data('Claim2Rev', folder=file_path, refresh=0)
Revenue_data = Revenue_data[~(Revenue_data.TicketNumber.isnull())]

#####################################
#  Read the Order Line Item Detail  #
#####################################

OLI_data = GData.OLI_detail('Claim2Rev', file_path, 0)
OLI_utilization = GData.OLI_detail('utilization', file_path, 0)

#########################################
#  Filter the scenario needs checking   #
#########################################

## Select and drop the OrderLineItem that has OLI value format is not OLnnnnn
## str.startswith does not take regexes, use str.match
b = list(Revenue_data[~((Revenue_data['OLIID'].str.match("OL\d")) | \
                        (Revenue_data['OLIID']=='Unknown()'))] \
                     ['OLIID'].unique())


#Select and drop the OrderLineItem that has multiple Test values (excluding 'Unknown'). Export the value for reporting bug
#pivot with OLIID, count the number of unique Test values

a = pd.pivot_table(Revenue_data, index=['OLIID'], values=['Test'], aggfunc = lambda Test: len(set(Test.unique()) - set(['Unknown'])))
b = b + list(a[(a.Test > 1)].index) # find the OrderLineItemID with inconsistent test value

## Select and drop the OrderLineItem that has multiple CurrencyCode, ignore the null CurrencyCode. Export the value for reporting bug
a = pd.pivot_table(Revenue_data, index=['OLIID'], values=['CurrencyCode'], aggfunc = lambda CurrencyCode: len(CurrencyCode.unique())-sum(pd.isnull(CurrencyCode)))
b = b + list(a[(a.CurrencyCode> 1)].index) # find the OrderLineItemID with inconsistent currency value

error_Revenue_data = Revenue_data[(Revenue_data['OLIID'].isin(b))]
Revenue_data = Revenue_data[(~Revenue_data['OLIID'].isin(b))].copy()

#################################################################################################
#   Group by OLI + TicketNumber  to get the Revenue collected from a OLI/Assay                  #
#   The total revenue recognized up to the data extraction date                                 #
#################################################################################################

# sep 28: change to group by OLI (and not Test)
# there are revenue rows which Test = Unknown, assume these are the same as whatever the other test value.
# and get the Test value from the OrderLineItem and not from Revenue file
# fctRevenue, mvwRevenue source the Test value from Netsuite & QDX feed, and there are missing Test values in those source
''''''
# Oct 4: group by OLI + TicketNumber, calculate the sum(TotalRevenue, TotalAccrualRevenue, TotalCashRevenue
# redirect to use NS tables for Billed, Payment, and Adjustment Amount
# assuming the non current ticket (if any) is charge error and not contribute to revenue

## http://www.shanelynn.ie/summarising-aggregation-and-grouping-data-in-python-pandas/
aggregrations = {
    'TotalUSDRevenue': {'TotalUSDRevenue':'sum'}
    , 'TotalUSDAccrualRevenue' : {'TotalUSDAccrualRevenue':'sum'}
    , 'TotalUSDCashRevenue' : {'TotalUSDCashRevenue':'sum'}
#    , 'TicketNumber' : {'TicketCnt' : 'nunique'} ## need to count the number of unique ticket
    , 'AccountingPeriod' : {'AcctPeriodCnt' : 'count'}
    }
#    , 'TotalBilledAmount' : {'TotalBilledAmount':'sum'} # no, cannot sum, as there is a bill amt for every row, actual total is sent for unique ticket
#    , 'TotalPaymentAmount' : {'TotalPayment':'sum'}
#    , 'TotalPayorPaymentAmount' : {'PayorPaid':'sum'}
#    , 'TotalPatientPaymentAmount' : {'PatientPaid':'sum'}
#    , 'TotalAdjustmentAmount' : {'TotalAdjustment' : 'sum'}

Summarized_Revenue_A = Revenue_data.groupby(['OLIID','TicketNumber']).agg(aggregrations)
Summarized_Revenue_A.columns = columns = ['_'.join(col).strip() for col in Summarized_Revenue_A.columns.values] # flatten the multilevel column name
Summarized_Revenue_A.columns = [['TotalUSDRevenue', 'TotalUSDAccrualRevenue', 'TotalUSDCashRevenue',
#                                 'TotalBilledAmount',
#                                 'TotalPayment','PayorPaid','PatientPaid',
#                                 'TotalAdjustment',
#                                 'TicketCnt',
                                  'AcctPeriodCnt'
                                 ]]
Summarized_Revenue_A = Summarized_Revenue_A.reset_index()# to enable write to excel

####################################################################
#     Join Order Line Detail with Summarized Revenue Summary       #
# http://pandas.pydata.org/pandas-docs/stable/merging.html#database-style-dataframe-joining-merging
####################################################################

#Reading the Bill Amount, Payment and Adjustment from NS exported file
NS_TXN = pd.read_csv (file_path + "NS_Transaction_PerOLI_and_Ticket.rpt", sep="|", encoding="ISO-8859-1")
temp = ['TicketNumber','TicketCnt']
for a in temp:
    NS_TXN[a] = NS_TXN[a].fillna(0.0)
    NS_TXN[a] = NS_TXN[a].astype(float).astype(int).astype('str')
    NS_TXN[a] = NS_TXN[a].replace('0',np.nan)

# replace the 0 values to blank
temp = ['TicketListPrice','TicketBilledAmount', 'TicketContractedPrice', 'TicketDiscountAmt', 'TicketAdjustment', 'TicketPayment', 'PayorPaid', 'PatientPaid']
for a in temp:
    NS_TXN.loc[(NS_TXN[a] == 0.0), a] = np.NaN


#### Question: why NS tables has null TicketNumber
# Convert TicketNumber into str

# Merge Revenue, Bill, Payment & Adjustment data
# For each Ticket captured in stgBills, there is 0..1 Revenue record depends on whether revenue is recognized
Claim_join_Revenue = pd.merge(NS_TXN, Summarized_Revenue_A,  how='left', left_on=['OLIID','TicketNumber'], right_on = ['OLIID','TicketNumber'])

Claim_Not_in_OLI = Claim_join_Revenue[~(Claim_join_Revenue.TicketNumber.isin(OLI_data.CurrentTicketNumber))].sort_values(by=['OLIID','TicketNumber'])
#Claim_Not_in_OLI[['TotalUSDRevenue','TotalUSDAccrualRevenue','TotalUSDCashRevenue','TicketAdjustment','TicketPayment']].sum().round(2)
#Export to verify the non current ticket do not have revenue recognized
#And the assumption is incorrect. Outdated tickets have revenue credit/debit, Adjustment, Payment values
# and QDX ticket have 1..n QDX cases
# Adjustment could be discount, adjustment to the charge, adjustment to the payment, write off ??


#try roll up the bill, payment, adjustment to the OLI
# net the CIE from the bill, net the refund from payment, compare the payment with revenue
# expecting not matching 100%, but check how much is the delta
# based on adjustment code, estimate the discount, CIE, refund, and the rest

# 1 OrderLineDetail has 0..N Revenue OLI+Ticket
# Assume: OrderLineDetail wo TicketNumber, there isn't claim issued for the order
# Assume: the non current ticket is charge error
Claim_join_Revenue = pd.merge(Claim_join_Revenue, OLI_data, how='right', \
                              left_on=['OLIID','TicketNumber'], right_on = ['OLIID','CurrentTicketNumber']).sort_values(by=['OLIID','TicketNumber'])

                              
# Extract the order with a current claim ticket and Test Delivered since 2016
Claim_join_Revenue = Claim_join_Revenue[(~(Claim_join_Revenue.CurrentTicketNumber.isnull()) &\
                                        (Claim_join_Revenue.TestDeliveredDate >='2016-01-01'))]

# attempted to format the amount to 2 decminal places. however, then the nan is showing when writing into excel
#temp = ['BilledAmount', 'CurrentOutstanding','ListPrice','ContractedPrice',
#        'TotalUSDRevenue', 'TotalUSDAccrualRevenue','TotalUSDCashRevenue',
#        'TicketBilledAmount', 'TicketAdjustment', 'TicketPayment', 'PayorPaid','PatientPaid']
#for a in temp:
#    Claim_join_Revenue[a] = Claim_join_Revenue[a].apply(lambda x: '{0:.2f}'.format(x))
# yet to replace the 0.0 value to nan
# and print nan as blank to excel

Claim_join_Revenue = Claim_join_Revenue[[
        'OLIID','Test','CurrentTicketNumber','CaseStatusSummaryLevel2',
        'TicketCnt','TicketNumber',
        'BilledCurrency',
        'ListPrice', 'ContractedPrice', 'BilledAmount', 'CurrentOutstanding',
        'TotalUSDRevenue', 'TotalUSDAccrualRevenue','TotalUSDCashRevenue',
        'TicketListPrice', 'TicketBilledAmount', 'TicketContractedPrice','TicketDiscountAmt',
        'TicketAdjustment', 'TicketPayment', 'PayorPaid', 'PatientPaid', 
        'AcctPeriodCnt',
        'Tier1PayorID', 'Tier1PayorName', 'Tier1Payor',
         'Tier2PayorID', 'Tier2Payor', 'Tier2PayorName', 'QDXInsPlanCode',
        'Tier4PayorID', 'Tier4Payor', 'Tier4PayorName', 'LineofBenefit',
        'FinancialCategory', 'NSInCriteria', 'RevenueStatus', 'Reportable', 'TestDelivered', 'IsClaim',
        'IsCharge', 'IsFullyAdjudicated', 'BusinessUnit', 'InternationalArea',
        'Country', 'OrderStartDate', 'OLIStartDate', 'TestDeliveredDate'
        ]].sort_values( by=['OLIID','TicketNumber'])
    
##########################################
#   Reshape Revenue and Payment Data     #
##########################################

TXN_Detail = pd.pivot_table(Claim_join_Revenue, index = ['OLIID','Test','OrderStartDate','TestDeliveredDate',
                                                         'ClaimPeriod','ClaimPeriodDate',
                                                         'Tier1Payor','Tier2Payor','Tier4Payor','FinancialCategory','LineofBenefit','Currency'], 
                            values = ['TotalUSDAccrualRevenue','TotalUSDCashRevenue',
                                      "PayorPaid", "PatientPaid", 
                                      "TotalPayment", "TotalAdjustment", "CurrentOutstanding", 
                                     # "TotalChargeError",
                                      "ListPrice","BilledAmount","ContractedPrice"])

TXN_Detail = TXN_Detail.stack().reset_index()
TXN_Detail.columns = [['OLIID','Test','OrderStartDate','TestDeliveredDate',
                       'ClaimPeriod','ClaimPeriodDate',
                       'Tier1Payor','Tier2Payor','Tier4Payor','FinancialCategory','LineofBenefit','Currency',
                       'TXNSubtype','Value']]

# drop the rows with zeros
TXN_Detail = TXN_Detail[~(TXN_Detail.Value == 0)]

## need to add the TXNSubtype
TXN_Detail.loc[TXN_Detail['TXNSubtype'].isin(['TotalUSDAccrualRevenue','TotalUSDCashRevenue']),'TXNType'] = 'Revenue'
TXN_Detail.loc[TXN_Detail['TXNSubtype'].isin(['PayorPaid','PatientPaid']),'TXNType'] = 'PaymentSRC'
TXN_Detail.loc[TXN_Detail['TXNSubtype'].isin(['TotalPayment','TotalAdjustment','CurrentOutstanding']),'TXNType'] = 'Receipt'
TXN_Detail.loc[TXN_Detail['TXNSubtype'].isin(["ListPrice","BilledAmount","ContractedPrice"]),'TXNType'] = 'PriceBook'

###############################################
#     Write the columns into a excel file     #
###############################################

output_file = 'Processed_Revenue_Data-Sep28.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl')
Revenue_data.to_excel(writer, sheet_name='Revenue_data', index=False)
writer.save()
writer.close()

output_file = 'error_Revenue_Data-Sep28.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl')
error_Revenue_data.to_excel(writer, sheet_name='error_Revenue_data', index=False)
writer.save()
writer.close()

output_file = 'OLI_Ticket_Payment_Revenue.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl')
Claim_join_Revenue.to_excel(writer, sheet_name='OLI_Payment_Revenue', index=False)
writer.save()
writer.close()

output_file = 'OLI_outdated_ticket.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl')
Claim_Not_in_OLI.to_excel(writer, sheet_name='OLI_outdated_Ticket', index=False)
writer.save()
writer.close()

output_file = 'TXN_Detail.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl')
TXN_Detail.to_excel(writer, sheet_name='TXN_Detail', index=False)
writer.save()
writer.close()

output_file = 'OLI_Criteria.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl')
OLI_utilization.to_excel(writer, sheet_name='OLI_ClinicalCriteria', index=False)
writer.save()
writer.close()



