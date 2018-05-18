'''
Created on Sep 7, 2017
@author: aliu

Building the Claim2Rev report using 
- EDWDB.Staging.Analytics.stgOrderDetail
- EDWDB.Staging.Analytics.mvwRevenue

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

Oct 11: Clean up the code to use the following data sources. The output files reconciles with BI Tableau Payor Summary, Payor Analysis dashboards.
    Data source :
    Analytics.OrderLineItem: OLI Billed Amount, Contracted Price, Payment, Adjustment & Outstanding
    Analytics.mvwRevenue: Revenue

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

Revenue_data = GData.revenue_data('OLI_Payment_Revenue', folder=file_path, refresh=0)
Revenue_data = Revenue_data[~(Revenue_data.TicketNumber.isnull())]

#####################################
#  Read the Order Line Item Detail  #
#####################################

OLI_data = GData.OLI_detail('OLI_Payment_Revenue', file_path, 0)

#########################################
#  Filter the scenario needs checking   #
#########################################

## In the Revenue data, select and drop the OrderLineItem that has OLI value format is not OLnnnnn
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
#   Group by OLI + Test  to get the Revenue collected from a OLI/Assay                          #
#   The total revenue, payment, adjustment received up to the data extraction date              #
#################################################################################################

# sep 28: change to group by OLI (and not Test)
# there are revenue rows which Test = Unknown, assume these are the same as whatever the other test value.
# and get the Test value from the OrderLineItem and not from Revenue file
# fctRevenue, mvwRevenue source the Test value from Netsuite & QDX feed, and there are missing Test values in those source
''''''
# Oct 4: group by OLI, calculate the sum(TotalRevenue, TotalAccrualRevenue, TotalCashRevenue
# group by OLI, calculate the OLI total Billed, Payment, Adjustment from Revenue table
# OrderLineDetail has the Billed Amount, Payment, Adjustment from the latest ticket only

# Oct 6: found issues with OrderLineItem and Revenue data
# build the script to compare OLI payment etc with Revenue payment etc - show the discrepancy 
# build the script to compare the Revenue data with NS table - show the reason not using the Revenue Table
# redirect to use NS tables for Billed, Payment, and Adjustment Amount

## http://www.shanelynn.ie/summarising-aggregation-and-grouping-data-in-python-pandas/
aggregrations = {
    'TotalUSDRevenue': {'TotalUSDRevenue':'sum'}
    , 'TotalUSDAccrualRevenue' : {'TotalUSDAccrualRevenue':'sum'}
    , 'TotalUSDCashRevenue' : {'TotalUSDCashRevenue':'sum'}
    , 'TicketNumber' : {'TicketCnt' : 'nunique'} ## need to count the number of unique ticket
    , 'AccountingPeriod' : {'AcctPeriodCnt' : 'count'}
    }
#    , 'TotalBilledAmount' : {'TotalBilledAmount':'sum'} # no, cannot sum, as there is a bill amt for every row, actual total is sent for unique ticket
#    , 'TotalPaymentAmount' : {'TotalPayment':'sum'}
#    , 'TotalPayorPaymentAmount' : {'PayorPaid':'sum'}
#    , 'TotalPatientPaymentAmount' : {'PatientPaid':'sum'}
#    , 'TotalAdjustmentAmount' : {'TotalAdjustment' : 'sum'}

Summarized_Revenue_A = Revenue_data.groupby(['OLIID']).agg(aggregrations)
Summarized_Revenue_A.columns = columns = ['_'.join(col).strip() for col in Summarized_Revenue_A.columns.values] # flatten the multilevel column name
Summarized_Revenue_A.columns = [['TotalUSDRevenue', 'TotalUSDAccrualRevenue', 'TotalUSDCashRevenue',
#                                 'TotalBilledAmount',
#                                 'TotalPayment','PayorPaid','PatientPaid',
#                                 'TotalAdjustment',
                                 'TicketCnt', 'AcctPeriodCnt'
                                 ]]
Summarized_Revenue_A = Summarized_Revenue_A.reset_index()# to enable write to excel

columns = ['AccountingPeriodDate','AccountingPeriod','ClaimPeriodDate','ClaimPeriod',
           'IsInCoverageCriteria','RevenueStatus','CurrencyCode',
           'OLIID','TicketNumber',
           'Tier1PayorID','Tier1PayorName','Tier1Payor',
           'Tier2PayorID','Tier2PayorName','Tier2Payor',
           'Tier4PayorID','Tier4PayorName','Tier4Payor',
           'Tier3PayorType']

# find the index with the min AccountingPeriodDate per group to get the initial payor & plan information, output a is a series of index number
temp = Revenue_data.groupby(['OLIID'])['AccountingPeriodDate'].idxmin(skipna = True)
Summarized_Revenue_B = Revenue_data.loc[temp][columns]
Summarized_Revenue_B.columns = [['AccountingPeriodDate_init','AccountingPeriod_init',
                                 'ClaimPeriodDate_init','ClaimPeriod_init',
                                 'IsInCoverageCriteria_init','RevenueStatus_init','CurrencyCode_init',
                                 'OLIID','TicketNumber_init',
                                 'Tier1PayorID_init','Tier1PayorName_init','Tier1Payor_init',
                                 'Tier2PayorID_init','Tier2PayorName_init','Tier2Payor_init',
                                 'Tier4PayorID_init','Tier4PayorName_init','Tier4Payor_init',
                                 'Tier3PayorType_init']]

temp = Revenue_data.groupby(['OLIID'])['AccountingPeriodDate'].idxmax(skipna = True)
Summarized_Revenue_C = Revenue_data.loc[temp][columns]
Summarized_Revenue_C.columns = [['AccountingPeriodDate_last','AccountingPeriod_last',
                                 'ClaimPeriodDate_last','ClaimPeriod_last',
                                 'IsInCoverageCriteria_last','RevenueStatus_last','CurrencyCode_last',
                                 'OLIID','TicketNumber_last',
                                 'Tier1PayorID_last','Tier1PayorName_last','Tier1Payor_last',
                                 'Tier2PayorID_last','Tier2PayorName_last','Tier2Payor_last',
                                 'Tier4PayorID_last','Tier4PayorName_last','Tier4Payor_last',
                                 'Tier3PayorType_last']]

# Join the summarized revenue data: Total Revenue, Accrual Revenue, Cash Revenue, Init Tier 1, Tier 2 and Tier 4 information
Summarized_Revenue_D = pd.merge(Summarized_Revenue_B, Summarized_Revenue_C, how='outer', \
                                left_on=['OLIID'], right_on = ['OLIID'])
Summarized_Revenue = pd.merge(Summarized_Revenue_A, Summarized_Revenue_D, how='outer', \
                              left_on=['OLIID'], right_on = ['OLIID'])

#Select columns
Summarized_Revenue = Summarized_Revenue[['OLIID',
                                         'TotalUSDRevenue', 'TotalUSDAccrualRevenue','TotalUSDCashRevenue', 
                                         'AcctPeriodCnt','TicketCnt',
                                         'CurrencyCode_last',
#                                         'TotalBilledAmount','TotalPayment','PayorPaid','PatientPaid',
#                                         'TotalAdjustment',
                                         'AccountingPeriod_init','AccountingPeriod_last',
                                         'AccountingPeriodDate_init','AccountingPeriodDate_last',
                                         'ClaimPeriod_last', 'ClaimPeriodDate_last',
                                         'TicketNumber_init', 'TicketNumber_last',
                                         'IsInCoverageCriteria_init', 'IsInCoverageCriteria_last',
                                         'RevenueStatus_init', 'RevenueStatus_last',
                                         'Tier1Payor_init','Tier1Payor_last',
                                         'Tier2Payor_init','Tier2Payor_last',
                                         'Tier4Payor_init','Tier4Payor_last',
                                         'Tier3PayorType_init','Tier3PayorType_last']]
#Rename columns
Summarized_Revenue.columns = [['OLIID', 'TotalUSDRevenue', 'TotalUSDAccrualRevenue', 'TotalUSDCashRevenue', 
                               'AcctPeriodCnt','TicketCnt',
                               'Currency',
#                               'TotalBilledAmount','TotalPayment','PayorPaid','PatientPaid',
#                               'TotalAdjustment',
                               'AccountingPeriod_init','AccountingPeriod_last',
                               'AccountingPeriodDate_init','AccountingPeriodDate_last',
                               'ClaimPeriod','ClaimPeriodDate',
                               'TicketNumber_init', 'TicketNumber_last',
                               'IsInCoverageCriteria_init', 'IsInCoverageCriteria_last',
                               'RevenueStatus_init', 'RevenueStatus_last',
                               'Tier1Payor_init','Tier1Payor_last',
                               'Tier2Payor_init','Tier2Payor_last',
                               'Tier4Payor_init','Tier4Payor_last',
                               'Tier3PayorType_init','Tier3PayorType_last']] 

####################################################################
#     Join Order Line Detail with Summarized Revenue Summary       #
# http://pandas.pydata.org/pandas-docs/stable/merging.html#database-style-dataframe-joining-merging
####################################################################
###Join OLI with Summarized Revenue

Claim_join_Revenue = pd.merge(OLI_data, Summarized_Revenue, how='outer', left_on=['OLIID'], right_on = ['OLIID'])

################################################
#     Compute and fill the missing data        #
################################################
## Fill in missing Payer Information with the Payer information from Revenue table##
temp = Claim_join_Revenue[(Claim_join_Revenue.Tier1Payor.isnull())].index

impute_Payor = {'Tier1Payor':'Tier1Payor_last',
                'Tier2Payor':'Tier2Payor_last',
                'Tier4Payor':'Tier4Payor_last'}

for a in list(impute_Payor.keys()):
    Claim_join_Revenue.loc[temp,a] = Claim_join_Revenue.loc[temp, impute_Payor.get(a)]
    Claim_join_Revenue.loc[temp,a+'Name'] = Claim_join_Revenue.loc[temp,impute_Payor.get(a)].str.split('(').str.get(0).str.strip()
    Claim_join_Revenue.loc[temp,a+'ID'] = Claim_join_Revenue.loc[temp,impute_Payor.get(a)].str.split('(').str.get(1).str.split(')').str.get(0)
  
# fill the missing Claim Period and Claim Period Date by
# copy the Test Delivered Date to ClaimAccountingPeriodDate if date is available
temp = (Claim_join_Revenue.ClaimPeriodDate.isnull() &\
        ~(Claim_join_Revenue.TestDeliveredDate.isnull()))
                           
Claim_join_Revenue.loc[temp,'ClaimPeriodDate'] = Claim_join_Revenue.TestDeliveredDate[temp]+pd.offsets.MonthBegin(-1)
Claim_join_Revenue.loc[temp,'ClaimPeriod'] = Claim_join_Revenue['ClaimPeriodDate'][temp].dt.strftime('%b %Y')


# Select and rearrange columns
Claim_join_Revenue = Claim_join_Revenue[
                    ['OLIID', 'Test', 'CurrentTicketNumber', 'OrderStartDate','TestDeliveredDate','CaseStatusSummaryLevel2'
                     , 'Tier1PayorID','Tier1PayorName', 'Tier1Payor'
                     , 'Tier2PayorID', 'Tier2PayorName','Tier2Payor'
                     , 'Tier4PayorID', 'Tier4PayorName','Tier4Payor'
                     , 'QDXInsPlanCode',  'LineofBenefit', 'FinancialCategory'
                     , 'NSInCriteria','RevenueStatus'
                     , 'BilledCurrency', 'ListPrice', 'ContractedPrice','BilledAmount'
                     , 'TotalPayment', 'PayorPaid','PatientPaid'
                     , 'TotalAdjustment','CurrentOutstanding', 'TotalChargeError'
                     , 'TotalUSDRevenue', 'TotalUSDAccrualRevenue','TotalUSDCashRevenue'
                     , 'Reportable', 'TestDelivered', 'IsClaim','IsCharge', 'IsFullyAdjudicated'
                     , 'ClaimPeriod', 'ClaimPeriodDate', 'TicketNumber_init', 'TicketNumber_last',
                     'BusinessUnit', 'InternationalArea','Country'
                     ]]

#                     'BusinessUnit', 'InternationalArea',
#                     'Country',  'OLIStartDate', '
#       'DateOfService', 'AcctPeriodCnt', 'TicketCnt', 'Currency',
#       'AccountingPeriod_init', 'AccountingPeriod_last',
#       'AccountingPeriodDate_init', 'AccountingPeriodDate_last', 
#       'IsInCoverageCriteria_init', 'IsInCoverageCriteria_last',
#       'RevenueStatus_init', 'RevenueStatus_last', 'Tier1Payor_init',
#       'Tier1Payor_last', 'Tier2Payor_init', 'Tier2Payor_last',
#       'Tier4Payor_init', 'Tier4Payor_last', 'Tier3PayorType_init',
#       'Tier3PayorType_last']       

##########################################
#   Reshape Revenue and Payment Data     #
##########################################

TXN_Detail = pd.pivot_table(Claim_join_Revenue, index = ['OLIID','Test','OrderStartDate','TestDeliveredDate',
                                                         'ClaimPeriod','ClaimPeriodDate',
                                                         'Tier1Payor','Tier2Payor','Tier4Payor','FinancialCategory','LineofBenefit','BilledCurrency'], 
                            values = ['TotalUSDAccrualRevenue','TotalUSDCashRevenue',
                                      "PayorPaid", "PatientPaid", 
                                      "TotalPayment", "TotalAdjustment", "CurrentOutstanding", 
                                     # "TotalChargeError",
                                      "ListPrice","BilledAmount","ContractedPrice"])

TXN_Detail = TXN_Detail.stack().reset_index()
TXN_Detail.columns = [['OLIID','Test','OrderStartDate','TestDeliveredDate',
                       'ClaimPeriod','ClaimPeriodDate',
                       'Tier1Payor','Tier2Payor','Tier4Payor','FinancialCategory','LineofBenefit','BilledCurrency',
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

output_file = 'Processed_Revenue_Data.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl')
Revenue_data.to_excel(writer, sheet_name='Revenue_data', index=False)
writer.save()
writer.close()

output_file = 'error_Revenue_Data.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl')
error_Revenue_data.to_excel(writer, sheet_name='error_Revenue_data', index=False)
writer.save()
writer.close()

output_file = 'OLI_Payment_Revenue.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl')
Claim_join_Revenue.to_excel(writer, sheet_name='OLI_Payment_Revenue', index=False)
writer.save()
writer.close()

output_file = 'TXN_Detail_GHI.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl')
TXN_Detail.to_excel(writer, sheet_name='TXN_Detail', index=False)
writer.save()
writer.close()

output_file = 'QDX_NS_Compare.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl')
#Test.to_excel(writer, index=False)
writer.save()
writer.close()


########### Extract for Medicare for ad hoc analysis ########
Medicare = Claim_join_Revenue[(Claim_join_Revenue['Tier2PayorName'].str.contains('Medicare', case = False)) &\
                              (Claim_join_Revenue['Test']=='Prostate')
                              ]

output_file = 'Medicare_OLI_Pymnt_Rev.xlsx'
writer = pd.ExcelWriter(output_file_path+output_file, engine='openpyxl')
Medicare.to_excel(writer, index=False)
writer.save()
writer.close()
