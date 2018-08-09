'''
Created on Jul 27, 2018

@author: aliu


Purpose of this script is to gather & prepare data for unit price analysis

Unit prices: List Price, Contract Rate, Paid Rate, Allowable Rate, Deductible, etc

GHI GNAM, users can use the information to 
- strategize Prostate PAMA rate in 2019
- assess PLA code impact  (https://www.ama-assn.org/practice-management/faq-cpt-pla)
'''

import pandas as pd
#import numpy as np
from datetime import datetime
#import time

#import project_io_config as cfg
#refresh = cfg.refresh

input_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Aug072018-UnitPrice\\"
output_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Aug072018-UnitPrice\\"

print ('UnitPrice_Analysis :: start :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

from data import GetQDXData as QData
Claim_bill = QData.stdClaim('Claim2Rev', input_file_path, 0)
Claim_pymnt = QData.stdPayment('UnitPrice_analysis', input_file_path, 0)

from data import GetGHIData as GData
OLI_data = GData.OLI_detail('Claim2Rev',input_file_path, 0)


## How insurance company determine allowable amount
## Why would insurance company change the allowable amount for a test from $1743 to $3735 (OL001006259), $1709 t0 $3713 (OL000823165)?
## Could both are legit data points?
## https://blog.getbetter.co/a-guide-to-allowed-amounts-45091af8c139


## Experiment 1
## Extract the RI rows of payment (i.e. TXNAmount < 0), get the Primary, Ticket and Payment Ins and the allowable amount
output = Claim_pymnt[(Claim_pymnt.TXNType=='RI') & (Claim_pymnt.TXNAmount <0)][['TicketNumber','OLIID','Test','OLIDOS',
                                                                                #'PrimaryInsPlan_GHICode','TicketInsPlan_GHICode',
                                                                                 'TXNLineNumber','TXNType','TXNCurrency','TXNAmount','PymntInsPlan_GHICode','stdPymntAllowedAmt',
                                                                                 'stdPymntDeductibleAmt','stdPymntCoinsAmt']]

## there could be multiple payments to a ticket
## the rows with negative payment amount are payment; the rows with positive amount are take back
## Payment amount and Allowable amount are provided in the Explanation of Benefit when insurance make a payment
## It is unlikely that total payment is greater than allowable amount. There could be multiple payment rows to make up to the allowable amount
## Experiment 2 : Group the selected rows by OLI and take the maximum allowable amount to be the allowable amount of the OLI

OLI_allowable = output.groupby(['OLIID']).agg({'stdPymntAllowedAmt':'max'})

select_columns = ['OrderID', 'OLIID', 'TestDeliveredDate', 'TestDelivered', 
       'Territory', 'TerritoryArea', 'TerritoryRegion', 'BusinessUnit',
       'InternationalArea', 'Division', 'Country', 'Specialty',
       'OrderingHCPName', 'OrderingHCPCity', 'OrderingHCPState',
       'OrderingHCPCountry', 'IsOrderingHCPCTR', 'IsOrderingHCPPECOS',
       'OrderingHCO', 'OrderingHCOCity', 'OrderingHCOState',
       'OrderingHCOCountry', 'Tier1Payor', 'Tier1PayorID', 'Tier1PayorName',
       'Tier2Payor', 'Tier2PayorID', 'Tier2PayorName', 'Tier4Payor',
       'Tier4PayorID', 'Tier4PayorName', 'QDXInsPlanCode', 'FinancialCategory',
       'QDXInsFC', 'LineOfBenefit', 'CurrentTicketNumber', 'BilledCurrency',
       'ListPrice', 'ContractedPrice', 'SubmittingDiagnosis', 'ReportingGroup',
       'NodalStatus', 'HCPProvidedClinicalStage', 'SubmittedERStatus',
       'SubmittedHER2', 'MultiplePrimaries', 'IsMultiplePrimaryConfirmed',
       'IsMultiplePrimaryRequested', 'HCPProvidedGleasonScore',
       'SubmittedNCCNRisk', 'SFDCSubmittedNCCNRisk', 'RiskGroup',
       'ClinicalStage', 'EstimatedNCCNRisk', 'FavorablePathologyComparison',
       'ExternalSpecimenID', 'RecurrenceScore', 'HER2GeneScore', 'DCISScore',
       'HCPProvidedPSA', 'PatientAgeAtOrderStart']

output = pd.merge(OLI_allowable, OLI_data[select_columns], how = 'left', on = 'OLIID')


output_file = 'Allowable_analysis.txt'
output.to_csv(output_file_path+output_file, sep='|',index=False)




#############################################################################

# exploration
# how many payment lines for a ticket

temp = pd.pivot_table(Claim_pymnt[(Claim_pymnt.TXNType=='RI') & (Claim_pymnt.OLIDOS >= '2017-01-01')], index=['TicketNumber'], values = 'TXNType', aggfunc='count')
#temp = pd.pivot_table(Claim_pymnt, index=['TicketNumber'], columns=['TXNType'], values = 'TicketNumber', aggfunc=lambda x: len(x.unique()))

TickCnt = (pd.pivot_table(Claim_bill, index=['OLIID'], values = 'TicketNumber',\
                          aggfunc = lambda TicketNumber: len(TicketNumber.unique()))).rename(columns = {'TicketNumber': 'QDXTickCnt'})

temp = Claim_pymnt[(Claim_pymnt.PrimaryInsPlan_GHICode != Claim_pymnt.TicketInsPlan_GHICode)]
temp_OLI = pd.pivot_table(temp[(temp.TXNType=='RI') & (temp.OLIDOS >= '2017-01-01')], index=['OLIID'], values = 'TXNType', aggfunc='count')

temp_OLI = pd.pivot_table(Claim_pymnt[(Claim_pymnt.TXNType=='RI') & (Claim_pymnt.OLIDOS >= '2017-01-01')], index=['OLIID'], values = 'TXNType', aggfunc='count')
                          
Claim_pymnt[(Claim_pymnt.OLIID == 'OL001003928') & (Claim_pymnt.TXNType=='RI')][['TicketNumber','OLIID','PrimaryInsPlan_GHICode','TicketInsPlan_GHICode',
                                                                                 'TXNLineNumber','TXNType','TXNAmount','PymntInsPlan_GHICode','stdPymntAllowedAmt',
                                                                                 'stdPymntDeductibleAmt','stdPymntCoinsAmt']]

Claim_pymnt[(Claim_pymnt.OLIID == 'OL000643182') & (Claim_pymnt.TXNType=='RI')][['TicketNumber','OLIID','PrimaryInsPlan_GHICode','TicketInsPlan_GHICode',
                                                                                 'TXNLineNumber','TXNType','TXNAmount','PymntInsPlan_GHICode','stdPymntAllowedAmt',
                                                                                 'stdPymntDeductibleAmt','stdPymntCoinsAmt']]

# Insurance paid multiple times and take back occured, the allowable should not be the sum
Claim_pymnt[(Claim_pymnt.OLIID == 'OL000823165') & (Claim_pymnt.TXNType=='RI')][['TicketNumber','OLIID','PrimaryInsPlan_GHICode','TicketInsPlan_GHICode',
                                                                                 'TXNLineNumber','TXNType','TXNAmount','PymntInsPlan_GHICode','stdPymntAllowedAmt',
                                                                                 'stdPymntDeductibleAmt','stdPymntCoinsAmt']]

# Insurance paid multiple times, but the allowable should not be the sum
Claim_pymnt[(Claim_pymnt.OLIID == 'OL000835320') & (Claim_pymnt.TXNType=='RI')][['TicketNumber','OLIID','PrimaryInsPlan_GHICode','TicketInsPlan_GHICode',
                                                                                 'TXNLineNumber','TXNType','TXNAmount','PymntInsPlan_GHICode','stdPymntAllowedAmt',
                                                                                 'stdPymntDeductibleAmt','stdPymntCoinsAmt']]

# Insurance paid multiple times, but the allowable should not be the sum
Claim_pymnt[(Claim_pymnt.OLIID == 'OL001006259') & (Claim_pymnt.TXNType=='RI')][['TicketNumber','OLIID','PrimaryInsPlan_GHICode','TicketInsPlan_GHICode',
                                                                                 'TXNLineNumber','TXNType','TXNAmount','PymntInsPlan_GHICode','stdPymntAllowedAmt',
                                                                                 'stdPymntDeductibleAmt','stdPymntCoinsAmt']]

# Payment by multiple insurance companies, the allowable is for the corresponding to the payment insurance company
Claim_pymnt[(Claim_pymnt.OLIID == 'OL001000653') & (Claim_pymnt.TXNType=='RI')][['TicketNumber','OLIID','PrimaryInsPlan_GHICode','TicketInsPlan_GHICode',
                                                                                 'TXNLineNumber','TXNType','TXNAmount','PymntInsPlan_GHICode','stdPymntAllowedAmt',
                                                                                 'stdPymntDeductibleAmt','stdPymntCoinsAmt']]


