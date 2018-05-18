'''
Created on Nov 13, 2017

@author: aliu
'''
import pandas as pd
#import numpy as np
from datetime import datetime

QDX_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Nov07\\"
GHI_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Nov07\\"

print ('Abandoned Appeal :: start :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

###############################################################
#   Read QDX Claim and Payment (Receipt) data                 #
###############################################################

from data import GetQDXData as QData

Claim_pymnt = QData.stdPayment('Claim2Rev', QDX_file_path, 0)
Claim_bill = QData.stdClaim('Claim2Rev', QDX_file_path, 0)


AbandonClaim_pymnt = Claim_pymnt[Claim_pymnt.QDXAdjustmentCode=='NAC']


# in the pymnt, a 3 of 1037 abandoned OLIID have multiple NAC rows
temp = AbandonClaim_pymnt.groupby('OLIID').agg({'OLIID' : 'count'})
AbandonClaim_pymnt[AbandonClaim_pymnt.OLIID.isin(temp[temp.OLIID>1].index)]
Claim_pymnt[Claim_pymnt.OLIID=='OL000812016'].sort_values(by='TXNLineNumber')


AbandonClaim_claim = Claim_bill[Claim_bill.OLIID.isin(AbandonClaim_pymnt.OLIID.unique())]
temp = AbandonClaim_claim.groupby('OLIID').agg({'OLIID' : 'count'})
AbandonClaim_claim[AbandonClaim_claim.OLIID.isin(temp[temp.OLIID>1].index)]
AbandonClaim_claim[AbandonClaim_claim.OLIID=='OL000788192'] # example of claim with 2 tickets, ticket#1 is CIE, ticket#2 is abandoned appeal claim

# from the AbandonClaim_claim
# OLIID, CurrentTicketNumber, Test, TestDeliveredDate, Payor info, BillingCase status,
# null appeal information, appealResult = Abandoned
# also some monetary information

temp_claim = AbandonClaim_claim[['OLIID','TicketNumber','Test','OLIDOS']]
#(inner join??) merge temp_claim with OLI_data by OLIID + Ticket to get the current ticket payor information and select the current ticket claim