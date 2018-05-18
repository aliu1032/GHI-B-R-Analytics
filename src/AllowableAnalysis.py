'''
Created on Nov 16, 2017

@author: aliu
'''
import pandas as pd
import numpy as np
from datetime import datetime

QDX_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Nov21\\"
GHI_file_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Nov21\\"
GHI_outfile_path = "C:\\Users\\aliu\\Box Sync\\aliu Cloud Drive\\Analytics\\Payor Analytics\\Nov21\\"

print('Allowable Analysis :: start :: ',datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

###############################################
#     Allowable analysis                      #
###############################################

from data import GetQDXData as QData
Claim_pymnt = QData.stdPayment('ClaimTicket', QDX_file_path, 0)
Claim_bill = QData.stdClaim('ClaimTicket', QDX_file_path, 0)

from data import GetGHIData as GData
OLI_data = GData.OLI_detail('ClaimTicket', GHI_file_path, 0)


Claim_pymnt_wo_OLI = Claim_pymnt[((Claim_pymnt.OLIID.isnull()) | (Claim_pymnt.OLIID == 'NONE'))]
Claim_pymnt = Claim_pymnt[(~(Claim_pymnt.OLIID.isnull()) & (Claim_pymnt.OLIID != 'NONE'))]

# find the OLI & Ticket with only 1 RI transaction
#PymntEvent = Claim_pymnt.groupby(['OLIID','Test','TicketNumber','TXNType']).agg({'OLIID': 'count'})
PymntEvent = pd.pivot_table(Claim_pymnt, index=['OLIID','Test','TicketNumber','TXNType'], values = 'TXNDate', aggfunc = 'count')
PymntEvent.reset_index(inplace=True)
PymntEvent.rename(columns = {'TXNDate':'RowCnt'}, inplace=True)

sum((PymntEvent.TXNType=='RI') & (PymntEvent.RowCnt==1)) / sum(PymntEvent.TXNType=='RI')

a = PymntEvent[(PymntEvent.TXNType=='RI') & (PymntEvent.RowCnt==1)]['OLIID']

single_pymnt = Claim_pymnt[Claim_pymnt.OLIID.isin(a) & (Claim_pymnt.TXNType=='RI')].copy()
single_pymnt['TXNAmount'] = single_pymnt['TXNAmount'] * -1
single_pymnt.rename(columns = {'TXNAmount':'PayorPaid'}, inplace=True)
single_pymnt['CompareAmt'] = single_pymnt[['PayorPaid', 'stdPymntDeductibleAmt','stdPymntCoinsAmt']].sum(axis=1)
single_pymnt = pd.merge(single_pymnt, OLI_data[['OLIID','CurrentTicketNumber','Tier1Payor','Tier2Payor','Tier4Payor','FinancialCategory']], how='left',\
                        left_on=['OLIID','TicketNumber'],right_on=['OLIID','CurrentTicketNumber'])

output_file = 'Allowable_Analysis.txt'
single_pymnt.to_csv(GHI_outfile_path+output_file, sep='|',index=False)

# find the OLI & Ticket that has multiple RI transactions
multi_pymnt = Claim_pymnt[~Claim_pymnt.OLIID.isin(a) & (Claim_pymnt.TXNType=='RI')].sort_values(by='OLIID')
output_file = 'Exclude_Allowable_Analysis.txt'
multi_pymnt.to_csv(GHI_outfile_path+output_file, sep='|',index=False)

