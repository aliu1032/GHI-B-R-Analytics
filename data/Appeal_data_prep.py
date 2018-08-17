'''
Created on Sep 7, 2017

@author: aliu

Generate the combined appeal.txt and appealsucces.txt to give a view of the appeal case history and complete detail
return appeal_status and e2e_appeal_history in a dictionary

Oct 20: group the appeal by ['appealCaseNum','appealTickNum'] 
        instead of ['appealCaseNum','appealTickNum','appealAccession','appealInsCode']
        because an OLI have multiple cases/tickets; and different appeal level could be appeal to different insurance, denial reason
        
        
Jan 3: to do: need the ODSProd01.Quadax refresh, update the GetQDXData appeal_case_status sql
              include appealDeadDt, update the logic to find the last Denial Reason using appealEntryDt and appealDeadDt
              - align with BI logic
'''

import pandas as pd
from data import GetQDXData as QData

def make_appeal_data(file_path, refresh):
    
    #################################################
    #        Read the QDX Appeal data files         #
    #################################################
    
    #included in the SQL
    #prep_file_name="QDX_Appeal.xlsx"
    #appeal_level = pd.read_excel(prep_file_path+prep_file_name, sheetname = "Appeal_Level", skiprows=2)
    #appeal_level.appealLvl = appeal_level.appealLvl.astype('str')
    
    appeal = QData.appeal_case_status(file_path, refresh)
    complete_appeal = QData.complete_appeal_case(file_path, refresh) # this file only contains cases of the prior 2 years of the day of extract
    case_reference = QData.claim_case_status('case_reference',file_path, refresh)

    ########################################################################################################
    #                                                                                                      #
    #  Appeal Data Preparation                                                                             #
    #                                                                                                      #
    ########################################################################################################
    
    ##############################################################################
    #  Merge appeal.txt with case information                                    #
    #  Drop the appeal rows if there is no information on                        #
    #  which ticket and OLI is the appeal case for                               #
    ##############################################################################
    
    
    # update the inconsistent campaign code
    appeal.loc[appeal.appealCampaignCode == 'uhclnp'] = 'UHCLNP'
    
    # case status is the billing case status in OLI
    # append OLI, claim ticket number, caseEntryYrMth to appeal status
    # drop the appeal row if there isn't caseTicketNum and caseAccession number 
    appeal = pd.merge(appeal, case_reference, how='left', left_on='appealCaseNumber', right_on='caseCaseNum')
    appeal = appeal.drop('caseCaseNum',1)
    appeal.rename(columns = {'caseTicketNum': 'appealTickNum',
                             'appealCaseNumber' : 'appealCaseNum',
                             'caseAccession':'appealAccession',
                             'appealLetDt' : 'appealReportDt',
                             'appealDenialLetterDt':'appealDenialDt'}, inplace = True)
    appeal = appeal[~(appeal.appealTickNum.isnull() & appeal.appealAccession.isnull())]

    #########################################
    #  Flip the sign of appealAmtClmRec     #
    #  and appealAmtAplRec                  #
    #########################################
    
    complete_appeal.loc[(complete_appeal.appealAmtAplRec != 0), 'appealAmtAplRec'] = complete_appeal.loc[(complete_appeal.appealAmtAplRec != 0), 'appealAmtAplRec'] * -1 

    ###################################################################
    #  Shape appeal status into appeal history - wide appeal level    #
    #  Assumption: AppealCaseNumber + Appeal Lvl is unique            #
    #  make the reshape before adding columns to appeal.txt           #
    ###################################################################
    
    # Create a mapping of Level# [1,2,3,..91,92,93] to Code [A1,A2,..ER, L1]
#    appeal_level_dict = dict(zip(appeal_level['appealLvl'].astype(str),appeal_level['appealLvlCode']))
    
    temp_appeal = appeal[['appealCaseNum','appealTickNum','appealAccession','appealInsCode',\
#                          'appealLvl','appealStatus',\
                          'appealLvlCode','appealStatus','appealStatusDesc',\
                          'appealDenialDt','appealReportDt','appealEntryDt','appealDenReason','appealDenReasonDesc','appealCampaignCode']].copy()
    # translate the appeallvl to appeallvl code by dict
#    temp_appeal['appealLvlCode'] = temp_appeal['appealLvl'].astype(str).replace(appeal_level_dict)
                                 
    # for each row, add the appeal lvl code corresponding column label for dates, inscode, denreason
    temp_appeal['Lvl_DenialDt'] = temp_appeal.appealLvlCode + '_DenialDt'
    temp_appeal['Lvl_InsCode'] = temp_appeal.appealLvlCode + '_InsCode'
    temp_appeal['Lvl_EntryDt'] = temp_appeal.appealLvlCode + '_EntryDt'
    temp_appeal['Lvl_ReportDt'] = temp_appeal.appealLvlCode + '_ReportDt'    
    temp_appeal['Lvl_DenReason'] = temp_appeal.appealLvlCode + '_DenReason'
    temp_appeal['Lvl_Status'] = temp_appeal.appealLvlCode + '_Status' ###

    LvlStatus = pd.pivot_table(temp_appeal, index= ['appealCaseNum','appealTickNum'],\
                              columns = 'Lvl_Status', values='appealStatus', aggfunc='first')    
    DenialDt = pd.pivot_table(temp_appeal, index= ['appealCaseNum','appealTickNum'],\
                              columns = 'Lvl_DenialDt', values='appealDenialDt', aggfunc='first')
    ReportDt = pd.pivot_table(temp_appeal, index= ['appealCaseNum','appealTickNum'],\
                              columns = 'Lvl_ReportDt', values='appealReportDt', aggfunc='first')
    Lvl_InsCode = pd.pivot_table(temp_appeal, index= ['appealCaseNum','appealTickNum'],\
                              columns = 'Lvl_InsCode', values='appealInsCode', aggfunc='first')
    EntryDt = pd.pivot_table(temp_appeal, index= ['appealCaseNum','appealTickNum'],\
                              columns = 'Lvl_EntryDt', values='appealEntryDt', aggfunc='first')
    LvlDenReason = pd.pivot_table(temp_appeal, index= ['appealCaseNum','appealTickNum'],\
                              columns = 'Lvl_DenReason', values='appealDenReason', aggfunc='first')
    ###
    Lvl_StatusDesc = pd.pivot_table(temp_appeal, index= ['appealCaseNum','appealTickNum'],\
                              columns = 'appealLvlCode', values='appealStatusDesc', aggfunc='first')
    
    # count the number of appeal level for a Ticket, for an OLI
    CaseappealLvlCnt = temp_appeal.groupby(['appealCaseNum','appealTickNum']).size().rename('CaseappealLvlCnt')  #number of level for the case
    OLIappealLvlCnt = temp_appeal.groupby(['appealAccession']).size().rename('OLIappealLvlCnt').to_frame()  # number of levels for the OLI, sometime, an OLI can have mutliple cases
 
    # the DenialDt, EntryDt, LvlStatus are dataframe with appealCaseNumber as the index, thus concat function join the columns by index
    appealcase_wide = pd.concat([CaseappealLvlCnt, Lvl_InsCode, LvlDenReason, LvlStatus, DenialDt, ReportDt, EntryDt, Lvl_StatusDesc], axis=1).reset_index()
    appealcase_wide = pd.merge(appealcase_wide, case_reference,
                               how='left', left_on = ['appealCaseNum','appealTickNum'], right_on = ['caseCaseNum','caseTicketNum'])
    appealcase_wide.rename(columns={'caseAccession':'appealAccession'}, inplace=True)
    appealcase_wide.drop(['caseCaseNum','caseTicketNum','caseEntryYrMth'], 1, inplace=True)

    appeal_LvlCode_Master = ['A1', 'A2','A3', 'A4','A5', 'ER','L1', 'L2', 'L3']
    for x in (set(appeal_LvlCode_Master) - set(temp_appeal.appealLvlCode.unique())):
        appealcase_wide[x] = ''
        appealcase_wide[x + '_DenialDt'] = ''
        appealcase_wide[x + '_ReportDt'] = ''        
        appealcase_wide[x + '_EntryDt'] = ''
        appealcase_wide[x + '_InsCode'] = ''
        appealcase_wide[x + '_DenReason'] = ''
        appealcase_wide[x + '_Status'] = ''
 
    appealcase_wide['A4_ReportDt'] = ''
    appealcase_wide['L2_ReportDt'] = ''
      
    # find the OLI latest denial reason from the latest record.
    # use appealDenialDt as it has less missing data than appealEntryDt
#    a = temp_appeal.groupby(['appealAccession'])['appealDenialDt'].idxmax(skipna = True)
    temp = temp_appeal.groupby(['appealAccession'])['appealDenialDt']
    a = temp.apply((lambda x : x.keys()[x.values.argmax()]))
    #a = temp_appeal.groupby(['appealAccession'])['appealEntryDt'].idxmax(skipna = True)
    OLI_latest_denial = temp_appeal.loc[a][['appealCaseNum','appealTickNum','appealAccession','appealInsCode',\
                                            'appealLvlCode', 'appealDenReason','appealDenReasonDesc',\
                                            'appealEntryDt','appealDenialDt', 'appealCampaignCode']].reset_index(drop = True)
                                            # 'appealReportDt',
    OLI_latest_denial.rename(columns = {'appealLvlCode' : 'Last Appeal level',
                                        'appealDenReason':'lastappealDenReason',
                                        'appealDenReasonDesc':'lastappealDenReasonDesc',
                                        'appealEntryDt' : 'lastappealEntryDt',                                        
                                        'appealInsCode':'lastDenialInsCode',
                                        'appealDenialDt':'lastappealDenialDt'}, inplace=True)
    OLI_latest_denial = pd.merge(OLI_latest_denial, OLIappealLvlCnt, how = 'left', left_on=['appealAccession'], right_index=True)
    
    # find the OLI first denial and get the appealReportDt
    # there are appealAccession without any information on the ReportDt. 
    # when groupby to find the init appealReportDt, OLI with no reportdt causes issue
#    a = temp_appeal.groupby(['appealAccession'])['appealEntryDt'].idxmin(skipna = True)
    temp = temp_appeal.groupby(['appealAccession'])['appealEntryDt']
    a = temp.apply((lambda x: x.keys()[x.values.argmin()]))
                                       
    OLI_init_denial = temp_appeal.loc[a][['appealAccession','appealEntryDt']].reset_index(drop = True)
    OLI_init_denial.columns = ['appealAccession','firstappealEntryDt']
    OLI_latest_denial = pd.merge(OLI_latest_denial, OLI_init_denial, how = 'left', on=['appealAccession'])

    # add the OLI_latest_denial information to the appealcase_wide. Note: 1 appealAccession has 1..N appealCaseNum
    appealcase_wide = pd.merge(appealcase_wide, OLI_latest_denial[['appealAccession','OLIappealLvlCnt','Last Appeal level',
                                                                   'lastappealDenReason','lastappealDenReasonDesc',
                                                                   'lastappealEntryDt',
                                                                   'lastDenialInsCode','lastappealDenialDt', 'firstappealEntryDt',
                                                                   'appealCampaignCode']],
                               how='left',left_on=['appealAccession'],right_on=['appealAccession'])
                                       
    #create appeal_history_wide with complete status
    ClaimTicket_appeal_wide = pd.merge(appealcase_wide, complete_appeal, how='left',\
                                       left_on=['appealCaseNum','appealTickNum','appealAccession'],\
                                       right_on=['appealCaseNum','appealTickNum','appealAccession'])
    
    # if the appeal is complete, then the lastestappeallvlCode and appealDenReason is available from the complete_appeal
    # if the appeal is incomplete, then copy the latestappealDenReason into appealDenReason
    a = ClaimTicket_appeal_wide.appealDenReasonDesc.isnull()
    ClaimTicket_appeal_wide.loc[a,'appealDenReason'] = ClaimTicket_appeal_wide.loc[a, 'lastappealDenReason']
    ClaimTicket_appeal_wide.loc[a,'appealDenReasonDesc'] = ClaimTicket_appeal_wide.loc[a, 'lastappealDenReasonDesc']
    ClaimTicket_appeal_wide.loc[a,'appealInsCode'] = ClaimTicket_appeal_wide.loc[a, 'lastDenialInsCode']

    #########################################################################################
    #  Enrich Data                                                                          #
    #  - resolve Appeal Level, Denial Reason, Appeal level status code                      #
    #  - Add the success appeal information from appealsuccess file to appeal status file            #
    #  - Adding the OLI#, Ticket Number from QDX Case to Appeal history wide                #
    #########################################################################################

    # append the success flag from complete_appeal file to appeal
    # if a claim appeal is succeed, it is assumed all appeal cases opened for the claim are succeed.
    # adding the complete appeal cnt
    appeal = pd.merge(appeal, complete_appeal[['appealTickNum', 'appealCaseNum','appealAccession','appealSuccess']],\
                             how='left', left_on=['appealCaseNum','appealAccession','appealTickNum'],\
                             right_on=['appealCaseNum','appealAccession','appealTickNum'])
    
    CaseappealLvlCnt = CaseappealLvlCnt.to_frame()
    # add the appeal level cnt for an appeal case
    appeal = pd.merge(appeal, CaseappealLvlCnt, how='left', left_on=['appealCaseNum','appealTickNum'], right_index = True)
    appeal = pd.merge(appeal, OLIappealLvlCnt, how='left', left_on=['appealAccession'], right_index=True)
    
    #########################################################################################
    #  Rearranging the columns                                                              #
    #########################################################################################

    appeal = appeal[['appealAccession','appealCaseNum', 'appealTickNum','caseEntryYrMth',
                     'appealInsCode', 'appealEntryDt', 
                     'appealDenReason', 'appealDenReasonDesc',
                     'appealLvl','appealLvlCode','appealLvlDesc',
                     'appealStatus','appealStatusDesc',
                     'appealSuccess',
                     'appealDenialDt','appealReportDt',  'appealCampaignCode',
                     'appealAllowed','CaseappealLvlCnt', 'OLIappealLvlCnt']].sort_values(by=['appealAccession','appealCaseNum','appealTickNum'])

       
    ClaimTicket_appeal_wide = ClaimTicket_appeal_wide[[
                    'appealAccession',
                    'appealCaseNum', 'appealTickNum', 'CaseappealLvlCnt',
                    
                    'A1_Status', 'A2_Status', 'A3_Status', 'A4_Status', 'A5_Status',
                    'ER_Status', 'L1_Status', 'L2_Status', 'L3_Status',

                    'A1', 'A2', 'A3', 'A4','A5',
                    'ER', 'L1', 'L2', 'L3',

                    'A1_InsCode','A2_InsCode', 'A3_InsCode', 'A4_InsCode', 'A5_InsCode',
                    'ER_InsCode', 'L1_InsCode','L2_InsCode', 'L3_InsCode',
                    
                    'A1_DenReason', 'A2_DenReason', 'A3_DenReason','A4_DenReason', 'A5_DenReason',
                    'ER_DenReason','L1_DenReason', 'L2_DenReason', 'L3_DenReason',
                    
                    'A1_DenialDt', 'A2_DenialDt', 'A3_DenialDt', 'A4_DenialDt', 'A5_DenialDt', 
                    'ER_DenialDt','L1_DenialDt', 'L2_DenialDt', 'L3_DenialDt',
                    
                    'A1_EntryDt', 'A2_EntryDt', 'A3_EntryDt', 'A4_EntryDt', 'A5_EntryDt',
                    'ER_EntryDt', 'L1_EntryDt', 'L2_EntryDt', 'L3_EntryDt',
                    
                    'A1_ReportDt', 'A2_ReportDt', 'A3_ReportDt', 'A4_ReportDt', 'A5_ReportDt',
                    'ER_ReportDt', 'L1_ReportDt', 'L2_ReportDt', 'L3_ReportDt',
                    
                    'OLIappealLvlCnt',
                    
                    'appealReqNum', 'appealDOS',
                    'appealInsCode', 'appealInsFC', 'Last Appeal level', 'appealDenReason', 'appealDenReasonDesc',

                    'lastDenialInsCode','lastappealDenialDt','firstappealEntryDt','lastappealEntryDt', 'appealCampaignCode',

                    'appealAmtChg', 'appealAmtChgExp', 'appealAmtAllow', 'appealAmtClmRec',
                    'appealAmt', 'appealAmtAplRec', 'appealRptDt', 'appealSuccess',
                    'appealCurrency'
                    ]].sort_values(by=['appealAccession','appealCaseNum','appealTickNum'])
        
    dfs = {"ClaimTicket_appeal_wide" : ClaimTicket_appeal_wide, "appeal" : appeal}

    return dfs

''' 
    output_file = 'Appeal_cases-Oct20.xlsx'
    writer = pd.ExcelWriter(file_path+output_file, engine = 'openpyxl')
    appeal.to_excel(writer, sheet_name='Appeal_cases', index=False, encoding='utf-8')
    writer.save()
    writer.close()
    
    output_file = 'ClaimTicket_appeal_wide-Oct20.xlsx'
    writer = pd.ExcelWriter(file_path+output_file, engine = 'openpyxl')
    ClaimTicket_appeal_wide.to_excel(writer, sheet_name='ClaimTicket_appeal_wide', index=False, encoding='utf-8')
    writer.save()
    writer.close()

'''
'''
    #########################################
    #  Data exploration                     #
    #  Filter the scenario needs checking   #
    #########################################
    
    ## Group appeal case and appeal case level. confirmed there is 1 row for appealCaseNum + appealLvl
    a = appeal.groupby(['appealCaseNum','appealLvl']).size()
    b = a[a>1].index.get_level_values('appealCaseNum')  
    
    ## Group the appeal case. CaseNum : TickNum is 1:1
    a = appeal.groupby(['appealCaseNum'])['appealTickNum'].nunique()
    b = a[a>1].index.get_level_values('appealCaseNum')  
    
    a = appeal.groupby(['appealTickNum'])['appealCaseNum'].nunique()
    b = a[a>1].index.get_level_values('appealTickNum') 

    ## Group the OLI. Find the OLI appeal with multiple cases/tickets
    a = appeal.groupby(['appealAccession'])['appealCaseNum'].nunique()
    b = a[a>1].index.get_level_values('appealAccession')  
            ## alt way
    a = appeal.pivot_table(index=['appealAccession'], values=['appealCaseNum'], aggfunc = lambda appealCaseNum: len(appealCaseNum.unique()))
    c = list(a[(a.appealCaseNum >1)].index)
    
    ## Group the appeal case and find the cases which have multiple appeal insurance at different level
    a = appeal.groupby(['appealCaseNum'])['appealInsCode'].nunique()
    b = a[a>1].index.get_level_values('appealCaseNum')  

    ## Group the appeal case and find the cases which have multiple appeal denial reason at different level
    a = appeal.groupby(['appealCaseNum'])['appealDenReason'].nunique()
    b = a[a>1].index.get_level_values('appealCaseNum')  
 
    ## Group completed appeal by OLI and check for multiple cases. Export the OLI for further research
    ## It is assumed a OLI is mapped to 1 QDX Case number: Wrong, an OLI can have multiple cases, each case can be an appeal cycle
    a = pd.pivot_table(complete_appeal, index=['appealAccession'], values=['appealCaseNum'], aggfunc = lambda appealCaseNum: len(appealCaseNum.unique()))
    b = list(a[(a.appealCaseNum > 1)].index) # find the OrderLineItemID with multiple cases
'''
'''
    There are rows for a Claim Case + n Appeal Level. The denial reason typically unique for a claim case. 
    If a claim have been appealed for multiple denial reasons, the denial reasons would be vary for different levels.
    
    #explore case status on OLI, case number, ticket number
    
    ## Group appeal case and appealEntryDt.
    ## assumed that every appeal level has different entry date, and the appeal level with the latest entry date is the latest appeal case
    ## however, there are cases with multiple appeal levels enters on the same date
    a = appeal_status.groupby(['appealCaseNumber','appealEntryDt']).size()
    b = a[a>1].index.get_level_values('appealCaseNumber')
    appeal_status[(appeal_status.appealCaseNumber.isin(b))][['appealCaseNumber','appealLvl','appealStatus','appealEntryDt','appealDenialDt']]
    ## -> change to use also appealDenial Letter Dt??
    
    ## Double check data model in case file
    #a = case_status.groupby(['caseAccession','caseCaseNum']).size()
    #b = a[a>1].index.get_level_values('caseAccession')
    
    a = pd.pivot_table(case_status, index=['caseAccession'], values=['caseCaseNum'], aggfunc = lambda caseCaseNum: len(caseCaseNum.unique()))
    b = list(a[(a.caseCaseNum > 1)].index) 
    ## There are multiple QDX case for an OLI
    
    a = pd.pivot_table(case_status, index=['caseAccession'], values=['caseTicketNum'], aggfunc = lambda caseTicketNum: len(caseTicketNum.unique()))
    b = list(a[(a.caseTicketNum > 1)].index) # find the OrderLineItemID with multiple cases
    ## There are multiple QDX ticket for an OLI
    
    a = pd.pivot_table(case_status, index=['caseCaseNum'], values=['caseTicketNum'], aggfunc = lambda caseTicketNum: len(caseTicketNum.unique()))
    b = list(a[(a.caseTicketNum > 1)].index) # find the OrderLineItemID with multiple cases
    ## Each QDX Case is associated with 1 QDX Ticket
'''

