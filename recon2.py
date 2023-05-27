import pandas as pd
import numpy as np

# setup the constant 2nd Try

_rg_glcode = 'RG_GLCODE'
_rg_lineid = 'RG_LINEID'
_rg_jplineid = 'RG_LINEID_JP'

_rg_plcode = 'RG_PLCODE'
_rg_pllineid = 'RG_PLLINEID'
_rg_jppllineid = 'RG_PLLINEID_JP'

_t24_hkglbal = '../LINE_BAL_SUM_HKGL.csv'
_t24_hkplbal = '../LINE_BAL_SUM_HKPL.csv'
_t24_jpglbal = '../LINE_BAL_SUM_JPGL.csv'
_t24_jpplbal = '../LINE_BAL_SUM_JPPL.csv'

_output = '../RECON_RESULT.xlsx'
_refoutput = '../RECON_REF.xlsx'
_setup = 'RECON_SETUP.xlsx'


# input file

_rawac = '../TBRAWACCOUNT.csv'
_plbal = '../TBRAW_PLBAL.csv'
_glbal = '../TBRAW_GLBAL.csv'
_placn = '../TBRAW_PLACN.csv'
_ccymap = '../TBRAWCCY_DL.csv'
_glacn = '../Tbrawglacn_20220930.xls'


# LIST OF GLCODE TO BE EXCLUDE 
exclude_ac = ['09501','09502','09503','09505']


def readlinebal (prefix, filename):
    df = pd.read_csv (filename, skiprows=1)
    
    #remove line is null
    df = df[df.Line.notnull()]

    #remove ccy is LOC
    df = df[df['CCY'] != 'LOC']

    #rmove date = PROFIT
    df = df[df['Date'] != 'PROFIT']


    # convert line to integer
    df['Line']=df['Line'].astype(int)
    df['LINEID']=prefix+'.'+df['Line'].astype(str).str.zfill(4)

    # remove the single quote from Closing Balance column
    df ['Closing Balance LCY']=df['Closing Balance LCY'].str.replace("[',]", "", regex=True).astype(float)

    return df




# read the data file
print ('READING SOURCE FILE')
rawac = pd.read_csv(_rawac)

plbal = pd.read_csv (_plbal)
plbal ['PL_CODE']=plbal['PL_CODE'].astype(str).str.zfill(6)
# override CCY_CODE with ORG_CCY
plbal ['CCY_CODE']=plbal['ORG_CCY']

glbal = pd.read_csv (_glbal)
glbal ['GL_CODE']=glbal['GL_CODE'].astype(str).str.zfill(5)

placn = pd.read_csv (_placn)
placn ['PL_CODE']=placn['PL_CODE'].astype(str).str.zfill(6)

glacn = pd.read_excel (_glacn)
glacn.rename(columns = {'Gl Code':'GL_CODE'}, inplace = True)

#rg_glcode = pd.read_excel (_rg_glcode)
rg_glcode = pd.read_excel (_setup, _rg_glcode)
rg_glcode.rename(columns = {'Gl Code':'GL_CODE'}, inplace = True)

rg_plcode = pd.read_excel (_setup, _rg_plcode)
rg_plcode ['PL_CODE']=rg_plcode['PL_CODE'].astype(str).str.zfill(6)

rg_pllineid = pd.read_excel (_setup, _rg_pllineid)
rg_lineid = pd.read_excel (_setup, _rg_lineid)

rg_jppllineid = pd.read_excel (_setup, _rg_jppllineid)
rg_jplineid = pd.read_excel (_setup, _rg_jplineid)

recon_rule = pd.read_excel (_setup, 'RECON_RULE')

#offbs_ac = pd.read_excel ("OFFBS.xlsx")
#ccy_map = pd.read_excel ("CCY_MAP.xlsx")
ccy_map = pd.read_csv (_ccymap)

#ALIGN COLUMN NAME TO CCY
ccy_map.rename(columns = {'CCY_NAME':'CCY'}, inplace = True)
#strip blank space
ccy_map['CCY']=ccy_map['CCY'].str.strip()


print ('READING LINE BAL')
#line_glbal = pd.read_excel ("line_glbal_HKGL.xlsx")
line_glbal  = readlinebal ('HKGL', _t24_hkglbal)
line_plbal  = readlinebal ('HKPL', _t24_hkplbal)
line_jpglbal= readlinebal ('JPGL', _t24_jpglbal) 
line_jpplbal= readlinebal ('JPPL', _t24_jpplbal) 

src_linebal = line_glbal
src_lineplbal = line_plbal

# format LINE ID
#line_glbal['LINEID']='HKGL.'+line_glbal['Line'].astype(str).str.zfill(4)

# join to create CCY IN GLBAL
glbal = glbal.merge (ccy_map, how='right', on=['CCY_CODE'])
plbal = plbal.merge (ccy_map, how='right', on=['CCY_CODE'])

# prepare line_glbal
line_glbal = line_glbal.merge(rg_lineid, how='left', on='LINEID')
line_plbal = line_plbal.merge(rg_pllineid, how='left', on='LINEID')
line_jpglbal = line_jpglbal.merge(rg_jplineid, how='left', on='LINEID')
line_jpplbal = line_jpplbal.merge(rg_jppllineid, how='left', on='LINEID')

print ('LINE ID WITHOUT MAP')
nomap_linebal = line_glbal[['LINEID','CCY','Closing Balance LCY']][line_glbal['RG_GROUP']==np.nan]
nomap_lineplbal = line_plbal[['LINEID','CCY','Closing Balance LCY']][line_plbal['RG_GROUP']==np.nan]
nomap_linejpbal = line_jpglbal[['LINEID','CCY','Closing Balance LCY']][line_jpglbal['JP_GROUP']==np.nan]
nomap_linejpplbal = line_jpplbal[['LINEID','CCY','Closing Balance LCY']][line_jpplbal['JP_GROUP']==np.nan]


line_glbal['RG_GROUP'].replace(np.nan, 'MISSING', inplace=True)
line_jpglbal['JP_GROUP'].replace(np.nan, 'MISSING', inplace=True)

line_glbal['RD'].replace(np.nan, 'MISSING', inplace=True)
line_glbal['RG_KEY']=line_glbal['CCY']+'-'+line_glbal['RG_GROUP']
line_glbal['RD_KEY']=line_glbal['CCY']+'-'+line_glbal['RD']
line_jpglbal['JP_KEY']=line_jpglbal['CCY']+'-'+line_jpglbal['JP_GROUP']


line_plbal['RG_GROUP'].replace(np.nan, 'MISSING', inplace=True)
line_jpplbal['JP_GROUP'].replace(np.nan, 'MISSING', inplace=True)

line_plbal['RG_KEY']=line_plbal['CCY']+'-'+line_plbal['RG_GROUP']
line_jpplbal['JP_KEY']=line_jpplbal['CCY']+'-'+line_jpplbal['JP_GROUP']


# MERGE GLBAL WITH RG TO GLCODE MAPPING
glbal = glbal.merge (rg_glcode, how='left', on='GL_CODE')

print ('GLBAL.GLCODE WITHOUT MAP')
nomap_glbal = glbal[['GL_CODE','CCY','TODAY_BALANCE']][glbal['RG_GROUP']==np.nan]

glbal['RG_GROUP'].replace(np.nan, 'MISSING', inplace=True)
glbal ['RG_KEY']=glbal['CCY']+'-'+glbal['RG_GROUP']
glbal ['JP_KEY']=glbal['CCY']+'-'+glbal['JP_GROUP']

glbal_result = glbal[['RG_KEY','TODAY_BALANCE']].copy(deep=True)
jpglbal_result = glbal[['JP_KEY','TODAY_BALANCE']].copy(deep=True)



# merge off balance sheet to GLBAL
rawac['GL_CODE']=rawac['GL_CODE'].astype(str).str.zfill(5)
rawac = rawac.merge(glacn[['GL_CODE','Asset Liab Flag','Account Type']], how='left', on=['GL_CODE'])
rawac = rawac.merge(ccy_map, how='inner', on=['CCY_CODE'])
rawac = rawac.merge(rg_glcode, how='left', on=['GL_CODE'])

print ('TBRAWACCT.GLCODE WITHOUT MAP')
nomap_rawac = rawac[['GL_CODE','CCY','LEDGER_BALANCE']][rawac['RG_GROUP']==np.nan]

rawac['RD_KEY']=rawac['CCY']+'-'+rawac['RD_GROUP']

def t24_sign_bal (bal, asset_liab, account_type):
    if asset_liab in ('D','X'):
        if account_type == 'S':
            return bal
        else:
            if bal > 0:
                return -bal
            else:
                return bal
    else:
        if bal > 0:
            return bal
        else:
            return -bal
    
rawac['TODAY_BALANCE'] = rawac.apply(lambda x: t24_sign_bal(x['LEDGER_BALANCE'], x['Asset Liab Flag'], x['Account Type']), axis=1)
rawac['RG_KEY']=rawac['CCY']+'-'+rawac['RG_GROUP']

# select offbs account only
offbs = ['X','Y']
offbs_bal = rawac[rawac['Asset Liab Flag'].isin(offbs)]
offbs_bal = offbs_bal[~offbs_bal['GL_CODE'].isin(exclude_ac)]
offbs_bal['RG_KEY'] = offbs_bal['CCY']+'-'+offbs_bal['RG_GROUP']

# merge offbs balance into GLBAL
glbal_result = pd.concat([glbal_result, offbs_bal[['RG_KEY','TODAY_BALANCE']]], axis=0)

def t24_sign_plbal (bal, profit_loss):
    if profit_loss in ('P'):
        if bal > 0:
            return bal
        else:
            return bal
    else:
        if bal > 0:
            return -bal
        else:
            return -bal


# MERGE PLBAL WITH RG TO PLCODE MAPPING
plbal = plbal.merge (placn, how='right', on=['PL_CODE'])

plbal = plbal.merge (rg_plcode, how='left', on='PL_CODE')

print ('PLBAL.PLCODE WITHOUT MAP')

# flip the balance based on printing code
plbal['TODAY_BALANCE'] = plbal.apply(lambda x: t24_sign_plbal(x['TODAY_BALANCE'], x['PRINTING_CODE']), axis=1)
nomap_plbal = plbal[['PL_CODE','CCY','TODAY_BALANCE']][plbal['RG_GROUP']==np.nan]
nomap_jpplbal = plbal[['PL_CODE','CCY','TODAY_BALANCE']][plbal['JP_GROUP']==np.nan]

plbal['RG_GROUP'].replace(np.nan, 'MISSING', inplace=True)
plbal['JP_GROUP'].replace(np.nan, 'MISSING', inplace=True)

plbal['RG_KEY']=plbal['CCY']+'-'+plbal['RG_GROUP']
plbal['JP_KEY']=plbal['CCY']+'-'+plbal['JP_GROUP']


plbal_result = plbal[['RG_KEY','TODAY_BALANCE']].copy(deep=True)
jpplbal_result = plbal[['JP_KEY','TODAY_BALANCE']].copy(deep=True)

# DETAIL GL COMPARE
detail_result = pd.DataFrame ({
    'RD_KEY':[]
})

detail_result['RD_KEY'] = pd.concat([rawac['RD_KEY'],line_glbal['RD_KEY']])
detail_result = detail_result.drop_duplicates('RD_KEY')

detail_result = detail_result.merge (rawac.groupby('RD_KEY')['TODAY_BALANCE'].sum(), how='left', on=['RD_KEY'])

detail_result = detail_result.merge (line_glbal.groupby('RD_KEY')['Closing Balance LCY'].sum(), how='left', on=['RD_KEY'])

detail_result.rename(columns = {'TODAY_BALANCE':'BAS_BAL', 'Closing Balance LCY':'T24_BAL'}, inplace= True)
detail_result['CCY']=detail_result['RD_KEY'].str.slice(start=0,stop=3)

detail_result['RD_GROUP']=detail_result['RD_KEY'].str.slice(start=4,stop=30)

detail_result = detail_result.merge (rg_glcode[['RG_GROUP','RD_GROUP','TYPE','Gl Ac Name','WS']].drop_duplicates('RD_GROUP', keep='first'), how='left', on='RD_GROUP')
detail_result.replace(np.nan, 0, inplace=True)
detail_result['DIFF']=detail_result['BAS_BAL']-detail_result['T24_BAL']

detail_result=detail_result.sort_values(by=['RD_KEY'])

detail_result = detail_result [detail_result[['BAS_BAL','T24_BAL']].ne(0).any(1)]

# GL ROOT GL COMPARE
result = pd.DataFrame({
        'RG_KEY':[]
})

result['RG_KEY'] = pd.concat([glbal['RG_KEY'],line_glbal['RG_KEY']])

result = result.drop_duplicates('RG_KEY')

result = result.merge (glbal_result.groupby('RG_KEY')['TODAY_BALANCE'].sum(), how='left', on=['RG_KEY'] )

result = result.merge (line_glbal.groupby('RG_KEY')['Closing Balance LCY'].sum(), how='left', on=['RG_KEY'])

result.rename(columns = {'TODAY_BALANCE':'BAS_BAL', 'Closing Balance LCY':'T24_BAL'}, inplace= True)

result['CCY']=result['RG_KEY'].str.slice(start=0,stop=3)

result['RG_GROUP']=result['RG_KEY'].str.slice(start=4,stop=30)

result = result.merge (rg_glcode[['RG_GROUP','ROOT GL DESC','WS']].drop_duplicates('RG_GROUP', keep='first'), how='left', on='RG_GROUP')
# remove null
result.replace(np.nan, 0, inplace=True)

result['DIFF']=result['BAS_BAL']-result['T24_BAL']

#result = result.assign (BAS_W_BAL=lambda x: 1 if x['BAS_BAL'] != 0 else 0)
result = result.sort_values(by=['RG_KEY'])

# filter out rows that balance is zero
result = result [result[['BAS_BAL','T24_BAL']].ne(0).any(1)]

# JPGL COMPARE

jpresult = pd.DataFrame({
        'JP_KEY':[]
})

jpresult['JP_KEY'] = pd.concat([jpglbal_result['JP_KEY'],line_jpglbal['JP_KEY']])

jpresult = jpresult.drop_duplicates('JP_KEY')

jpresult = jpresult.merge (jpglbal_result.groupby('JP_KEY')['TODAY_BALANCE'].sum(), how='left', on=['JP_KEY'] )

jpresult = jpresult.merge (line_jpglbal.groupby('JP_KEY')['Closing Balance LCY'].sum(), how='left', on=['JP_KEY'])

jpresult.rename(columns = {'TODAY_BALANCE':'BAS_BAL', 'Closing Balance LCY':'T24_BAL'}, inplace= True)

jpresult['CCY']=jpresult['JP_KEY'].str.slice(start=0,stop=3)

jpresult['JP_GROUP']=jpresult['JP_KEY'].str.slice(start=4,stop=30)

jpresult = jpresult.merge (rg_glcode[['JP_GROUP','WS']].drop_duplicates('JP_GROUP', keep='first'), how='left', on='JP_GROUP')

# GET DESC FROM RG_JPLINEID
jpresult = jpresult.merge (rg_jplineid[['JP_GROUP','JP_DESC']].drop_duplicates('JP_GROUP', keep='first'), how='left', on='JP_GROUP')


# remove null
jpresult.replace(np.nan, 0, inplace=True)

jpresult['DIFF']=jpresult['BAS_BAL']-jpresult['T24_BAL']

#result = result.assign (BAS_W_BAL=lambda x: 1 if x['BAS_BAL'] != 0 else 0)

jpresult['JP_KEY']=jpresult['JP_KEY'].astype(str)
jpresult = jpresult.sort_values(by=['JP_KEY'])

jpresult = jpresult [jpresult[['BAS_BAL','T24_BAL']].ne(0).any(1)]

# PL COMPARE
plresult = pd.DataFrame({
        'RG_KEY':[]
})

plresult['RG_KEY'] = pd.concat([plbal['RG_KEY'],line_plbal['RG_KEY']])

plresult = plresult.drop_duplicates('RG_KEY')

plresult = plresult.merge (plbal_result.groupby('RG_KEY')['TODAY_BALANCE'].sum(), how='left', on=['RG_KEY'] )

plresult = plresult.merge (line_plbal.groupby('RG_KEY')['Closing Balance LCY'].sum(), how='left', on=['RG_KEY'])

plresult.rename(columns = {'TODAY_BALANCE':'BAS_BAL', 'Closing Balance LCY':'T24_BAL'}, inplace= True)

plresult['CCY']=plresult['RG_KEY'].str.slice(start=0,stop=3)

plresult['RG_GROUP']=plresult['RG_KEY'].str.slice(start=4,stop=30)

plresult = plresult.merge (rg_plcode[['RG_GROUP','DESC','WS']].drop_duplicates('RG_GROUP', keep='first'), how='left', on='RG_GROUP')

plresult.replace(np.nan, 0, inplace=True)

plresult['DIFF']=plresult['BAS_BAL']-plresult['T24_BAL']

#result = result.assign (BAS_W_BAL=lambda x: 1 if x['BAS_BAL'] != 0 else 0)

plresult['RG_KEY']=plresult['RG_KEY'].astype(str)
plresult = plresult.sort_values(by=['RG_KEY'])

plresult = plresult [plresult[['BAS_BAL','T24_BAL']].ne(0).any(1)]

# JPPL COMPARE

jpplresult = pd.DataFrame({
        'JP_KEY':[]
})

jpplresult['JP_KEY'] = pd.concat([jpplbal_result['JP_KEY'],line_jpplbal['JP_KEY']])

jpplresult = jpplresult.drop_duplicates('JP_KEY')

jpplresult = jpplresult.merge (jpplbal_result.groupby('JP_KEY')['TODAY_BALANCE'].sum(), how='left', on=['JP_KEY'] )

jpplresult = jpplresult.merge (line_jpplbal.groupby('JP_KEY')['Closing Balance LCY'].sum(), how='left', on=['JP_KEY'])

jpplresult.rename(columns = {'TODAY_BALANCE':'BAS_BAL', 'Closing Balance LCY':'T24_BAL'}, inplace= True)

jpplresult['CCY']=jpplresult['JP_KEY'].str.slice(start=0,stop=3)

jpplresult['JP_GROUP']=jpplresult['JP_KEY'].str.slice(start=4,stop=30)

jpplresult = jpplresult.merge (rg_plcode[['JP_GROUP','JP_DESC','WS']].drop_duplicates('JP_GROUP', keep='first'), how='left', on='JP_GROUP')

jpplresult.replace(np.nan, 0, inplace=True)

jpplresult['DIFF']=jpplresult['BAS_BAL']-jpplresult['T24_BAL']

jpplresult['JP_KEY']=jpplresult['JP_KEY'].astype(str)
jpplresult = jpplresult.sort_values(by=['JP_KEY'])

jpplresult = jpplresult [jpplresult[['BAS_BAL','T24_BAL']].ne(0).any(1)]
 
def check_bal (bal):
    if bal == 0:
        return 0
    else:
        return 1

def rec_result (diff):
    if round(diff,2) == 0:
            return 'RECONCILE'
    else:
            return 'OTHER'



def apply_rec (df, _recordset):
    df['BAS_HAS_BAL']=df.apply(lambda x: check_bal(x['BAS_BAL']), axis=1)
    df['STATUS']=df.apply(lambda x: rec_result(x['DIFF']), axis=1)
    #df=df[df.STATUS == 'OTHER']
    df['FINDINGS']=''
    df['CLASSIFICATION']=''
    # mark EXPECTED DIFF RECORDS
   
   
    for _ind, _row in recon_rule.loc[recon_rule['REPORT'] == _recordset].iterrows():
        if _recordset in ['HKGL','HKPL']:
            df.loc[df.RG_GROUP == _row ['RG_GROUP'], 'STATUS'] = _row['STATUS']
            df.loc[df.RG_GROUP == _row ['RG_GROUP'], 'FINDINGS'] = _row['FINDINGS']
            df.loc[df.RG_GROUP == _row ['RG_GROUP'], 'CLASSIFICATION'] = _row['CLASSIFICATION']
        else:
            df.loc[df.JP_GROUP == _row ['RG_GROUP'], 'STATUS'] = _row['STATUS']
            df.loc[df.JP_GROUP == _row ['RG_GROUP'], 'FINDINGS'] = _row['FINDINGS']
            df.loc[df.JP_GROUP == _row ['RG_GROUP'], 'CLASSIFICATION'] = _row['CLASSIFICATION']

#    _expected_diff_LCADV = ['RG_15800']
#    _expected_diff_BC = ['RG_15700']    
#    _expected_diff_RG = ['RG_37000']

#    if 'RG_GROUP' in df.columns:
#        for _RECORD in _expected_diff_RG:
#            df.loc[df.RG_GROUP == _RECORD, 'STATUS'] = 'EXPECTED DIFF' 
#            df.loc[df.RG_GROUP == _RECORD, 'FINDINGS'] = 'T24 EAC INCLUDE DAILY PL POSTING'
        
#        for _RECORD in _expected_diff_LCADV:
#            df.loc[df.RG_GROUP == _RECORD, 'STATUS'] = 'EXPECTED DIFF'
#            df.loc[df.RG_GROUP == _RECORD, 'FINDINGS'] = 'SMILE LC BALANCE WONT BE REDUCED'
        
#        for _RECORD in _expected_diff_BC:
#            df.loc[df.RG_GROUP == _RECORD, 'STATUS'] = 'EXPECTED DIFF'
#            df.loc[df.RG_GROUP == _RECORD, 'FINDINGS'] = 'T24 CANT SEPARATE BB/BC'


apply_rec (result, 'HKGL')
apply_rec (detail_result, 'HKGL.DETAL')
apply_rec (plresult,  'HKPL')
apply_rec (jpresult,  'JPGL')
apply_rec (jpplresult,  'JPPL')

# create a compare dataframe with details 
consol_result = pd.DataFrame(data=None, columns=result.columns)

# add column to consol_result dataframe
consol_result = result

consol_result['TYPE']="T"
consol_result['LINEID']=""
consol_result['GL_CODE']=""

consol_detail = pd.DataFrame(data=None)
consol_bas_detail = pd.DataFrame(data=None)


consol_detail [['CCY','RG_GROUP','RG_KEY','LINEID','LINE DESC','T24_BAL']]= line_glbal [['CCY','RG_GROUP','RG_KEY','LINEID','LINE DESC','Closing Balance LCY']]
# add a sequence no group by RG_KEY
consol_detail['SEQ']=consol_detail.groupby(['RG_KEY'])['RG_KEY'].cumcount()+1


#consol_bas_detail [['CCY','RG_GROUP','RG_KEY','GL_CODE','GL DESC','ROOT GL DESC','BAS_BAL']]= rawac [['CCY','RG_GROUP','RG_KEY','GL_CODE','Gl Ac Name','ROOT GL DESC','TODAY_BALANCE']]

consol_bas_detail [['CCY','RG_GROUP','RG_KEY','GL_CODE','GL DESC','ROOT GL DESC','BAS_BAL']]= glbal [['CCY','RG_GROUP','RG_KEY','GL_CODE','Gl Ac Name','ROOT GL DESC','TODAY_BALANCE']]

# select ccy, rg_group from rawac where asset liab flag is off balance

offbs_glbal = pd.DataFrame(data=None)

# get off balance sheet balance
offbs_glbal[['CCY','RG_GROUP','RG_KEY','GL_CODE','GL DESC','ROOT GL DESC','BAS_BAL']] = rawac[['CCY','RG_GROUP','RG_KEY','GL_CODE','Gl Ac Name','ROOT GL DESC','TODAY_BALANCE']].loc[rawac['Asset Liab Flag'].isin(offbs)]
offbs_glbal = offbs_glbal.groupby(['CCY','RG_GROUP','RG_KEY','GL_CODE','GL DESC','ROOT GL DESC']).sum().reset_index()

# group obs from rawac 

consol_bas_detail = pd.concat([consol_bas_detail, offbs_glbal], axis=0)

# remove rows with zero BAS_BAL from consol_bas_detail
consol_bas_detail = consol_bas_detail[consol_bas_detail['BAS_BAL'] != 0]

# add a sequence no group by RG_KEY
consol_bas_detail['SEQ']=consol_bas_detail.groupby(['RG_KEY'])['RG_KEY'].cumcount()+1

# merge consol_detail and consol_bas_detail

consol_detail = consol_detail.merge(consol_bas_detail, how='outer', on=['CCY','RG_GROUP','RG_KEY','SEQ'])

consol_detail['TYPE']="D"

consol_result = pd.concat ([consol_result, consol_detail], axis=0) 

# sort consol_result by RG_KEY and TYPE
consol_result = consol_result.sort_values(by=['RG_KEY','TYPE'])

print ('WRITING EXCEL')

writer = pd.ExcelWriter(_output, engine='xlsxwriter')


result[['CCY','RG_GROUP','RG_KEY','ROOT GL DESC','BAS_BAL','T24_BAL','DIFF','WS','BAS_HAS_BAL','STATUS','FINDINGS','CLASSIFICATION']].to_excel(writer, sheet_name ='BS-COMPARE')

consol_result[['CCY','RG_GROUP','RG_KEY','ROOT GL DESC','TYPE','SEQ','GL_CODE','GL DESC','BAS_BAL','LINEID','LINE DESC','T24_BAL','DIFF','WS','BAS_HAS_BAL','STATUS','FINDINGS','CLASSIFICATION']].to_excel(writer, sheet_name = 'BS-CONSOL')

detail_result[['CCY','RG_GROUP', 'RD_GROUP','RD_KEY','Gl Ac Name','BAS_BAL','T24_BAL','DIFF','WS','BAS_HAS_BAL','STATUS','FINDINGS','CLASSIFICATION']].to_excel(writer, sheet_name = 'BS-DETAIL')

plresult[['CCY','RG_GROUP','RG_KEY','DESC','BAS_BAL','T24_BAL','DIFF','WS','BAS_HAS_BAL','STATUS','FINDINGS','CLASSIFICATION']].to_excel(writer, sheet_name ='PL-COMPARE')

jpresult[['CCY','JP_GROUP','JP_KEY','JP_DESC','BAS_BAL','T24_BAL','DIFF','WS','BAS_HAS_BAL','STATUS','FINDINGS','CLASSIFICATION']].to_excel(writer, sheet_name ='JPBS-COMPARE')
jpplresult[['CCY','JP_GROUP','JP_KEY','JP_DESC','BAS_BAL','T24_BAL','DIFF','WS','BAS_HAS_BAL','STATUS','FINDINGS','CLASSIFICATION']].to_excel(writer, sheet_name ='JPPL-COMPARE')

glbal_result[glbal_result['TODAY_BALANCE'] != 0].to_excel(writer, sheet_name='GLBAL W OFFBS')
glbal[glbal['TODAY_BALANCE'] !=0].to_excel(writer, sheet_name='GLBAL')
#rawac[rawac['TODAY_BALANCE'] !=0].to_excel (writer, sheet_name='TBRAWACCT')

line_glbal.to_excel(writer, sheet_name='HKGL_LINEBAL')
line_plbal.to_excel(writer, sheet_name='HKPL_LINEPLBAL')

line_jpglbal.to_excel(writer, sheet_name='JPGL_LINEBAL')
line_jpplbal.to_excel(writer, sheet_name='JPPL_LINEPLBAL')


rg_glcode.to_excel (writer, sheet_name='RG_GLCODE')
rg_plcode.to_excel (writer, sheet_name='RG_PLCODE')

rg_lineid.to_excel (writer, sheet_name='HKGL_RG_LINEID')
rg_pllineid.to_excel (writer, sheet_name='HKPL_RG_LINEID')

rg_jplineid.to_excel (writer, sheet_name='JPGL_RG_LINEID')
rg_jppllineid.to_excel (writer, sheet_name='JPPL_RG_LINEID')


glacn.to_excel (writer, sheet_name='GLACN')
placn.to_excel (writer, sheet_name='PLACN')
plbal.to_excel (writer, sheet_name='PLBAL')

writer.close ()

#print ('WRITING DEAL DETAIL')
#detail_writer = pd.ExcelWriter(_refoutput, engine='xlsxwriter')
#rawac[rawac['TODAY_BALANCE'] !=0].to_excel (detail_writer, sheet_name='TBRAWACCT')
#detail_writer.close ()

print ('DONE')

#print (result.sort_values(by=['RG_KEY']))
