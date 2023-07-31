##########################################
# BBW (2019) Enhanced TRACE Data Process #
# Part (vi): Create TRACE database       #
# Alexander Dickerson                    #
# Email: a.dickerson@warwick.ac.uk       #
# Date: January 2023                     #
# Updated:  July 2023                    #
# Version:  1.0.1                        #
##########################################

'''
Overview
-------------
This Python script accesses the respective monthly data files processed in the
prior scripts and generates the TRACE version of the bond database.
 
Requirements
-------------
Access to the WRDS server and associated databases.
No data requirements.


Package versions 
-------------
pandas v1.4.4
numpy v1.21.5
tqdm v4.64.1
datetime v3.9.13
zipfile v3.9.13
wrds v3.1.2
'''

#* ************************************** */
#* Libraries                              */
#* ************************************** */ 
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm
import pandas_datareader as pdr
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
import datetime as datetime
from pandas.tseries.offsets import *
import wrds
import pandasql as ps
import urllib.request
import zipfile
tqdm.pandas()

#* ************************************** */
#* Connect to WRDS                        */
#* ************************************** */  
db = wrds.Connection()

#* ************************************** */
#* Download Mergent File                  */
#* ************************************** */  
fisd_issuer = db.raw_sql("""SELECT issuer_id,country_domicile,sic_code                
                  FROM fisd.fisd_mergedissuer 
                  """)

fisd_issue = db.raw_sql("""SELECT complete_cusip, issue_id,
                  issuer_id, foreign_currency,
                  coupon_type,coupon,convertible,
                  asset_backed,rule_144a,
                  bond_type,private_placement,
                  interest_frequency,dated_date,
                  day_count_basis,offering_date,
                  offering_amt, maturity
                  FROM fisd.fisd_mergedissue  
                  """)
                  
fisd = pd.merge(fisd_issue, fisd_issuer, on = ['issuer_id'], how = "left")                              
#* ************************************** */
#* Apply BBW Bond Filters                 */
#* ************************************** */  
#1: Discard all non-US Bonds (i) in BBW
fisd = fisd[(fisd.country_domicile == 'USA')]

#2.1: US FX
fisd = fisd[(fisd.foreign_currency == 'N')]

#3: Must have a fixed coupon
fisd = fisd[(fisd.coupon_type != 'V')]

#4: Discard ALL convertible bonds
fisd = fisd[(fisd.convertible == 'N')]

#5: Discard all asset-backed bonds
fisd = fisd[(fisd.asset_backed == 'N')]

#6: Discard all bonds under Rule 144A
fisd = fisd[(fisd.rule_144a == 'N')]

#7: Remove Agency bonds, Muni Bonds, Government Bonds, 
mask_corp = ((fisd.bond_type != 'TXMU')&  (fisd.bond_type != 'CCOV') &\
             (fisd.bond_type != 'CPAS')\
            &  (fisd.bond_type != 'MBS') &  (fisd.bond_type != 'FGOV')\
            &  (fisd.bond_type != 'USTC')   &  (fisd.bond_type != 'USBD')\
            &  (fisd.bond_type != 'USNT')  &  (fisd.bond_type != 'USSP')\
            &  (fisd.bond_type != 'USSI') &  (fisd.bond_type != 'FGS')\
            &  (fisd.bond_type != 'USBL') &  (fisd.bond_type != 'ABS')\
            &  (fisd.bond_type != 'O30Y')\
            &  (fisd.bond_type != 'O10Y') &  (fisd.bond_type != 'O3Y')\
            &  (fisd.bond_type != 'O5Y') &  (fisd.bond_type != 'O4W')\
            &  (fisd.bond_type != 'CCUR') &  (fisd.bond_type != 'O13W')\
            &  (fisd.bond_type != 'O52W')\
            &  (fisd.bond_type != 'O26W')\
            # Remove all Agency backed / Agency bonds #
            &  (fisd.bond_type != 'ADEB')\
            &  (fisd.bond_type != 'AMTN')\
            &  (fisd.bond_type != 'ASPZ')\
            &  (fisd.bond_type != 'EMTN')\
            &  (fisd.bond_type != 'ADNT')\
            &  (fisd.bond_type != 'ARNT'))
fisd = fisd[(mask_corp)]

#8: No Private Placement
fisd = fisd[(fisd.private_placement == 'N')]

#9: Remove floating-rate, bi-monthly and unclassified coupons
fisd = fisd[(fisd.interest_frequency != 13) ] # Variable Coupon (V)

#10 Remove bonds lacking information for accrued interest (and hence returns)
fisd['offering_date']            = pd.to_datetime(fisd['offering_date'], 
                                                  format='%Y-%m-%d')
fisd['dated_date']               = pd.to_datetime(fisd['dated_date'], 
                                                  format='%Y-%m-%d')
fisd['maturity']                 = pd.to_datetime(fisd['maturity'], 
                                                  format='%Y-%m-%d')
fisd.rename(columns={'complete_cusip':'cusip'}, inplace=True)

#* ************************************** */
#* Read in TRACE data                     */
#* ************************************** */  

#### Monthly bond return data ####

df =\
 pd.read_hdf\
 ('enhanced_trace_monthly_returns.h5').\
        reset_index()


#* ************************************** */
#* Returns - FISD Merge                   */
#* ************************************** */  
df = df.merge(fisd, left_on  = ['cusip'], 
                    right_on = ['cusip'], 
                    how      = "inner"   )

#* ************************************** */
#* Compute maturity                       */
#* ************************************** */  
df['bond_maturity'] = ((df.maturity -\
                        df.date)/np.timedelta64(1, 'M')) / 12

#* ************************************** */
#* Amount Out                             */
#* ************************************** */  

#### Monthly amount outstanding ####
#### (new way of merging)       ####
amt = pd.read_hdf('amount_outstanding.h5')
amt = amt[['date','cusip','action_amount']]
amt.columns = ['date','cusip','action_amount']

df  = df.sort_values(['date' ,'cusip'])
amt = amt.sort_values(['date','cusip'])

df  = pd.merge_asof(df,
              amt, 
              on= "date", 
              by= "cusip")  

df['action_amount']    = np.where(df['action_amount'].isnull(),
                               0,df['action_amount'] )

df['bond_amount_out'] =df['offering_amt'] - df['action_amount']
df['bond_amount_out'] = np.where(df['bond_amount_out'] < 0 , 0,
                                  df['bond_amount_out'])

# Forward fill here, for the seperate TRACE dataset analysis #
df['bond_amount_out'] = df.groupby("cusip")['bond_amount_out'].ffill()

# Where missing, set to offering_amt
df['bond_amount_out'] = np.where(df['bond_amount_out'].isnull(),
                                 df['offering_amt'], df['bond_amount_out'])

#* ************************************** */
#* Amount Out                             */
#* ************************************** */ 
 
#### Monthly amount outstanding ####
#### (old way of merging)       ####
dfAO= pd.read_hdf('amount_outstanding_dmr.h5')
dfAO.drop(['bond_amount_out_lag'], axis = 1, inplace = True)
dfAO.columns = ['cusip', 'date', 'bond_amount_out_dmr']

df = df.merge(dfAO, left_on = ['date','cusip'], 
              right_on = ['date','cusip'], how = "left")#

# Forward fill here, for the seperate TRACE dataset analysis #
df['bond_amount_out_dmr'] = df.groupby("cusip")['bond_amount_out_dmr'].ffill()

# Where missing, set to offering_amt
df['bond_amount_out_dmr'] = np.where(df['bond_amount_out_dmr'].isnull(),
                                 df['offering_amt'], df['bond_amount_out_dmr'])

#* ************************************** */
#* Ratings                                */
#* ************************************** */  
#### Monthly ratings            ####
#### (new way of merging)       ####
mdr = pd.read_hdf('moody_ratings.h5')
mdr['date'] = mdr['rating_date']+MonthEnd(0)
spr = pd.read_hdf('sp_ratings.h5')
spr['date'] = spr['rating_date']+MonthEnd(0)

df  = df.sort_values(['date','issue_id'])
spr = spr.sort_values(['date','issue_id'])
mdr = mdr.sort_values(['date','issue_id'])

df['issue_id']  = df['issue_id'] .astype(int)
spr['issue_id'] = spr['issue_id'].astype(int)
mdr['issue_id'] = mdr['issue_id'].astype(int)

spr = spr[spr['spr'] <= 21]
mdr = mdr[mdr['mr']  <= 21]
 
df  = pd.merge_asof(df,
              spr[['issue_id','date','spr']], 
              on= "date", 
              by= "issue_id")   

df  = pd.merge_asof(df,
              mdr[['issue_id','date','mr']], 
              on= "date", 
              by= "issue_id")   

df['spr_mr_fill']  = np.where(df['spr'].isnull(),
                               df['mr'] , 
                               df['spr'])


# Forward fill here, for the seperate TRACE dataset analysis #
# Do NOT back-fill #
df['spr_mr_fill'] = df.groupby("cusip")['spr_mr_fill'].ffill()

#* ************************************** */
#* Ratings                                */
#* ************************************** */  
#### Monthly ratings            ####
#### (old way of merging)       ####
dfR = pd.read_hdf('ratings_dmr.h5').reset_index()
dfR.drop(['bond_rating_lag', 'sp_bond_rating_lag', 'mr_bond_rating_lag'],
         axis= 1, inplace = True)
dfR.columns = ['cusip', 'date', 'mratg', 'spr_mr_fill_dmr', 'mr_spr_fill_dmr']

df = df.merge(dfR, left_on = ['date','cusip'], 
              right_on = ['date','cusip'], how = "left")

# Forward fill here, for the seperate TRACE dataset analysis #
# Do NOT back-fill #
df['spr_mr_fill_dmr'] = df.groupby("cusip")['spr_mr_fill_dmr'].ffill()

#* ************************************** */
#* Variable choice                        */
#* ************************************** */  
# Bond Maturity
df['tmt'] = df['bond_maturity']

# Bond Yield
df['yld'] = df['bond_yield']

#* ************************************** */
#* Subset columns                         */
#* ************************************** */ 
df = df.set_index(['date',
                   'cusip'])

df = df[[ 'bond_ret', 'bond_amount_out','bond_amount_out_dmr',
          'offering_amt' ,  'spr_mr_fill', 'spr_mr_fill_dmr', 
          'yld','tmt' ,'sic_code']]

#* ************************************** */
#* Merge to credit spreads                */
#* ************************************** */ 
# The code to compute credit spreads is available here:
# https://github.com/Alexander-M-Dickerson/TRACE-corporate-bond-processing/tree/main/TRACE
dfC =\
pd.read_hdf('trace_credit_spreads_returns.h5')

df = df.merge(dfC, left_on = ['date','cusip'], 
              right_on = ['date','cusip'], how = "left")

#* ************************************** */
#* Fama French Industry 12                */
#* ************************************** */ 
# Fama French 12
ffi12 = pd.read_csv\
  ('ind12.csv')

# Fama French 12
sqlcode ='''
SELECT a.cusip, a.date, b.ind_num
FROM df AS a
LEFT JOIN 
ffi12 AS b
ON a.sic_code BETWEEN b.sic_low AND b.sic_high;
'''

dfi12 = ps.sqldf(sqlcode,locals())
dfi12['date'] = pd.to_datetime( dfi12['date'])
dfi12.rename(columns={'ind_num':'ind_num_12'}, inplace=True)

dfi12['ind_num_12'].value_counts()
dfi12['ind_num_12'] = np.where(dfi12['ind_num_12'].isnull(), 12,
                            dfi12['ind_num_12'])
dfi12['ind_num_12'].value_counts(normalize = True)    

df   = df.merge(dfi12,how = "left",
                                    left_on = ['date','cusip'],
                                    right_on = ['date','cusip'])   

df['ind_num_12'] = df.groupby("cusip")['ind_num_12'].ffill()

#* ************************************** */
#* Compute VaR-95,90 and ES-90            */
#* ************************************** */ 
df = df.set_index(['cusip','date']).\
    sort_index(level = ['cusip','date'])
dfVAR = df[['bond_ret']].dropna()

# This records the 2nd lowest value in the 24-month-36-month window
# The BBW DRF and CRF factors, which are publicly available (with errors),
# start on 2004-08, which means BBW used a 24-Month initial burn-in period,#
# which then expands up to 36-Months and then starts rolling forward.

dfVAR['var5br']  = dfVAR['bond_ret'].groupby(level='cusip').\
    progress_apply(lambda x:  x.rolling(36,min_periods = 24).\
                   apply(lambda x: np.sort(x)[1]) )   
            
# Scale VaR by -1
dfVAR = dfVAR[['var5br']] * -1
dfVAR = dfVAR.reset_index()

#* ************************************** */
#* Merge df and dfVAR                     */
#* ************************************** */ 
df = df.reset_index()
df = df.merge(dfVAR, how = "left", left_on = ['date','cusip'],
                                   right_on = ['date','cusip'])

#* ************************************** */
#* Load and left_merge ILLIQ              */
#* ************************************** */ 
# illiq is the bond illiquidity proxy from Bao et al. (2011) in JF
# roll is the Roll illiquidity proxy
# amihud is the Amihud illiquidity proxy
# vov is the VoV illiquidity proxy
# n is the amount of daily trades recorded in the month.
# This data is generated with the script file:
# "trace_illiquidity_characteristics" available here:
# https://github.com/Alexander-M-Dickerson/TRACE-corporate-bond-processing/tree/main/TRACE

dfI  = pd.read_hdf\
('bond_illiq_monthly_dick_nielsen.h5')

df = df.merge(dfI, how = "left",    left_on = ['date','cusip'],
                                   right_on = ['date','cusip'])

df = df.set_index(['cusip',
                   'date']).sort_index(level = ['cusip',
                                      'date'])

# Drop duplicates (there are not any)
df = df[~df.index.duplicated()]

#* ************************************** */
#* Credit spread / credit return          */
#* ************************************** */ 
df['bond_credit_spread'] = df['yld']     -df['yld_interp_tmt']
df['exretnc']            = df['bond_ret']-df['ret_interp_tmt']

df['bond_credit_spread_dur'] = df['yld']     -df['yld_interp_dur']
df['exretnc_dur']            = df['bond_ret']-df['ret_interp_dur']

df.drop(['yld_interp_dur','yld_interp_tmt',
         'yld_interp_ttm', 'ret_interp_dur', 
         'ret_interp_tmt','ret_interp_ttm'],
        axis = 1, inplace = True)
#* ************************************** */
#* Bond Excess return                     */
# In excess of the one-month rf-rate      */
#* ************************************** */ 
ff_url = str("https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/")+\
str("F-F_Research_Data_Factors_CSV.zip")

# Download the file and save it
# We will name it fama_french.zip file

urllib.request.urlretrieve(ff_url,'fama_french.zip')
zip_file = zipfile.ZipFile('fama_french.zip', 'r')

# Next we extact the file data
# We will call it ff_factors.csv

zip_file.extractall()
# Make sure you close the file after extraction
zip_file.close()

ff_factors = pd.read_csv('F-F_Research_Data_Factors.csv', skiprows = 3)
ff_factors = ff_factors.iloc[:1157,:]
ff_factors.rename(columns={'Unnamed: 0':'date'}, inplace=True)
ff_factors['date'] = pd.to_datetime(ff_factors['date'], format = "%Y%m")
ff_factors['date'] = ff_factors['date'] + MonthEnd(0)
ff_factors = ff_factors.set_index('date')
rf = pd.to_numeric( ff_factors['RF'] )/100

df = df.merge(rf, how = "inner", left_index=True, 
              right_index = True)
df['exretn'] = df['bond_ret'] - df['RF']

#* ************************************** */
#* Lead excess returns                    */
#* ************************************** */ 
# This means the returns at a given date
# are from t:t+1 and the characteristics
# are at time t.
df['exretn_t+1']   = df.groupby("cusip")['exretn'] .shift(-1)
df['exretnc_t+1']  = df.groupby("cusip")['exretnc'].shift(-1)
df['exretnc_dur_t+1']  = df.groupby("cusip")['exretnc_dur'].shift(-1)

#* ************************************** */
#* Days between Months check              */
#* ************************************** */ 
df = df.reset_index()
df['date_t+1']   = df.groupby("cusip")['date'] .shift(-1)
df['days']       = (df['date_t+1'] - df['date']).dt.days
df['days'].max()

#* ************************************** */
#* Drop                                   */
#* ************************************** */ 
df.drop(['date_t+1','days'], 
        inplace = True, 
        axis = 1)

#* ************************************** */
#* Index + Sort                           */
#* ************************************** */ 
df = df.set_index(       ['date','cusip']).\
      sort_index(level = ['date','cusip'] )


#* ************************************** */
#* Compute new version of credit spreads  */
#* ************************************** */ 
df['cs_lag']  = df.groupby("cusip")['bond_credit_spread_dur'].shift(1)
dfCS = df
dfCS = dfCS[~dfCS.cs_lag.isnull()]
dfCS['cs']  = dfCS.groupby("cusip")['cs_lag'].progress_apply(\
                    lambda x: x.rolling(window = 12).mean())

dfCS = dfCS.reset_index()[['date', 'cusip','cs']].dropna()

df   = df.reset_index().merge(dfCS,
                how = "left",
                left_on = ['date','cusip'],
                right_on = ['date','cusip'])
df.drop(['cs_lag'], axis = 1, inplace = True)
#* ************************************** */
#* Export                                 */
#* ************************************** */ 
# This resulting dataframe from the processed
# WRDS Bond Database contains the following variables:

# Variables recorded at the end of month t, i.e. the value for month:
# August 2002 is the value for August 2002 month-end.
# i.e. These are the variables available to the bond investor
# in forming their portfolios.

# (1) bond_ret: Bond Return computed according to BBW Equation (1)
# in decimal form, i.e. 0.01 is 1%. The return uses bond prices in the last
# 5-business days in any given month t
# (2) bond_prc: The clean price of the bond if it traded in the last
# 5-business days of the month.
# (3) bond_amount_out: The bond amount oustanding in units.
# (4) offering_amt: the initial bond offering amount in units. 
# (5) spr_mr_fill: the S&P Rating, or the Moody's rating conditional on
# the S&P rating being missing. In numerical form, i.e. 1 == AAA and
# 21 == D (Default), BBW explicitly EXCLUDE defaulted bonds.
# (6) yld: The bond yield in decimal format, 0.01 is 1%.
# (7) tmt: the bond maturity in years, i.e. 2.45 is 2.45-Years.
# (8) sic_code: The industry SIC code from Mergent FISD
# (9) ind_num_30: The Fama French 30 Industry classification number
# (10) ind_num_17: The Fama French 17 Industry classification number
# (11) bond_credit spread: The bond credit spread. Maturity matched
# (12) exretnc: The "Excess" return of the bond, in excess of the maturity-
# matched T-Bond return
# (12) bond_credit spread_dur: The bond credit spread. Duration matched
# (14) exretnc_dur: The "Excess" return of the bond, in excess of the duration-
# matched T-Bond return
# (15) RF: the one-month risk-free rate of return from K. French's website
# (16) exretn: The "excess" returnm in excess of the one-month RF-rate

# Variables recorded at from t:t+1, i.e. the value for month:
# August 2002 is the value for September 2002 month-end.
# i.e. these variables have a 1-Month look-ahead (are 1-Month in the future)
    
# (17) exretn_t+1     : 1-Month ahead excess return (1-Month rf-rate)
# (18) exretnc_t+1    : 1-Month ahead excess return (mat. matched T-Bond)
# (19) exretnc_dur_t+1: 1-Month ahead excess return (dur. matched T-Bond)

df.to_hdf('trace_2002_2022.h5',
          key = 'daily')
######################################