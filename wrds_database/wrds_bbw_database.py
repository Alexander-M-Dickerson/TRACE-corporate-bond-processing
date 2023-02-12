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
import pyreadstat
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
#* WRDS Bond Returns                      */
#* ************************************** */  
# We only require 6 variables from the WRDS Bond Returns Module
traced = db.raw_sql("""SELECT  DATE, CUSIP, RET_L5M, TMT, 
                       AMOUNT_OUTSTANDING,OFFERING_AMT, N_SP, N_MR, YIELD,
                       PRICE_L5M
                       FROM wrdsapps.bondret
                  """)

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
                  offering_amt
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
mask_corp = ((fisd.bond_type != 'TXMU')&  (fisd.bond_type != 'CCOV') &  (fisd.bond_type != 'CPAS')\
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

fisd['offering_date']            = pd.to_datetime(fisd['offering_date'], 
                                                  format='%Y-%m-%d')
fisd['dated_date']               = pd.to_datetime(fisd['dated_date'], 
                                                  format='%Y-%m-%d')

fisd.rename(columns={'complete_cusip':'cusip'}, inplace=True)


#* ************************************** */
#* Merge                                  */
#* ************************************** */ 
fisd_w = fisd[['cusip','sic_code']]

df = traced.merge(fisd_w, left_on = ['cusip'], right_on = ['cusip'], 
                  how = "inner")
df = df.set_index(['date',
                   'cusip']).sort_index(level = "cusip").reset_index()

#* ************************************** */
#* Compute maturity                       */
#* ************************************** */  
df['date']     = pd.to_datetime(df['date'])
df['bond_maturity'] = df['tmt']
#* ************************************** */
#* Variable choice                        */
#* ************************************** */  

# Bond Amtout 
df['bond_amount_out'] = df['amount_outstanding']

# Bond Rating
df['spr_mr_fill'] = np.where(df['n_sp'].isnull(),df['n_mr'],df['n_sp'])
df['mr_spr_fill'] = np.where(df['n_mr'].isnull(),df['n_sp'],df['n_mr'])

# Average of the two-ratings #
# BBW use the average of the S&P and Moody's ratings to form their
# quintile break-points #
df['ave_rating'] = (df['spr_mr_fill'] + df['mr_spr_fill'] ) / 2

# Bond Yield 
df['bond_yield'] = df['yield'] 

#* ************************************** */
#* Subset columns                         */
#* ************************************** */ 
df.rename(columns={'ret_l5m':'bond_ret',
                   'yield':'yld',
                   'price_l5m':'bond_prc'}, inplace=True) 

df = df.set_index(['date',
                   'cusip'])

df = df[[ 'bond_ret','bond_prc', 'bond_amount_out' ,'offering_amt' , 
          'spr_mr_fill','ave_rating', 'yld','tmt' ,'sic_code']]
 
#* ************************************** */
#* Merge to credit spreads                */
#* ************************************** */ 
# The code to compute credit spreads is available here:
# https://github.com/Alexander-M-Dickerson/TRACE-corporate-bond-processing/tree/main/bond_credit_spreads
dfC = pd.read_hdf\
    (r'~\bond_credit_spreads\wrds_credit_spreads_returns.h5')

df  = df.merge(dfC, how = "left", left_on = ['date','cusip'],
                                   right_on = ['date','cusip'])

#* ************************************** */
#* AO                                     */
#* ************************************** */ 
# If AO is missing, forward fill it with the last available value #
df['bond_amount_out'] = df.groupby("cusip")['bond_amount_out'].ffill()

# If AO is NaN, replace with offering amount of the bond #
df['bond_amount_out'] = np.where(df['bond_amount_out'].isnull(),
                                 df['offering_amt'],
                                 df['bond_amount_out'])

#* ************************************** */
#* Fama French Industry 17 and 30         */
#* ************************************** */ 
# The ind30.csv and ind17.csv files are available here:
# https://github.com/Alexander-M-Dickerson/TRACE-corporate-bond-processing/tree/main/additional_data
# They are .csv files with the processed Fama French Industy 30 and 17
# classification system #

ffi30 = pd.read_csv\
  (r'~\additional_data\Fama French Industry\ind30.csv')
ffi17 = pd.read_csv\
  (r'~\additional_data\Fama French Industry\ind17.csv')

# Fama French 30
sqlcode ='''
SELECT a.cusip, a.date, b.ind_num
FROM df AS a
LEFT JOIN 
ffi30 AS b
ON a.sic_code BETWEEN b.sic_low AND b.sic_high;
'''

dfi30 = ps.sqldf(sqlcode,locals())
dfi30['date'] = pd.to_datetime( dfi30['date'])
dfi30.rename(columns={'ind_num':'ind_num_30'}, inplace=True)

# Fama French 17
sqlcode ='''
SELECT a.cusip, a.date, b.ind_num
FROM df AS a
LEFT JOIN 
ffi17 AS b
ON a.sic_code BETWEEN b.sic_low AND b.sic_high;
'''

dfi17 = ps.sqldf(sqlcode,locals())
dfi17['date'] = pd.to_datetime( dfi17['date'])
dfi17.rename(columns={'ind_num':'ind_num_17'}, inplace=True)

#* ************************************** */
#* Merge Industry to main df              */
#* ************************************** */
df = df.merge(dfi30, how = "left", left_on = ['date','cusip'],
                                   right_on = ['date','cusip'])

df = df.merge(dfi17, how = "left", left_on = ['date','cusip'],
                                   right_on = ['date','cusip'])

# If ind_num30 or ind_num17 is missing, forward fill it with the last 
#available value 
df['ind_num_30'] = df.groupby("cusip")['ind_num_30'].ffill()
df['ind_num_17'] = df.groupby("cusip")['ind_num_17'].ffill()

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
            
# Scale VaR and ES by -1
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
# https://github.com/Alexander-M-Dickerson/TRACE-corporate-bond-processing/tree/main/bond_illiquidity

dfI  = pd.read_hdf\
(r'~\bond_illiquidity\bond_illiq_monthly_dick_nielsen.h5')

df = df.merge(dfI, how = "left",    left_on = ['date','cusip'],
                                   right_on = ['date','cusip'])


#* ************************************** */
#* Resample                               */
#* ************************************** */
# Break into chunks based on cusip #
# Resample each cusip to get contiguous returns #
# This makes the process quicker -- Python struggles to resample the large
# dataframe and return it in one go.

CUSIPs = list(df['cusip'].unique())
c       = 5000
chunks  = [CUSIPs[x:x+c] for x in range(0, len(CUSIPs), c)]
df_resamp = pd.DataFrame()

dfR = df.set_index(['date'])[['cusip']]
dfR['filler'] = 1

for l in range(0,len(chunks)):
    print(l)
    CUSIP_Chunk = pd.DataFrame(chunks[l], columns = ['cusip'])
    dfChunk = dfR[(dfR['cusip'].isin(CUSIP_Chunk['cusip']))]
    dfResample = dfChunk.groupby(["cusip"])[ dfR.columns[1:] ].\
        apply(lambda x: x.resample("M").last())
    df_resamp = pd.concat([df_resamp, dfResample], axis = 0)

df = df_resamp.reset_index().merge(df, how = "left",
                   left_on = ['date','cusip'],
                   right_on=['date','cusip']).drop(['filler'],
                                                   axis = 1)
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


# Unlike the way BBW process the data, the WRDS Bond Module ONLY
# uses end-of month prices within the last 5-business days
# This means the first return observation is August 2002
# The WRDS Bond Returns module data begins on this date
df = df[df.index.get_level_values(0) >= "2002-08-31"]

#* ************************************** */
#* BBW Price and Rating Filters           */
#* ************************************** */ 

# (1) No bond in the sample trading below $5 or greater than $1,000
# Drop NaN values for prices
# This is filter #4 on Page 5/24 of BBW published JFE paper.

df = df[(( df.bond_prc > 5) &\
         ( df.bond_prc < 1000) &\
         (~df.bond_prc.isnull()))]

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

df.to_hdf(r'~\wrds_database\wrds_2002_2021.h5',
          key = 'daily')
