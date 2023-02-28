import pandas as pd
import numpy as np
from tqdm import tqdm
import datetime as dt
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from datetime import datetime, timedelta
from datetime import datetime
tqdm.pandas()

#* ************************************** */
#* Load daily data                        */
#* ************************************** */
df  = pd.read_csv\
    ('Prices_BBW_TRACE_Enhanced_Dick_Nielsen.csv.gzip',
     compression='gzip')
dfv = pd.read_csv\
    ('Volumes_BBW_TRACE_Enhanced_Dick_Nielsen.csv.gzip',
     compression = "gzip")

#* ************************************** */
#* Convert column names to upper (for now)*/
#* ************************************** */

df.columns = map(str.upper , df.columns)
dfv.columns = map(str.upper, dfv .columns)

#* ************************************** */
#* Convert Trade Execution date to a      */
#* date type                              */
#* ************************************** */

df[ 'TRD_EXCTN_DT'] =  df['TRD_EXCTN_DT'].values.astype('M8[D]')
dfv['TRD_EXCTN_DT'] = dfv['TRD_EXCTN_DT'].values.astype('M8[D]')

#* ************************************** */
#* Merge                                  */
#* ************************************** */
df = df.merge(dfv, how = "inner", left_on = ['TRD_EXCTN_DT','CUSIP_ID'],
                                  right_on = ['TRD_EXCTN_DT','CUSIP_ID'])

df         = df.set_index(['CUSIP_ID', 'TRD_EXCTN_DT'])

# Rename #
df.rename(columns={'PRC_VW':'close' }, inplace=True)

# Create a month-year column for resampling 
df = df.reset_index()
df['month_year']   = pd.to_datetime(df['TRD_EXCTN_DT']).dt.to_period('M')

# Create Price column which is series of numerics of the closing price
# Do we compute daily return WITHIN each month? #
# Try intraday ILLIQ as well                    #
df['Price'] = pd.to_numeric(df['close'])
# Log price
df['logprc']     = np.log(df['Price'])
# Lag log price
df['logprc_lag'] = df.groupby( 'CUSIP_ID' )['logprc'].shift(1)
# Difference in log prices #
df['deltap']     = df['logprc'] - df['logprc_lag']

#* ************************************** */
#* Restrict log returns to be in the      */
#* interval [1,1]                         */
#* ************************************** */
# This trims some very extreme daily returns
# which could to do TRACE data errors etc.
# the trimming helps to give a more accurate
# value to the liquidity characteristics
# we compute 

df['deltap'] = np.where(df['deltap'] > 1, 1,
                        df['deltap'])
df['deltap'] = np.where(df['deltap'] <-1, -1,
                        df['deltap'])

# Convert deltap to % i.e. returns in % as opposed
# to decimals #
df['deltap']     = df['deltap']     * 100

# Lags days for day_counts
df['TRD_EXCTN_DT_LAG'] = df.\
    groupby( 'CUSIP_ID')['TRD_EXCTN_DT'].shift(1)

#* ************************************** */
#* Drop all NaNs for day count            */
#* ************************************** */
dfDC = df.dropna()

#* ************************************** */
#* U.S. Business Days                     */
#* ************************************** */

# With U.S. Holidays #
# 2. Generate a list of holidays over this period
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
calendar = USFederalHolidayCalendar()
start_date = '01JUL2002'
end_date   = '31DEC2022'
holidays = calendar.holidays(start_date, end_date)
holidays
holiday_date_list = holidays.date.tolist()

dfDC['n']  = np.busday_count(dfDC['TRD_EXCTN_DT_LAG'].values.astype('M8[D]') , 
                                      dfDC['TRD_EXCTN_DT'].values.astype('M8[D]'),
                                      holidays = holiday_date_list)

df = df.merge(
              dfDC[['CUSIP_ID',
                    'TRD_EXCTN_DT',
                    'n']], left_on = ['CUSIP_ID','TRD_EXCTN_DT'],
                           right_on = ['CUSIP_ID','TRD_EXCTN_DT'] ,
                           how = "left")
del(dfDC)
del(dfv)

# Lags days for day_counts
df['deltap_lag'] = df.\
    groupby( 'CUSIP_ID')['deltap'].shift(1)
#* ************************************** */
#* Num.bus.days between 2-Trades < 1_week */
#* ************************************** */
# This follows Bao, Pan and Wang (BPW), 2011 in
# The Journal of Finance 
# BBW state in their paper they follow 
# the methodology of BPW

# only include "daily" return if the gap between 
# trades is less than 1-Week 
# Assumed to be 1-Week of Business days (they are unclear).
df = df[df.n <= 7]
df = df.dropna()

df['month_year']   = pd.to_datetime(df['TRD_EXCTN_DT']).dt.to_period('M') 
df['trade_counts'] = df.groupby(['CUSIP_ID',
                                 'month_year'] )['deltap'].transform("count")    
df['abs_ret']      = np.abs(df['deltap'])

#* ************************************** */
#* Winsorize all input vars at 1%         */
#* ************************************** */

# You may choose to cross-sectionally winsorize or not
# This is not mentioned in the Bao paper OR
# in BBW so we do not do it -- code is available if desired

WINZ = 0

if WINZ == 1:
    def XS_Scale(series):
        x = 99.50    
        series.loc[series > np.nanpercentile(series,x)]\
            = np.nanpercentile(series,x)
        series.loc[series < np.nanpercentile(series,(100-x))]\
            = np.nanpercentile(series,(100-x))              
        return series

    winz_list = ['deltap','deltap_lag']
    df[winz_list].describe().round(2)

    for col in winz_list:
        print(col)
        df[col] = df.groupby('month_year')[col].apply(XS_Scale) 

#* ************************************** */
#* Compute Illiq from Bao et al. 2011     */
#* ************************************** */
# illiq = -cov(deltap, deltap_lag), groupby in month #
# Equation (2) in
# "The Illiquidity of Corporate Bonds" by Bao, Pan and Wang (2011)
# In The Journal of Finance
# Equation (2) in BBW's JFE Paper.

Illiq = df.groupby(['CUSIP_ID','month_year'] )[['deltap','deltap_lag']]\
    .progress_apply(lambda x: \
                    x.cov().iloc[0,1]) * -1
    
Illiq = Illiq.reset_index()
Illiq.columns = ['cusip','date','illiq']
Illiq['date'] = pd.to_datetime( Illiq['date'].astype(str) )
Illiq['date'] = Illiq['date'] + pd.offsets.MonthEnd(0)   
Illiq['roll'] = np.where(Illiq['illiq'] >  0,
                         (2 * np.sqrt(Illiq['illiq'])),
                         0 )

#* ************************************** */
#* Compute Amihud, Roll and VoV           */
#* ************************************** */
# This computes the Amihud illiquidity proxy
# BBW discuss the computation in their Online Appendix
# available: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2819910
# Equation (A.2) on Page 58/75 of the SSRN Working Paper
# This also whwere they mention the amount of matched pairs of
# daily returns and lagged returns they require for a valid
# illiquidity measure, this value is 5-Days.
# i.e. we require a minimum of 5 ['deltap','deltap_lag']
# in any month to compute Illiq.

df['abs_ret']      = np.abs(df['deltap'])
df['abs_ret'].min()

AbsReturn           = df.groupby(['CUSIP_ID','month_year'] )[['abs_ret']].sum() 
Qd                  = df.groupby(['CUSIP_ID','month_year'] )[['DVOLUME']].sum() / 1000000
TradeCount          = df.groupby(['CUSIP_ID','month_year'] )[['QVOLUME']].count()
TradeCount.columns  = ['N']
Sigma               = df.groupby(['CUSIP_ID','month_year'] )[['deltap']].std()
Sigma .columns = ['sigma']
Qdmu                = df.groupby(['CUSIP_ID','month_year'] )[['DVOLUME']].mean()
Qdmu.columns = ['dvolume_mu']

AHx                 = pd.concat([AbsReturn, Qd]  , axis = 1)
AHx                 = pd.concat([AHx, TradeCount], axis = 1)
AHx                 = pd.concat([AHx, Sigma], axis = 1)
AHx                 = pd.concat([AHx, Qdmu], axis = 1)

AHx['amihud']       = ((AHx['abs_ret'] / AHx['DVOLUME']) * 1/AHx['N']) 
AHx['vov']          = (8 * (AHx['sigma']**(2/3)) ) / (AHx['dvolume_mu']**(1/3))

AHx[['amihud','vov']].describe().round(3)
np.nanpercentile(AHx['amihud'] , 99)

AHx = AHx.reset_index()
AHx = AHx[['CUSIP_ID', 'month_year','amihud', 'vov','N']]
AHx.columns = ['cusip','date','amihud','vov','n']
AHx['date'] = pd.to_datetime(AHx['date'].astype(str) )
AHx['date'] = AHx['date'] + pd.offsets.MonthEnd(0)   

dfExport =    Illiq.merge(AHx, how = "inner", left_on  = ['cusip',
                                                         'date'],
                                              right_on = ['cusip',
                                                         'date'])
dfExport\
.to_hdf\
('bond_illiq_monthly_dick_nielsen.h5'
 , key='daily', mode='w')
# =============================================================================
