import pandas as pd
import numpy as np
from tqdm import tqdm
import datetime as dt
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from datetime import datetime, timedelta
from datetime import datetime
import statsmodels.api as sm
import pandas_datareader as pdr
import datetime as datetime
tqdm.pandas()

#* ************************************** */
#* Load daily data                        */
#* ************************************** */
df  = pd.read_csv\
    (r'~\Prices_BBW_TRACE_Enhanced_Dick_Nielsen.csv.gzip',
     compression='gzip')
dfv = pd.read_csv\
    (r'~\Volumes_BBW_TRACE_Enhanced_Dick_Nielsen.csv.gzip',
     compression = "gzip")

df.columns = map(str.upper , df.columns)
dfv.columns = map(str.upper, dfv .columns)

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
# Lag this difference
df['deltap_lag'] = df.groupby( 'CUSIP_ID' )['deltap'].shift(1)
df['TRD_EXCTN_DT_LAG'] = df.groupby( 'CUSIP_ID')['TRD_EXCTN_DT'].shift(1)


#* ************************************** */
#* Restrict log returns to be in the      */
#* interval [1,1]                         */
#* ************************************** */
df['deltap'] = np.where(df['deltap'] > 1, 1,
                        df['deltap'])
df['deltap'] = np.where(df['deltap'] <-1, -1,
                        df['deltap'])

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

#* ************************************** */
#* Create TRACE-based daily MKT index     */
#* ************************************** */

dfNaN = df.dropna()
dfNaN['n']  = np.busday_count(dfNaN['TRD_EXCTN_DT_LAG'].values.astype('M8[D]') , 
                                      dfNaN['TRD_EXCTN_DT'].values.astype('M8[D]'),
                                      holidays = holiday_date_list)
dfNaN = dfNaN[dfNaN.n <= 7]

MKTx = dfNaN.groupby('TRD_EXCTN_DT')[['deltap']].mean()
MKTx.cumsum().plot()

#* ************************************** */
#* Load BAML Indices                      */
#* ************************************** */
start = datetime.datetime (1970, 1, 31)
end   = datetime.datetime (2022, 12, 31)

# ICE BofA US Corporate Indices [Daily]
# This dataset provides a more reliable daily market return
# than the TRACE daily data #
ice = pdr.DataReader(['BAMLCC0A0CMTRIV',
                       'BAMLHYH0A0HYM2TRIV'], 
                      'fred', 
                      start, 
                      end)

ice.reset_index(inplace = True)
ice.columns = ['date','ig','hy']
ice = ice.set_index(['date'])
ice = ice.dropna()
ice['deltap_ig'] = np.log( 1+ ice['ig'].pct_change())
ice['deltap_hy'] = np.log( 1+ ice['hy'].pct_change())
ice[['deltap_ig', 'deltap_hy']].cumsum().plot()
ice['mktb_ice'] =         0.70   * ice['deltap_ig']+\
                          0.30   * ice['deltap_hy'] 
ice = ice[['mktb_ice']]

#* ************************************** */
#* Merge both Indices                     */
#* ************************************** */
MKTx = MKTx.merge(ice, how = "left", left_index = True,
                  right_index = True)

MKTx.columns = ['mkt_trace','mkt_ice']
MKTx = MKTx[['mkt_trace','mkt_ice']].reset_index()
MKTx['TRD_EXCTN_DT'] = pd.to_datetime(MKTx['TRD_EXCTN_DT'])

df = df.merge(MKTx.reset_index(), how = "left", left_on = ['TRD_EXCTN_DT'],
              right_on = ['TRD_EXCTN_DT'])
df.isnull().sum()

#* ************************************** */
#* Compute Excess Return (re) in PS       */
#* ************************************** */
df['re']  = df['deltap'] - df['mkt_ice']

#* ************************************** */
#* Compute Signed Volume                  */
#* ************************************** */
df['sign']  = np.where(df['re'] > 0 ,1,-1)


# Scale returns to be in % #
df['re']     = df['re']     * 100  # Returns in % per-day
df['deltap'] = df['deltap'] * 100  # Returns in % per-day

# Scale volume to be in millions of U.S. Dollars #
df['DVOLUME']    = df['DVOLUME'] / 1000000
df['signed_vol'] = df['sign'] * df['DVOLUME']


# Lead daily return for each CUSIP_ID within each month #
df['re_t+1'] = df.groupby( ['CUSIP_ID','month_year'])['re'].shift(-1)


#* ************************************** */
#* Drop all NaNs                          */
#* ************************************** */
df = df.dropna()

df['deltap']     = pd.to_numeric(df['deltap'], errors = "coerce")
df['signed_vol'] = pd.to_numeric(df['signed_vol'], errors = "coerce")
df['re_t+1']     = pd.to_numeric(df['re_t+1'], errors = "coerce")


#* ************************************** */
#* Counts days between trades             */
#* ************************************** */
df['n']  = np.busday_count(df['TRD_EXCTN_DT_LAG'].values.astype('M8[D]') , 
                                      df['TRD_EXCTN_DT'].values.astype('M8[D]'),
                                      holidays = holiday_date_list)
df['n'].describe().round(0)
#* ************************************** */
#* Num.bus.days between 2-Trades < 1_week */
#* ************************************** */
df = df[df.n <= 7]
df['trade_counts'] = df.groupby(['CUSIP_ID','month_year'] )['deltap'].transform("count")    
df['abs_ret']      = np.abs(df['deltap'])
#########################
def Risk(x):
        if len(x) < 10:
            return np.nan
        else:
            exret =    x[['re_t+1']]            
            factors =  x[['deltap', 'signed_vol']]
                       
            try:               
                model =  sm.OLS(exret, sm.add_constant(factors), missing='drop').fit()
                               
                return  float(model.params[2])                                       
                                                                 
            except ValueError:
                return np.nan
########################    

#* ************************************** */
#* At least 10-Matched Obs                */
#* ************************************** */
df['N'] = df.groupby( ['CUSIP_ID','month_year'])['re'].transform("count")
df      = df[df.N >= 10]

#* ************************************** */
#* Winsorize all input vars at 1%         */
#* ************************************** */
def XS_Scale(series):
    x = 99.50    
    series.loc[series > np.nanpercentile(series,x)]     = np.nanpercentile(series,x)
    series.loc[series < np.nanpercentile(series,(100-x))] = np.nanpercentile(series,(100-x))              
    return series

winz_list = ['re_t+1','deltap','signed_vol']
df[winz_list].describe().round(1)

for col in winz_list:
    print(col)
    df[col] = df.groupby('month_year')[col].apply(XS_Scale) 

#* ************************************** */
#* Estimate the Coefficient
#* This is Equation (1) in:
#* "Liquidity Risk & Expected Stock Returns
#*  by Pastor & Stambaugh (2003) &
#*  Equation (4) in Lin et al. (2011)
#* The methodology is applied verbatim
#* ************************************** */

df = df[['CUSIP_ID', 'TRD_EXCTN_DT','month_year',
          're_t+1','deltap', 'signed_vol']].dropna()

Risk = df.groupby(['month_year','CUSIP_ID']).progress_apply(lambda x: Risk(x))  
Risk = Risk.to_frame()
Risk.columns = ['dat']

# We denote the estimated coefficient "pi" #
# As in Equation (2) of Lin et al. (2011)  #
# from the published JFE paper             #
Risk.columns = ['pi']
Risk.head()

Risk = Risk.reset_index()
Risk = Risk.set_index('month_year')
Risk.index = Risk.index.to_timestamp()
Risk = Risk.reset_index()
Risk.columns = ['date','cusip','pi']
Risk['date'] = Risk['date'] + pd.offsets.MonthEnd(0)

#* ************************************** */
#* Export to file                         */
#* ************************************** */

Risk.\
to_hdf(r'~\pastor_stambaugh_bond_re_ice.h5',
       key = 'daily')

#* ************************************** */
#* The Amihud Illiquidity Measure         */
#* Section 2.2.2 of the Lin et al. (2011) */
#* JFE Paper                              */
#* ************************************** */

# We strictly follow Equation (6),      #
# of their JFE paper to construct ILLIQ #

# We also compute the VoV measure of
# illiquidity to use as robustness      #

#* ************************************** */
#* Amihud and VoV Illiquidity             */
#* ************************************** */
df['abs_ret']      = np.abs(df['deltap'])

# Note, deltap in % and volume is in millions of USD alread #
AbsReturn           = df.groupby(['CUSIP_ID','month_year'] )[['abs_ret']].sum() 
df['DVOLUME']       = df['signed_vol'].abs()
Qd                  = df.groupby(['CUSIP_ID','month_year'] )[['DVOLUME']].sum() 
TradeCount          = df.groupby(['CUSIP_ID','month_year'] )[['DVOLUME']].count()
TradeCount.columns  = ['N']
Sigma               = df.groupby(['CUSIP_ID','month_year'] )[['deltap']].std()
Sigma .columns = ['sigma']
Qdmu                = df.groupby(['CUSIP_ID','month_year'] )[['DVOLUME']].mean()
Qdmu.columns = ['dvolume_mu']

# Concat #
AHx                 = pd.concat([AbsReturn, Qd]  , axis = 1)
AHx                 = pd.concat([AHx, TradeCount], axis = 1)
AHx                 = pd.concat([AHx, Sigma], axis = 1)
AHx                 = pd.concat([AHx, Qdmu], axis = 1)

AHx['amihud']       = ((AHx['abs_ret'] / AHx['DVOLUME']) * 1/AHx['N']) 
AHx['vov']          = (8 * (AHx['sigma']**(2/3)) ) / (AHx['dvolume_mu']**(1/3))

AHx = AHx.reset_index()
AHx = AHx[['CUSIP_ID', 'month_year','amihud', 'vov','N']]
AHx.columns = ['cusip','date','amihud','vov','n']
AHx['date'] = pd.to_datetime(AHx['date'].astype(str) )
AHx['date'] = AHx['date'] + pd.offsets.MonthEnd(0)   


#* ************************************** */
#* Export to file                         */
#* ************************************** */
AHx.\
to_hdf(r'~\amihud_vov.h5',
       key = 'daily')
