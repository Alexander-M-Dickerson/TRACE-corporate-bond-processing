#* ************************************** */
#* Libraries                              */
#* ************************************** */ 
import pandas as pd
import numpy as np
import datetime as datetime
import gzip
from tqdm import tqdm
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
import pandas_datareader as pdr
import datetime as datetime
from joblib import Parallel, delayed    
import wrds
tqdm.pandas()


#* ************************************** */
#* WRDS Credit Spreads and Returns        */
#* ************************************** */ 

# This script file estimates the maturity, fixed term maturity
# and duration matched U.S. T-Bond interpolated yields and returns.
# The script interpolates between the U.S. T-Bond key rates:
# which are the 1, 3, 5, 7, 10, 20 and 30-Year T-Bond
# yields and returns.
# For example, if a corporate bond has a duration of 2.50, 
# the script interpolates for U.S. T-Bonds between the closes
# key rates, which are the 3 and 5-Year.
# The file exports the interpolated U.S. T-Bond yields and returns
# which are used to compute the credit spread and "Excess"
# bond return

#* ************************************** */
#* Connect to WRDS                        */
#* ************************************** */  
db = wrds.Connection()

#* ************************************** */
#* CRSP Fixed Term Indices                */
#* ************************************** */  

# Load CRSP Fixed Term Indices Header Information
crsp_tr_headers = db.raw_sql("""SELECT *             
                  FROM crsp.tfz_idx
                  """)


crsp_tr_headers = crsp_tr_headers[crsp_tr_headers.tidxfam == "FIXEDTERM"]
crsp_tr_headers = crsp_tr_headers.assign(term = [1,2,5,7,10,20,30])
crsp_tr_headers = crsp_tr_headers[['kytreasnox',
                                   'term',
                                   ]]

# Load CRSP Fixed Term Indices Data
crsp_tr_   = db.raw_sql("""SELECT  kytreasnox,mcaldt,
                           tmyearstm,tmduratn,tmretadj,tmytm
                  FROM crsp.tfz_mth_ft
                  """)
                  
crsp_tr_ .columns = ['kytreasnox', 'date', 
                     't_tmt', 't_dur', 
                     't_ret', 't_yld']
# Merge
crsp_tr_ = crsp_tr_.merge(crsp_tr_headers,
                         on  = ['kytreasnox'],
                         how = 'left')

crsp_tr_['kytreasnox'] = crsp_tr_['kytreasnox'].astype(int)

# Convert Macauly Duration to Years #
crsp_tr_['date'] = pd.to_datetime(crsp_tr_['date'])
crsp_tr_['total_days'] = np.where(crsp_tr_['date'].dt.is_leap_year, 
                                  366, 
                                  365)
crsp_tr_['t_dur'] = crsp_tr_['t_dur']/crsp_tr_['total_days']

# Convert Macauly Duration to Modified Duration #
# Scale return and yield to be in decimal format. #
crsp_tr_['t_ret'] = crsp_tr_['t_ret']/100
crsp_tr_['t_yld'] = crsp_tr_['t_yld']/100

# We assume semi-annual coupon payments for U.S. T-Bonds.
# Empirically, this is the case.
crsp_tr_['t_mod_dur'] = ((crsp_tr_['t_dur']*2)/ (1+(crsp_tr_['t_yld']/2)))/2

# Align the dates #
crsp_tr_  = crsp_tr_[ crsp_tr_['date'] >= "1973-01-31"]
crsp_tr_ ['date'] = crsp_tr_ ['date'] + MonthEnd(0)

# Create date indexed dataframe, where in each element of the "dat"
# column we will store the entire yield curve sub-dataframe.
crsp_dat = pd.DataFrame(np.nan, 
                        index = crsp_tr_['date'].unique(),
                        columns = ['dat'])

for m,t in enumerate(crsp_tr_['date'].unique()):   
    dfc = crsp_tr_[ crsp_tr_['date'] == t].T  
    dfc.columns = dfc.loc['term'] 
    crsp_dat['dat'].iloc[m] = dfc


# In each element of the "dat" column, we store all the data we
# need including the duration, maturity and so on.    
crsp_dat = crsp_dat.reset_index()
crsp_dat.columns = ['date','dat']
crsp_dat = crsp_dat.set_index(['date'])
crsp_dat.index = crsp_dat.index+MonthEnd(0)
crsp_dat = crsp_dat.reset_index()

#* ************************************** */
#* WRDS Bond Returns                      */
#* ************************************** */  
traced = db.raw_sql("""SELECT DATE, CUSIP, TMT, DURATION, YIELD, RET_L5M             
                  FROM wrdsapps.bondret
                  """)

traced = traced.set_index(['date','cusip'])\
    .sort_index(level = ['date','cusip']).reset_index()

#* ************************************** */
#* Merge                                  */
#* ************************************** */ 
traced['date'] = pd.to_datetime(traced['date'])
crsp_dat['date'] = pd.to_datetime(crsp_dat['date'])


df = traced.merge(  crsp_dat , left_on  = ['date'], 
                               right_on = ['date'], 
                               how      = "inner"  )

#* ************************************** */
#* Credit spread function (paralell)      */
#* ************************************** */
def ComputeCredit(x):  
    
    x_dur = x.duration
    x_tmt = x.tmt
      
    _df_dur = x.dat.T.t_mod_dur  
    _df_tmt = x.dat.T.t_tmt
    
              
    idx_dur = _df_dur .iloc[(_df_dur - x_dur).abs().argsort()]\
        .reset_index().term[:2].sort_values() 
    idx_tmt = _df_tmt .iloc[(_df_tmt - x_tmt).abs().argsort()]\
        .reset_index().term[:2].sort_values()     
                     
    x_dur_ = list(x.dat.T.t_dur[ idx_dur ])
    x_tmt_ = list(x.dat.T.t_dur[ idx_dur ])
    x_tmt_ = list(x.dat.T.t_tmt[ idx_tmt ])
    x_ttm_ = list(x.dat.T.term[  idx_tmt ])
    
    # Yields
    y_dur_ = list(x.dat.T.t_yld[ idx_dur ])
    y_tmt_ = list(x.dat.T.t_yld[ idx_tmt ])
    y_ttm_ = list(x.dat.T.t_yld[ idx_tmt ])
    
    # Interpolation (Linear)
    yld_interp_dur = np.interp( x_dur, x_dur_, y_dur_ )  
    yld_interp_tmt = np.interp( x_tmt, x_tmt_, y_tmt_ )  
    yld_interp_ttm = np.interp( x_tmt, x_ttm_, y_ttm_ )  
    
    # Returns
    r_dur_ = list(x.dat.T.t_ret[ idx_dur ])
    r_tmt_ = list(x.dat.T.t_ret[ idx_tmt ])
    r_ttm_ = list(x.dat.T.t_ret[ idx_tmt ])
    
    # Interpolation (Linear)
    ret_interp_dur = np.interp( x_dur, x_dur_, r_dur_ )  
    ret_interp_tmt = np.interp( x_tmt, x_tmt_, r_tmt_ )  
    ret_interp_ttm = np.interp( x_tmt, x_ttm_, r_ttm_ )   
    
    cusip = x.cusip 
    date  = x.date   
          
    return (cusip, date, 
            yld_interp_dur, yld_interp_tmt, yld_interp_ttm,
            ret_interp_dur, ret_interp_tmt, ret_interp_ttm
            )       
           
#* ************************************** */
#* Credit spread compute                  */
#* ************************************** */
df_export = pd.DataFrame(
    Parallel(n_jobs=14)(delayed(ComputeCredit)(x)
                       for x in tqdm(df.itertuples(index=False))),
    columns=['cusip','date', 
             'yld_interp_dur', 'yld_interp_tmt','yld_interp_ttm',
             'ret_interp_dur', 'ret_interp_tmt','ret_interp_ttm']
    ) 

#* ************************************** */
#* Export                                 */
#* ************************************** */
df_export\
.to_hdf('wrds_credit_spreads_returns.h5',
        key='daily',
        mode='w')        
#* ************************************** */
#* End                                    */
#* ************************************** */
