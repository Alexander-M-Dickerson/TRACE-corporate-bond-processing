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
#* Connect to WRDS                        */
#* ************************************** */  
db = wrds.Connection()

#* ************************************** */
#* Download Mergent File                  */
#* ************************************** */  
fisd = db.raw_sql("""SELECT complete_cusip, maturity
                  FROM fisd.fisd_mergedissue  
                  """)
                  
fisd['maturity'] = pd.to_datetime(fisd['maturity'], 
                   format='%Y-%m-%d')
fisd.rename(columns={'complete_cusip':'cusip'}, inplace=True)

#* ************************************** */
#* Load Monthly TRACE data                */
#* ************************************** */  
traced =\
 pd.read_hdf\
 ('enhanced_trace_monthly_returns.h5').\
        reset_index()

traced = traced.merge(fisd, how = "inner", on = 'cusip')

#* ************************************** */
#* Compute maturity                       */
#* ************************************** */  
traced['tmt'] = ((traced.maturity -\
                        traced.date)/np.timedelta64(1, 'M')) / 12
  
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
crsp_tr_['t_ret'] = crsp_tr_['t_ret']/100
crsp_tr_['t_yld'] = crsp_tr_['t_yld']/100
crsp_tr_['t_mod_dur'] = crsp_tr_['t_dur']/ (1+crsp_tr_['t_yld'])

crsp_tr_  = crsp_tr_[ crsp_tr_['date'] >= "1973-01-31"]
crsp_tr_ ['date'] = crsp_tr_ ['date'] + MonthEnd(0)

crsp_dat = pd.DataFrame(np.nan, 
                        index = crsp_tr_['date'].unique(),
                        columns = ['dat'])

for m,t in enumerate(crsp_tr_['date'].unique()):   
    dfc = crsp_tr_[ crsp_tr_['date'] == t].T  
    dfc.columns = dfc.loc['term']
    crsp_dat .loc[t] = dfc.to_dict()  
    crsp_dat['dat'].iloc[m] = dfc
    
crsp_dat = crsp_dat.reset_index()
crsp_dat.columns = ['date','dat']
crsp_dat = crsp_dat.set_index(['date'])
crsp_dat.index = crsp_dat.index+MonthEnd(0)
crsp_dat = crsp_dat.reset_index()

#* ************************************** */
#* Merge                                  */
#* ************************************** */ 
traced['date'] = pd.to_datetime(traced['date'])
crsp_dat['date'] = pd.to_datetime(crsp_dat['date'])


df = traced.merge(  crsp_dat , left_on  = ['date'], 
                               right_on = ['date'], 
                               how      = "inner"  )

df.rename(columns={'mod_dur':'duration'}, inplace=True) 
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
.to_hdf('trace_credit_spreads_returns.h5',
        key='daily',
        mode='w')        
#* ************************************** */
#* End                                    */
#* ************************************** */