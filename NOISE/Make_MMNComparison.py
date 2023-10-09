'''
Script to illustrate the nasty effects of market microstrcuture noise (MMN)
in TRACE-based bond prices.
MMN affects ANY price-based metric such as bond prices, credit spread, yield,
and so on.

What does this script does:
    
    (1) Reads in the Dickerson, Robotti and Rossetti (2023) MMN-corrected
    TRACE-based WRDS panel.
    (2) Performs simple decile sorts on the uncorrected MMN characteristics 
    -- we keep these in lower case and the MMN-adjusted variables,
    always in UPPERCASE.
    (3) We compute some t-stats and CAPMB alphas.
    
'''

#* ************************************** */
#* Libraries                              */
#* ************************************** */ 

import pandas as pd
import numpy as np
from tqdm import tqdm
from dateutil.relativedelta import *
import datetime as datetime
from pandas.tseries.offsets import *
import statsmodels.api as sm
import statsmodels.formula.api as smf
import urllib.request
tqdm.pandas()

# Load MMN-adjusted WRDS Panel
df = pd.read_csv('WRDS_MMN_Corrected_Data.csv.gzip',
                 compression = 'gzip')
df['date'] = pd.to_datetime(df['date'])

df    = df[df.tmt > 1.0]
df    = df[~df.rating.isnull()]
df    = df.sort_values(['cusip','date'])

# Load some factors      #
_url = 'https://openbondassetpricing.com/wp-content/uploads/2023/10/bbw_wrds_oct_2023_lastest.csv'
dfF  = pd.read_csv(_url)[['date','MKTB']]
dfF['date'] = pd.to_datetime(dfF['date'])
dfF  = dfF.set_index(['date'])

yy = 'exretn_t+1'    # One-month ahead bond return #

# The sorting variables, lower-case are variables WITH MMN and
# upper case are variables WITHOUT MMN.

cols = [
        'cs',
        'CS',
        'bond_yield',
        'BOND_YIELD',
        'bond_ret',
        'BOND_RET',  
        'bond_value',
        'BOND_VALUE',                  
        ]

q   = 10 # Deciles

tnw = 12 # t-stats with 12-Lags

AveRetExport          = pd.DataFrame()
AlphaExport           = pd.DataFrame()
NumNulls              = pd.DataFrame()

for i,s in enumerate(cols):
    print(s)
    
    if s.isupper():
        W = 'BOND_VALUE'
    else:
        W = 'bond_value'
        
    dfc = df[~df[s].isnull()][['date','cusip',yy,W,s]].dropna()  
    dfc = dfc.loc[:,~dfc.columns.duplicated()].copy()

            
    dfc['Q'] = dfc.groupby(by = ['date'])[  s ]\
        .apply( lambda x: pd.qcut(x.rank(method="first"),
                                           q,labels=False,duplicates='drop')+1)    
            
    dfc['value-weights'] = dfc.groupby([ 'date',
                                             'Q' ])[W].\
        apply( lambda x: x/np.nansum(x) )

    sorts = dfc.groupby(['date',
                           'Q',
                           ])[yy,
                           'value-weights'].\
                            apply( lambda x:\
                                           np.nansum( x[yy] *\
                                                     x['value-weights']) ).to_frame()                               
    sorts.columns = ['retn']

    sorts  = sorts.pivot_table( index = ['date'],
                               columns = ['Q'],
                               values = "retn")
    
    sorts['HML'] = sorts.iloc[:,(q-1)] - sorts.iloc[:,0]
    sorts.index = sorts.index + MonthEnd(1)
    sorts = sorts[sorts.index >= "2002-09-30"]
   
    ##### Ave. Ret     #####
    ########################
    tstats    =  list()
   
    for i in range(0,(q+1)):        
        regB = sm.OLS(sorts.iloc[:,i].values, 
                      pd.DataFrame(np.tile(1,(len(sorts.iloc[:,i]),1)) ).values
                      ).fit(cov_type='HAC',cov_kwds={'maxlags':tnw})
        
        tstats.append(np.round(regB.tvalues[0],2))  
        
    Mean       = (pd.DataFrame(np.array(sorts.mean())) * 100).round(2).T  
    Mean.index = [s ]
    Tstats     = pd.DataFrame(np.array(tstats)).T
    Tstats.index = [str('t-Stat_') + str(s)     ]
    Tstats  = "(" + Tstats.astype(str)  + ")" 
    
    
    sorts = sorts.merge(dfF,how= "inner",
                        left_index = True, right_index = True)
         
               
    ##### CAPMB Alpha  #####
    ########################
    alpha     =  list()
    tstats    =  list()   
    for i in range(0,(q+1)):        
        regB = sm.OLS(sorts.iloc[:,i].values, 
                      sm.add_constant(sorts[['MKTB']]),                      
                      ).fit(cov_type='HAC',cov_kwds={'maxlags':tnw})
        
        tstats.append(np.round(regB.tvalues[0] ,2)) 
        alpha.append( np.round(regB.params [0]*100,2))
        
    Alpha       = (pd.DataFrame(np.array(alpha)) ).round(2).T  
    Alpha.index = [s ]
    TstatsA     = pd.DataFrame(np.array(tstats)).T
    TstatsA.index = [str('t-Stat_') + str(s)     ]
    TstatsA  = "(" + TstatsA.astype(str)  + ")" 
               
    AveRet  =  pd.concat([Mean , Tstats], axis = 0)
    Alpha   =  pd.concat([Alpha, TstatsA], axis = 0)
    
    AveRetExport  =  pd.concat([AveRetExport , AveRet], axis = 0)
    AlphaExport   =  pd.concat([AlphaExport  , Alpha],  axis = 0)

    ##### Count Nulls  #####
    ########################
    Nulls       = pd.DataFrame( sorts.isnull().sum()).T
    Nulls.index = [s]
    NumNulls    =  pd.concat([NumNulls , Nulls],
                           axis = 0)
# =============================================================================
