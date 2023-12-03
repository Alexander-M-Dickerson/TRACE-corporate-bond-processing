############################################
# Daily Credit Spreads                     #
# Compute Daily Credit Spreads             #
# Alexander Dickerson                      #
# Email: a.dickerson@warwick.ac.uk  OR     #
# alexander.dickerson1@unsw.edu.au         #
# Webpage: https://www.alexdickerson.com/  #
# https://openbondassetpricing.com/        #
# Date: December 2023                      #
# Updated:  December 2023                  #
# Version:  1.0.0                          #
############################################

'''
Overview
-------------
This Python script computes daily bond credit spreads.
 
Requirements
-------------
Data output from "TRACE/MakeBondDailyMetrics.py" from GitHub

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
from tqdm import tqdm
import datetime as dt
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from datetime import datetime, timedelta
import urllib.request
import zipfile
import wrds  
tqdm.pandas()

#* ************************************** */
#* Read daily prices and AI               */
#* ************************************** */ 
# We start off by loading the daily price, AI volume and yield file
# This can be downloaded here:
# https://openbondassetpricing.com/wp-content/uploads/2023/12/BondDailyDataPublic.zip
    
df=pd.read_csv\
        ('BondDailyDataPublic.csv.gzip',
         compression = 'gzip')     

if 'Unnamed: 0' in df.columns:
    df.drop(columns=['Unnamed: 0'], inplace=True)

# Convert to date
df['trd_exctn_dt'] = pd.to_datetime(df['trd_exctn_dt'])
df['maturity']     = pd.to_datetime(df['maturity'])

# NOTE:
# ytm is the bond equiv. yield (BEY) assuming semi-annual coupons
# ytmt is the actual (true) yield 
# For the spread we use ytmt
# Feel feel to use the BEY, difference is minor.
    
#* ************************************** */
#* Compute maturity                       */
#* ************************************** */  
df['tmt'] = ((df.maturity -\
                        df['trd_exctn_dt'])/np.timedelta64(1, 'M')) 

# Remove any bonds with a negative maturity # 
df = df[df['tmt'] > 0 ]

#* ************************************** */
#* FRED Yields                            */
#* ************************************** */ 
# If you like, feel free to use yields direct from WRDS or the yield data
# from: https://sites.google.com/view/jingcynthiawu/yield-data
# Using this data gives extremely similar spreads.

# If you want to use this data, run the code below:
from io import BytesIO
import requests

# Note this link is subject to change as per Wu's webpage
url = 'https://drive.google.com/uc?export=download&id=1_u9cRxmOSiwp_tFvlaORuhS-zwl935s0'

# Send a request to the URL
response = requests.get(url)

# Read the Excel file into a Pandas DataFrame
wu_ylds    = pd.read_excel(BytesIO(response.content),skiprows=7)
new_header = wu_ylds.iloc[0,:] 
wu_ylds    = wu_ylds.iloc[1:,:]

# Rename the columns
wu_ylds.columns = new_header
wu_ylds.rename(columns={wu_ylds.columns[0]: 'date'}, inplace=True)
wu_ylds['date'] = pd.to_datetime(wu_ylds['date'], format = "%Y%m")
wu_ylds['date'] = wu_ylds['date'] + MonthEnd(0)
wu_ylds = wu_ylds.set_index(['date'])
wu_ylds.columns = wu_ylds.columns.str.replace(' ', '')

# Select Key Rates#
wu_ylds = wu_ylds[['12m','24m','60m','84m','120m','240m','360m']].reset_index()

# Rename
wu_ylds .columns = ['date','oneyr','twoyr','fiveyr','sevyr','tenyr','twentyr',
                'thirtyr']
wu_ylds  = wu_ylds .set_index(['date'])
wu_ylds  = wu_ylds .reset_index()
wu_ylds .rename(columns={'date':'trd_exctn_dt'}, inplace=True)

import pandas_datareader as pdr
import datetime as datetime

start = datetime.datetime (2000, 1, 31)
end   = datetime.datetime (2022, 12, 31)

# Treasury yields -- "key rates"
# i.e., 1, 2, 5, 7, 10, 20 and 30-year tenors.
ylds = pdr.DataReader(['DGS1','DGS2','DGS5','DGS7','DGS10','DGS20','DGS30'],
                      'fred', start, end)

# Some daily values that are randomly missing, ffill them
ylds = ylds.ffill()
ylds = ylds/100 # Decimal form
ylds.reset_index(inplace = True)

# Rename
ylds.columns = ['date','oneyr','twoyr','fiveyr','sevyr','tenyr','twentyr',
                'thirtyr']
ylds = ylds.set_index(['date'])
ylds = ylds.reset_index()
ylds.rename(columns={'date':'trd_exctn_dt'}, inplace=True)

df = df[['cusip_id', 'trd_exctn_dt',
         'ytmt','mod_dur', 'convexity', 'tmt']] .\
    merge(ylds, left_on  = ['trd_exctn_dt'],
                            right_on = ['trd_exctn_dt'],
                            how = "left")

# Rename cusip and date #
df.rename(columns={'trd_exctn_dt':'date',
                   'cusip_id':'cusip'}, inplace=True)

df = df.set_index(['cusip','date'])

#* ************************************** */
#* Change tmt/yld                         */
#* ************************************** */
df = df[~df.tmt.isnull()]

# Annualize duration #
df['dur'] = df['mod_dur']*12

# Check same scale (monthly)
df[['tmt','dur']].mean() # Monthly #

#* ************************************** */
#* Credit spread function (paralell)      */
#* ************************************** */
def ComputeCredit(x):   
    x_new = x.tmt
    cusip = x.cusip 
    date  = x.date   
    
    if   (x.tmt >= 12) &  (x.tmt <= 24):
        y = [  x.oneyr , x.twoyr  ]
        x = [12,24]       
        yld_interp = np.interp(x_new, x, y)   
         
    elif (x.tmt > 24)  &  (x.tmt <= 60):
        y = [  x.twoyr  , x.fiveyr  ]
        x = [24,60]
        yld_interp = np.interp(x_new, x, y)   
         
    elif (x.tmt > 60)  &  (x.tmt <= 84):
        y = [  x.fiveyr   , x.sevyr  ]    
        x = [60,84]
        yld_interp = np.interp(x_new, x, y)   
       
    elif (x.tmt > 84)  &  (x.tmt <= 120):
        y = [  x.sevyr    , x.tenyr  ]        
        x =  [84,120]
        yld_interp = np.interp(x_new, x, y)   
         
    elif (x.tmt > 120)  &  (x.tmt <= 240):
        y =  [  x.tenyr     , x.twentyr  ]       
        x =  [120,240]
        yld_interp = np.interp(x_new, x, y)   
        
    elif (x.tmt > 240)  &  (x.tmt <= 360):
        y =   [  x.twentyr   , x.thirtyr  ]       
        x = [240,360]
        yld_interp = np.interp(x_new, x, y)   
        
    elif (x.tmt > 360):
        y = x.thirtyr       
        yld_interp = y
       
    elif (x.tmt < 12 ):
        y  = x.oneyr       
        yld_interp = y # We drop these bonds // no need to change.
               
    return (cusip, date, 
            yld_interp
            ) 
                 
#* ************************************** */
#* Credit spread compute                  */
#* ************************************** */
# Drop NaNs
df = df.dropna()
df = df.reset_index() 

# For now, I run seperate things seperately.
# Feel free to augment function to do maturity and duration
# interpolation in one go #
# I use 14 cores on my machine 


# Maturity Interpolated # 
from joblib import Parallel, delayed  
dfTMT = pd.DataFrame(
    Parallel(n_jobs=14)(delayed(ComputeCredit)(x)
                       for x in tqdm(df.itertuples(index=False))),
     columns=['cusip', 'date', 'yld_interp']
    ) 

# Duration Interpolated # 
# --> replace tmt with dur #
df['tmt'] = df['dur']

# i.e., the tmt variable becomes the duration variable
# we now interpolate over duration as opposed to maturity.

dfDUR = pd.DataFrame(
    Parallel(n_jobs=14)(delayed(ComputeCredit)(x)
                       for x in tqdm(df.itertuples(index=False))),
     columns=['cusip', 'date', 'yld_interp_dur']
    ) 
 
# Merge
dfExport = dfTMT.merge(dfDUR, how = "inner", left_on  = ['date','cusip'],
                                             right_on = ['date','cusip'])

# Check #
( dfExport[['yld_interp', 'yld_interp_dur']] ).describe().round(2)

dfExport = dfExport .merge(df[['cusip', 'date', 'ytmt']], 
                           how = "inner", left_on  = ['date','cusip'],
                                             right_on = ['date','cusip'])


# Compute the credit spreads #
dfExport['cs_dur'] = dfExport['ytmt']-dfExport['yld_interp_dur']
dfExport['cs']     = dfExport['ytmt']-dfExport['yld_interp']

dfExport = dfExport[['cusip', 'date','cs_dur', 'cs']]
dfExport.columns = ['cusip_id' , 'trd_exctn_dt','cs_dur', 'cs']

#* ************************************** */
#* Export                                 */
#* ************************************** */
dfExport.to_hdf('TRACE_Daily_Spreads.h5',
                key='daily', mode='w')  
