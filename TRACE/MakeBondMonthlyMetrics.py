##########################################
# BBW (2019) Enhanced TRACE Data Process #
# Part (iii): Monthly Bond Metrics       #
# Compute Monthly bond returns, yields,  #
# duration, volume and convexity         #
# Alexander Dickerson                    #
# Email: a.dickerson@warwick.ac.uk       #
# Date: January 2023                     #
# Updated:  July 2023                    #
# Version:  1.0.1                        #
##########################################

'''
Overview
-------------
This Python script computes monthly bond returns, excess returns, duration and
convexity.
 
Requirements
-------------
Data output from "MakeBondDailyMetrics.py" including
    (1) AI_Yield_BBW_TRACE_Enhanced_Dick_Nielsen.csv.gzip

Package versions 
-------------
pandas v1.4.4
numpy v1.21.5
tqdm v4.64.1
datetime v3.9.13
zipfile v3.9.13
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
tqdm.pandas()

#* ************************************** */
#* Read daily prices and AI               */
#* ************************************** */ 
# We start off by loading the daily price, AI volume and yield file
# This is the output from the script file: "MakeBondDailyMetrics.py"
# or "MakeBondDailyMetrics_2.py"
# Change below to 'DirtyPrices.csv.gzip' for v2 (recomended)

PriceC = \
pd.read_csv\
    (r'~\AI_Yield_BBW_TRACE_Enhanced_Dick_Nielsen.csv.gzip', 
                     compression = "gzip")

# Convert columns to uppercase (for now)
PriceC.columns = PriceC.columns.str.upper()

#* ************************************** */
#* Set Indices                            */
#* ************************************** */ 
PriceC         = PriceC.set_index(['CUSIP_ID', 'TRD_EXCTN_DT'])
PriceC = PriceC.reset_index()
PriceC['TRD_EXCTN_DT'] = pd.to_datetime( PriceC['TRD_EXCTN_DT']  )

#* ************************************** */
#* Remove any missing Price values        */
#* ************************************** */ 
# Remove any vales for which we do not have valid price data
PriceC = PriceC[~PriceC.PRCLEAN.isnull()]

#* ************************************** */
#* Create month begin / end column        */
#* ************************************** */ 
PriceC['month_begin']=np.where( PriceC['TRD_EXCTN_DT'].dt.day != 1,
                             PriceC['TRD_EXCTN_DT'] + pd.offsets.MonthBegin(-1),
                             PriceC['TRD_EXCTN_DT'])
PriceC['month_end']    = PriceC['month_begin'] + pd.offsets.MonthEnd(0)


#* ************************************** */
# Page 623-624 of BBWs published paper in JFE:
# where the end (beginning) of month refers to the last (first) 
# five trading days within each month
# "trading" days is assumed to be business days
# Hence, use USFederalHolidayCalendar to account for this and BDay
#* ************************************** */

# Set U.S. Trading day calendars #
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
calendar = USFederalHolidayCalendar()

start_date = '01JUL2002'
end_date   = '31DEC2022'
holidays = calendar.holidays(start_date, end_date)
holidays
bday_us = CustomBusinessDay(holidays=holidays)

#* ************************************** */
#* n-1 = 4 Businnes Days                  */
#* ************************************** */ 
# Use n-1 days: if the first business day is 1st March, then the 5th
# is t+4 = 5th March, and so on
dtiB = pd.DataFrame( pd.Series(PriceC['month_begin'].unique()).sort_values() )
dtiB['cut_off_begin'] = dtiB + 4 * bday_us
dtiB.columns = ['month_begin','cut_off_begin']

dtiE = pd.DataFrame( pd.Series(PriceC['month_end'].unique()).sort_values() )
dtiE['cut_off_begin'] = dtiE - 4 * bday_us
dtiE.columns = ['month_end','cut_off_end']

#* ************************************** */
#* Merge eligible dates to PriceC         */
#* ************************************** */   
PriceC = PriceC.merge(dtiB,       left_on      = ['month_begin'], 
                                  right_on     = ['month_begin'],
                                  how          = "left")

PriceC = PriceC.merge(dtiE,       left_on      = ['month_end'], 
                                  right_on     = ['month_end'],
                                  how          = "left")

#* ************************************** */
#* Filter dates in PriceC         */
#* ************************************** */ 
mask = (PriceC['TRD_EXCTN_DT'] <= PriceC['cut_off_begin'] ) |\
       (PriceC['TRD_EXCTN_DT'] >= PriceC['cut_off_end'] )
PriceC = PriceC[mask]

#* ************************************** */
#* Set min, max dates                     */
#* ************************************** */ 
PriceC['month_year'] = pd.to_datetime(PriceC['TRD_EXCTN_DT']).dt.to_period('M')
PriceC['min_date']   =\
    PriceC.groupby(['CUSIP_ID',
                    'month_year'])['TRD_EXCTN_DT'].transform("min")
PriceC['max_date']   =\
    PriceC.groupby(['CUSIP_ID',
                    'month_year'])['TRD_EXCTN_DT'].transform("max")

#* ************************************** */
#* Keep the obs. that are closest to      */
#* the beginning or end of the month      */
#* ************************************** */ 
PriceC = PriceC[ ((  PriceC['TRD_EXCTN_DT'] == PriceC['min_date'])        \
             &    (  PriceC['TRD_EXCTN_DT'] <= PriceC['cut_off_begin'] )) \
             |   ((  PriceC['TRD_EXCTN_DT'] == PriceC['max_date'])        \
             &    (  PriceC['TRD_EXCTN_DT'] >= PriceC['cut_off_end'] ))    ]                                

                 
#* ************************************** */
#* Dummies for return type                */
#* ************************************** */ 

PriceC['month_begin_dummy'] =  ( PriceC['TRD_EXCTN_DT']\
                                <= PriceC['cut_off_begin'] ) * 1           
PriceC['month_end_dummy']   =  ( PriceC['TRD_EXCTN_DT']\
                                >= PriceC['cut_off_end'] )   * 1           

#* ************************************** */
#* Set date reference for end of month ret*/
#* ************************************** */     
    
PriceC['date_end'] = np.where( PriceC['month_end_dummy']   == 1,
                              PriceC['TRD_EXCTN_DT'] + pd.offsets.MonthEnd(0),
                              PriceC['TRD_EXCTN_DT']   )       

#* ************************************** */
#* Count Obs in a month                   */
#* There will always be 2 for month begin */ 
#* And always 1 for month end             */
#* ************************************** */ 

PriceC['count'] = PriceC.groupby(['CUSIP_ID',
                                  'month_year'])['PR'].transform('count')

Month_End   = PriceC[ PriceC['month_end_dummy'] == 1 ]
Month_Begin = PriceC[ PriceC['count'] == 2 ]

#* ************************************** */
#* Return Type #1: Last 5-Days            */
#* ************************************** */ 
Month_End = Month_End.set_index('date_end')
Month_End = Month_End[['PR', 'PRCLEAN', 'PRFULL','ACCLAST', 'ACCPMT', 
                       'ACCALL','CUSIP_ID','YTM','QVOLUME', 'DVOLUME',
                       'MOD_DUR','CONVEXITY']]

Month_End        = Month_End.reset_index()

#* ************************************** */
#* Compute days between 2-Month-End Trades*/
#* ************************************** */ 
Month_End['n']   = ( Month_End['date_end'] -\
                     Month_End.groupby( "CUSIP_ID")['date_end'].shift(1) ) /\
                     np.timedelta64( 1, 'D' ) 

#* ************************************** */
#* Compute Returns                        */
#* ************************************** */ 
Month_End['ret']      = ( Month_End['PR'] /\
                         Month_End.groupby(['CUSIP_ID'])['PR'].shift(1)) - 1
Month_End['retf'] =  ( Month_End['PR'] + Month_End['ACCALL'] -\
                       Month_End.groupby(['CUSIP_ID'])['PR'].shift(1)\
                     - Month_End.groupby(['CUSIP_ID'])['ACCALL'].shift(1)) /\
                       Month_End.groupby(['CUSIP_ID'])['PR'].shift(1)
Month_End['retff'] =  (Month_End['PR'] + Month_End['ACCALL'] -\
                       Month_End.groupby(['CUSIP_ID'])['PR'].shift(1)\
                     - Month_End.groupby(['CUSIP_ID'])['ACCALL'].shift(1)) /\
                       Month_End.groupby(['CUSIP_ID'])['PRFULL'].shift(1)

#* ************************************** */
#* Force contiguous return                */
#* ************************************** */ 
Month_End = Month_End[Month_End.n <= 31]
Month_End.columns
Month_End   = Month_End[['CUSIP_ID','date_end','ret','retf','retff','PR','YTM',
                         'QVOLUME', 'DVOLUME','MOD_DUR', 'CONVEXITY']]
Month_End.columns = ['cusip','date','ret','retf','retff','pr','ytm', 
                     'qvolume', 'dvolume','mod_dur','convexity']
Month_End['dummy']   = 'END'
Month_End = Month_End.dropna()

#* ************************************** */
#* Return Type #2: First/Last 5-Days      */
#* ************************************** */ 
Month_Begin['ret']  = ( Month_Begin['PR'] /\
                        Month_Begin.groupby(['CUSIP_ID'])['PR'].shift(1)) - 1
Month_Begin['retf'] =  (Month_Begin['PR'] + Month_Begin['ACCALL'] -\
                        Month_Begin.groupby(['CUSIP_ID'])['PR'].shift(1)-\
                        Month_Begin.groupby(['CUSIP_ID'])['ACCALL'].shift(1)) /\
                        Month_Begin.groupby(['CUSIP_ID'])['PR'].shift(1)

Month_Begin['retff'] =  (Month_Begin['PR'] + Month_Begin['ACCALL'] -\
                         Month_Begin.groupby(['CUSIP_ID'])['PR'].shift(1)-\
                         Month_Begin.groupby(['CUSIP_ID'])['ACCALL'].shift(1))/\
                         Month_Begin.groupby(['CUSIP_ID'])['PRFULL'].shift(1)

Month_Begin['day'] = Month_Begin['TRD_EXCTN_DT'].dt.day
Month_Begin = Month_Begin[Month_Begin.day > 15]

Month_Begin = Month_Begin[['CUSIP_ID','date_end','ret','retf','retff','PR',
                           'YTM', 'QVOLUME', 'DVOLUME','MOD_DUR', 'CONVEXITY']]
Month_Begin.columns = ['cusip','date','ret','retf','retff','pr',
                       'ytm', 'qvolume', 'dvolume','mod_dur','convexity']
Month_Begin['dummy'] = 'BEGIN'
Month_Begin = Month_Begin.dropna()

# Dummies for END and BEGIN
Month_End['ret_type']   = "END"
Month_Begin['ret_type'] = "BEGIN"


#* ************************************** */
#* Concatenate Return Types               */
#* ************************************** */ 
df = pd.concat([Month_End, Month_Begin], axis = 0)

df = df.set_index(['date','cusip'])
df = df.sort_index(level = 'cusip')

#* ************************************** */
#* Check for duplicates 
#* Keep Month End then Begin, if End is
#* Missing
#* ************************************** */ 
df['duplicated_begin_end'] = df.index.duplicated(False) * 1
df['DS_Dup']  = ((  df['duplicated_begin_end'] == 1) &\
                 (df['dummy'] == 'BEGIN'  )) * 1 
df = df[ df['DS_Dup'] == 0 ]

# Trim returns here #
# This removes returns from incorrect data #
# This is standard, compact data to the interval [1,1],
# to avoid crazy outliers -- see WRDS Bond returns Module
# https://wrds-www.wharton.upenn.edu/documents/248/WRDS_Corporate_Bond_Database_Manual.pdf

df['retff'] = np.where(df['retff'] >  1, 1, df['retff'])
df['retff'] = np.where(df['retff'] < -1,-1, df['retff'])

df['retf'] = np.where(df['retf'] >  1, 1, df['retf'])
df['retf'] = np.where(df['retf'] < -1,-1, df['retf'])

#* ************************************** */
#* Pick Columns                           */
#* ************************************** */ 
# NOTE: retff is the TOTAL BOND RETURN as in th BBW Paper
# Equation (1)
# We do not need the coupon, we use quantmod to
# compute the cumulative coupon which is how the return is computed
df = df[['retff', 'ytm', 'qvolume', 'dvolume','ret_type','mod_dur','convexity']]
df.columns = ['bond_ret', 'bond_yield', 'par_volume', 'dol_volume','ret_type',
              'mod_dur','convexity']

df = df[['bond_ret', 'bond_yield','ret_type',
         'mod_dur','convexity']]
        
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

#* ************************************** */
#* Merge rf to df
#* ************************************** */ 
df = df.merge(rf, how = "inner", left_index = True, 
              right_index = True)
df.columns
df['exretn']  = df['bond_ret'] - df['RF']
df.isnull().sum()

df = df[['bond_ret','exretn', 'bond_yield','ret_type',
         'mod_dur', 'convexity']].reset_index()

#* ************************************** */
#* Resample                               */
#* ************************************** */
# Break into chunks based on cusip #
# Resample each cusip to get contiguous returns #
# This makes the process quicker -- Python struggles to resample the large
# dataframe and return it in one go.
# Outputs for each bond in the panel a contiguous time-series of
# monthly returns, NaN means there was no trade to compute a return!

CUSIPs = list(df['cusip'].unique())
c       = 5000
chunks  = [CUSIPs[x:x+c] for x in range(0, len(CUSIPs), c)]
df_resamp = pd.DataFrame()

dfR = df.reset_index().set_index(['date'])
dfR['filler'] = 1
dfR = dfR[['cusip','filler']]


for l in range(0,len(chunks)):
    print(l)
    CUSIP_Chunk = pd.DataFrame(chunks[l], columns = ['cusip'])
    dfChunk = dfR[(dfR['cusip'].isin(CUSIP_Chunk['cusip']))]
    dfResample = dfChunk.groupby(["cusip"])[ dfR.columns[1:] ].\
        apply(lambda x: x.resample("M").last())
    df_resamp = pd.concat([df_resamp, dfResample], axis = 0)

df = df_resamp.reset_index().merge(df.reset_index(), how = "left",
                   left_on = ['date','cusip'],
                   right_on=['date','cusip']).drop(['filler'],
                                                   axis = 1)
df = df.set_index(['cusip',
                   'date']).sort_index(level = ['cusip',
                                      'date'])

# Drop duplicates (there are not any)
df = df[~df.index.duplicated()]
#* ************************************** */
#* Write to file
#* ************************************** */ 
dfExport = df
dfExport\
.to_hdf(r'~\enhanced_trace_monthly_returns.h5',
        key='daily', mode='w')
