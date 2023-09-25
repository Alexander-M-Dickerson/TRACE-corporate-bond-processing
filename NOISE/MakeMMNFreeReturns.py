import pandas as pd
import numpy as np
from tqdm import tqdm
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
import datetime as datetime
import wrds
tqdm.pandas()

#* ************************************** */
#* Read daily prices and AI               */
#* ************************************** */ 
df = \
pd.read_csv\
    (r'DirtyPrices.csv.gzip', 
                     compression = "gzip")

# Convert columns to uppercase (for now)
df.columns = df.columns.str.upper()

#* ************************************** */
#* Set Indices                            */
#* ************************************** */ 
df         = df.set_index(['CUSIP_ID', 'TRD_EXCTN_DT'])
df = df.reset_index()
df['TRD_EXCTN_DT'] = pd.to_datetime( df['TRD_EXCTN_DT']  )

#* ************************************** */
#* Remove any missing Price values        */
#* ************************************** */ 
df = df[~df.PRCLEAN.isnull()]

#* ************************************** */
#* Create month begin / end column        */
#* ************************************** */ 
df['month_begin']=np.where(  df['TRD_EXCTN_DT'].dt.day != 1,
                             df['TRD_EXCTN_DT'] + pd.offsets.MonthBegin(-1),
                             df['TRD_EXCTN_DT'])

df['month_end']    = df['month_begin'] + pd.offsets.MonthEnd(0)
df.rename(columns={'TRD_EXCTN_DT':'date'}, inplace=True)

df['date'] = pd.to_datetime(df['date'])

# Set U.S. Trading day calendars #
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
calendar = USFederalHolidayCalendar()

start_date = '01JAN1997'
end_date   = '31DEC2022'
holidays = calendar.holidays(start_date, end_date)
holidays
bday_us = CustomBusinessDay(holidays=holidays)

df['month_end']    = df['date'] + pd.offsets.MonthEnd(0)

#* ************************************** */
#* Connect to WRDS                        */
#* ************************************** */  
db = wrds.Connection()

#* ************************************** */
#* WRDS Bond Returns                      */
#* ************************************** */  
# We only require 2 variables from the WRDS Bond Returns Module
# T_DATE, is the actual bond transaction date
# CUSIP is the identifier

traced = db.raw_sql("""SELECT  T_DATE, CUSIP
                        FROM wrdsapps.bondret
                    """)

traced = traced[['t_date','cusip']]
traced['t_date'] = pd.to_datetime(traced['t_date'])
traced['month_end']    = traced['t_date'] + pd.offsets.MonthEnd(0)

df.rename(columns={'CUSIP_ID':'cusip'}, inplace=True)

df = df.merge(traced,
              how = "left",
              left_on  = ['cusip','month_end'],
              right_on = ['cusip','month_end'])

dtiE                  = pd.DataFrame( pd.Series(df['t_date'].unique())\
                                     .sort_values() )
dtiE.columns         = ['t_date']

dtiE['cut_off_end1'] = dtiE['t_date'] - 1 * bday_us
dtiE['cut_off_end2'] = dtiE['t_date'] - 2 * bday_us
dtiE['cut_off_end3'] = dtiE['t_date'] - 3 * bday_us
dtiE['cut_off_end5'] = dtiE['t_date'] - 5 * bday_us


df = df.merge(dtiE,                 left_on      = ['t_date'], 
                                    right_on     = ['t_date'],
                                    how          = "left")

df['cut_off_begin'] = np.where( df['date'].dt.day != 1,
                             df['date']+ pd.offsets.MonthBegin(-1),
                             df['date'])
# =============================================================================   
COs = ['cut_off_end1','cut_off_end2','cut_off_end3','cut_off_end5']

for co in COs:
    print(co)
    df[co] = np.where(df[co] <= df['cut_off_begin'],
                      df['cut_off_begin'],
                      df[co])

# =============================================================================   
### Choose cut-off here ###
### In the the main paper, we are ULTRA conservative and use a single-business
### day gap --- but following Bartram, Grinblatt and Nozawa (2023), you should
### use gap >3, up until 7.

CO = 'cut_off_end1'

mask = (df['date'] >= df['cut_off_begin'] ) &\
       (df['date'] <= df[CO] )
       
df = df[mask]

df = df[[  'date', 'cusip',
           'PR', 'PRCLEAN', 'PRFULL',
                  'ACCLAST', 'ACCPMT', 'ACCALL']]

PriceC = df.copy()

#* ************************************** */
#* Set min, max dates                     */
#* ************************************** */ 
PriceC['month_year'] = pd.to_datetime(PriceC['date']).dt.to_period('M')

PriceC['min_date']   =\
    PriceC.groupby(['cusip',
                    'month_year'])['date'].transform("min")
PriceC['max_date']   =\
    PriceC.groupby(['cusip',
                    'month_year'])['date'].transform("max")

#* ************************************** */
#* Keep the obs. that are closest to      */
#* the beginning or end of the month      */
#* ************************************** */ 
    
PriceC = PriceC[ ((  PriceC['date'] == PriceC['min_date'])\
             |   ((  PriceC['date'] == PriceC['max_date'])) ) ]                                 

#* ************************************** */
#* Count Obs in a month                   */
#* There will always be 2 for month begin */ 
#* And always 1 for month end             */
#* ************************************** */ 

PriceC['count'] = PriceC.groupby(['cusip',
                                  'month_year'])['PR'].transform('count')

Month_Begin = PriceC[ PriceC['count'] == 2 ]
Month_Begin['month_min'] = PriceC['min_date'].dt.month
Month_Begin['month_max'] = PriceC['max_date'].dt.month

Month_Begin['n'] = (Month_Begin['max_date'] - Month_Begin['min_date']).dt.days

#* ************************************** */
#* Return Type #2: First/Last 5-Days      */
#* ************************************** */ 
Month_Begin['ret']  = ( Month_Begin['PR'] /\
                        Month_Begin.groupby(['cusip'])['PR'].shift(1)) - 1
Month_Begin['retf'] =  (Month_Begin['PR'] + Month_Begin['ACCALL'] -\
                        Month_Begin.groupby(['cusip'])['PR'].shift(1)-\
                        Month_Begin.groupby(['cusip'])['ACCALL'].shift(1)) /\
                        Month_Begin.groupby(['cusip'])['PR'].shift(1)

Month_Begin['retff'] =  (Month_Begin['PR'] + Month_Begin['ACCALL'] -\
                         Month_Begin.groupby(['cusip'])['PR'].shift(1)-\
                         Month_Begin.groupby(['cusip'])['ACCALL'].shift(1))/\
                         Month_Begin.groupby(['cusip'])['PRFULL'].shift(1)

Month_Begin['day'] = Month_Begin['date'].dt.day
Month_Begin = Month_Begin[ ( Month_Begin['date'] == Month_Begin['max_date']) ]


Month_Begin = Month_Begin[['cusip','date','ret', 'retf','retff',
                       'PRCLEAN','n']]

Month_Begin.columns = ['cusip','date','ret', 'retf','retff',
                       'prc','n_days']

Month_Begin['date'] = Month_Begin['date'] + MonthEnd(0)
Month_Begin = Month_Begin.dropna()
Month_Begin = Month_Begin[['cusip','date','retff']]
Month_Begin.columns = ['cusip','date','bond_ret_bab']

Month_Begin.to_hdf(r'Reversals_t1_day.h5',
                  key = 'daily')
#####################################
