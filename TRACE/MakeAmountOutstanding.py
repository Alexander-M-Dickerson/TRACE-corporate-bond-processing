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
#* Download Mergent File                  */
#* ************************************** */  
# Amount Outstanding   #
amt = db.raw_sql("""SELECT issue_id,
                           action_type, effective_date, action_price,
                           action_amount, amount_outstanding
                  FROM fisd.fisd_mergedissue  
                  """)
                  
# Offering Outstanding # 
fisd_issue = db.raw_sql("""SELECT complete_cusip, issue_id,                  
                  offering_amt
                  FROM fisd.fisd_mergedissue  
                  """)
# Merge #                 
amt = pd.merge( amt, 
                fisd_issue, 
                on = ['issue_id'], 
                how = "left")   

# Adjust #
# Issue matured (IM), set action amount to the full value of offering_amt #
amt['action_amount'] = np.where(amt['action_type'] == "IM", 
                                amt['offering_amt'], 
                                amt['action_amount'])

# Initial offering of an issue (I), sets to zero if any NaN#
amt['action_amount'] = np.where(amt['action_type'] == "I", 
                                0, 
                                amt['action_amount'])

# Inactive issue (IA), set to offering_amount is NaN #
amt['action_amount'] = np.where((amt['action_type'] == "IA") &\
                                amt['action_amount'].isnull(),
                                amt['offering_amt'], amt['action_amount'])

# Entire issue called (E) #
amt['action_amount'] = np.where(amt['action_type'] == "E", 
                                amt['offering_amt'], 
                                amt['action_amount'])

# Review (R), set action_amount to 0 #
amt['action_amount'] = np.where(amt['action_type'] == "REV", 
                                0, 
                                amt['action_amount'])

# Reopening (RO)
amt['action_amount'] = np.where((amt['action_type'] == "RO") &\
                                 amt['action_amount'].isnull(),
                                (amt['amount_outstanding']-amt['offering_amt']),
                                amt['action_amount'])

amt['action_amount'] = np.where(amt['action_type'] == "RO", 
                                amt['action_amount'] * -1, 
                                amt['action_amount'])

# Reorganization
amt['action_amount'] = np.where((amt['action_type'] == "R") &\
                                amt['action_amount'].isnull(),
                                0, amt['action_amount'])

# Drop any values where offering_amt is a NaN
amt = amt[~amt.offering_amt.isnull()]

# Compute bond amount outstanding #
amt['bond_amount_out'] = amt['offering_amt'] - amt['action_amount']

# Format dates / clean #
amt = amt[['effective_date','issue_id','complete_cusip',
           'offering_amt','bond_amount_out','amount_outstanding',
           'action_amount','action_type']]

amt['effective_date'] = pd.to_datetime(amt['effective_date'], 
                                       format='%Y/%m/%d')


amt = amt[~amt.effective_date .isnull()]
amt = amt[~amt.bond_amount_out.isnull()]
              
amt.rename(columns={ 'complete_cusip':'cusip',
                     'issue_id':'issueid'}, inplace=True)

amt['date'] = amt['effective_date'] + pd.offsets.MonthEnd(0)

amt = amt[['cusip', 'date','offering_amt','bond_amount_out','amount_outstanding',
                       'action_amount','action_type']]                  
                  
                  
amt= amt.set_index(['date','cusip'])
amt = amt.sort_index(level = ['cusip','date'])
amt['N'] = amt.groupby(level = "cusip")['bond_amount_out'].transform("count")

# Balance of issue called, set bond_amount_out to zero #
amt['bond_amount_out'] = np.where((amt['action_type'] == "B" ) & (amt['bond_amount_out'] < 0) ,
                                0 ,amt['bond_amount_out'])

# Reorganization increases amount_out
amt['bond_amount_out'] = np.where((amt['action_type'] == "R" ) &\
                                  (amt['bond_amount_out'] < 0) ,
                                 amt['offering_amt']+amt['action_amount'],
                                 amt['bond_amount_out'])

#### Export ####
amt.reset_index(inplace = True)
amt.to_hdf('amount_outstanding.h5',
             key = 'daily')
#################              