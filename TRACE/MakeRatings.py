##########################################
# BBW (2019) Enhanced TRACE Data Process #
# Part (v): Monthly Rating data          #
# Alexander Dickerson                    #
# Email: a.dickerson@warwick.ac.uk       #
# Date: January 2023                     #
# Updated:  July 2023                    #
# Version:  1.0.1                        #
##########################################

'''
Overview
-------------
This Python script downloads the S&P and Moody's ratings and converts them
into numerical scores, i.e., AAA == 1, C ==21.
 
Requirements
-------------
Access to the WRDS server and associated databases.
No data requirements.


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
from datetime import datetime, timedelta
from tqdm import tqdm
import pandas_datareader as pdr
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
import datetime as datetime
from pandas.tseries.offsets import *
import wrds
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
rat = db.raw_sql("""SELECT issue_id, rating_type, rating_date,rating               
                  FROM fisd.fisd_ratings
                  """)

# Keep SP and Moody's Ratings #
rat = rat[ ( (rat['rating_type'] == "SPR")|\
             (rat['rating_type'] == "MR") ) ]

# Remove from sample, ALL bonds with an "NR" (not rated) and the "NR",
# derivatives category #
rat = rat[rat['rating'] != "NR"]
rat = rat[rat['rating'] != 'NR/NR']
rat = rat[rat['rating'] != 'SUSP']
rat = rat[rat['rating'] != 'P-1']
rat = rat[rat['rating'] != '0']
rat = rat[rat['rating'] != 'NAV']

# S&P Ratings #
ratsp = rat[  (rat['rating_type'] == "SPR") ]
  
# Define the mapping from SP ratings to numeric values
sp_rating_mapping = {
    "AAA": 1,
    "AA+": 2,
    "AA": 3,
    "AA/A-1+":3,
    "AA-": 4,
    "AA-/A-1+":4,
    "A+": 5,
    "A": 6,
    "A-": 7,
    "BBB+": 8,
    "BBB": 9,
    "BBB/A-2":9,
    "BBB-": 10,
    "BB+": 11,
    "BB": 12,
    "BB-": 13,
    "B+": 14,
    "B": 15,
    "B-": 16,
    "CCC+": 17,
    "CCC": 18,
    "CCC-": 19,
    "CC": 20,
    "C": 21,
    "D":22
}

# Assuming "df" is your DataFrame with the panel of SP ratings
# Replace the ratings in the "rating" column with numeric values
ratsp["spr"] = ratsp["rating"].map(sp_rating_mapping)
ratsp["spr"].value_counts()

# Moody's Ratings #
ratmd = rat[  (rat['rating_type'] == "MR") ]
# Define the mapping from Moody's ratings to numeric values
moody_rating_mapping = {
    "Aaa": 1,
    "Aa1": 2,
    "Aa2": 3,
    "Aa3": 4,
    "A1": 5,
    "A2": 6,
    "A3": 7,
    "Baa1": 8,
    "Baa2": 9,
    "Baa3": 10,
    "Ba1": 11,
    "Ba2": 12,
    "Ba3": 13,
    "B1": 14,
    "B2": 15,
    "B3": 16,
    "Caa1": 17,
    "Caa2": 18,
    "Caa3": 19,
    "Ca": 20,
    "C": 21,
}

ratmd["mr"] = ratmd["rating"].map(moody_rating_mapping)

#### Export ####
ratmd.to_hdf('moody_ratings.h5',
             key = 'daily')

ratsp.to_hdf('sp_ratings.h5',
               key = 'daily')  
#################