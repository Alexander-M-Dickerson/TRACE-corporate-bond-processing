import pandas as pd
import numpy as np
import wrds

# =============================================================================
# Read in data directly from WRDS
# =============================================================================
# Assumes you have a valid WRDS account and have set-up your cloud access #
# See:
# https://wrds-www.wharton.upenn.edu/pages/support/programming-wrds/programming-python/python-wrds-cloud/
wrds_username = '' # Input your WRDS username
db = wrds.Connection(wrds_username = wrds_username )

tbl1 = db.raw_sql("""SELECT  DATE, ISSUE_ID,CUSIP, RATING_NUM, RET_L5M,AMOUNT_OUTSTANDING,
                                TMT, N_SP, PRICE_L5M                         
                        FROM wrdsapps.bondret
                  """)
                  
# Required because the WRDS data comes with "duplicates" in the index
# does not affect data, but the "index" needs to be re-defined #                 
tbl1 = tbl1.reset_index()
tbl1['index'] = range(1,(len(tbl1)+1))

# Format the data
tbl1.columns = tbl1.columns.str.upper()
tbl1['DATE'] = pd.to_datetime(tbl1['DATE'])
tbl1 = tbl1.sort_values(['CUSIP','DATE'])

# Create ISSUER_CUSIP, first 6 digits of the full bond CUSIP
tbl1['ISSUER_CUSIP'] = tbl1['CUSIP'].str[:6]

# =============================================================================
# Read in the Open Source Asset Pricing Linker File #
# =============================================================================
url = "https://openbondassetpricing.com/wp-content/uploads/2024/10/OSBAP_Linker_October_2024.csv"
# Read the CSV file directly from the URL into a pandas DataFrame
linker = pd.read_csv(url)

# Formate DATE
linker['DATE'] = pd.to_datetime(linker['DATE'])

# Merge the WRDS TRACE bond data to the OSBAP Linker File 
tbl1 = tbl1.merge(linker, left_on  = ['ISSUER_CUSIP','DATE'],
                          right_on = ['ISSUER_CUSIP','DATE'],
                          how      = "left")

# =============================================================================
# Merge complete #
# =============================================================================

# tbl1 can now be merged to CRSP data via PERMNO or COMPUSTAT via GVKEY
# OR first merge CRSP and COMPUSTAT and then merge to tbl1 via PERMNO

# =============================================================================
