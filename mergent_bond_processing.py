import pandas as pd
import numpy as np

# =============================================================================
# # Script to clean Mergent FISD File                                         #
# =============================================================================
FISD_Char = pd.read_csv('mergent_fisd.gz')
FISD_Char =  FISD_Char.drop_duplicates() 

# =============================================================================
# Filters for the FISD data to remove bonds                                   #
# =============================================================================
#1: Discard all non-US Bonds (i) in BBW
print( FISD_Char.COUNTRY_DOMICILE.unique() )
mask_USA = (FISD_Char.COUNTRY_DOMICILE == 'USA')
FISD_Char = FISD_Char[(mask_USA)]

#2: US FX
print( FISD_Char.FOREIGN_CURRENCY.unique() )
mask_usa_fx = (FISD_Char.FOREIGN_CURRENCY == 'N')
FISD_Char = FISD_Char[(mask_usa_fx)]

#3: Must have a fixed coupon
print( FISD_Char.COUPON_TYPE.unique() )
mask_coupon = (FISD_Char.COUPON_TYPE != 'V')
FISD_Char = FISD_Char[(mask_coupon)]

#4: Discard ALL convertible bonds
print( FISD_Char.CONVERTIBLE.unique() )
mask_con = (FISD_Char.CONVERTIBLE == 'N')
FISD_Char = FISD_Char[(mask_con)]

#5: Discard all asset-backed bonds
print( FISD_Char.ASSET_BACKED.unique() )
mask_asset_backed = (FISD_Char.ASSET_BACKED == 'N')
FISD_Char = FISD_Char[(mask_asset_backed)]

#6: Only U.S Corporate Bonds of specific type (WRDS Bond Module)
print( FISD_Char.BOND_TYPE.unique() )
mask_corp = (FISD_Char.BOND_TYPE == 'CCPI') |  (FISD_Char.BOND_TYPE == 'CDEB') |  (FISD_Char.BOND_TYPE == 'CMTN')\
                                            |  (FISD_Char.BOND_TYPE == 'CMTZ') |  (FISD_Char.BOND_TYPE == 'CP')\
                                            |  (FISD_Char.BOND_TYPE == 'CZ')   |  (FISD_Char.BOND_TYPE == 'UCID')\
                                            |  (FISD_Char.BOND_TYPE == 'RNT')  |  (FISD_Char.BOND_TYPE == 'CLOC')\
                                            |  (FISD_Char.BOND_TYPE == 'CPIK') |  (FISD_Char.BOND_TYPE == 'CUIT')\
                                            |  (FISD_Char.BOND_TYPE == 'USBN')    
FISD_Char = FISD_Char[(mask_corp)]

# ============================================================================
# Now evaluate call options / dummy variable for calls                       #
# ============================================================================
# Read in from WRDS and left-merge to FISD_Char #
# Code as 1 for callable and 0 for normal       #

FISD_Call = pd.read_csv('mergent_fisd_callable.gz')
FISD_Call =  FISD_Call.drop_duplicates() 
FISD_Call = FISD_Call.replace(['Y'],1)
FISD_Call = FISD_Call.replace(['N'],0)

FISD_Call['callable_total'] = FISD_Call['CALLABLE'] + FISD_Call['DISCRETE_CALL'] + FISD_Call['MAKE_WHOLE']+ FISD_Call['CALL_IN_WHOLE']
FISD_Call['CALLABLE']      = np.where( FISD_Call['callable_total']>0, 1, 0  )

FISD_Call = FISD_Call[[ 'CALLABLE','ISSUE_ID']]
# Merge #
FISD_Char = FISD_Char.merge(FISD_Call, left_on = ['ISSUE_ID'],
                            right_on = ['ISSUE_ID'], how = "left")

# ============================================================================
FISD_Char = FISD_Char[['ISSUE_ID','ISSUER_ID' ,'ISSUER_CUSIP', 'COMPLETE_CUSIP','CUSIP_NAME' , 'OFFERING_AMT' , 'CALLABLE' ,'PUTABLE','MATURITY','OFFERING_DATE']]
FISD_Char['PUTABLE'] = np.where( FISD_Char['PUTABLE']=='Y', 1, 0   )
# ============================================================================
# FISD_Industry Codes + Additional Info #
FISD_Issuer = pd.read_csv(r'C:\Users\Alexander\Dropbox\Corporate_Bonds_Inflation\data\mergent_fisd_issuer.gz')

FISD_Char_Export = FISD_Char.merge(FISD_Issuer , left_on = ['ISSUER_ID'],
                                            right_on = ['ISSUER_ID'], how = "left")

# If callable is missing, assume NO call option
print(FISD_Char_Export.isnull().sum())
FISD_Char_Export['CALLABLE'] = np.where(FISD_Char_Export['CALLABLE'].isnull(), 0 , FISD_Char_Export['CALLABLE']   )

FISD_Char_Export.to_csv('FISD_2022.gz')
# ============================================================================
# End #
# ============================================================================
