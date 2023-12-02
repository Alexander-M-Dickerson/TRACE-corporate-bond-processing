##########################################
# Enhanced TRACE Data Proces             #
# Part (ii): Dirty Prices, AI & Yields   #
# Compute Accrued Interest, Dirty Prices,#
# Bond Duration, Convexity & Yield       #
# Alexander Dickerson                    #
# Email: a.dickerson@warwick.ac.uk       #
# Date: December 2023                    #
# Updated:  December 2023                #
# Version:  3.0.0                        #
##########################################

##########################################
# I ackowledge                           #
# Francis Cong from McGill               #
# for providing me with his code to      #
# compute the dirty prices, accrued      #
# interest and bond yields               #
# I augment his code with updated        #
# parameters from the quantlib package   #
# The functions are augmented to         #
# compute bond duration and convexity    #
##########################################
# Full credit to Francis Cong for the    #
# original code in this script file      #
# of which the majority remains "as is". #
# Francis' GitHub page is available here:#
# https://github.com/flcong              #
##########################################
# Thanks to Zhiyao (Nicholas) Chen for   #
# pointing out data errors in the FISD   #
# princimal amounts and our assumptions  #
# regarding semi-annual compounding.     #
# See his webpage here:                  #
# https://sites.google.com/site/nicholaszhiyaochen/    
##########################################


'''
Overview
-------------
This Python script computes bond accrued interest, yields, duration and 
convexity for the daily panel from running the script called:
    "MakeBondIntra_Daily.py"
 
Requirements
-------------
Data output from "MakeBondIntra_Daily.py" including
    (1) Prices_BBW_TRACE_Enhanced_Dick_Nielsen.csv.gzip
    (2) Volumes_BBW_TRACE_Enhanced_Dick_Nielsen.csv.gzip

Package versions 
-------------
pandas v1.4.4
numpy v1.21.5
QuantLib v1.29
Joblib v1.1.1
wrds v3.1.2
zipfile v3.9.13
itertools v3.9.13
csv v1.0
gzip v3.9.13
tqdm v4.64.1
'''

#* ************************************** */
#* Libraries                              */
#* ************************************** */ 
import pandas as pd
import numpy as np
import QuantLib as ql
from joblib import Parallel, delayed
import wrds
import zipfile
import csv
import gzip
from tqdm import tqdm
tqdm.pandas()

#* ************************************** */
#* Load Data                              */
#* ************************************** */ 

# This data is the output from running the #
# "MakeBondIntra_Daily.py" .py script    #
# file from Alex Dickerson's GitHub page:  #
# https://github.com/Alexander-M-Dickerson/TRACE-corporate-bond-processing/tree/main/TRACE #

# Prices
traced = pd.read_csv\
    (r'~\Prices_BBW_TRACE_Enhanced_Dick_Nielsen.csv.gzip',
     compression='gzip')
traced.columns = traced.columns.str.lower()
traced['trd_exctn_dt'] = pd.to_datetime(traced['trd_exctn_dt'])

# Volumes
tracedv = pd.read_csv\
    (r'~\\Volumes_BBW_TRACE_Enhanced_Dick_Nielsen.csv.gzip',
     compression='gzip')
tracedv.columns = tracedv.columns.str.lower()
tracedv['trd_exctn_dt'] = pd.to_datetime(tracedv['trd_exctn_dt'])

# Merge
traced = traced.merge(
    tracedv ,
    left_on=['cusip_id','trd_exctn_dt'],
    right_on=['cusip_id','trd_exctn_dt'],
    how='left'
    )

del(tracedv)

#* ************************************** */
#* Connect to WRDS                        */
#* ************************************** */  
db = wrds.Connection()

#* ************************************** */
#* Download Mergent File                  */
#* ************************************** */  
fisd_issuer = db.raw_sql("""SELECT issuer_id,country_domicile,sic_code                
                  FROM fisd.fisd_mergedissuer 
                  """)

fisd_issue = db.raw_sql("""SELECT complete_cusip, issue_id,
                  issuer_id, foreign_currency,
                  coupon_type,coupon,convertible,
                  asset_backed,rule_144a,
                  bond_type,private_placement,
                  interest_frequency,dated_date,
                  day_count_basis,offering_date,
                  offering_amt, maturity, principal_amt
                  FROM fisd.fisd_mergedissue  
                  """)
                  
fisd = pd.merge(fisd_issue, fisd_issuer, on = ['issuer_id'], how = "left")                              
#* ************************************** */
#* Apply BBW Bond Filters                 */
#* ************************************** */  
#1: Discard all non-US Bonds (i) in BBW
fisd = fisd[(fisd.country_domicile == 'USA')]

#2.1: US FX
fisd = fisd[(fisd.foreign_currency == 'N')]

#3: Must have a fixed coupon
fisd = fisd[(fisd.coupon_type != 'V')]

#4: Discard ALL convertible bonds
fisd = fisd[(fisd.convertible == 'N')]

#5: Discard all asset-backed bonds
fisd = fisd[(fisd.asset_backed == 'N')]

#6: Discard all bonds under Rule 144A
fisd = fisd[(fisd.rule_144a == 'N')]

#7: Remove Agency bonds, Muni Bonds, Government Bonds, 
mask_corp = ((fisd.bond_type != 'TXMU')&  (fisd.bond_type != 'CCOV') &  (fisd.bond_type != 'CPAS')\
            &  (fisd.bond_type != 'MBS') &  (fisd.bond_type != 'FGOV')\
            &  (fisd.bond_type != 'USTC')   &  (fisd.bond_type != 'USBD')\
            &  (fisd.bond_type != 'USNT')  &  (fisd.bond_type != 'USSP')\
            &  (fisd.bond_type != 'USSI') &  (fisd.bond_type != 'FGS')\
            &  (fisd.bond_type != 'USBL') &  (fisd.bond_type != 'ABS')\
            &  (fisd.bond_type != 'O30Y')\
            &  (fisd.bond_type != 'O10Y') &  (fisd.bond_type != 'O3Y')\
            &  (fisd.bond_type != 'O5Y') &  (fisd.bond_type != 'O4W')\
            &  (fisd.bond_type != 'CCUR') &  (fisd.bond_type != 'O13W')\
            &  (fisd.bond_type != 'O52W')\
            &  (fisd.bond_type != 'O26W')\
            # Remove all Agency backed / Agency bonds #
            &  (fisd.bond_type != 'ADEB')\
            &  (fisd.bond_type != 'AMTN')\
            &  (fisd.bond_type != 'ASPZ')\
            &  (fisd.bond_type != 'EMTN')\
            &  (fisd.bond_type != 'ADNT')\
            &  (fisd.bond_type != 'ARNT'))
fisd = fisd[(mask_corp)]

#8: No Private Placement
fisd = fisd[(fisd.private_placement == 'N')]

#9: Remove floating-rate, bi-monthly and unclassified coupons
fisd = fisd[(fisd.interest_frequency != 13) ] # Variable Coupon (V)

#10 Remove bonds lacking information for accrued interest (and hence returns)
fisd['offering_date']            = pd.to_datetime(fisd['offering_date'], 
                                                  format='%Y-%m-%d')
fisd['dated_date']               = pd.to_datetime(fisd['dated_date'], 
                                                  format='%Y-%m-%d')
fisd['maturity']               = pd.to_datetime(fisd['maturity'], 
                                                  format='%Y-%m-%d')

fisd.rename(columns={'complete_cusip':'cusip'}, inplace=True)
  
fisd.isnull().sum()
fisd['interest_frequency'].unique()
fisd['day_count_basis'].value_counts(normalize = True)
#* ************************************** */
#* Filter fisd file                       */
#* ************************************** */ 
fisd = fisd[(fisd.interest_frequency != "-1") ]   # Unclassified by Mergent
fisd = fisd[(fisd.interest_frequency != "13") ]   # Variable Coupon (V)
fisd = fisd[(fisd.interest_frequency != "14") ]   # Bi-Monthly Coupon
fisd = fisd[(fisd.interest_frequency != "16") ]   # Unclassified by Mergent
fisd = fisd[(fisd.interest_frequency != "15") ]   # Unclassified by Mergent
fisd = fisd[(~fisd.interest_frequency.isnull()) ] # Unclassified by Mergent
fisd = fisd[(~fisd.offering_date.isnull()) ] # Unclassified by Mergent
fisd['day_count_basis'] = np.where(fisd['day_count_basis'].isnull(),
                                 "30/360", fisd['day_count_basis'])

# Error in data: 61768T613 dated_date should be 2018-12-05 #
# Picked up by Francis Cong                                #
fisd.loc[lambda x: x['cusip'] == '61768T613', 'dated_date'] = \
    pd.to_datetime('2018-12-05')

traced.rename(columns={'cusip_id':'cusip'}, inplace=True)

#* ************************************** */
#* Merge                                  */
#* ************************************** */ 
traced = traced.merge(
    fisd[['cusip', 'offering_date', 'dated_date', 
           'interest_frequency', 'coupon', 'day_count_basis',
           'coupon_type','maturity','principal_amt']],
    left_on='cusip',
    right_on='cusip',
    how='left'
    )

traced['bond_maturity'] = ((traced.maturity -\
                            traced.trd_exctn_dt)/np.timedelta64(1, 'M')) / 12

traced = traced.dropna()
traced['interest_frequency'] = traced['interest_frequency'].astype(int)
traced['interest_frequency'] = traced['interest_frequency'].astype(str)

#* ************************************** */
#* re-name                              */
#* ************************************** */ 
traced.rename(columns={'prc_vw':'pr',}, inplace=True)
traced.rename(columns={'cusip':'cusip_id',}, inplace=True)

#### Remove -ve prices ####
Desc   = traced['pr'].describe().round(3)
traced = traced[traced['pr'] > 0 ]
traced['pr'].min()

#* ************************************** */
#* Par (Principal Amount) Data Errors     */
#* ************************************** */

# Thanks to Zhiyao (Nicholas) Chen from:
# Lingnan University, Hong Kong for bringing up
# these issues #

# First, note that not all par values are 1,000 USD
# i.e., 100% of par 
traced['principal_amt'].value_counts(normalize = True)*100

# Examine "non-1000" par value bonds #
tracednon = traced[traced['principal_amt'] != 1000]
tracednon['principal_amt'].value_counts(normalize = True)*100

# Most have a par value of 10, 5000 and 2000

# Step 1: I do not have enough manpower to manually
# inspect all of these non-1000 par bonds
# ----> Only keep bonds with par of 1000, 10, 5000, 2000
# i.e. those which are most frequent #
par_mask = ((traced.principal_amt == 1000) |\
            (traced.principal_amt == 10)   |\
            (traced.principal_amt == 5000) |\
            (traced.principal_amt == 2000)  )
traced = traced[par_mask]

# Step 2: $10 Par Bonds Inspection
tracednon10 = traced[traced['principal_amt'] == 10]

# Examine pricing
tracednon10.groupby("interest_frequency")['pr'].mean()
tracednon10.groupby("interest_frequency")['coupon'].mean()
tracednon10.groupby("interest_frequency")['pr'].count()

# Examine all bonds without semi-annual coupons
tracednon10_semi = tracednon10[tracednon10["interest_frequency"] != '2']
tracednon10_semi.groupby("interest_frequency")['pr'].mean()
tracednon10_semi.groupby("interest_frequency")['pr'].count()

# All bond except those with frequency == 2 have prices which correctly match
# there par values (10)
# Step 2.1: Scale all bonds (par==10) whose int_freq != 2 to be in 100 range 
# Note: Coupons (if any) can remain as is / they seem correct
mask_scale = ( (traced.principal_amt      == 10)&\
               (traced.interest_frequency != '2') )
traced.loc[mask_scale, 'prc_ew'] = traced.loc[mask_scale, 'prc_ew'] * 10
traced.loc[mask_scale, 'pr']     = traced.loc[mask_scale, 'pr']     * 10

# Outcome, all of the par == 10 bonds have correctly scaled prices #

# Step 3: Par values of $5000
tracednon5 = traced[traced['principal_amt'] == 5000]

# Examine pricing
tracednon5.groupby("interest_frequency")['pr'].mean()
tracednon5.groupby("interest_frequency")['coupon'].mean()
tracednon5.groupby("interest_frequency")['pr'].count()

# Can leave the prices unchanged

# Step 4: Par values of $2000
tracednon2 = traced[traced['principal_amt'] == 2000]

# Examine pricing
tracednon2.groupby("interest_frequency")['pr'].mean()
tracednon2.groupby("interest_frequency")['pr'].count()

# Can leave the prices unchanged

# Check #
traced['principal_amt'].value_counts(normalize = True)*100
traced.groupby("interest_frequency")['pr'].mean()
traced.groupby(["principal_amt","interest_frequency"])['pr'].mean()

# All prices look reasonable #

#* ************************************** */
#* Functions                              */
#* ************************************** */ 
def Timestamp2Date(ts):
    return ql.Date(ts.day, ts.month, min(2199, ts.year))


def Date2Timestamp(d):
    return pd.Timestamp(d.year(), d.month(), d.dayOfMonth())


def GetNewVarsPy(x):
    # Market price (clean) -- prc_vw for me -- HAVE
    MktCleanPrice = x.pr
    # Issue date
    IssueDate = Timestamp2Date(x.offering_date)# -- HAVE
    # Start date
    StartDate = Timestamp2Date(x.dated_date) if not pd.isna(x.dated_date) \
        else Timestamp2Date(x.offering_date) # -- HAVE
    # Transaction date
    TransactionDate = Timestamp2Date(x.trd_exctn_dt) # -- HAVE
    # Settlement date
    # Clarify! #
    SettlementDate = ql.UnitedStates(ql.UnitedStates.NYSE).advance(
            TransactionDate, 2, ql.Days, ql.ModifiedFollowing
        )
    sttldt = Date2Timestamp(SettlementDate)
    # Maturity date
    MaturityDate = Timestamp2Date(x.maturity)
    # Day count basis
    if x.day_count_basis in ["30/360", ""]:
        DayCountBasis = ql.Thirty360(ql.Thirty360.BondBasis)
    elif x.day_count_basis == "ACT/ACT":
        DayCountBasis = ql.ActualActual(ql.ActualActual.ISDA)
    elif x.day_count_basis == "ACT/360":
        DayCountBasis = ql.Actual360()
    elif x.day_count_basis in ["ACT/365", "ACT/366"]:
        DayCountBasis = ql.Actual365Fixed()
    else:
        raise ValueError("Invalid day_count_basis", x)
    # Interest frequency
    if x.interest_frequency == '1':
        InterestFrequency = ql.Annual
    elif x.interest_frequency == '2':
        InterestFrequency = ql.Semiannual
    elif x.interest_frequency == '4':
        InterestFrequency = ql.Quarterly
    elif x.interest_frequency == '12':
        InterestFrequency = ql.Monthly
    elif x.interest_frequency in ['0', '99']:
        # Recognize a coupon-paying bond with positive coupon even if
        # interest_frequency is not correct
        if x.coupon > 0 and not np.isnan(x.coupon):
            InterestFrequency = ql.Semiannual
        else:
            InterestFrequency = ql.NoFrequency
    else:
        raise ValueError('Invalid interest_frequency', x)
    # Coupon
    Coupon = x.coupon / 100
    # Construct bond
    if x.coupon_type == 'Z' or (x.coupon_type == 'F'
                                and (x.coupon == 0 or np.isnan(x.coupon))
                                and MktCleanPrice < 100):
        bond = ql.ZeroCouponBond(
            2,
            ql.UnitedStates(ql.UnitedStates.NYSE),
            100,
            MaturityDate,
            ql.ModifiedFollowing,
            100,
            IssueDate
            )
    elif x.coupon_type == 'F' and x.coupon > 0 and not np.isnan(x.coupon):
        bond = ql.FixedRateBond(
            2,
            ql.UnitedStates(ql.UnitedStates.NYSE),
            100,
            StartDate,
            MaturityDate,
            ql.Period(InterestFrequency),
            [Coupon],
            DayCountBasis,
            ql.ModifiedFollowing,
            ql.ModifiedFollowing,
            100,
            IssueDate
        )
    else:
        bond = None
    
    # Ifelse for the coupon #
    if x.coupon_type == 'Z' or (x.coupon_type == 'F'
                                and (x.coupon == 0 or np.isnan(x.coupon))
                                and MktCleanPrice < 100):
        InterestFrequency = ql.Annual
    
    # Get result
    if bond is not None and sttldt < x.maturity \
            and np.isfinite(MktCleanPrice):
        try:
            # Yield to maturity (Equivalent semi-annual compounded)
            ytm = bond.bondYield(
                MktCleanPrice,
                DayCountBasis,
                ql.Compounded,
                ql.Semiannual,
                SettlementDate
            )
            
            # Yield to maturity -- True
            ytmt= bond.bondYield(
                MktCleanPrice,
                DayCountBasis,
                ql.Compounded,
                InterestFrequency,
                SettlementDate
            )
            
            # Clean price
            prclean = bond.cleanPrice(
                ytm,
                DayCountBasis,
                ql.Compounded,
                InterestFrequency,
                SettlementDate
            )
            # Dirty price
            prfull = bond.dirtyPrice(
                ytm,
                DayCountBasis,
                ql.Compounded,
                InterestFrequency,
                SettlementDate
            )
            
            # Bond Duration            
            dur_bond = ql.BondFunctions.duration(
                                      bond,
                                      ytm,
                                      DayCountBasis,
                                      ql.Compounded,  
                                      InterestFrequency,
                                      ql.Duration.Modified,
                                      SettlementDate
                                      )
            
            # Bond Convexity            
            conv_bond = ql.BondFunctions.convexity(
                                      bond,
                                      ytm,
                                      DayCountBasis,
                                      ql.Compounded,  
                                      InterestFrequency,                                    
                                      SettlementDate
                                      )
                     
            # Accrued interest from last day
            acclast = bond.accruedAmount(SettlementDate)
            # Accumulated payments before sttldt
            accpmt = sum(cf.amount() for cf in bond.cashflows()
                         if cf.date() <= SettlementDate)
            accall = acclast + accpmt
            
           
                                                                    
        except RuntimeError:
            ytm = np.nan
            ytmt= np.nan
            prfull = np.nan
            prclean = np.nan
            acclast = np.nan
            accpmt = np.nan
            accall = np.nan
            dur_bond = np.nan
            conv_bond = np.nan
    else:
        ytm = np.nan
        ytmt= np.nan
        prclean = np.nan
        prfull = np.nan
        acclast = np.nan
        accpmt = np.nan
        accall = np.nan
        dur_bond = np.nan
        conv_bond = np.nan
        
    return (
        x.cusip_id, x.trd_exctn_dt, sttldt, x.pr, prclean, prfull,
        acclast, accpmt, accall, ytm,ytmt, x.qvolume, x.dvolume, x.offering_date,
        x.coupon, x.maturity, x.day_count_basis, x.interest_frequency,
        dur_bond, conv_bond
        )

#* ************************************** */
#* Run in paralell                        */
#* ************************************** */ 
# Choose n_jobs based on how many cores your machine / cloud compute has #
traced = pd.DataFrame(
    Parallel(n_jobs=14)(delayed(GetNewVarsPy)(x)
                       for x in tqdm(traced.itertuples(index=False))),
    columns=['cusip_id', 'trd_exctn_dt', 'sttldt', 'pr', 'prclean', 'prfull',
             'acclast', 'accpmt', 'accall', 'ytm','ytmt', 'qvolume','dvolume','offering_date',
             'coupon', 'maturity', 'day_count_basis', 'interest_frequency',
             'mod_dur','convexity']
    )

#* ************************************** */
#* Export to file                         */
#* ************************************** */ 
traced.to_csv(r'DirtyPrices.csv.gzip' ,
              compression='gzip')   
# =============================================================================      
