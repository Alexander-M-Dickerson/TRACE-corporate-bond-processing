##########################################
# BBW (2019) Enhanced TRACE Data Proces  #
# Part (i): Intra day to Daily           #
# Alexander Dickerson                    #
# Email: a.dickerson@warwick.ac.uk       #
# Date: January 2023                     #
# Updated:  January 2023                 #
# Version:  1.0.0                        #
##########################################

##########################################
# I ackowledge                           #
# Qingyi (Freda) Song Drechsler          #
# for writing similar SAS code available #
# on the WRDS Bond Returns Module        #
# This code translates large portions of #
# this SAS code to Python                #
##########################################

## ==================================================== ##
##                         Contributors                 ##
## I would like to thank (in no particular order)
## (1) Lihai Yang (CityUHK) for spotting an error in the
##  volume-weighting scheme for daily prices.
## (2) Francis Cong (McGill) for providing me with his
##     replicated BBW 4-factors.
## (3) Mathias Buchner (Trafigura) for help with 
##     vectorizing chunks of Python code.
## ==================================================== ##

#* ************************************** */
#* Packages                               */
#* ************************************** */  
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np
import wrds
from itertools import chain
import datetime as dt
import zipfile
import csv
import gzip

#* ************************************** */
#* Connect to WRDS                        */
#* ************************************** */  
db = wrds.Connection()

#* ************************************** */
#* Download Mergent File                  */
#* ************************************** */  
fisd_issuer = db.raw_sql("""SELECT issuer_id,country_domicile                 
                  FROM fisd.fisd_mergedissuer 
                  """)

fisd_issue = db.raw_sql("""SELECT complete_cusip, issue_id,
                  issuer_id, foreign_currency,
                  coupon_type,coupon,convertible,
                  asset_backed,rule_144a,
                  bond_type,private_placement,
                  interest_frequency,dated_date,
                  day_count_basis,offering_date                 
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
fisd = fisd[(fisd.interest_frequency != -1) ] # Unclassified by Mergent
fisd = fisd[(fisd.interest_frequency != 13) ] # Variable Coupon (V)
fisd = fisd[(fisd.interest_frequency != 14) ] # Bi-Monthly Coupon
fisd = fisd[(fisd.interest_frequency != 16) ] # Unclassified by Mergent
fisd = fisd[(fisd.interest_frequency != 15) ] # Unclassified by Mergent

#10 Remove bonds lacking information for accrued interest (and hence returns)
fisd['offering_date']            = pd.to_datetime(fisd['offering_date'], format='%Y-%m-%d')
fisd['dated_date']               = pd.to_datetime(fisd['dated_date'],    format='%Y-%m-%d')

# 10.1 Dated date
fisd = fisd[~fisd.dated_date.isnull()]
# 10.2 Interest frequency
fisd = fisd[~fisd.interest_frequency.isnull()]
# 10.3 Day count basis
fisd = fisd[~fisd.day_count_basis.isnull()]
# 10.4 Offering date
fisd = fisd[~fisd.offering_date.isnull()]
# 10.5 Coupon type
fisd = fisd[~fisd.coupon_type.isnull()]
# 10.6 Coupon value
fisd = fisd[~fisd.coupon.isnull()]


#* ************************************** */
#* Parse out bonds for processing         */
#* ************************************** */  
           
IDs = fisd[['complete_cusip','issue_id']]
#* ************************************** */
#* Break into chunks for WRDS             */
#* ************************************** */  

CUSIP_Sample = list( fisd['complete_cusip'].unique() )
def divide_chunks(l, n): 	
	# looping till length l 
	for i in range(0, len(l), n): 
		yield l[i:i + n] 

cusip_chunks  = list(divide_chunks(CUSIP_Sample, 500)) 
#* ************************************** */
#* Pre-allocate for Price/Volume          */
#* ************************************** */ 
PricesExport          = pd.DataFrame()
VolumesExport         = pd.DataFrame()             

#* ************************************** */
#* Pre-allocate for Cleaning Statistics   */
#* ************************************** */ 
CleaningExport   = pd.DataFrame( index   = range(0,len(cusip_chunks)),
                               columns = ['Obs.Pre',
                                          'Obs.PostBBW',
                                          'Obs.PostDickNielsen'])
#* ************************************** */
#* Iterate over the chunks                */
#* ************************************** */ 

for i in range(0,len(cusip_chunks)):  
    print(i)
    tempList = cusip_chunks[i]    
    tempTuple = tuple(tempList)
    parm = {'cusip_id': (tempTuple)}
    
    #* ************************************** */
    #* Load data from WRDS per chunk          */
    #* ************************************** */ 
        
    trace = db.raw_sql('SELECT cusip_id,bond_sym_id,trd_exctn_dt,trd_exctn_tm,days_to_sttl_ct,lckd_in_ind,wis_fl,sale_cndtn_cd,msg_seq_nb, trc_st, trd_rpt_dt,trd_rpt_tm, entrd_vol_qt, rptd_pr,yld_pt,asof_cd,orig_msg_seq_nb,rpt_side_cd,cntra_mp_id FROM trace.trace_enhanced WHERE cusip_id in %(cusip_id)s', 
                  params=parm)
           
    CleaningExport['Obs.Pre'].iloc[i] = int(len(trace))
        
    if len(trace) == 0:
        CleaningExport['Obs.PostBBW'].iloc[i] = int(len(trace))
        CleaningExport['Obs.PostDickNielsen'].iloc[i] = int(len(trace))
        continue
    else:
        
        # Convert dates to datetime        
        trace['trd_exctn_dt']         = pd.to_datetime(trace['trd_exctn_dt'], format = '%Y-%m-%d')
        trace['trd_rpt_dt']           = pd.to_datetime(trace['trd_rpt_dt'],   format = '%Y-%m-%d')         
               
        #* ************************************ */
        #* 0.0 Filters specific to BBW          */
        #* ************************************ */
        # Convert Settlement indicator to string     
        trace['days_to_sttl_ct'] = trace['days_to_sttl_ct'].astype('str')                   
        
        # Convert when-issued indicator to string    
        trace['wis_fl'] = trace['wis_fl'].astype('str')     
        
        # Convert locked-in indicator to string    
        trace['lckd_in_ind'] = trace['lckd_in_ind'].astype('str') 
        
        # Convert sale condition indicator to string    
        trace['sale_cndtn_cd'] = trace['sale_cndtn_cd'].astype('str') 
                                                  
        # Apply initial Bai, Bali and Wen filters here #
        
        # Remove trades with > 2-days to settlement #
        # Keep all with days_to_sttl_ct equal to None, 000, 001 or 002
        trace = trace[   (trace['days_to_sttl_ct'] == '002') | (trace['days_to_sttl_ct'] == '000')\
                       | (trace['days_to_sttl_ct'] == '001') | (trace['days_to_sttl_ct'] == 'None') ]
        
        # Remove when-issued indicator #
        trace = trace[  (trace['wis_fl'] != 'Y')        ]  

        # Remove locked-in indicator #
        trace = trace[  (trace['lckd_in_ind'] != 'Y')   ]
        
        # Remove trades with special conditions #
        trace = trace[  (trace['sale_cndtn_cd'] == 'None') | (trace['sale_cndtn_cd'] == '@')   ]
        
        # Remove trades with volume < $10,000
        trace = trace[ (trace['entrd_vol_qt']) >= 10000  ]      
        
        # Remove bonds with prices < $5 and > $1,000
        trace = trace[ ((trace['rptd_pr'] > 5) & (trace['rptd_pr'] < 1000)) ]     
        
        CleaningExport['Obs.PostBBW'].iloc[i] = int(len(trace))                                                                                          
    
        #* ************************************ */
        #* 1.0 Parsing out Post 2012/02/06 Data */
        #* ************************************ */              
        post= trace[(trace['cusip_id'] != '') & (trace['trd_rpt_dt'] >="2012-02-06")]
        pre = trace[(trace['cusip_id'] != '') & (trace['trd_rpt_dt'] < "2012-02-06")]        
                                                             
        #* ************************************** */
        #* 1.1 Remove Cancellation and Correction */
        #* ************************************** */        
        
        # * Match Cancellation and Correction using following 7 keys:
        # * Cusip_id, Execution Date and Time, Quantity, Price, Buy/Sell Indicator, Contra Party
        # * C and X records show the same MSG_SEQ_NB as the original record;
                
        post_tr = post[(post['trc_st'] == 'T') | (post['trc_st'] == 'R')]       
        post_xc = post[(post['trc_st'] == 'X') | (post['trc_st'] == 'C')]       
        post_y  = post[(post['trc_st'] == 'Y')]      
        
        _clean_post1 = pd.merge(post_tr.drop_duplicates(), post_xc[[
                          'cusip_id',        # 1
                          'trd_exctn_dt',    # 2
                          'trd_exctn_tm',    # 3
                          'rptd_pr',         # 4
                          'entrd_vol_qt',    # 5
                          'rpt_side_cd',     # 6
                          'cntra_mp_id',     # 7
                          'msg_seq_nb',      # 8
                          'trc_st']],    
                           left_on=[
                              'cusip_id',        # 1
                              'trd_exctn_dt',    # 2
                              'trd_exctn_tm',    # 3
                              'rptd_pr',         # 4
                              'entrd_vol_qt',    # 5
                              'rpt_side_cd',     # 6
                              'cntra_mp_id',     # 7
                              'msg_seq_nb'],     # 8
                                          right_on=[ 
                                         'cusip_id',          # 1
                                           'trd_exctn_dt',    # 2
                                           'trd_exctn_tm',    # 3
                                           'rptd_pr',         # 4
                                           'entrd_vol_qt',    # 5
                                           'rpt_side_cd',     # 6
                                           'cntra_mp_id',     # 7
                                           'msg_seq_nb'],     # 8                       
                        how = "left")
                             
        # Remove the matched "Trade Report" observations;
        clean_post1 = _clean_post1[_clean_post1['trc_st_y'].isnull()]
                        
        # Clean-up clean_post1#
        clean_post1.drop(['trc_st_y'], axis = 1, inplace = True)
        clean_post1.rename(columns={'trc_st_x':'trc_st'}, inplace=True) 
                
        #* ******************** */
        #* 1.2 Remove Reversals */
        #* ******************** */

        # * Match Reversal using the same 7 keys:
        # * Cusip_id, Execution Date and Time, Quantity, Price, Buy/Sell Indicator, Contra Party
        # * R records show ORIG_MSG_SEQ_NB matching orignal record MSG_SEQ_NB;
        _clean_post2 = pd.merge(_clean_post1.drop_duplicates(), post_y[[
                          'cusip_id',        # 1
                          'trd_exctn_dt',    # 2
                          'trd_exctn_tm',    # 3
                          'rptd_pr',         # 4
                          'entrd_vol_qt',    # 5
                          'rpt_side_cd',     # 6
                          'cntra_mp_id',     # 7
                          'orig_msg_seq_nb', # 8
                          'trc_st']],    
                           left_on=[
                              'cusip_id',        # 1
                              'trd_exctn_dt',    # 2
                              'trd_exctn_tm',    # 3
                              'rptd_pr',         # 4
                              'entrd_vol_qt',    # 5
                              'rpt_side_cd',     # 6
                              'cntra_mp_id',     # 7
                              'msg_seq_nb'],     # 8
                                          right_on=[ 
                                         'cusip_id',          # 1
                                           'trd_exctn_dt',    # 2
                                           'trd_exctn_tm',    # 3
                                           'rptd_pr',         # 4
                                           'entrd_vol_qt',    # 5
                                           'rpt_side_cd',     # 6
                                           'cntra_mp_id',     # 7
                                           'orig_msg_seq_nb'],# 8                       
                        how = "left")
                             
        # Remove the matched "Trade Report" observations;
        clean_post2 = _clean_post2[_clean_post2['trc_st_y'].isnull()].drop_duplicates()
                        
        # Clean-up clean_post1#
        clean_post2.drop(['orig_msg_seq_nb_y','trc_st_y','trc_st'], axis = 1, inplace = True)
        clean_post2.rename(columns={'orig_msg_seq_nb_x':'orig_msg_seq_nb',
                                    'trc_st_x':'trc_st'}, inplace=True) 
              
        #* ********************************* */
        #* Pre 2012-02-06 Data               */
        #* ********************************* */
                    
        #* ********************************* */
        #* 2.1 Remove Cancellation Cases (C) */
        #* ********************************* */
        pre_c = pre[pre['trc_st'] == 'C']       
        pre_w = pre[pre['trc_st'] == 'W']       
        pre_t = pre[pre['trc_st'] == 'T']        
        
        # Match Cancellation by the 7 keys:
        # Cusip_ID, Execution Date and Time, Quantity, Price, Buy/Sell Indicator, Contra Party
        # C records show ORIG_MSG_SEQ_NB matching orignal record MSG_SEQ_NB;
        merged = pd.merge(pre_t.drop_duplicates(), pre_c[[
                          'cusip_id',
                          'trd_exctn_dt',
                          'trd_exctn_tm',
                          'rptd_pr',
                          'entrd_vol_qt',
                          'trd_rpt_dt',
                          'orig_msg_seq_nb',
                          'trc_st']],
                           left_on=[        'cusip_id', 
                                            'trd_exctn_dt', 
                                            'trd_exctn_tm', 
                                            'rptd_pr', 
                                            'entrd_vol_qt', 
                                            'trd_rpt_dt', 
                                            'msg_seq_nb'], # msg
                                          right_on=['cusip_id', 
                                            'trd_exctn_dt', 
                                            'trd_exctn_tm', 
                                            'rptd_pr', 
                                            'entrd_vol_qt', 
                                            'trd_rpt_dt', 
                                            'orig_msg_seq_nb']  ,  # orig_msg                      
                        how = "left")
        
        
        merged = merged.drop_duplicates()
        
        # Filter out C cases
        _del_c     = merged[merged['trc_st_y'] == 'C']
        clean_pre1 = merged[merged['trc_st_y'] != 'C']
        
        # Clean-up clean_pre1#
        clean_pre1.drop(['orig_msg_seq_nb_y', 'trc_st_y'], axis = 1, inplace = True)
        clean_pre1.rename(columns={'trc_st_x':'trc_st',
                                   'orig_msg_seq_nb_x':'orig_msg_seq_nb'}, inplace=True) 
        
        #* ******************************* */
        #* 2.2 Remove Correction Cases (W) */
        #* ******************************* */
        
        # * NOTE: on a given day, a bond can have more than one round of correction
        # * One W to correct an older W, which then corrects the original T
        # * Before joining back to the T data, first need to clean out the W to
        # * handle the situation described above;
        # * The following section handles the chain of W cases;
        
        # 2.2.1 Sort out all msg_seq_nb;
        w_msg = pre_w[['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb']]
        w_msg['flag'] = 'msg'
        
        # 2.2.1 Sort out all mapped original msg_seq_nb;
        w_omsg = pre_w[['cusip_id', 'bond_sym_id', 'trd_exctn_dt', 'trd_exctn_tm', 'orig_msg_seq_nb']]
        w_omsg = w_omsg.rename(columns={'orig_msg_seq_nb': 'msg_seq_nb'})
        w_omsg['flag'] = 'omsg'
        
        w = pd.concat([w_omsg, w_msg])
        
        # 2.2.2 Count the number of appearance (napp) of a msg_seq_nb: 
        w_napp = w.groupby(['cusip_id', 
                            'bond_sym_id', 
                            'trd_exctn_dt', 
                            'trd_exctn_tm', 
                            'msg_seq_nb']).size().reset_index(name='napp')
        
        # * 2.2.3 Check whether one msg_seq_nb is associated with both msg and orig_msg or only to orig_msg;
        # * If msg_seq_nb appearing more than once is associated with only orig_msg - 
        # * It means that more than one msg_seq_nb is linked to the same orig_msg_seq_nb for correction. 
        # * Examples: cusip_id='362320AX1' and trd_Exctn_dt='04FEB2005'd (3 cases like this in total)
        # * If ntype=2 then a msg_seq_nb is associated with being both msg_seq_nb and orig_msg_seq_nb;
        
        w_mult = w.drop_duplicates(subset = [ 'cusip_id', 
                                              'bond_sym_id', 
                                              'trd_exctn_dt', 
                                              'trd_exctn_tm', 
                                              'msg_seq_nb', 
                                              'flag'])
       
    
        w_mult1 = w_mult.groupby(['cusip_id', 
                                  'bond_sym_id', 
                                  'trd_exctn_dt', 
                                  'trd_exctn_tm', 
                                  'msg_seq_nb',
                                  ]).size().reset_index(name='ntype')
        
        # 2.2.4 Combine the npair and ntype info;       
        w_comb = pd.merge(w_napp, w_mult1, on=['cusip_id', 
                                               'bond_sym_id', 
                                               'trd_exctn_dt', 
                                               'trd_exctn_tm', 
                                               'msg_seq_nb'], 
                    how='left').sort_values(by= ['cusip_id',  
                                                 'trd_exctn_dt', 
                                                 'trd_exctn_tm'])
                                       
        # Map back by matching CUSIP Excution Date and Time to remove msg_seq_nb that appears more than once;
        # If napp=1 or (napp>1 but ntype=1);
        __w_keep = pd.merge(w_comb[(w_comb['napp'] == 1) | ((w_comb['napp'] > 1) & (w_comb['ntype'] == 1))], 
                          w, 
                          on=['cusip_id',                               
                              'trd_exctn_dt', 
                              'trd_exctn_tm', 
                              'msg_seq_nb',
                              ], 
          how = "inner",
          suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)').sort_values(by=
                                   ['cusip_id',                               
                                   'trd_exctn_dt', 
                                   'trd_exctn_tm'])
  
        # =====================================================================
       
        # 2.2.5 Caluclate no of pair of records;
        # Assuming the original table is named "__w_keep"
       
        __w_keep['npair'] = __w_keep.drop_duplicates().groupby(by=[
                                                 'cusip_id', 
                                                 'trd_exctn_dt',
                                                 'trd_exctn_tm'])['cusip_id'].transform("count")/2
        __w_keep =  __w_keep.sort_values(by=
                                 ['cusip_id',                               
                                 'trd_exctn_dt', 
                                 'trd_exctn_tm'])
        
        # For records with only one pair of entry at a given time stamp 
        # - transpose using the flag information;
        __w_keep1 = __w_keep[__w_keep['npair']==1].pivot(index=['cusip_id', 
                                                                'trd_exctn_dt', 
                                                                'trd_exctn_tm',
                                                                ],
                                                         columns='flag', 
                                                         values='msg_seq_nb')
       
        
        
        __w_keep1.reset_index(inplace=True)
        __w_keep1.rename(columns={'msg': 'msg_seq_nb', 'omsg': 'orig_msg_seq_nb'}, inplace=True)
        
        # For records with more than one pair of entry at a given time stamp 
        # - join back the original msg_seq_nb;
        __w_keep2 = pd.merge(__w_keep[(__w_keep['flag'] == 'msg') & (__w_keep['npair'] > 1)], pre_w, 
                    left_on = ['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb'], 
                    right_on = ['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb'], 
                    how = 'left',
                    suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)').sort_values(by=
                                             ['cusip_id',                               
                                             'trd_exctn_dt', 
                                             'trd_exctn_tm'])
              
        __w_keep2 = __w_keep2[['cusip_id',                             
                               'trd_exctn_dt',
                               'trd_exctn_tm',
                               'msg_seq_nb',
                               'orig_msg_seq_nb']].drop_duplicates()
        
        
        __w_clean = pd.concat([__w_keep1, __w_keep2], axis=0)
        
        # * 2.2.6 Join back to get all the other information;
        w_clean = pd.merge(__w_clean, pre_w.drop(columns = ['orig_msg_seq_nb']), 
                   left_on  = ['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb'], 
                   right_on = ['cusip_id', 'trd_exctn_dt', 'trd_exctn_tm', 'msg_seq_nb'], 
                   how = 'left').drop_duplicates(subset = ['orig_msg_seq_nb',
                                                           'cusip_id',
                                                           'trd_exctn_dt',
                                                           'trd_exctn_tm',
                                                           'msg_seq_nb'])
        
        
        
        # /* 2.2.7 Match up with Trade Record data to delete the matched T record */;
        # * Matching by Cusip_ID, Date, and MSG_SEQ_NB;
        # * W records show ORIG_MSG_SEQ_NB matching orignal record MSG_SEQ_NB;
        clean_pre2 = pd.merge(clean_pre1.drop_duplicates(), w_clean[[
                                                   'cusip_id', 
                                                   'trd_exctn_dt', 
                                                   'msg_seq_nb',
                                                   'orig_msg_seq_nb',
                                                   'trc_st' ]], 
                      left_on  = ['cusip_id', 'trd_exctn_dt', 'msg_seq_nb'], 
                      right_on = ['cusip_id', 'trd_exctn_dt', 'orig_msg_seq_nb'], 
                      how = 'left')
               
        # Clean-up clean_pre2 #
        clean_pre2.rename(columns={ 'trc_st_x':'trc_st',
                                    'trc_st_y':'trc_st_w',
                                    'msg_seq_nb_x':'msg_seq_nb',
                                    'msg_seq_nb_y':'mod_msg_seq_nb',
                                    'orig_msg_seq_nb_x':'orig_msg_seq_nb',
                                    'orig_msg_seq_nb_y':'mod_orig_msg_seq_nb'}, inplace=True) 
        
        _del_w =  clean_pre2[clean_pre2.trc_st_w == "W"]                                                                              

       # * Delete matched T records;
        _clean_pre2 =  clean_pre2[clean_pre2['trc_st_w'].isnull()]
                       
        _clean_pre2 = _clean_pre2.drop(columns = ['trc_st_w', 
                                                  'mod_msg_seq_nb', 
                                                  'mod_orig_msg_seq_nb'])
        
        # * Replace T records with corresponding W records;
        # * Filter out W records with valid matching T from the previous step;

        rep_w = pd.merge(w_clean.drop_duplicates(), _del_w[['cusip_id',
                                                            'trd_exctn_dt',
                                                            'trc_st_w',
                                                            'mod_msg_seq_nb',
                                                            'mod_orig_msg_seq_nb']], 
                 left_on  = ['cusip_id', 'trd_exctn_dt', 'msg_seq_nb'], 
                 right_on = ['cusip_id', 'trd_exctn_dt', 'mod_msg_seq_nb'], 
                how = 'left')
        
        
        rep_w = rep_w[rep_w['trc_st_w'] == 'W']
        
        rep_w = rep_w.drop_duplicates(subset = ['cusip_id',
                                                'trd_exctn_dt',
                                                'msg_seq_nb',
                                                'orig_msg_seq_nb',
                                                'rptd_pr',
                                                'entrd_vol_qt'])
        rep_w = rep_w.drop(columns = [            'trc_st_w', 
                                                  'mod_msg_seq_nb', 
                                                  'mod_orig_msg_seq_nb'])
              
        clean_pre3 = pd.concat([_clean_pre2, rep_w], axis = 0)
        
       #* ***************** */
       #* 2.3 Reversal Case */
       #* ***************** */
       # Filter data by asof_cd = 'R' and keep only certain columns
       
        _rev_header = clean_pre3[ clean_pre3['asof_cd'] == 'R'][[  'cusip_id', 
                                                                   'bond_sym_id',
                                                                   'trd_exctn_dt',
                                                                   'trd_exctn_tm',
                                                                   'trd_rpt_dt',
                                                                   'trd_rpt_tm',
                                                                   'entrd_vol_qt', 
                                                                   'rptd_pr',
                                                                   'rpt_side_cd',
                                                                   'cntra_mp_id']]       
        #* Option B: Match by only 6 keys: CUSIP_ID, 
        # Execution Date, Vol, Price, B/S and C/D (remove the time dimension);
        _rev_header = _rev_header.sort_values(by=['cusip_id',
                                                 'bond_sym_id', 
                                                 'trd_exctn_dt', 
                                                 'entrd_vol_qt', 
                                                 'rptd_pr', 
                                                 'rpt_side_cd',
                                                 'cntra_mp_id', 
                                                 'trd_exctn_tm', 
                                                 'trd_rpt_dt', 
                                                 'trd_rpt_tm'])
              
        _rev_header6 = _rev_header.copy()
        _rev_header6['seq'] = _rev_header6.groupby(['cusip_id', 
                                                    'bond_sym_id',
                                                    'trd_exctn_dt',
                                                    'entrd_vol_qt',
                                                    'rptd_pr',
                                                    'rpt_side_cd', 
                                                    'cntra_mp_id']).cumcount() + 1
        
        # * Create the same ordering among the non-reversal records;
        # * Remove records that are R (reversal) D (Delayed dissemination) and 
        # X (delayed reversal);
        _clean_pre4 = clean_pre3[~clean_pre3['asof_cd'].isin(['R', 'X', 'D'])]
        
        _clean_pre4_header = _clean_pre4[['cusip_id',
                                          'bond_sym_id',
                                          'trd_exctn_dt',
                                          'trd_exctn_tm',
                                          'entrd_vol_qt',
                                          'rptd_pr', 
                                          'rpt_side_cd',
                                          'cntra_mp_id',
                                          'trd_rpt_dt', 
                                          'trd_rpt_tm', 
                                          'msg_seq_nb']]
        
        # Match by 6 keys (excluding execution time);
        _clean_pre4_header = _clean_pre4_header.sort_values(by=['cusip_id', 
                                                                'bond_sym_id',
                                                                'trd_exctn_dt',
                                                                'entrd_vol_qt',
                                                                'rptd_pr', 
                                                                'rpt_side_cd', 
                                                                'cntra_mp_id',
                                                                'trd_exctn_tm', 
                                                                'trd_rpt_dt', 
                                                                'trd_rpt_tm', 
                                                                'msg_seq_nb'])
        
        _clean_pre4_header['seq6'] = _clean_pre4_header.groupby(['cusip_id', 
                                                                'bond_sym_id', 
                                                                'trd_exctn_dt', 
                                                                'entrd_vol_qt',
                                                                'rptd_pr', 
                                                                'rpt_side_cd', 
                                                                'cntra_mp_id']).cumcount() + 1
        
        _clean_pre5_header = pd.merge(_clean_pre4_header.drop_duplicates(), _rev_header6, left_on=['cusip_id',
                                                                            'trd_exctn_dt', 
                                                                            'entrd_vol_qt',
                                                                            'rptd_pr', 
                                                                            'rpt_side_cd', 
                                                                            'cntra_mp_id',
                                                                            'seq6'], 
                                                                        right_on=['cusip_id',
                                                                            'trd_exctn_dt', 
                                                                            'entrd_vol_qt',
                                                                            'rptd_pr', 
                                                                            'rpt_side_cd', 
                                                                            'cntra_mp_id',
                                                                           'seq'],                                    
                                                        how = "left", 
                                                        suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)')
                               
        _clean_pre5_header = _clean_pre5_header.rename(columns={'seq': 'rev_seq6'}).drop_duplicates()
        _rev_matched6      = _clean_pre5_header[_clean_pre5_header['rev_seq6'].notna()]


        # As 6 key matching has a higher record of finding reversal match, 
        # use the 6 keys results now;
        _clean_pre5_header = _clean_pre5_header[_clean_pre5_header['rev_seq6'].isna()]
        _clean_pre5_header = _clean_pre5_header.drop(columns=['rev_seq6',
                                                              'seq6']    )
        
        
        _clean_pre5 = _clean_pre4.merge(_clean_pre5_header, on=['cusip_id',
                                                                'trd_exctn_dt',
                                                                'trd_exctn_tm',
                                                                'entrd_vol_qt', 
                                                                'rptd_pr', 
                                                                'rpt_side_cd', 
                                                                'cntra_mp_id', 
                                                                'msg_seq_nb', 
                                                                'trd_rpt_dt',
                                                                'trd_rpt_tm'], how='inner',                                      
                                        suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)')
        
        _clean_pre5 = _clean_pre5.drop_duplicates()
        
        # =====================================================================
        # * Combine the pre and post data together */;
        clean_post2 = clean_post2[[                    'cusip_id',
                                                       'trd_exctn_dt',                                                    
                                                       'rptd_pr',
                                                       'entrd_vol_qt',
                                                       'rpt_side_cd',
                                                       ]]
        _clean_pre5 = _clean_pre5[clean_post2.columns]
              
        trace_post = pd.concat([_clean_pre5, clean_post2], ignore_index=True)
    
        trace = trace_post.set_index(['cusip_id','trd_exctn_dt']).sort_index(level = 'cusip_id') 
        
        CleaningExport['Obs.PostDickNielsen'].iloc[i] = int(len(trace))
        
        #* ***************** */
        #* Prices / Volume   */
        #* ***************** */
        # Price - Equal-Weight   #
        prc_EW = trace.groupby(['cusip_id','trd_exctn_dt'])[['rptd_pr']].mean().sort_index(level  =  'cusip_id').round(4) 
        prc_EW.columns = ['prc_ew']
        
        # Price - Volume-Weight # 
        trace['dollar_vol']    = ( trace['entrd_vol_qt'] * trace['rptd_pr']/100 ).round(0) # units x clean prc                               
        trace['value-weights'] = trace.groupby([ 'cusip_id','trd_exctn_dt'],
                                                group_keys=False)[['entrd_vol_qt']].apply( lambda x: x/np.nansum(x) )
        prc_VW = trace.groupby(['cusip_id','trd_exctn_dt'])[['rptd_pr','value-weights']].apply( lambda x: np.nansum( x['rptd_pr'] * x['value-weights']) ).to_frame().round(4)
        prc_VW.columns = ['prc_vw']
        
        PricesAll = prc_EW.merge(prc_VW, how = "inner", left_index = True, right_index = True)  
        PricesAll.columns                = ['prc_ew','prc_vw']              
        # Volume #
        VolumesAll                        = trace.groupby(['cusip_id','trd_exctn_dt'])[['entrd_vol_qt']].sum().sort_index(level  =  "cusip_id")                       
        VolumesAll['dollar_volume']       = trace.groupby(['cusip_id','trd_exctn_dt'])[['dollar_vol']].sum().sort_index(level  =  "cusip_id").round(0)
        VolumesAll.columns                = ['qvolume','dvolume']                                                                                                                                                                                                                     
        # =============================================================================   
        # Prices      #
        PricesExport          = pd.concat([PricesExport      , PricesAll]             , axis = 0)      
        # Volumes     #
        VolumesExport         = pd.concat([VolumesExport     , VolumesAll]            , axis = 0)                                     
        # =============================================================================  

# Save in compressed GZIP format # 
PricesExport.to_csv('Prices_BBW_TRACE_Enhanced_Dick_Nielsen.csv.gzip'     , compression='gzip')   
VolumesExport.to_csv('Volumes_BBW_TRACE_Enhanced_Dick_Nielsen.csv.gzip'   , compression='gzip')     
CleaningExport.to_csv('Cleaning_TRACE_Enhanced_Dick_Nielsen.csv')     