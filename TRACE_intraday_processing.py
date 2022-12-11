# =============================================================================
# Example Code to Process TRACE Data from Intraday to Daily
# =============================================================================

import pandas as pd
import numpy as np
import wrds
from itertools import chain
import datetime as dt
import zipfile
import csv
import gzip

print("BEGIN")

# Requires access to WRDS Cloud #

db = wrds.Connection()
fisd =  pd.read_csv('FISD_2022.gz') # pre-processed corporate bonds

IDs = fisd[['COMPLETE_CUSIP','ISSUE_ID']]

## ========================================== ##
CUSIP_Sample = list( fisd['COMPLETE_CUSIP'].unique() )
## ========================================== ##
def divide_chunks(l, n): 	
	# looping till length l 
	for i in range(0, len(l), n): 
		yield l[i:i + n] 

cusip_chuncks = list(divide_chunks(CUSIP_Sample, 500)) 
## ========================================== ##
PricesExport          = pd.DataFrame()
VolumesExport         = pd.DataFrame()
IlliquidityExport     = pd.DataFrame()



for i in range(0,len(cusip_chuncks)):  
    print(i)
    tempList = cusip_chuncks[i]    
    tempTuple = tuple(tempList)
    parm = {'cusip_id': (tempTuple)}
        
    trace = db.raw_sql('SELECT cusip_id,trd_exctn_dt,trd_exctn_tm,days_to_sttl_ct,lckd_in_ind,wis_fl,sale_cndtn_cd,msg_seq_nb, trc_st, trd_rpt_dt,trd_rpt_tm, entrd_vol_qt, rptd_pr,yld_pt,asof_cd,orig_msg_seq_nb,rpt_side_cd,cntra_mp_id FROM trace.trace_enhanced WHERE cusip_id in %(cusip_id)s', 
                  params=parm)
        
    if len(trace) == 0:
        continue
    else:
        
        # Convert dates to datetime        
        trace['trd_exctn_dt']         = pd.to_datetime(trace['trd_exctn_dt'], format = '%Y-%m-%d')
        trace['trd_rpt_dt']           = pd.to_datetime(trace['trd_rpt_dt'], format = '%Y-%m-%d')         
        
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
                                                                                                  
        trace.columns = map(str.upper, trace.columns)
                
        trace_post_2012 = trace[ trace['TRD_EXCTN_DT'] >=  "2012-02-06"]
        trace_pre_2012  = trace[ trace['TRD_EXCTN_DT'] <  "2012-02-06"]
                                                        
        # Post 2012 Filter #1 -->             
        delete_match = trace_post_2012[ (trace_post_2012['TRC_ST'] == 'X') | (trace_post_2012['TRC_ST'] == 'C')] 
                
        df_delete = trace_post_2012.merge(delete_match, left_on = ['CUSIP_ID',
                                                    'ENTRD_VOL_QT',
                                                    'RPTD_PR',
                                                    'TRD_EXCTN_DT',
                                                    'RPT_SIDE_CD',
                                                    'CNTRA_MP_ID',
                                                    'MSG_SEQ_NB'],
                                          right_on = ['CUSIP_ID',
                                                    'ENTRD_VOL_QT',
                                                    'RPTD_PR',
                                                    'TRD_EXCTN_DT',
                                                    'RPT_SIDE_CD',
                                                    'CNTRA_MP_ID',
                                                    'MSG_SEQ_NB'],
                                                     how = "inner", 
                                                     suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)')
            
        trace_post_2012_clean = trace_post_2012.merge( df_delete , left_on = ['CUSIP_ID',
                                                    'ENTRD_VOL_QT',
                                                    'RPTD_PR',
                                                    'TRD_EXCTN_DT',
                                                    'RPT_SIDE_CD',
                                                    'CNTRA_MP_ID',
                                                    'MSG_SEQ_NB'],
                                          right_on = ['CUSIP_ID',
                                                    'ENTRD_VOL_QT',
                                                    'RPTD_PR',
                                                    'TRD_EXCTN_DT',
                                                    'RPT_SIDE_CD',
                                                    'CNTRA_MP_ID',
                                                    'MSG_SEQ_NB'],
                                                     how = "left", indicator = True, 
                                                     suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)')
        
        trace_post_2012_clean = trace_post_2012_clean[ trace_post_2012_clean['_merge'] == "left_only" ]
        trace_post_2012_clean.drop( ["_merge"] , axis = 1, inplace = True )
                        
        # Post 2012 Filter #2 -->  Reversals            
        delete_match = trace_post_2012_clean[ (trace_post_2012_clean['TRC_ST'] == 'Y') ] 
                
        df_delete = trace_post_2012_clean.merge(delete_match, left_on = ['CUSIP_ID',
                                                    'ENTRD_VOL_QT',
                                                    'RPTD_PR',
                                                    'TRD_EXCTN_DT',
                                                    'RPT_SIDE_CD',
                                                    'CNTRA_MP_ID',
                                                    'MSG_SEQ_NB'],
                                          right_on = ['CUSIP_ID',
                                                    'ENTRD_VOL_QT',
                                                    'RPTD_PR',
                                                    'TRD_EXCTN_DT',
                                                    'RPT_SIDE_CD',
                                                    'CNTRA_MP_ID',
                                                    'MSG_SEQ_NB'],
                                                     how = "inner", 
                                                     suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)')
            
        trace_post_2012_clean = trace_post_2012_clean.merge( df_delete , left_on = ['CUSIP_ID',
                                                    'ENTRD_VOL_QT',
                                                    'RPTD_PR',
                                                    'TRD_EXCTN_DT',
                                                    'RPT_SIDE_CD',
                                                    'CNTRA_MP_ID',
                                                    'MSG_SEQ_NB'],
                                          right_on = ['CUSIP_ID',
                                                    'ENTRD_VOL_QT',
                                                    'RPTD_PR',
                                                    'TRD_EXCTN_DT',
                                                    'RPT_SIDE_CD',
                                                    'CNTRA_MP_ID',
                                                    'MSG_SEQ_NB'],
                                                     how = "left", indicator = True, 
                                                     suffixes=('', '_DROP')).filter(regex='^(?!.*_DROP)')
        
        trace_post_2012_clean = trace_post_2012_clean[ trace_post_2012_clean['_merge'] == "left_only" ]
        trace_post_2012_clean.drop( ["_merge"] , axis = 1, inplace = True )
        # =============================================================================                           
        # Pre 2012 Code # 
        # =============================================================================
        trace_pre_2012_clean = trace_pre_2012[ (trace_pre_2012['TRC_ST'] != 'I') & (trace_pre_2012['TRC_ST'] != 'W')]         
        trace_same_day_cancel = trace_pre_2012_clean[(trace_pre_2012_clean['TRC_ST'] == 'H') | (trace_pre_2012_clean['TRC_ST'] == 'C')]
        trace_same_day_cancel = trace_same_day_cancel[['CUSIP_ID','TRD_EXCTN_DT','ORIG_MSG_SEQ_NB']]
        trace_same_day_cancel['CANCEL_TRD'] = 1
        trace_same_day_cancel = trace_same_day_cancel.rename(columns={'ORIG_MSG_SEQ_NB':'MSG_SEQ_NB'})    
        trace_pre_2012_clean = pd.merge(trace_pre_2012_clean,trace_same_day_cancel,on=['CUSIP_ID','TRD_EXCTN_DT','MSG_SEQ_NB'],how = 'outer')
    
    
        trace_pre_2012_clean = trace_pre_2012_clean[(trace_pre_2012_clean['TRC_ST'] != 'H') & (trace_pre_2012_clean['TRC_ST'] != 'C')]
        trace_pre_2012_clean = trace_pre_2012_clean[pd.notnull(trace_pre_2012_clean['TRC_ST'])]
        trace_pre_2012_clean = trace_pre_2012_clean[(trace_pre_2012_clean['CANCEL_TRD'] != 1)]
        trace_pre_2012_clean = trace_pre_2012_clean[(pd.notnull(trace_pre_2012_clean['TRD_EXCTN_DT']))]
        trace_pre_2012_clean.drop(['TRC_ST', 'CANCEL_TRD', 'ORIG_MSG_SEQ_NB'], axis=1,inplace = True)
            
        trace_pre_2012_clean = trace_pre_2012_clean[(trace_pre_2012_clean['ASOF_CD'] != 'X')]
    
        trace_diff_day_cancel = trace_pre_2012_clean[(trace_pre_2012_clean['ASOF_CD'] == 'R')]
        trace_pre_2012_clean = trace_pre_2012_clean[(trace_pre_2012_clean['ASOF_CD'] != 'R')]
    
        trace_diff_day_cancel = trace_diff_day_cancel[['CUSIP_ID','RPTD_PR','ENTRD_VOL_QT']]
        trace_diff_day_cancel['CANCEL_TRD'] = 1
    
        trace_pre_2012_clean = pd.merge(trace_pre_2012_clean,trace_diff_day_cancel,on=['CUSIP_ID','RPTD_PR','ENTRD_VOL_QT'],how = 'outer')
        trace_pre_2012_clean = trace_pre_2012_clean[(trace_pre_2012_clean['ASOF_CD'] != 'R')]
        trace_pre_2012_clean = trace_pre_2012_clean[(trace_pre_2012_clean['CANCEL_TRD'] != 1)]
        trace_pre_2012_clean = trace_pre_2012_clean[(pd.notnull(trace_pre_2012_clean['TRD_EXCTN_TM']))]
        trace_pre_2012_clean.drop(['ASOF_CD', 'CANCEL_TRD'], axis=1,inplace = True)
                
        # =============================================================================        
        trace_post_2012_clean = trace_post_2012_clean[['CUSIP_ID','TRD_EXCTN_DT','RPTD_PR','ENTRD_VOL_QT','RPT_SIDE_CD','YLD_PT']]
        trace_pre_2012_clean  = trace_pre_2012_clean[[ 'CUSIP_ID','TRD_EXCTN_DT','RPTD_PR','ENTRD_VOL_QT','RPT_SIDE_CD','YLD_PT']]
               
        trace = pd.concat([trace_pre_2012_clean, trace_post_2012_clean], ignore_index=True)
    
        trace = trace.set_index(['CUSIP_ID','TRD_EXCTN_DT']).sort_index(level = 'CUSIP_ID') 
        # =============================================================================
        # Trades with small volumes #
        # Remove trades with volume < $10,000
        # Need to double check this with TRACE / FINRA #                
        # Assume ENTRD_VOL_QT is actual UNITS of the bond - $10,000 would be 10 units ($1,000 PAR)    
        # For now, keep small trades == comment line 212 out.
        # trace = trace[ trace['ENTRD_VOL_QT'] >= 10  ]      
        
        # =============================================================================
        # Code to generate daily corporate bond characteristics                       #
        # =============================================================================
        # Price - Equal-Weight   #
        prc_EW = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].mean().sort_index(level  =  "CUSIP_ID").round(3) 
        prc_EW.columns = ['prc_ew']
        
        # Price - Volume-Weight # 
        trace['dollar_vol']    = ( trace['ENTRD_VOL_QT'] * trace['RPTD_PR'] ).round(0) # units x clean prc                               
        trace['value-weights'] = trace.groupby([ 'CUSIP_ID','TRD_EXCTN_DT' ])['dollar_vol'].apply( lambda x: x/np.nansum(x) )
        prc_VW = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])['RPTD_PR','value-weights'].apply( lambda x: np.nansum( x['RPTD_PR'] * x['value-weights']) ).to_frame().round(4)
        prc_VW.columns = ['prc_vw']
        
        PricesAll = prc_EW.merge(prc_VW, how = "inner", left_index = True, right_index = True)  
                   
        # Volume - Sum and average - units - agnostic on basis         #
        VolumesAll                        = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['ENTRD_VOL_QT']].sum().sort_index(level  =  "CUSIP_ID").sort_index(level  =  "CUSIP_ID")                         
        VolumesAll['dollar_volume']       = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['dollar_vol']].sum().sort_index(level  =  "CUSIP_ID").sort_index(level  =  "CUSIP_ID").round(0)
        VolumesAll.columns                = ['qvolume','dvolume']
                                                                                                                                                                                              
        # Intraday Illiq                              
        trace['deltalogprice']       =  np.log(  trace['RPTD_PR']) - np.log( trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])['RPTD_PR'].shift(1) )
        trace['deltalogprice_lag']   =  trace.groupby("CUSIP_ID")['deltalogprice'].shift(1)
                        
        Gamma = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['deltalogprice','deltalogprice_lag']].\
                                  apply(lambda x: x.cov().iloc[0,1] * - 1).to_frame()
        Gamma.columns = ['intraday_gamma']                          
                       
        # Intraday Amihud Ratio 
        trace['intradayretabs']    = np.abs( trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])['RPTD_PR'].pct_change() )
        Return_Sum                 = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])['intradayretabs'].sum().to_frame().sort_index(level  =  "CUSIP_ID")  
        Return_Sum.columns         = ['absret']        
        Trade_Count                = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])['RPTD_PR'].count().to_frame().sort_index(level  =  "CUSIP_ID")
        Trade_Count.columns        = ['trade_count']
                                            
        Amihud         = Return_Sum.merge(VolumesAll['dvolume'], left_index = True, right_index = True, how = "inner")
        Amihud         = Amihud.merge(Trade_Count, left_index = True, right_index = True, how = "inner").sort_index(level = "CUSIP_ID")                
                
        Amihud['intraday_amihud']  = ( Amihud['absret']*10000 / Amihud['dvolume'] ) * ( 1 / Amihud['trade_count'] ) # return in bp x10,000 #        
        
        IlliquidityAll = Gamma.merge(Amihud, how = "inner", left_index = True, right_index = True)
        
        # Intraday volatility #
        trace['intradayret']       =  trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])['RPTD_PR'].pct_change() 
        Daily_Vol                  = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])['intradayret'].std().to_frame().sort_index(level  =  "CUSIP_ID")  
        
        IlliquidityAll = IlliquidityAll.merge(Daily_Vol, how = "inner", left_index = True, right_index = True)
       
        # High-low spread estimator(P HighLow) [ required data ]                          
        PriceOpen   = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].first().sort_index(level  =  "CUSIP_ID").round(3)     
        PriceOpen.columns = ['prc_open']                    
        PriceHigh   = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].max().sort_index(level  =  "CUSIP_ID").round(3) 
        PriceHigh.columns = ['prc_high']  
        PriceLow    = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].min().sort_index(level  =  "CUSIP_ID").round(3)  
        PriceLow.columns = ['prc_low']  
        PriceClose  = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].last().sort_index(level  =  "CUSIP_ID").round(3) 
        PriceClose.columns = ['prc_close']  
         
        PricesAll   = PricesAll.merge(PriceOpen , how = "inner", left_index = True, right_index = True)
        PricesAll   = PricesAll.merge(PriceHigh , how = "inner", left_index = True, right_index = True)
        PricesAll   = PricesAll.merge(PriceLow  , how = "inner", left_index = True, right_index = True)
        PricesAll   = PricesAll.merge(PriceClose, how = "inner", left_index = True, right_index = True)
                
        # Yields - Equal-weighted yield on the day #
        YLDx   = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['YLD_PT']].mean().sort_index(level  =  "CUSIP_ID").round(4)  
        YLDx.columns = ['intraday_yld']
                                        
        # Difference of average bid and ask prices (AvgBidAsk)
        PRC_Buy   = trace[trace['RPT_SIDE_CD'] == 'S']
        PRC_Sell  = trace[trace['RPT_SIDE_CD'] == 'B']
        PriceBuy  = PRC_Buy.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].mean()  
        PriceBuy.columns = ['Buy']
        PriceSell = PRC_Sell.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].mean()  
        PriceSell.columns = ['Sell']
        PriceBuy_Sell =  PriceBuy.merge(PriceSell, how = "inner", left_index = True, right_index = True) 
        AvgBidAsk = ( ( PriceBuy_Sell['Buy'] - PriceBuy_Sell['Sell'] ) / (0.5*(PriceBuy_Sell['Buy'] + PriceBuy_Sell['Sell'])) ).to_frame()
        AvgBidAsk.columns = ['bid_ask']
        # Interquartile range (TC IQR)
        Price75  = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].quantile(.75)
        Price25  = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].quantile(.25)
        TC_IQRx  = (Price75 - Price25) / trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].mean()
        TC_IQRx.columns = ['tc_iqr']
        
        IlliquidityAll = IlliquidityAll.merge(AvgBidAsk, how = "inner", left_index = True, right_index = True)
        IlliquidityAll = IlliquidityAll.merge(TC_IQRx  , how = "inner", left_index = True, right_index = True)
        
        PricesAll   = PricesAll.merge(YLDx, how = "inner", left_index = True, right_index = True)        
        # =============================================================================   
        # Prices      #
        PricesExport          = pd.concat([PricesExport      , PricesAll]             , axis = 0)      
        # Volumes     #
        VolumesExport         = pd.concat([VolumesExport     , VolumesAll]            , axis = 0)      
        # Illiquidity #
        IlliquidityExport     = pd.concat([IlliquidityExport , IlliquidityAll]        , axis = 0)                        
        # =============================================================================  

# Save in compressed GZIP format # 
# GZIP #
PricesExport.to_csv('Prices_TRACE.csv.gzip'            , compression='gzip')   
VolumesExport.to_csv('Volumes_TRACE.csv.gzip'          , compression='gzip')     
IlliquidityExport.to_csv('Illiquidity_TRACE.csv.gzip'  , compression='gzip') 
