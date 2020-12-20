# =============================================================================
# Example Code to Process TRACE Data from Intraday to Daily
# =============================================================================

import pandas as pd
import numpy as np
from tqdm import tqdm
import wrds
from itertools import chain
import datetime as dt
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from itertools import permutations
import math
tqdm.pandas()

df      = pd.read_hdf(r'C:\Users\Alexander\Dropbox\Corporate Bond Interaction Anomalies\1.DATA\TRACEPanelMerged.h5', 'daily')
df = df[~df['PERMNO'].isnull()]
CUSIP_Sample = list(df['cusip'].unique() )
db = wrds.Connection()
# =============================================================================
# Yield successive n-sized 
def divide_chunks(l, n): 		
	for i in range(0, len(l), n): 
		yield l[i:i + n] 

cusip_chuncks = list(divide_chunks(CUSIP_Sample, 50)) 

PriceEW      = pd.DataFrame()
AmountTraded = pd.DataFrame()
PriceO       = pd.DataFrame()
PriceH       = pd.DataFrame()
PriceL       = pd.DataFrame()
PriceC       = pd.DataFrame()

IntraRoll    = pd.DataFrame()
Lix          = pd.DataFrame()
Vov          = pd.DataFrame()
BidAsk       = pd.DataFrame()
Tc_iqr       = pd.DataFrame()

start = 0
i = 0
# =============================================================================
for i in range(start, len(cusip_chuncks)):
    print(i)           
    tempList = cusip_chuncks[i]   
    tempTuple = tuple(tempList)
    parm = {'cusip_id': (tempTuple)}
    
    trace = db.raw_sql('SELECT cusip_id,bond_sym_id, company_symbol, trd_exctn_dt,trd_exctn_tm,msg_seq_nb, trc_st, entrd_vol_qt, rptd_pr,asof_cd,orig_msg_seq_nb,rpt_side_cd FROM trace.trace_enhanced WHERE cusip_id in %(cusip_id)s', 
                  params=parm)

    trace['trd_exctn_dt'] = pd.to_datetime(trace['trd_exctn_dt'], format = '%Y-%m-%d')
    trace.columns = map(str.upper, trace.columns)
    
    if len(trace) == 0:
        continue
    else:
        # =============================================================================
        trace_len_pre_filter = len(trace)
        #print('pre filter length', trace_len_pre_filter)
    
        #delete I/W
        trace = trace[ (trace['TRC_ST'] != 'I') & (trace['TRC_ST'] != 'W')] 
        #print( 'post I/W delete length', trace_len_pre_filter )
        #create dataframe of H/C trades
        trace_same_day_cancel = trace[(trace['TRC_ST'] == 'H') | (trace['TRC_ST'] == 'C')]
        trace_same_day_cancel = trace_same_day_cancel[['CUSIP_ID','TRD_EXCTN_DT','ORIG_MSG_SEQ_NB']]
        trace_same_day_cancel['CANCEL_TRD'] = 1
        trace_same_day_cancel = trace_same_day_cancel.rename(columns={'ORIG_MSG_SEQ_NB':'MSG_SEQ_NB'})
    
        trace = pd.merge(trace,trace_same_day_cancel,on=['CUSIP_ID','TRD_EXCTN_DT','MSG_SEQ_NB'],how = 'outer')
    
    
        trace = trace[(trace['TRC_ST'] != 'H') & (trace['TRC_ST'] != 'C')]
        trace = trace[pd.notnull(trace['TRC_ST'])]
        trace = trace[(trace['CANCEL_TRD'] != 1)]
        trace = trace[(pd.notnull(trace['TRD_EXCTN_DT']))]
        trace.drop(['TRC_ST', 'CANCEL_TRD', 'ORIG_MSG_SEQ_NB'], axis=1,inplace = True)
    
        #print( 'final length', len(trace) )
    
        #filter part 2 - different days
        #print( len(trace) )
        trace = trace[(trace['ASOF_CD'] != 'X')]
    
        trace_diff_day_cancel = trace[(trace['ASOF_CD'] == 'R')]
        trace = trace[(trace['ASOF_CD'] != 'R')]
    
        trace_diff_day_cancel = trace_diff_day_cancel[['CUSIP_ID','RPTD_PR','ENTRD_VOL_QT']]
        trace_diff_day_cancel['CANCEL_TRD'] = 1
    
        trace = pd.merge(trace,trace_diff_day_cancel,on=['CUSIP_ID','RPTD_PR','ENTRD_VOL_QT'],how = 'outer')
        trace = trace[(trace['ASOF_CD'] != 'R')]
        trace = trace[(trace['CANCEL_TRD'] != 1)]
        trace = trace[(pd.notnull(trace['TRD_EXCTN_TM']))]
        trace.drop(['ASOF_CD', 'CANCEL_TRD'], axis=1,inplace = True)
        #print( len(trace) )
    
        # =============================================================================
        # if-else for the next iteration of the filters #
        # =============================================================================
        # Compute all LIQUIDITY-based anomalies HERE ==================================
        
        # Price - Equal-Weight #
        prc_EW = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].mean()
        # Volume - Sum         #
        vol = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['ENTRD_VOL_QT']].sum()
        
        # 12. Roll’s intraday measure of illiquidity (TC Roll)
        trace['intradayret']    = trace.groupby("CUSIP_ID")['RPTD_PR'].pct_change()
        trace['intradayretlag'] = trace.groupby("CUSIP_ID")['intradayret'].shift(1)
        
        IntraDayRoll   = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['intradayret','intradayretlag']].\
                                  apply(lambda x: x.cov().iloc[0,1] * - 1)                                                       
#        IntraDayRoll = 2 * np.sqrt(IntraDayRoll)
#        IntraDayRoll.isnull().sum()
        # 13. High-low spread estimator(P HighLow) [ required data ]                          
        PriceOpen   = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].first()                           
        PriceHigh   = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].max() 
        PriceLow    = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].min() 
        PriceClose  = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].last() 
        
        # LIX Measure
        LIXx = np.log10( (vol['ENTRD_VOL_QT'] * PriceClose['RPTD_PR']) / (PriceHigh['RPTD_PR'] - PriceLow['RPTD_PR']) )
        
        # Modified VoV
        VoVx = (np.log((PriceHigh['RPTD_PR']/PriceLow['RPTD_PR']))**0.6) / ( vol['ENTRD_VOL_QT']**0.25)
        
        # 22.  Difference of average bid and ask prices (AvgBidAsk)
        PRC_Buy   = trace[trace['RPT_SIDE_CD'] == 'S']
        PRC_Sell  = trace[trace['RPT_SIDE_CD'] == 'B']
        PriceBuy  = PRC_Buy.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].mean()  
        PriceBuy.columns = ['Buy']
        PriceSell = PRC_Sell.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].mean()  
        PriceSell.columns = ['Sell']
        PriceBuy_Sell =  PriceBuy.merge(PriceSell, how = "inner", left_index = True, right_index = True) 
        AvgBidAsk = ( PriceBuy_Sell['Buy'] - PriceBuy_Sell['Sell'] ) / (0.5*(PriceBuy_Sell['Buy'] + PriceBuy_Sell['Sell']))
        # 23.  Interquartile range (TC IQR)
        Price75  = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].quantile(.75)
        Price25  = trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].quantile(.25)
        TC_IQRx   = (Price75 - Price25) / trace.groupby(['CUSIP_ID','TRD_EXCTN_DT'])[['RPTD_PR']].mean()
        
        # =============================================================================                
        # Price Equal Weight
        PriceEW         = pd.concat([PriceEW,prc_EW]        , axis = 0)
        # Volume
        AmountTraded    = pd.concat([AmountTraded,vol]      , axis = 0)
        # Price - Open
        PriceO          = pd.concat([PriceO,PriceOpen]      , axis = 0)
        # Price - High
        PriceH          = pd.concat([PriceH,PriceHigh]      , axis = 0)
        # Price - Low
        PriceL          = pd.concat([PriceL,PriceLow]       , axis = 0)
        # Price - Close
        PriceC          = pd.concat([PriceC,PriceClose]     , axis = 0)
        # Intraday Roll
        IntraRoll       = pd.concat([IntraRoll,IntraDayRoll], axis = 0)
        # LIX
        Lix             = pd.concat([Lix,LIXx]              , axis = 0)
        # Vov
        Vov             = pd.concat([Vov,VoVx]              , axis = 0)
        # BidAsk
        BidAsk          = pd.concat([BidAsk,AvgBidAsk]      , axis = 0)
        # TC_IQR
        Tc_iqr          = pd.concat([Tc_iqr,TC_IQRx]        , axis = 0)                      
        # =============================================================================
        
    
PriceEW.to_hdf(r'C:\Users\Alexander\Dropbox\Corporate Bond Interaction Anomalies\2.TRACE_Daily\EW_Price.h5', key='daily', mode='w')    
AmountTraded.to_hdf(r'C:\Users\Alexander\Dropbox\Corporate Bond Interaction Anomalies\2.TRACE_Daily\Volume_Sum.h5', key='daily', mode='w')    
PriceO.to_hdf(r'C:\Users\Alexander\Dropbox\Corporate Bond Interaction Anomalies\2.TRACE_Daily\PriceOpen.h5', key='daily', mode='w')    
PriceH.to_hdf(r'C:\Users\Alexander\Dropbox\Corporate Bond Interaction Anomalies\2.TRACE_Daily\PriceHigh.h5', key='daily', mode='w')    
PriceL.to_hdf(r'C:\Users\Alexander\Dropbox\Corporate Bond Interaction Anomalies\2.TRACE_Daily\PriceLow.h5', key='daily', mode='w')    
PriceC.to_hdf(r'C:\Users\Alexander\Dropbox\Corporate Bond Interaction Anomalies\2.TRACE_Daily\PriceClose.h5', key='daily', mode='w')    
IntraRoll.to_hdf(r'C:\Users\Alexander\Dropbox\Corporate Bond Interaction Anomalies\2.TRACE_Daily\IntraRoll.h5', key='daily', mode='w')    
Lix.to_hdf(r'C:\Users\Alexander\Dropbox\Corporate Bond Interaction Anomalies\2.TRACE_Daily\LIX.h5', key='daily', mode='w')    
Vov.to_hdf(r'C:\Users\Alexander\Dropbox\Corporate Bond Interaction Anomalies\2.TRACE_Daily\VOV.h5', key='daily', mode='w')    
BidAsk.to_hdf(r'C:\Users\Alexander\Dropbox\Corporate Bond Interaction Anomalies\2.TRACE_Daily\BidAsk.h5', key='daily', mode='w')    
Tc_iqr.to_hdf(r'C:\Users\Alexander\Dropbox\Corporate Bond Interaction Anomalies\2.TRACE_Daily\TC_IQR.h5', key='daily', mode='w')    
# =============================================================================
