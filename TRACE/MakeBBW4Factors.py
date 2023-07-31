# Load libraries #
import pandas as pd
import numpy as np
from tqdm import tqdm
from dateutil.relativedelta import *
from pandas.tseries.offsets import *    
import datetime as datetime
import os
tqdm.pandas()
pd.options.mode.chained_assignment = None  

def bbw4_factor_construction(    database_type    = 'trace'        ,
                                 sample_type      = 'bbw'          ,
                                 weighting_scheme = 'vw'           ,
                                 return_type      = 'excess_rf'    ,
                                 factor_type      = 'v0'           ,
                                 export           = False          ,
                                 file_dir         = ''                ):
    
    if database_type == 'wrds':     
        df = pd.read_hdf( os.path.join(file_dir , 
                'wrds_2002_2022.h5') )\
             .reset_index()                       
        export_path   = 'all_factors_wrds'
        base_dir  = file_dir
        
    elif database_type == 'trace':
        df = pd.read_hdf( os.path.join(file_dir , 
                'trace_2002_2022.h5') )\
             .reset_index()                       
        export_path   = 'all_factors_trace'
        base_dir  = file_dir
                                 
    # Weighting scheme  
    if weighting_scheme == 'vw':
        W = 'bond_amount_out'
    elif weighting_scheme == 'ew':
        df['const'] = 1
        W = 'const'
            
    # Remove bonds with less than 1-Year to maturity
    df = df[df.tmt > 1.0]
       
    # Return Type
    if return_type == 'excess_rf':
        yy = 'exretn_t+1'
        df = df[~df[yy].isnull()]
        R  = 'bond_ret'
    elif return_type == 'duration_adj':
        yy = 'exretnc_dur_t+1'
        df = df[~df[yy].isnull()]      
        R  = 'exretnc_dur'  
    elif return_type == 'maturity_adj':
        yy = 'exretnc_t+1'
        df = df[~df[yy].isnull()]      
        R  = 'exretnc' 
        
    # Ifelse for v0 or v1 of bond amount out / ratings #
    # ONLY applies to the TRACE-only data              #
    if (factor_type == 'v0') &\
        (database_type == 'trace' ) &\
         (weighting_scheme == 'vw')   :
        W = W+str('_dmr')
    
    if (factor_type == 'v0') &\
        (database_type == 'trace' ) :
        RAT = 'spr_mr_fill_dmr'
    else:
        RAT = 'spr_mr_fill'
                       
    #* ************************************** */
    #* MKTbond factor                          */
    #* ************************************** */ 
    df['value-weights'] = df.groupby(by = 'date' )[W]\
        .progress_apply( lambda x: x/np.nansum(x) )
        
    MKTx = df.groupby('date')[[yy,
                                'value-weights']].\
        apply( lambda x: np.nansum( x[yy] * x['value-weights']) ).to_frame()
        
    MKTx.columns = ['MKTx']
    MKTx.index = MKTx.index + MonthEnd(1)
       
    #* ************************************** */
    #* DRF factor                             */
    #* ************************************** */
    df    = df[~df[RAT].isnull()]
    VaR = 'var5br'
    dfVAR = df[~df[VaR].isnull()]

    dfVAR['ratingQ2'] = dfVAR.groupby(by = 'date')[  RAT ]\
        .progress_apply( lambda x: pd.qcut(x,5,labels=False,duplicates='drop')+1)    
        
    dfVAR['drfQ1']    = dfVAR.groupby(by = 'date')[  VaR   ].\
        progress_apply( lambda x: pd.qcut(x,5,labels=False,duplicates='drop')+1)    

    # Value-weights #
    dfVAR['value-weights'] = dfVAR.groupby([ 'date',
                                            'drfQ1',
                                            'ratingQ2' ])[W].\
        progress_apply( lambda x: x/np.nansum(x) )

    sorts = dfVAR.groupby(['date',
                           'drfQ1',
                           'ratingQ2'])[[yy,
                           'value-weights']].\
                            progress_apply( lambda x:\
                            np.nansum( x[yy] * x['value-weights']) ).to_frame()
                                
    sorts.columns = ['retn']
    sorts  = sorts.pivot_table( index = ['date'],
                               columns = ['drfQ1','ratingQ2'], 
                               values = "retn")

    n = 1
    sub1 = (sorts.loc[:,(5, 1)] - (1*sorts.loc[:,(1, 1)]))/n
    sub2 = (sorts.loc[:,(5, 2)] - (1*sorts.loc[:,(1, 2)]))/n
    sub3 = (sorts.loc[:,(5, 3)] - (1*sorts.loc[:,(1, 3)]))/n
    sub4 = (sorts.loc[:,(5, 4)] - (1*sorts.loc[:,(1, 4)]))/n
    sub5 = (sorts.loc[:,(5, 5)] - (1*sorts.loc[:,(1, 5)]))/n

    DRFx = pd.concat([sub1,sub2,sub3,sub4,sub5], axis = 1)
    DRFx = DRFx.mean(axis = 1).to_frame() 
    DRFx.columns = ['DRFx']
    DRFx.index = DRFx.index + MonthEnd(1)
    
    # CRF_drf #
    n = 1
    sub1 = (sorts.loc[:,(1, 5)] - (1*sorts.loc[:,(1, 1)]))/n
    sub2 = (sorts.loc[:,(2, 5)] - (1*sorts.loc[:,(2, 1)]))/n
    sub3 = (sorts.loc[:,(3, 5)] - (1*sorts.loc[:,(3, 1)]))/n
    sub4 = (sorts.loc[:,(4, 5)] - (1*sorts.loc[:,(4, 1)]))/n
    sub5 = (sorts.loc[:,(5, 5)] - (1*sorts.loc[:,(5, 1)]))/n

    CRF_drf = pd.concat([sub1,sub2,sub3,sub4,sub5], axis = 1)
    CRF_drf = CRF_drf.mean(axis = 1).to_frame() 
    CRF_drf.columns = ['CRF_drf']

    #* ************************************** */
    #* REV factor                             */
    #* ************************************** */
    dfREV = df[~df[R].isnull()]
    dfREV['revQ1']    = dfREV.groupby(by = ['date'])[ R      ]\
        .progress_apply(lambda x: pd.qcut(x,5,labels=False,duplicates='drop')+1)    
    dfREV['ratingQ2'] = dfREV.groupby(by = ['date'])[ RAT ]\
        .progress_apply(lambda x: pd.qcut(x,5,labels=False,duplicates='drop')+1)    

    # Value-weights #
    dfREV['value-weights'] = dfREV.groupby([ 'date','revQ1','ratingQ2' ])\
        [W].progress_apply( lambda x: x/np.nansum(x) )

    sorts = dfREV.groupby(['date','revQ1','ratingQ2'])[[yy,'value-weights']]\
        .progress_apply( lambda x: np.nansum( x[yy] * x['value-weights']) )\
            .to_frame()
    sorts.columns = ['retn']
    sorts  = sorts.pivot_table( index = ['date'],
                               columns = ['revQ1','ratingQ2'], values = "retn")

    n = 1
    sub1 = (sorts.loc[:,(1, 1)] - (1*sorts.loc[:,(5, 1)]))/n
    sub2 = (sorts.loc[:,(1, 2)] - (1*sorts.loc[:,(5, 2)]))/n
    sub3 = (sorts.loc[:,(1, 3)] - (1*sorts.loc[:,(5, 3)]))/n
    sub4 = (sorts.loc[:,(1, 4)] - (1*sorts.loc[:,(5, 4)]))/n
    sub5 = (sorts.loc[:,(1, 5)] - (1*sorts.loc[:,(5, 5)]))/n

    REVx = pd.concat([sub1,sub2,sub3,sub4,sub5], axis = 1)
    REVx = REVx.mean(axis = 1).to_frame() 
    REVx.columns = ['REVx']
    REVx.index = REVx.index + MonthEnd(1)

    # CRF_rev #
    n = 1
    sub1 = (sorts.loc[:,(1, 5)] - (1*sorts.loc[:,(1, 1)]))/n
    sub2 = (sorts.loc[:,(2, 5)] - (1*sorts.loc[:,(2, 1)]))/n
    sub3 = (sorts.loc[:,(3, 5)] - (1*sorts.loc[:,(3, 1)]))/n
    sub4 = (sorts.loc[:,(4, 5)] - (1*sorts.loc[:,(4, 1)]))/n
    sub5 = (sorts.loc[:,(5, 5)] - (1*sorts.loc[:,(5, 1)]))/n

    CRF_rev = pd.concat([sub1,sub2,sub3,sub4,sub5], axis = 1)
    CRF_rev = CRF_rev.mean(axis = 1).to_frame() 
    CRF_rev.columns = ['CRF_rev']

  
    dfLIQ = df[~df.illiq.isnull()]
    dfLIQ = dfLIQ[dfLIQ.n >= 5]
     
    dfLIQ['liqQ1']    = dfLIQ.groupby(by = ['date'])[  'illiq']\
        .progress_apply(lambda x: pd.qcut(x,5,labels=False,duplicates='drop')+1)    
    dfLIQ['ratingQ2'] = dfLIQ.groupby(by = ['date'])[  RAT ]\
        .progress_apply( lambda x: pd.qcut(x,5,labels=False,duplicates='drop')+1)    

    # Value-weights #
    dfLIQ['value-weights'] = dfLIQ.groupby([ 'date',
                                            'liqQ1',
                                            'ratingQ2' ])[W]\
        .progress_apply( lambda x: x/np.nansum(x) )

    sorts = dfLIQ.groupby(['date','liqQ1','ratingQ2'])[[yy,'value-weights']]\
        .progress_apply( lambda x: np.nansum( x[yy] * x['value-weights']) )\
            .to_frame()
            
    sorts.columns = ['retn']
    sorts  = sorts.pivot_table( index = ['date'],
                               columns = ['liqQ1','ratingQ2'],
                               values = "retn")

    n = 1
    sub1 = (sorts.loc[:,(5, 1)] - (1*sorts.loc[:,(1, 1)]))/n
    sub2 = (sorts.loc[:,(5, 2)] - (1*sorts.loc[:,(1, 2)]))/n
    sub3 = (sorts.loc[:,(5, 3)] - (1*sorts.loc[:,(1, 3)]))/n
    sub4 = (sorts.loc[:,(5, 4)] - (1*sorts.loc[:,(1, 4)]))/n
    sub5 = (sorts.loc[:,(5, 5)] - (1*sorts.loc[:,(1, 5)]))/n

    LRFx = pd.concat([sub1,sub2,sub3,sub4,sub5], axis = 1)
    LRFx.index = LRFx.index + MonthEnd(1)
    LRFx = LRFx.mean(axis = 1).to_frame() 
    LRFx.columns = ['LRFx']

    # CRF_lrf #
    n = 1
    sub1 = (sorts.loc[:,(1, 5)] - (1*sorts.loc[:,(1, 1)]))/n
    sub2 = (sorts.loc[:,(2, 5)] - (1*sorts.loc[:,(2, 1)]))/n
    sub3 = (sorts.loc[:,(3, 5)] - (1*sorts.loc[:,(3, 1)]))/n
    sub4 = (sorts.loc[:,(4, 5)] - (1*sorts.loc[:,(4, 1)]))/n
    sub5 = (sorts.loc[:,(5, 5)] - (1*sorts.loc[:,(5, 1)]))/n

    CRF_lrf = pd.concat([sub1,sub2,sub3,sub4,sub5], axis = 1)
    CRF_lrf = CRF_lrf.mean(axis = 1).to_frame() 
    CRF_lrf.columns = ['CRF_lrf']

    
    # Create the CRF Factor #
    CRF_Merge = CRF_rev.merge(CRF_drf, how = "inner",
                              left_index = True, 
                              right_index = True)
    CRF_Merge = CRF_Merge.merge(CRF_lrf, how = "inner",
                                left_index = True,
                                right_index = True)

    CRFx = CRF_Merge.mean(axis = 1).to_frame()
    CRFx.columns = ['CRFx']
    CRFx.index =CRFx.index + MonthEnd(1)
        
    #* ************************************** */
    #* Merge                                  */
    #* ************************************** */
    Replicated_Factors = MKTx.merge(DRFx, how = "left",
                                    left_index = True, right_index = True )
    Replicated_Factors = Replicated_Factors.merge(CRFx,
                                                  how = "left",
                                                  left_index = True, 
                                                  right_index = True )
   
    Replicated_Factors = Replicated_Factors.merge(LRFx,
                                                  how = "left", 
                                                  left_index = True, 
                                                  right_index = True )
    Replicated_Factors = Replicated_Factors.merge(REVx, 
                                                  how = "left",
                                                  left_index = True, 
                                                  right_index = True )
    Replicated_Factors = Replicated_Factors[['MKTx', 'DRFx', 'CRFx', 'LRFx']]\
        .dropna()
    Replicated_Factors.columns = ['MKTB','DRF','CRF','LRF']
        

    if sample_type == 'bbw':
        Replicated_Factors = Replicated_Factors\
            [Replicated_Factors.index<="2021-12-31"]
        Replicated_Factors = Replicated_Factors\
            [Replicated_Factors.index>="2004-08-31"] 
           
    #* ************************************** */
    #* Export                                 */
    #* ************************************** */
   
    if export:
        if return_type == 'excess_rf':
            export_path = str(export_path) + str('.csv')
            Replicated_Factors.to_csv(  os.path.join(base_dir, export_path)  )
                        
        elif return_type == 'duration_adj':
            export_path = str(export_path) + str('_dur.csv')
            Replicated_Factors.to_csv(  os.path.join(base_dir, export_path)    )
               
    return   Replicated_Factors  
