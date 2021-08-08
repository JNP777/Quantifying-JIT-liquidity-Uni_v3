
"""
Library to calculate Just-in-time liquidity returns in a Uniswap_v3 pool
Created on Sun May 23 10:11:41 2021
@author: JNP
"""

import os
from datetime import datetime
import pandas as pd
import numpy as np

import pooldata as p_data

#Given a begin and end date together with a pool adress provides 
# all JIT operations happened in a Uniswap_v3 pool
def JIT_pool(date_a,date_b,pool_adress):

    #Calculate which is the tick_space of the pool (minimum range available to deploy liquidity)
    tick_space=int(p_data.get_slot0(pool_adress)['feeTier'].iloc[0])/100*2

    #Fetch all mints (increase liquidity events on smart constracts) 
    mints=p_data.extract_mints(date_a,date_b,pool_adress)
    mints['OP']="Mint"
    #Fetch all burns (decrease liquidity events on smart constracts)       
    #Burns does not include fees collected
    burns=p_data.extract_burns(date_a,date_b,pool_adress)
    burns['OP']="Burn"

    #Concat mints and burns and format columns
    data_positions=pd.concat([mints,burns],ignore_index=True)
    data_positions[['amount0','amount1','amountUSD']]=data_positions[['amount0','amount1','amountUSD']].astype(float)
    data_positions[['tickUpper','tickLower']]=data_positions[['tickUpper','tickLower']].astype(int)
    data_positions['tickspaces']=(data_positions['tickUpper']-data_positions['tickLower'])/tick_space

    #Filter transactions with gas price equal to 0 gwei (routed through flashbots)
    #This should change in order to accomodate EIP 1559
    data_jit=data_positions.loc[data_positions['transaction.gasPrice']=='0']

    #Accounting mints as negative capital for the JIT LP
    data_jit['amount0']=np.where(data_jit['OP']=='Mint',-data_jit['amount0'],data_jit['amount0'])
    data_jit['amount1']=np.where(data_jit['OP']=='Mint',-data_jit['amount1'],data_jit['amount1'])
    data_jit['amountUSD']=np.where(data_jit['OP']=='Mint',-data_jit['amountUSD'],data_jit['amountUSD'])

    #Grouping mints and burns by block and origin EOA
    data_jit_group=data_jit.groupby(['transaction.blockNumber','origin']).agg({
                'amount0': ['first','last'],
                'amount1':  ['first','last'], 
                'amountUSD':  ['first','last'], 
                'id': lambda x: list(x), 
                'origin':'last',
                'tickLower': np.max,
                'tickUpper': np.max, 
                'timestamp': np.max,
                'transaction.blockNumber':'last',
                'transaction.gasPrice':'last', 
                'transaction.id':lambda x: list(x),
                'timestamp_1': 'last', 
                'OP':['first', 'last'], 
                'tickspaces': 'last'
    })  
    data_jit_group.columns= list(map(''.join, data_jit_group.columns.values))
    
    # Filtering agroupations with more than 2 txs as there are some blocks in which the bot make some like fake txs
    data_jit_group=data_jit_group.loc[data_jit_group['transaction.id<lambda>'].str.len()==2]
    
    # Splitting mint and burn tx id
    data_jit_group[['tx_mint_id','tx_burn_id']]=data_jit_group['transaction.id<lambda>'].to_list()

    # Fetch all collects events (tokens burn plus fees collected)
    collects=p_data.extract_collects(date_a,date_b,pool_adress)

    # As in this JIT operations burns and collects events happen in the same tx,
    # we use the tx id to match burn and collects events
    to_merge=collects[['transaction.id','amount0','amount1','amountUSD']]
    data_jit_group=data_jit_group.merge(to_merge,how='left',left_on='tx_burn_id',right_on='transaction.id')
    data_jit_group.columns

    #Cleaning and formatting data for better usability
    data_jit_group=data_jit_group.rename(columns={
        'amount0first':'amount0_mint', 
        'amount0last':'amount0_remove', 
        'amount1first':'amount1_mint', 
        'amount1last':'amount1_remove', 
        'amountUSDfirst':'amountUSD_mint', 
        'amountUSDlast':'amountUSD_remove', 
        'originlast':'origin', 
        'tickLoweramax':'tickLower',
        'tickUpperamax':'tickUpper', 
        'timestampamax':'timestamp', 
        'transaction.blockNumberlast':'block',
        'transaction.gasPricelast':'gasPrice', 
        'timestamp_1last':'timestamp_1',
        'amount0':'amount0_collect', 
        'amount1':'amount1_collect', 
        'amountUSD':'amountUSD_collect'})

    jit_final=data_jit_group.copy()

    jit_final=jit_final[['amount0_mint','amount0_remove','amount1_mint','amount1_remove','amountUSD_mint','amountUSD_remove',
        'amount0_collect','amount1_collect', 'amountUSD_collect','origin', 'tickLower','tickUpper',
            'timestamp', 'block','gasPrice',  'timestamp_1',
        'OPfirst', 'OPlast', 'tx_mint_id', 'tx_burn_id',
        ]]
    jit_final=jit_final.sort_values('block')
    jit_final[['amount0_collect','amount1_collect','amountUSD_collect']]=jit_final[['amount0_collect','amount1_collect','amountUSD_collect']].astype(float)

    #Fees collected by the JIT operations is calculated subtrating tokens burned from tokens collected
    #This values represented the value extracted from conventional LPs to JIT LP and miners
    jit_final['fees0']   = jit_final['amount0_collect'] - jit_final['amount0_remove']      
    jit_final['fees1']   = jit_final['amount1_collect'] - jit_final['amount1_remove']    
    jit_final['feesUSD'] = jit_final['amountUSD_collect'] - jit_final['amountUSD_remove']   

    #Based on amountUSD values provided by the uniswap subgraph data (this could result in low accuracy)
    # a PNL of the operation is calculated with USD notionals
    jit_final['liquidity_PnL']= jit_final['amountUSD_remove'] + jit_final['amountUSD_mint'] 
    jit_final['operation_PnL']= jit_final['liquidity_PnL'] + jit_final['feesUSD']

    #Return a df with all JIT operations and return results
    return jit_final
