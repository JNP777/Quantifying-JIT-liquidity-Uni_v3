
"""
Library to query Uniswap_v3 data from the Graph
Created on Sun May 23 10:11:41 2021
@author: JNP
"""

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import pandas as pd
from pandas.io.json import json_normalize
from datetime import datetime


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''GraphQl client'''

def query_univ3(url,query_a,params):

    sample_transport=RequestsHTTPTransport(
       url=url,
       verify=True,
       retries=5,)
    client = Client(transport=sample_transport)
    query = gql(query_a)
    response = client.execute(query,variable_values=params)
    
    return response


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''Query all the mints in a loop for a given date-range'''

def extract_mints(begin,end,pool_id):
    
    url='https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-prod'

    query='''query manymints($last_ts: String, $pool_id:String)
        {
            mints(first:1000,orderBy:timestamp,orderDirection:asc 
            where:{timestamp_gte: $last_ts pool: $pool_id }) 
            {id,sender,origin,owner,timestamp,token0, token1, amount0,amount1,amountUSD,tickLower,tickUpper,transaction {blockNumber,gasPrice,id}}
            }
            '''
    #Generating begin and end datetime timestamps
    ts_inicio = int((begin - datetime(1970, 1, 1)).total_seconds())
    ts_fin = int((end - datetime(1970, 1, 1)).total_seconds())

    #Df to store data and initial id value   
    mints=pd.DataFrame()  
    last_mintId=""
    
    #Loop to fetch data from begin datetime to end datetime
    while int(ts_inicio)<ts_fin-10:
        
        #Query parameters and client call
        params ={"last_ts": str(ts_inicio),"pool_id":pool_id}   
        a=query_univ3(url,query,params)
    
        #df with queried data
        mint_data=pd.io.json.json_normalize(a['mints'])
        #timestamp transformation
        mint_data['timestamp_1']=[datetime.utcfromtimestamp((int(x))) for x in mint_data['timestamp']]
        
        mintId=mint_data['id'].iloc[-1]
        #Breaking at the last value if not continue the loop
        if mintId==last_mintId:
            break
        else:
            #Appending data queried
            mints=mints.append(mint_data)
            #Assigning last datetime queried as begin date of the new loop
            ts_inicio=mint_data['timestamp'].iloc[-1]
            #Assigning last id queried
            last_mintId=mint_data['id'].iloc[-1]
    
    return mints 

''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''Query all the burns in a loop for a given date-range'''

def extract_burns(begin,end,pool_id):
    
    url='https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-prod'

    query='''query manyburns($last_ts: String, $pool_id:String)
        {
            burns(first:1000,orderBy:timestamp,orderDirection:asc 
            where:{timestamp_gte: $last_ts pool: $pool_id }) 
            {id,origin,timestamp,token0, token1, amount0,amount1,amountUSD,tickLower,tickUpper,transaction {blockNumber,gasPrice,id}}
            }
            '''
    #Generating begin and end datetime timestamps
    ts_inicio = int((begin - datetime(1970, 1, 1)).total_seconds())
    ts_fin = int((end - datetime(1970, 1, 1)).total_seconds())
    
    #Df to store data and initial id value  
    burns=pd.DataFrame()  
    last_burnId=""

    #Loop to fetch data from begin datetime to end datetime
    while int(ts_inicio)<ts_fin-10:
        
        #Query parameters and client call
        params ={"last_ts": str(ts_inicio),"pool_id":pool_id}   
        a=query_univ3(url,query,params)
    
        #df with burn_data
        burn_data=pd.io.json.json_normalize(a['burns'])
        burn_data['timestamp_1']=[datetime.utcfromtimestamp((int(x))) for x in burn_data['timestamp']]
        
        burnId=burn_data['id'].iloc[-1]
        #Breaking at the last value if not continue the loop
        if burnId==last_burnId:
            break
        else:
            #Appending data queried
            burns=burns.append(burn_data)
            #Assigning last datetime queried as begin date of the new loop
            ts_inicio=burn_data['timestamp'].iloc[-1]
            #Assigning last id queried
            last_burnId=burn_data['id'].iloc[-1]
    
    return burns 


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''Query all the collects in a loop for a given date-range'''

def extract_collects(begin,end,pool_id):
    
    #Different URL in order to get collect events
    url="https://api.thegraph.com/subgraphs/name/laktek/uniswap-v3-mainnet"
 
    query='''query manycollects($last_ts: String, $pool_id:String)
        {
            collects(first:1000,orderBy:timestamp,orderDirection:asc 
            where:{timestamp_gte: $last_ts pool: $pool_id }) 
            {id,timestamp, amount0,amount1,amountUSD,tickLower,tickUpper,
            transaction {id,blockNumber,gasPrice}}
            }
            '''
    #Generating begin and end datetime timestamps
    ts_inicio = int((begin - datetime(1970, 1, 1)).total_seconds())
    ts_fin = int((end - datetime(1970, 1, 1)).total_seconds())

    #Df to store data and initial id value   
    collects=pd.DataFrame()  
    last_collectId=""

    #Loop to fetch data from begin datetime to end datetime
    while int(ts_inicio)<ts_fin-10:
        
        #Query parameters and client call
        params ={"last_ts": str(ts_inicio),"pool_id":pool_id}   
        a=query_univ3(url,query,params)
    
        #df with burn_data
        collect_data=pd.io.json.json_normalize(a['collects'])
        collect_data['timestamp_1']=[datetime.utcfromtimestamp((int(x))) for x in collect_data['timestamp']]
        
        collectId=collect_data['id'].iloc[-1]
        #Breaking at the last value if not continue the loop
        if collectId==last_collectId:
            break
        else:
            #Appending data queried
            collects=collects.append(collect_data)
            #Assigning last datetime queried as begin date of the new loop
            ts_inicio=collect_data['timestamp'].iloc[-1]
            #Assigning last id queried
            last_collectId=collect_data['id'].iloc[-1]
    
    return collects 


''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''Query liquidity for poolDaydata in a loop for a given date range'''

def extract_pooldayData(begin,end,pool_id):
    
    #Different URL in order to get poolDaydata
    url="https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"

    query='''query hourly_data($last_ts: Int, $pool_id:String)
        {
            poolDayDatas(first:1000,orderBy:date,orderDirection:asc 
            where:{date_gte: $last_ts pool: $pool_id }) 
            {id,date,volumeToken0,volumeToken1,volumeUSD,feesUSD}
            }
    
            '''
    #Generating begin and end datetime timestamps
    ts_inicio = int((begin - datetime(1970, 1, 1)).total_seconds())-60*60
    ts_fin = int((end - datetime(1970, 1, 1)).total_seconds())

    #Df to store data and initial id value   
    pooldayData=pd.DataFrame()  
    last_DayId=""
    
    #Loop to fetch data from begin datetime to end datetime
    while int(ts_inicio)<ts_fin-10:
        
        #Query parameters and client call
        params ={"last_ts": int(ts_inicio),"pool_id":pool_id}   
        a=query_univ3(url,query,params)
    
        #df with pool_data
        pooldayData_row=pd.io.json.json_normalize(a['poolDayDatas'])
        pooldayData_row['timestamp_1']=[datetime.utcfromtimestamp((int(x))) for x in pooldayData_row['date']]

        DayId=pooldayData_row['id'].iloc[-1]
        #Breaking at the last value if not continue the loop
        if DayId==last_DayId:
            break
        else:
            #Appending data queried
            pooldayData=pooldayData.append(pooldayData_row)
            #Assigning last datetime queried as begin date of the new loop
            ts_inicio=pooldayData['date'].iloc[-1]
            #Assigning last id queried
            last_DayId=pooldayData['id'].iloc[-1]
    
    #timestamp refers to beginning and data to end so it is shifted to match    
    pooldayData[['volumeToken0','volumeToken1','volumeUSD','feesUSD']]=pooldayData[['volumeToken0','volumeToken1','volumeUSD','feesUSD']].shift(1)
    pooldayData=pooldayData.dropna()
    
    return pooldayData

''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''Slot0 data extraction'''

def get_slot0(pool_id):
    
    url='https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-prod'

    query_slot0='''query pl($pool_id:String) 
    {pools(where: { id: $pool_id } ) 
        {
        token0 {symbol decimals derivedETH}
        token1 {symbol decimals derivedETH}
        createdAtTimestamp
        token0Price
        token1Price
        feeTier
        tick
        liquidity
        sqrtPrice
        }
        }'''
    params ={"pool_id":pool_id}          
    a=query_univ3(url,query_slot0,params)
    slot0_data=pd.io.json.json_normalize(a['pools'])
     
    return slot0_data

''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''Query the top pools by a feature given'''

def query_top_pools(top_n,feature):

    url='https://api.thegraph.com/subgraphs/name/ianlapham/uniswap-v3-prod'
    
    query_top_pools='''query pl($top_n:Int, $feature:String) 
    {pools(first:$top_n,orderBy:$feature,orderDirection:desc  ) 
        {
        id
        createdAtTimestamp
        token0 {symbol decimals derivedETH}
        token1 {symbol decimals derivedETH}
        feeTier
        volumeUSD
        }
        }'''

    params ={"top_n":top_n,"feature":feature}          
    a=query_univ3(url,query_top_pools,params)
    top_pools_data=pd.io.json.json_normalize(a['pools'])
    top_pools_data['pool_name']=top_pools_data['token0.symbol']+"-"+top_pools_data['token1.symbol']+"-"+top_pools_data['feeTier']

    return top_pools_data
