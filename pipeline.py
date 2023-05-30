import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from Funciones import *
from Type_map import *

url='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
data_base="data-engineer-database"
port='5439'
user="pazmartinexe_coderhouse"
with open("pass.txt",'r') as f:
    pwd=f.read()

conn=conect_db(url=url,data_base=data_base,port=port,user=user,pwd=pwd)





if __name__ == '__main__':
    #EXTRACT
    print('''
    ---------------------                                  ---------------------
    ---------------------    initiating the extraction     ---------------------
    ---------------------                                  ---------------------
    ''')
    dir='./data/yellow_tripdata_2022-07.parquet'
    df=extract_directory(directorio=dir)
    dir2='./data/taxi+_zone_lookup.csv'
    taxi_zone_lookup=pd.read_csv(dir2)

    #transform
    print('''
    ---------------------                                  ---------------------
    ---------------------   initiating transformations     ---------------------
    ---------------------                                  ---------------------
    ''')
    print('''
    --------------------- data types will be changed, columns renamed, new columns created  --------------------- 
    --------------------- pickup_time and dropoff_time and the datetime64[ns] field will be ---------------------
    --------------------- changed to datetime                                               ---------------------
    ''')
    print('--------------------- A sample will be taken between 2022-07-05 and 2022-07-06 ---------------------')
    df=outliers_obt(df,'total_amount','0.25','0.75',valoriqr=4.5)
    df=outliers_obt(df,'improvement_surcharge','0.25','0.75',valoriqr=4.5)
    df=outliers_obt(df,'tip_amount','0.25','0.75',valoriqr=2)
    df=outliers_obt(df,'mta_tax','0.25','0.75',valoriqr=1.5)
    df=outliers_obt(df,'extra','0.25','0.75',valoriqr=1.5)
    df=outliers_obt(df,'fare_amount','0.25','0.75',valoriqr=4.5)
    df=outliers_obt(df,'trip_distance','0.25','0.75',valoriqr=3)
    df=outliers_obt(df,'tpep_dropoff_datetime','0.25','0.75',valoriqr=1.5)

    df=df[(df['tpep_pickup_datetime'] >= '2022-07-04') & (df['tpep_dropoff_datetime'] <= '2022-07-05')]
    df.reset_index(inplace=True)
    df.drop(columns=['index'],inplace=True)
    df.index=df.index + 1
    df=df.reset_index().rename(columns={'index':'ID'})
    df=df.dropna(subset=['passenger_count','RatecodeID'], axis=0)
    df['passenger_count']=df['passenger_count'].astype('int64')
    df['RatecodeID']=df['RatecodeID'].astype('int64')
    df['pickup_time']=df['tpep_pickup_datetime'].astype('str').str.extract('((?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d)')
    df['dropoff_time']=df['tpep_dropoff_datetime'].astype('str').str.extract('((?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d)')
    df['tpep_pickup_datetime']=df['tpep_pickup_datetime'].apply(lambda x: x.strftime('%Y-%m-%d'))
    df['tpep_dropoff_datetime']=df['tpep_dropoff_datetime'].apply(lambda x: x.strftime('%Y-%m-%d'))
    print('''
    ---------------------                                  ---------------------
    --------------------- creating fact and dimension tables ---------------------
    ---------------------                                  ---------------------
    ''')
    print('--------------------- Create vendor table --------------------- ')
    print('---------------------------------------------------------------')
    dicc_vendor={
    '1':'Creative Mobile Technologies',
    '2':'VeriFone Inc'}
    colums_vendor=['VendorID','name_vendor']
    vendor=create_df_dicc(dictionary=dicc_vendor,colums=colums_vendor)
    print('---------------------------------------------------------------')
    print('--------------------- Create ratecode table ---------------------')
    print('---------------------------------------------------------------')
    dicc_retcodeID={
    '1':'Standard rate',
    '2':'JFK',
    '3':'Newark',
    '4':'Nassau or Westchester',
    '5':'Negotiated fare',
    '6':'Group ride'}
    columns_retocde=['RateCodeID','name_ratecode']
    ratecode=create_df_dicc(dictionary=dicc_retcodeID,colums=columns_retocde)
    print('---------------------------------------------------------------')
    print('--------------------- Create store_forward table ---------------------')
    print('---------------------------------------------------------------')
    dicc_store_forward={
    'Y':'store and forward trip',
    'N':'not a store and forward trip'}
    columns_store_forward=['store_and_fwd_flag','name_store_forward']
    store_forward=create_df_dicc(dictionary=dicc_store_forward,colums=columns_store_forward)
    print('---------------------------------------------------------------')
    print('--------------------- Create payment_type table ---------------------')
    print('---------------------------------------------------------------')
    dicc_payment={
    '1': 'Credit card',
    '2': 'Cash',
    '3': 'No charge',
    '4': 'Dispute',
    '5': 'Unknown',
    '6': 'Voided trip'}
    columns_payments=['payment_type','name_payment_type']
    payment_type=create_df_dicc(dictionary=dicc_payment,colums=columns_payments)
    print('---------------------------------------------------------------')
    print('--------------------- Create trip_duration_date table ---------------------')
    print('---------------------------------------------------------------')
    trip_duration_date=df[['tpep_pickup_datetime','tpep_dropoff_datetime','pickup_time','dropoff_time']]
    trip_duration_date.index=trip_duration_date.index+1
    trip_duration_date=trip_duration_date.reset_index().rename(columns={'index':'TripID'})
    df.drop(columns=['tpep_pickup_datetime','tpep_dropoff_datetime','pickup_time','dropoff_time'], axis=1, inplace=True)
    print('---------------------------------------------------------------')
    print('--------------------- Create yellow_taxis_ny table ---------------------')
    print('---------------------------------------------------------------')
    yellow_taxis_ny=df

    #LOAD
    print('''
    ---------------------                                  ---------------------
    ---------------------           Initiating Load        ---------------------
    ---------------------                                  ---------------------
    ''')
    tables=['yellow_taxis_ny','taxi_zone_lookup','vendor','ratecode','store_forward','payment_type','trip_duration_date']
    
    print('--------------------- loading yellow_taxis_ny table in database ---------------------')
    cargar_en_redshift(conn=conn,table_name=tables[0],type_data=type_map_yellow,dataframe=yellow_taxis_ny)
    print('--------------------- finishing loading yellow_taxis_ny table ---------------------')

    print('--------------------- loading taxi_zone_lookup table in database ---------------------')
    cargar_en_redshift(conn=conn,table_name=tables[1],type_data=type_map_zone_lookup,dataframe=taxi_zone_lookup)
    print('--------------------- finishing loading taxi_zone_lookup table ---------------------')

    print('--------------------- loading vendor table in database ---------------------')
    cargar_en_redshift(conn=conn,table_name=tables[2],type_data=type_map_vendor,dataframe=vendor)
    print('--------------------- finishing loading vendor table ---------------------')

    print('--------------------- loading ratecode table in database ---------------------')
    cargar_en_redshift(conn=conn,table_name=tables[3],type_data=type_map_ratecode,dataframe=ratecode)
    print('--------------------- finishing loading ratecode table ---------------------')

    print('--------------------- loading store_forward table in database ---------------------')
    cargar_en_redshift(conn=conn,table_name=tables[4],type_data=type_map_store_forward,dataframe=store_forward)
    print('--------------------- finishing loading store_forward table ---------------------')

    print('--------------------- loading payment_type table in database ---------------------')
    cargar_en_redshift(conn=conn,table_name=tables[5],type_data=type_map_payment_type,dataframe=payment_type)
    print('--------------------- ffinishing loading payment_type table ---------------------')

    print('--------------------- loading trip_duration_date table in database ---------------------')
    cargar_en_redshift(conn=conn,table_name=tables[6],type_data=type_map_trip_duration,dataframe=trip_duration_date)
    print('--------------------- finishing loading trip_duration_date table ---------------------')

    print(f'closing the session of the redshift database {conn.close()}')