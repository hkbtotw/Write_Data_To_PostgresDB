# -*- coding: utf-8 -*-
import pandas as pd
import pyodbc as db
from datetime import date, datetime, timedelta
from postgress_lib import PostgreSQLOperator


""" Connect Database """    
po = PostgreSQLOperator("10.7.40.1", "AREA_MNMT", "tawan.t", "oQmB4v8%")
conn = db.connect('Driver={ODBC Driver 17 for SQL Server};'
                            'Server=SBNDCBIPBST02;'
                            'Database=TSR_ADHOC;'
                            'Trusted_Connection=yes;')
cursor = conn.cursor()

#Original code
def updatedata():
    
    """" Sorce """
    start_date = datetime.now()
    print(start_date, '-- Select execute --')
        
    df = pd.read_sql_query("""SELECT [CUSTM_CODE]
      ,[CUSTM_NAME]
      ,[CUSTM_LAT]
      ,[CUSTM_LNG]
      ,[WORK_SUB_DISTR]
      ,[WORK_DISTR]
      ,[WORK_PROVI]
      ,[WORK_ZIP]
      ,[CUST_CAT]
      ,[CUST_SUB_CAT]
      ,[s_region]
      ,[p_code]
      ,[a_code]
      ,[t_code]
      ,[p_name_t]
      ,[p_name_e]
      ,[a_name_t]
      ,[a_name_e]
      ,[t_name_t]
      ,[t_name_e]
      ,[prov_idn]
      ,[amphoe_idn]
      ,[tambon_idn]
      ,[MIN_PYMT_DATE]
      ,[CUST_CODE_12DIGI]
      ,[IS_VALID_LOC]
      ,[MAX_PYMT_DATE]
  FROM [TSR_ADHOC].[dbo].[DIM_LOC_CVM_CUST_NEW] """, conn)
    
    print(datetime.now(),"--- count row: ",len(df)," ---")
    columns = [column for column in df.columns]
    print("---columns---")
    print(columns)
    dictOfDatas = []
    
    print('--- construct dict ----',datetime.now())
    count = 0 
    for index, row in df.iterrows():
                    
        dictsRow = { columns[i] : row[i] for i in range(0, len(columns)) }
        #print("dictRow: ",index, dictsRow)
        dictOfDatas.append(dictsRow)
        count = count+1
            
    print("---dictOfDatas---")
    print(type(dictOfDatas))
    print('--- Insert - ', count,' ---',datetime.datetime.now())
    po.insertDatabaseList("public.raw_cvm_location_new", dictOfDatas)
        
    po.close()
    cursor.close()

#Change column name in origin to name in destination
def View_Data():
    
    """" Sorce """
    start_date = datetime.now()
    print(start_date, '-- Select execute --')
        
    df = pd.read_sql_query("""SELECT [CUSTM_CODE]
      ,[CUSTM_NAME]
      ,[CUSTM_LAT]
      ,[CUSTM_LNG]
      ,[WORK_SUB_DISTR]
      ,[WORK_DISTR]
      ,[WORK_PROVI]
      ,[WORK_ZIP]
      ,[CUST_CAT]
      ,[CUST_SUB_CAT]
      ,[s_region]
      ,[p_code]
      ,[a_code]
      ,[t_code]
      ,[p_name_t]
      ,[p_name_e]
      ,[a_name_t]
      ,[a_name_e]
      ,[t_name_t]
      ,[t_name_e]
      ,[prov_idn]
      ,[amphoe_idn]
      ,[tambon_idn]
      ,[MIN_PYMT_DATE]
      ,[CUST_CODE_12DIGI]
      ,[IS_VALID_LOC]
      ,[MAX_PYMT_DATE]
  FROM [TSR_ADHOC].[dbo].[DIM_LOC_CVM_CUST_NEW] """, conn)
    
    print(datetime.now(),"--- count row: ",len(df)," ---")
    columns = [column for column in df.columns]
    print("---columns---")
    print(columns)
    print(" ==> ",df.head(10))

    df['collected_at']=datetime.now()

    df=df.rename(columns={"CUSTM_CODE": "custm_code", "CUSTM_NAME": "custm_name", "CUSTM_LAT": "custm_lat", "CUSTM_LNG":"custm_lng"})
    df=df[['custm_code','custm_name','custm_lat','custm_lng','p_name_t','a_name_t','t_name_t','s_region','prov_idn' ,'amphoe_idn','tambon_idn','collected_at' ]].copy().reset_index(drop=True)
    print(" After ==> ",df.head(10))

def BatchInsert(df):
    print(datetime.now(),"--- count row: ",len(df)," ---")
    columns = [column for column in df.columns]
    print("---columns---")
    print(columns)
    dictOfDatas = []
    
    print('--- construct dict ----',datetime.now())
    count = 0 
    for index, row in df.iterrows():
                    
        dictsRow = { columns[i] : row[i] for i in range(0, len(columns)) }
        #print("dictRow: ",index, dictsRow)
        dictOfDatas.append(dictsRow)
        count = count+1
            
    print("---dictOfDatas---")
    print(type(dictOfDatas))
    print('--- Insert - ', count,' ---',datetime.now())
    po.insertDatabaseList("public.raw_cvm_location_new", dictOfDatas)

    return None

def split_dataframe(df, chunk_size): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

#Write edited code to database in sandbox
def updatedata_2():
    
    """" Sorce """
    start_date = datetime.now()
    print(start_date, '-- Select execute --')
        
    df = pd.read_sql_query("""SELECT [CUSTM_CODE]
      ,[CUSTM_NAME]
      ,[CUSTM_LAT]
      ,[CUSTM_LNG]
      ,[WORK_SUB_DISTR]
      ,[WORK_DISTR]
      ,[WORK_PROVI]
      ,[WORK_ZIP]
      ,[CUST_CAT]
      ,[CUST_SUB_CAT]
      ,[s_region]
      ,[p_code]
      ,[a_code]
      ,[t_code]
      ,[p_name_t]
      ,[p_name_e]
      ,[a_name_t]
      ,[a_name_e]
      ,[t_name_t]
      ,[t_name_e]
      ,[prov_idn]
      ,[amphoe_idn]
      ,[tambon_idn]
      ,[MIN_PYMT_DATE]
      ,[CUST_CODE_12DIGI]
      ,[IS_VALID_LOC]
      ,[MAX_PYMT_DATE]
  FROM [TSR_ADHOC].[dbo].[DIM_LOC_CVM_CUST_NEW] """, conn)
    
    print(datetime.now(),"--- count row: ",len(df)," ---")
    columns = [column for column in df.columns]
    print("---columns---")
    print(columns)
    print(" ==> ",df.head(10))

    df['collected_at']=datetime.now()

    df=df.rename(columns={"CUSTM_CODE": "custm_code", "CUSTM_NAME": "custm_name", "CUSTM_LAT": "custm_lat", "CUSTM_LNG":"custm_lng"})
    df=df[['custm_code','custm_name','custm_lat','custm_lng','p_name_t','a_name_t','t_name_t','s_region','prov_idn' ,'amphoe_idn','tambon_idn','collected_at' ]].copy().reset_index(drop=True)
    print(" After ==> ",df.head(10))
    
    #df=df.head(1109)
    #print("New Df ==> ",df)
    dfResult=split_dataframe(df, 20000)

    print(' : ',dfResult,' --- ',type(dfResult))

    count=0
    for dfIn in dfResult:
        count+=1
        BatchInsert(dfIn)
        print(' end write : ', count,' :: ', len(dfIn))

    po.close()
    cursor.close()
    
#updatedata()
View_Data()

updatedata_2()

print('--- complete ----')
print(datetime.now())