"""
Created on 2020.07
Updated on 2020.10
Author: MCHsu1
"""
import pymssql
from sinbad import *
from datetime import datetime
import pandas as pd
from pandas.io.json import json_normalize
import configparser

# read config.ini
def conf(a):
    config = configparser.ConfigParser()
    config.read(r'\\LABAP01\Emergency\02_Code\config.ini')
    config.sections()
    server = config['DEFAULT']['server']
    user = config['DEFAULT']['user']
    pwd = config['DEFAULT']['password']
    db = config['DEFAULT']['database']
    
    if a == 'server':
        return server
    elif a == 'user':
        return user
    elif a == 'pwd':
        return pwd
    elif a == 'db':
        return db

# Create Connection and Cursor objects
conn = pymssql.connect(conf('server'), conf('user'), conf('pwd'), conf('db'))
cursor = conn.cursor()  


# Fetch data from USGS
def usgs():
    ds = Data_Source.connect("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.geojson")
    ds.set_cache_timeout(180)    # force cache refresh every 3 minutes
    
    ds.load()
    data = ds.fetch(base_path = "features")

    return data

def ts_to_time(v):
    return datetime.fromtimestamp(float(v)/1000).strftime("%Y-%m-%d %H:%M:%S")
def ts_to_updated(x): 
    return datetime.fromtimestamp(float(x)/1000).strftime("%Y-%m-%d %H:%M:%S")


# Shape your DataFrame
DF=usgs()

DF_Feature = pd.json_normalize(DF['features'])
DF_Feature = DF_Feature[['id','properties.time','properties.updated','properties.place',
                       'properties.type','geometry.coordinates','properties.mag','properties.magType']]
 
    
# Excecute SQL and insert into MSSQL    
for i in range (0, DF_Feature.shape[0]):                      
    long1     = float(DF_Feature.iloc[i]['geometry.coordinates'][0])
    lat1      = float(DF_Feature.iloc[i]['geometry.coordinates'][1])
    eventID   = DF_Feature.iloc[i]['id']
    time      = DF_Feature.iloc[i]['properties.time']
    updated   = DF_Feature.iloc[i]['properties.updated']
    eventType = DF_Feature.iloc[i]['properties.type']   
    place     = DF_Feature.iloc[i]['properties.place']
    mag       = DF_Feature.iloc[i]['properties.mag']
    magType   = DF_Feature.iloc[i]['properties.magType']
    
    
    # Insert data into MSSQL
    sql_usgs = '''
    INSERT [WEC_EDW_DEV].[STG].[tbl_eas_usgs_epic_tmp] 
    (id, event_time, updated_time, event_type, place, latitude, longitude, mag_type, mag)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    '''    
    values = (eventID, ts_to_time(time), ts_to_updated(updated), eventType, place, lat1, long1, magType, mag)  
    
    # Delete dupicated rows, partition by id
    del_dup = '''
    WITH cte AS (
    SELECT *
    FROM (select *, ROW_NUMBER() over (
    partition by id order by updated_time desc) row_num
    from [WEC_EDW_DEV].[STG].[tbl_eas_usgs_epic_tmp] ) a
    where row_num not in ('1')
    and CAST(event_time AS DATE) 
    between cast(getdate()-10 as date) 
    and cast(getdate() as date)
    ) DELETE FROM cte
    '''
    
    if(float(mag) >= 3.0):
        cursor.execute(sql_usgs, values) 
        cursor.execute(del_dup)
        conn.commit()
    
# Successful notification    
print('Insert successfully!')

cursor.close()
conn.close()
