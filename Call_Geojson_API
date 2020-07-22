"""
Created on 2020.07
Author: MCHsu1
"""
import pymssql
from sinbad import *
from datetime import datetime
from pandas.io.json import json_normalize

# Create Connection and Cursor objects
conn = pymssql.connect(server='labdb01', user='sys_DB40', password='Winb0nd1@dB40', database='WEC_EDW_DEV')  
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

DF_Feature = json_normalize(DF['features'])
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
  
    
    # Delete data where eventTime > 7 days (與目前時間比較)
    Query = '''
    DELETE FROM [WEC_EDW_DEV].[MDL].[tblEAS_Event_EQ]
    WHERE DateDiff(Day, eventTime, getdate()) > 7 
    '''
    
    # Insert data into MSSQL
    Query1 = '''
    INSERT [WEC_EDW_DEV].[MDL].[tblEAS_Event_EQ] 
    (id, eventTime, updatedTime, eventType, place, latitude, longitude, magType, mag)
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    '''
    
    values = (eventID, ts_to_time(time), ts_to_updated(updated), eventType, place, lat1, long1, magType, mag)  
    
    # Delete dupicated rows
    Query2 = '''
    WITH cte AS (
    SELECT id, eventTime, eventType, place, latitude, longitude, magType, mag
    FROM (select *, ROW_NUMBER() over (
    partition by  latitude, longitude, eventTime order by updatedTime desc) row_num
    from [WEC_EDW_DEV].[MDL].[tblEAS_Event_EQ] ) a
    where row_num not in ('1')
    and CAST(eventTime AS DATE) 
    between cast(getdate()-10 as date) 
    and cast(getdate() as date)
    ) DELETE FROM cte
    '''
    
    cursor.execute(Query)
    cursor.execute(Query1, values) 
    cursor.execute(Query2)
    
    conn.commit()
    
# Successful notification    
print('Insert successfully!')
print('Delete duplicated rows!')

cursor.close()
conn.close()
