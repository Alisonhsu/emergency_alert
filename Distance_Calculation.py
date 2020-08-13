#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import datetime
from geopy.distance import geodesic
import pymssql

# Create Connection and Cursor objects
# Get earthquake data
conn = pymssql.connect(server='labdb01', user='sys_DB40', password='Winb0nd1@dB40', database='WEC_EDW_DEV')  
cursor = conn.cursor()  

sql = '''
SELECT distinct id
,convert(varchar,  CAST(eventTime AS DATE), 112) as eventTime
,eventType
,place
,latitude
,longitude
,magType
,mag
FROM (select *, ROW_NUMBER() over (
partition by  latitude, longitude, eventTime order by updatedTime desc) rnk
from [MDL].[tblEAS_Event_EQ]) a
where rnk = '1'
and
CAST(eventTime AS DATE) between cast(getdate()-61 as date) and cast(getdate()-1 as date)
and cast(mag as float) >=4
order by eventTime desc
'''

df = pd.read_sql(sql, conn)
earth_df = pd.DataFrame(df)
conn.commit()
cursor.close()
conn.close()

ven_df = pd.read_excel('//LABAP01/Emergency/User_upload/Vendors.xlsx')


#Calculate distance
#Get unique latitude, longitude pair
dist_earth_df = earth_df[['latitude','longitude']].drop_duplicates()
dist_ven_df = ven_df[['venLat','venLong']].drop_duplicates()

dis_df = pd.DataFrame()
for idx, row in dist_earth_df.iterrows():
    newport_ri = (row['latitude'], row['longitude'])
    
    for idx_2, row_2 in dist_ven_df.iterrows():
        cleveland_oh = (row_2['venLat'], row_2['venLong'])
        
        d = geodesic(newport_ri, cleveland_oh).km
        
        data = {'latitude':newport_ri[0], 'longitude':newport_ri[1], 'Latitude_Ven':cleveland_oh[0], 'Longitude_Ven':cleveland_oh[1], 'Distance':d}
        
        df = pd.DataFrame(data, index = [0])
        dis_df = pd.concat([dis_df, df])

#math
'''
import math


def distance(lat1, lon1, lat2, lon2):
    radius = 6371 # km
    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c
    return d
    '''

tmp_df = pd.merge(earth_df,dis_df, how='left', on=['latitude','longitude'])

#若有經緯度相同的不同venders，這邊筆數將膨脹
viz_df = pd.merge(tmp_df, ven_df, left_on=['Latitude_Ven','Longitude_Ven'], right_on=['venLat','venLong'])

today = earth_df.eventTime[0]
today

#if file exist (To do)
viz_df.to_csv('//LABAP01/Emergency/03_Outputdata/' + 'Emergency_Response_VizData' + today + '.csv')
