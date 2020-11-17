"""
Created on 2020.09
Updated on 2020.10
Author: @Alison
"""
import json
import requests as rq 
import pandas as pd
import pymssql
import xlrd
import time 
from datetime import datetime
from geopy.geocoders import Nominatim
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

# Geolocator Nomination
geolocator = Nominatim(user_agent="tw_eq", timeout=20)

# 小區域有感地震
url = 'https://opendata.cwb.gov.tw/api/v1/rest/datastore/E-A0016-001?Authorization=CWB-646556A9-1281-4B13-A7AA-D23CC67BE0C7'

# 顯著有感地震
url2 = 'https://opendata.cwb.gov.tw/api/v1/rest/datastore/E-A0015-001?Authorization=CWB-646556A9-1281-4B13-A7AA-D23CC67BE0C7'

def json_structure(b):
    if b == 'url':
        return json.loads(rq.get(url).text)
    elif b == 'url2': 
        return json.loads(rq.get(url2).text)

# epic info
ID = json_structure('url')['records']['earthquake'][0]['earthquakeNo']
eventTime = json_structure('url')['records']['earthquake'][0]['earthquakeInfo']['originTime']
updatedTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
epic_name = json_structure('url')['records']['earthquake'][0]['earthquakeInfo']['epiCenter']['location']
epic_lat = json_structure('url')['records']['earthquake'][0]['earthquakeInfo']['epiCenter']['epiCenterLat']['value']
epic_lon = json_structure('url')['records']['earthquake'][0]['earthquakeInfo']['epiCenter']['epiCenterLon']['value']

ID2 = json_structure('url2')['records']['earthquake'][0]['earthquakeNo']
eventTime2 = json_structure('url2')['records']['earthquake'][0]['earthquakeInfo']['originTime']
updatedTime2 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
epic_name2 = json_structure('url2')['records']['earthquake'][0]['earthquakeInfo']['epiCenter']['location']
epic_lat2 = json_structure('url2')['records']['earthquake'][0]['earthquakeInfo']['epiCenter']['epiCenterLat']['value']
epic_lon2 = json_structure('url2')['records']['earthquake'][0]['earthquakeInfo']['epiCenter']['epiCenterLon']['value']

# area info 
area = json_structure('url')['records']['earthquake'][0]['intensity']['shakingArea']
area2 = json_structure('url2')['records']['earthquake'][0]['intensity']['shakingArea']


# Open the workbook and define the worksheet
xlsx_data = xlrd.open_workbook(r'\\LABAP01\Emergency\User_upload\Vendors.xlsx')
sheet = xlsx_data.sheet_by_name("Vendors")
sheet1 = xlsx_data.sheet_by_name("tw_postcode")

# Create Connection and Cursor objects
conn = pymssql.connect(conf('server'), conf('user'), conf('pwd'), conf('db'))
cursor = conn.cursor()  

# Insert area data into MSSQL
sql_area = '''
INSERT [WEC_EDW_DEV].[STG].[tbl_eas_tw_area_tmp]
(id, event_time, updated_time, area_name, area_lat, area_long, mag, is_impacted, ven_name)
values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

# Delete dupicated rows, partition by id, areaLat, areaLong
del_dup = '''
WITH cte AS (
SELECT *
FROM (select *, ROW_NUMBER() over (
partition by id, area_name, area_lat, area_long order by id desc) row_num
from [WEC_EDW_DEV].[STG].[tbl_eas_tw_area_tmp] ) a
where row_num not in ('1')
) DELETE FROM cte
'''

# Update data 
updated_query = '''
UPDATE [WEC_EDW_DEV].[STG].[tbl_eas_tw_area_tmp]
SET updated_time = %s, is_impacted = %s, ven_name = %s
WHERE updated_time = 
(SELECT MAX(updated_time) FROM [WEC_EDW_DEV].[STG].[tbl_eas_tw_area_tmp])
'''

# Delete duplicates lat, long
del_latlon = '''
WITH cte AS (
SELECT *
FROM (select *, ROW_NUMBER() over (
partition by area_lat, area_long order by area_lat, is_impacted desc) row_num
from [WEC_EDW_DEV].[STG].[tbl_eas_tw_area_tmp] ) a
where row_num not in ('1')
) DELETE FROM cte
'''


# 小區域有感地震
tStart = time.time()
cnt = 0
for i in area:
    area_name = i['areaName']
    mag = i['areaIntensity']['value']
    intensity = i['eqStation']
    
    if int(mag) >= 3:
        cnt += 1
        print(cnt, 'areaName: ' + area_name)
        
        for jj in intensity:
            station_mag = jj['stationIntensity']['value']
            station_lat = jj['stationLat']['value']
            station_lon = jj['stationLon']['value']
            
            if int(station_mag) >= 3:
                print('id: ', ID)
                print('eventTime: ' + eventTime)
                print('mag: ', station_mag)
                print('epi_lat', epic_lat)
                print('epi_lon', epic_lon)
                print('area_lat: ', station_lat)
                print('area_lon: ', station_lon, '\n')
                
                # read postcode from Vendors.xls, judge if postcode is mapping
                for r in range(1, sheet.nrows):  
                    venName  = sheet.cell(r, 3).value
                    venCntry = sheet.cell(r,7).value
                    venCode  = sheet.cell(r, 11).value
                    
                    if venCntry == 'Taiwan':
                    
                        # tw_postcode
                        for r1 in range(1, sheet1.nrows):
                            city = sheet1.cell(r1, 0).value
                            area = sheet1.cell(r1, 2).value
                            postCode = sheet1.cell(r1, 3).value
                            
                            if area_name == city:                           
                                
                                    if venCode == postCode:
                                        Y = 'Y'
                                        valuesEY = (ID, eventTime, updatedTime, epic_name, epic_lat, epic_lon, station_mag, Y, venName)
                                        valuesY = (ID, eventTime, updatedTime, epic_name + ' - ' + area_name, station_lat, station_lon, station_mag, Y, venName)
                                        updated_value = (updatedTime, Y, venName)
                                        
                                        cursor.execute(sql_area, valuesEY) 
                                        cursor.execute(sql_area, valuesY)
                                        cursor.execute(updated_query, area_name, updated_value)
                                        break
                                
                                    else:                                       
                                        N = 'N'
                                        valuesEN = (ID, eventTime, updatedTime, epic_name, epic_lat, epic_lon, station_mag, N, 'Null')
                                        valuesN = (ID, eventTime, updatedTime, epic_name + ' - ' + area_name, station_lat, station_lon, station_mag, N, 'Null')
                                        updated_valueN = (updatedTime, N, 'Null')
                                        
                                        cursor.execute(sql_area, valuesEN)
                                        cursor.execute(sql_area, valuesN)
                                        cursor.execute(updated_query, updated_valueN)
                         
                        cursor.execute(del_dup)
                        cursor.execute(del_latlon)
                        conn.commit()   

# 顯著有感地震
cnt2 = 0                
for i2 in area2:
    area_name2 = i2['areaName']
    mag2 = i2['areaIntensity']['value']
    intensity2 = i2['eqStation']
    
    if int(mag2) >= 3:
        cnt2 += 1
        print(cnt2, 'areaName2: ' + area_name2)
        
        for jj2 in intensity2:
            station_mag2 = jj2['stationIntensity']['value']
            station_lat2 = jj2['stationLat']['value']
            station_lon2 = jj2['stationLon']['value']
            
            if int(station_mag2) >= 3:
                print('id: ', ID2)
                print('eventTime: ' + eventTime2)
                print('mag: ', station_mag2)
                print('epi_lat', epic_lat2)
                print('epi_lon', epic_lon2)
                print('lat: ', station_lat2)
                print('lon: ', station_lon2, '\n')             
                            
                # read postcode from Vendors.xls, judge if postcode is mapping
                for e in range(1, sheet.nrows):  
                    venName2  = sheet.cell(e, 2).value
                    venCntry2 = sheet.cell(e, 7).value
                    venCode2  = sheet.cell(e, 11).value
                    
                    if venCntry2 == 'Taiwan':
                    
                        # tw_postcode
                        for e1 in range(1, sheet1.nrows):
                            city2 = sheet1.cell(e1, 0).value
                            area2 = sheet1.cell(e1, 2).value
                            postCode2 = sheet1.cell(e1, 3).value
                            
                            if area_name2 == city2:   
                                
                                    if venCode2 == postCode2:
                                        Y2 = 'Y'
                                        valuesEY2 = (ID2, eventTime2, updatedTime2, epic_name2, epic_lat2, epic_lon2, station_mag2, Y2, venName2)
                                        valuesY2 = (ID2, eventTime2, updatedTime2, epic_name2 + ' - ' + area_name2, station_lat2, station_lon2, station_mag2, Y2, venName2)
                                        updated_value2 = (updatedTime2, Y2, venName2)
                                        
                                        cursor.execute(sql_area, valuesEY2)
                                        cursor.execute(sql_area, valuesY2)
                                        cursor.execute(updated_query, updated_value2)
                                        break                                   
                                    else:
                                        N2 = 'N'
                                        valuesEN2 = (ID2, eventTime2, updatedTime2, epic_name2, epic_lat2, epic_lon2, station_mag2, N2, 'Null')
                                        valuesN2 = (ID2, eventTime2, updatedTime2, epic_name2 + ' - ' + area_name2, station_lat2, station_lon2, station_mag2, N2, 'Null')                                     
                                        updated_valueN2 = (updatedTime2, N2, 'Null')
                                        
                                        cursor.execute(sql_area, valuesEN2)
                                        cursor.execute(sql_area, valuesN2)
                                        cursor.execute(updated_query, updated_valueN2)  

                        cursor.execute(del_dup)
                        cursor.execute(del_latlon)
                        conn.commit()                                           

tEnd = time.time()                     
time.sleep(5)               
print('\n' + 'Cost %f sec' % (tEnd - tStart))
