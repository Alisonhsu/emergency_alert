"""
Created on 2020.08
Updated on 2020.11
Author: @Alison
"""
import time
import re 
import calendar
import xlrd
import pandas as pd
import requests as rq2
import urllib.request as rq
import xml.etree.ElementTree as et
from bs4 import BeautifulSoup as bs
from datetime import datetime
#from lxml import etree
import pymssql
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

    
earthquake_num = 0      
cnt = 0  
area_sum = 0    

# HTTP Request, BS
req = rq.urlopen('http://www.data.jma.go.jp/developer/xml/feed/eqvol_l.xml')
xml = bs(req, 'lxml')
#xml.prettify

# Create Connection and Cursor objects
conn = pymssql.connect(conf('server'), conf('user'), conf('pwd'), conf('db'))
cursor = conn.cursor()  

# Open the workbook and define the worksheet
xlsx_data = xlrd.open_workbook(r'\\LABAP01\Emergency\User_upload\Vendors.xlsx')
sheet = xlsx_data.sheet_by_name("Vendors")


# Insert data into MSSQL
sql_area = '''
INSERT [WEC_EDW_DEV].[STG].[tbl_eas_jp_area_tmp]
(id, event_time, updated_time, area_name, area_lat, area_long, mag, is_impacted, ven_name)
values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

# Delete dupicated rows, partition by id, postcode
del_dup = '''
WITH cte AS (
SELECT *
FROM (select *, ROW_NUMBER() over (
partition by id, area_name, area_lat, area_long order by event_time desc) row_num
from [WEC_EDW_DEV].[STG].[tbl_eas_jp_area_tmp] ) a
where row_num not in ('1')
) DELETE FROM cte
'''

# Update data 
updated_query = '''
UPDATE [WEC_EDW_DEV].[STG].[tbl_eas_jp_area_tmp]
SET updated_time = %d, is_impacted = %s, ven_name = %s
WHERE updated_time = 
(SELECT MAX(updated_time) FROM [WEC_EDW_DEV].[STG].[tbl_eas_jp_area_tmp])
'''

entry = xml.select('entry')

# 計時開始，主程式
tStart = time.time() 
for earthquake_num in range(len(entry)):  
    # page 1
    title = entry[earthquake_num].title.string
    link = entry[earthquake_num].link['href']
    
    # page 2
    next_link = rq.urlopen(link) # for bs4 elements
    next_xml = bs(next_link, 'lxml')
    next_link_ET = rq2.get(str(link)) # for ET elements
    
    # epic
    epic_id = next_xml.select_one('EventID')
    epic_name = next_xml.select_one('Earthquake Hypocenter Name')
    epic_code = next_xml.select_one('Earthquake Hypocenter Code') 
    datatime = next_xml.select_one('DateTime')
    d = datetime.strptime(datatime.text, "%Y-%m-%dT%H:%M:%SZ") 
    utc_ts = calendar.timegm(d.utctimetuple()) # UTC Time --> timestamp(epoch time)
    local_ts = datetime.fromtimestamp(utc_ts) # timestamp --> local time
    epic_latlon = '{http://xml.kishou.go.jp/jmaxml1/elementBasis1/}Coordinate'
   
    # area
    observe = next_xml.select_one('Observation CodeDefine') 
    observe_MaxInt = next_xml.select_one('Observation MaxInt') 
    pref = next_xml.select('Pref')
 
    # find the page title
    if (title.find('震度') or title.find('震源')) == -1: 
        continue
    else:
        cnt += 1 
        print('\n', cnt, earthquake_num + 1, link)  
        
        root = et.fromstring(next_link_ET.content)
        
        # area info
        try:              
            if (int(observe_MaxInt.text)) >= 3:
                if observe.text.find('都道府県等')== -1:
                    continue
                else: 
                    area_sum += 1
                    print('\n' + 'Area info: ')
                    print('event_id: ' + epic_id.text)
                    print('event_time: ' + str(local_ts)) 
                    
                    for latlon in root.iter(epic_latlon):
                        string = latlon.text
                        reg = re.split(r'/|\'|-|\+', string)
                        epic_lat = reg[1]
                        epic_lon = reg[2]
                        print('lat: ', epic_lat, ' / lon: ', epic_lon + '\n')
                                              
                    for len_pref in range(len(pref)):
                    
                        # 提取數字(list)
                        num_lst = re.findall(r'[0-9]+|[a-z]+', pref[len_pref].text)
                    
                        # 提取文字(list)
                        txt_lst = re.sub("[A-Za-z0-9\!\%\[\]\,\。\＊]", "", pref[len_pref].text)
                        
                        for item, nl in enumerate(num_lst):
                            maxint= nl[-1]
                            postcode = nl[0:7]
                            tail = nl[5:7]
                            
                            if int(maxint)>=3 and len(postcode)==7 and str(tail) != '00':
                                
                                # areaname regular expression, 解除 tab, space, 空行問題
                                regex = re.compile('\s+')
                                final_area = regex.split(str(txt_lst))
                                print(maxint, postcode, final_area[item], epic_lat, epic_lon)

                                # 比對 vendorsheet postcode
                                for ven_cell in range(1, sheet.nrows):
                                    venName = sheet.cell(ven_cell, 2).value
                                    venCntry = sheet.cell(ven_cell, 7).value
                                    venCode = sheet.cell(ven_cell, 11).value  
                                    v1 = str(venCode).replace('-','')

                                    if venCntry == 'Japan':        
                                        if v1.find(postcode) == -1:
                                            no = 'N'
                                            currenttime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                            valuesEN = (epic_id.text, str(local_ts), currenttime, epic_name.text, epic_lat, epic_lon, maxint, no, 'Null')
                                            values = (epic_id.text, str(local_ts), currenttime, epic_name.text + ' - ' +  final_area[item], epic_lat, epic_lon, maxint, no, 'Null')                              
                                            updated_value = (currenttime, no, 'Null')

                                            #print('values: ', values)
                                            cursor.execute(sql_area, valuesEN)
                                            cursor.execute(sql_area, values)  
                                            cursor.execute(updated_query, updated_value) 
                                        else:
                                            yes = 'Y'
                                            currenttime2 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                            valuesEY = (epic_id.text, str(local_ts), currenttime2, epic_name.text, epic_lat, epic_lon, maxint, yes, venName)
                                            values2 = (epic_id.text, str(local_ts), currenttime2, epic_name.text + ' - ' + final_area[item], epic_lat, epic_lon, maxint, yes, venName)
                                            updated_value2 = (currenttime, yes, venName)

                                            # print('values2: ', values2)
                                            cursor.execute(sql_area, valuesEY)
                                            cursor.execute(sql_area, values2) 
                                            cursor.execute(updated_query, updated_value2) 
                                            break

                        cursor.execute(del_dup) 
                        conn.commit()
            else:
                continue  
                            
        except:
            print('No Observated Area!' + '\n')   
print('\n' + '--- End ---' + '\n')

tEnd = time.time() 
print('Total Area info: ', area_sum)
time.sleep(5)
print ("It cost %f sec" % (tEnd - tStart)) 
