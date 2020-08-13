# -*- coding: utf-8 -*-
"""
Created on 2020.07
Author: MCHsu1
"""
from geopy.geocoders import Nominatim
from openpyxl import *
import pandas as pd
import pymssql
import time

# Read data from excel and define a DataFrame
data = pd.read_excel (r'\\LABAP01\Emergency\User_upload\Vendors.xlsx')
df = pd.DataFrame(data, columns = ['venCity', 'venCntry'])

# Load data, sheetname from excel
wd = load_workbook(r'\\LABAP01\Emergency\User_upload\Vendors.xlsx')
sheet = wd["vendors"]

# Define a function for reture Lat, Long
def geo_lat_long(query): #location='新磯子町,橫濱市神奈川縣, 日本'
    geolocator = Nominatim(user_agent='lat_long_transform', timeout=10)
    loc = geolocator.geocode(query)
    lat = loc.latitude ; long = loc.longitude
    return lat, long

for i in range(0, df.shape[0]):
    CITY = df.iloc[i]['venCity']
    CNTRY = df.iloc[i]['venCntry']
    RESULT = CITY + ' ' + CNTRY
    print(i, RESULT)
    
    # Exception handling
    try:
        print(geo_lat_long(RESULT))
        print('')
    except AttributeError:
        print('')
        print('Exception: NoneType object has no attribute latitude')
        
    
    #print(type(geo_lat_long(RESULT)))
    # Loop for the geo_lat_long(RESULT) and tranform into Strings
    Output = ','.join('%s' %id for id in geo_lat_long(RESULT))
   
    # Split a String into 2 strings
    LAT, LONG = Output.split(',', 1)
    #print("Lat:" + str(LAT) + '; ' + "Long:"+ str(LONG))
    print('')
    
    
    # Write the values of Lat, Long and save into Vendors.xlsx 
    wcell1       = sheet.cell(i+2, 8)
    wcell1.value = str(LAT)  # transform the data type into String
    wcell2       = sheet.cell(i+2, 9)
    wcell2.value = str(LONG) # transform the data type into String
    wd.save(r'\\LABAP01\Emergency\User_upload\Vendors.xlsx')
    
