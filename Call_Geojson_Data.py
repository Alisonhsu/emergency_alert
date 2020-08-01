import urllib.request as rq
import openpyxl
from datetime import datetime
from pandas.io.json import json_normalize

usgs_url = rq.urlopen('https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.geojson')
jsonRaw = usgs_url.read()
jsonData = json.loads(jsonRaw)

df_features = json_normalize(jsonData['features'])
df_features = df_features[['id','properties.time','properties.updated','properties.place',
                       'properties.type','geometry.coordinates','properties.mag','properties.magType']]

def ts_to_time(v):
    return datetime.fromtimestamp(float(v)/1000).strftime("%Y-%m-%d %H:%M:%S")
def ts_to_updated(x): 
    return datetime.fromtimestamp(float(x)/1000).strftime("%Y-%m-%d %H:%M:%S")

wb = openpyxl.Workbook()
sheet = wd["Sheet"]

for i in range (0, df_features.shape[0]):                      
    long1     = float(df_features.iloc[i]['geometry.coordinates'][0])
    lat1      = float(df_features.iloc[i]['geometry.coordinates'][1])
    eventID   = df_features.iloc[i]['id']
    time      = df_features.iloc[i]['properties.time']
    updated   = df_features.iloc[i]['properties.updated']
    eventType = df_features.iloc[i]['properties.type']   
    place     = df_features.iloc[i]['properties.place']
    mag       = df_features.iloc[i]['properties.mag']
    magType   = df_features.iloc[i]['properties.magType']
    
    #value = (eventID, ts_to_time(time), ts_to_updated(updated), eventType, place, lat1, long1, magType, mag)  
    
    wcell1_col  = sheet.cell(1, 1)
    wcell1_col.value = 'eventID'
    wcell1       = sheet.cell(i+2, 1)
    wcell1.value = eventID
    
    wcell2_col  = sheet.cell(1, 2)
    wcell2_col.value = 'time'
    wcell2       = sheet.cell(i+2, 2)
    wcell2.value = ts_to_time(time) 
    
    wcell3_col  = sheet.cell(1, 3)
    wcell3_col.value = 'lat'
    wcell3       = sheet.cell(i+2, 3)
    wcell3.value = str(lat1)
    
    wcell4_col  = sheet.cell(1, 4)
    wcell4_col.value = 'long'
    wcell4       = sheet.cell(i+2, 4)
    wcell4.value = str(long1)
    
    wd.save(r'/Users/gagaga1106/Desktop/python/eq_data.xlsx')

    #print(type(eventType))
    #output.to_excel('/Users/gagaga1106/Desktop/python/eq_data.xlsx')
  
print('Done!')
