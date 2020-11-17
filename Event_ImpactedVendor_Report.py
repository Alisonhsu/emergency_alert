#!/usr/bin/env python
# coding: utf-8

# In[1]:


# 1109_change data periof from last 60 mins to last hour in order to avoid tha gap between 00:00:00 to 00:15:00 
# 1102_change impacted condition for UAT (temporarily, only for UAT process)
# 1028_Edit python script (Event_ImpactedVendor_Report.py) due to the change of source tables (tbl_eas_usgs_epic, tbl_eas_area and tbl_eas_vendor)
# 1027_Pancho e-mail added to both hourly and daily notification email
#      change column name of table within email (suggested by Pancho) 
# 1022_change data source table 
#      add impacted condition calculation for USGS earthquake 
#      store visulization data into DB instead of csv
#      add email notification function
# 1005_add new data source 1. JP 2. TW
# 0906_change vendor datasource from xlsx to DB table 


# In[1]:


import pandas as pd
import datetime
from geopy.distance import geodesic
import pymssql
import os
import shutil
from datetime import datetime, timedelta


# #### Get earthquake, vendor data

# In[26]:


conn = pymssql.connect(server='labdb01', user='sys_DB40', password='Winb0nd1@dB40', database='WEC_EDW_DEV')  

# USGS DATA
sql = '''
SELECT distinct id as event_id
,event_time
--,convert(varchar,  CAST(eventTime AS DATE), 112) as eventTime
,'USGS' as event_source
,'Earthquake' as event_type
,place as event_place
,latitude as event_lat
,longitude as event_long
--,null as magType
,mag as event_level
FROM (select *,
     ROW_NUMBER() over (partition by  latitude, longitude, event_time order by updated_time desc) rnk
	from
	[ODS].[tbl_eas_usgs_epic]) a
where rnk = '1'
and convert(varchar(13), event_time, 120) = convert(varchar(13),  DATEADD(HH, -1, GETDATE()), 120)
and cast(mag as float) >=3 --mag>4, temporarily change to mag>3 for UAT
order by event_time desc
'''
earth_df = pd.read_sql(sql, conn)

# Vendor DATA
sql2 = '''
SELECT ven_id,
       ven_code,
       ven_name,
       ven_lat,
       ven_long,
       ven_city,
       ven_cntry,
       ven_adr as ven_addr,
       ven_port,
       ven_type,
       owner as ven_owner,
       ven_contact
FROM [WEC_EDW_DEV].[ODS].[tbl_eas_vendor]
where ven_lat <> '' and ven_long <> '' and ven_city <>''
'''
ven_df = pd.read_sql(sql2, conn)


# Japan/TW DATA 
sql3 = '''
select id as event_id,
       event_time,
       case when id like '1%' then 'TW' else 'JP' end as event_source,
       'Earthquake' as event_type,
       area_name as event_place,
       area_lat as event_lat,
       area_long as event_long,
       mag as event_level,
       is_impacted  as impacted_flag,
       ven_name as ven_name
INTO #JP_TW
from (
select *
       ,row_number() over (partition by id, ven_name order by mag, updated_time desc) rnk
from [WEC_EDW_DEV].[ODS].[tbl_eas_area]
) b
where b.rnk = 1
and  convert(varchar(13), event_time, 120) = convert(varchar(13),  DATEADD(HH, -1, GETDATE()), 120)
--event_time >= DATEADD(HH, -1, GETDATE()) 
--event_time >= DATEADD(day, -1, GETDATE()) 



SELECT  JP_TW.*
       ,case when VEN.ven_id is null then 'not impacted' else VEN.ven_id end as ven_id 
       ,VEN.ven_code
       ,VEN.ven_lat
	   ,VEN.ven_long
       ,VEN.ven_city
	   ,VEN.ven_cntry
	   ,VEN.ven_adr as ven_addr
	   ,VEN.ven_port
	   ,Ven.ven_type
	   ,VEN.owner as ven_owner 
	   ,VEN.ven_contact
	   --,
FROM #JP_TW JP_TW
left join  [WEC_EDW_DEV].[ODS].[tbl_eas_vendor] VEN
on JP_TW.ven_name = VEN.ven_name
'''
jp_tw_df = pd.read_sql(sql3, conn)




conn.commit()
conn.close()


# In[27]:


earth_df


# In[28]:


jp_tw_df


# #### Impacted condition - USGS

# In[29]:


#Calculate distance for USGS data

#Get unique latitude, longitude pair
#多筆vendors有同樣的經緯度

dist_earth_df = earth_df[['event_lat','event_long']].drop_duplicates()
dist_ven_df = ven_df[['ven_lat','ven_long']].drop_duplicates()

dis_df = pd.DataFrame(columns=['event_lat','event_long','ven_lat','ven_long','distance'])
for idx, row in dist_earth_df.iterrows():
    newport_ri = (row['event_lat'], row['event_long'])
    
    
    for idx_2, row_2 in dist_ven_df.iterrows():
        cleveland_oh = (row_2['ven_lat'], row_2['ven_long'])
        
        d = geodesic(newport_ri, cleveland_oh).km
        
        data = {'event_lat':newport_ri[0], 'event_long':newport_ri[1], 'ven_lat':cleveland_oh[0], 'ven_long':cleveland_oh[1], 'distance':d}
        
        df = pd.DataFrame(data, index = [0])
        dis_df = pd.concat([dis_df, df])


# In[30]:


tmp_df = pd.merge(earth_df,dis_df, how='left', on=['event_lat','event_long'])


# In[31]:


#impacted condiction 4級:100km，5級:150km，6級:200km，7級:250km
# 20201102_add 1 more impact condition: event_level between 3 and 4 and distance <=50 

impacted_flag = []
for idx, row in tmp_df.iterrows():
    if (float(row['event_level']) >=3 and float(row['event_level']) <4 and float(row['distance']) <= 50) or (float(row['event_level']) >=4 and float(row['event_level']) <5 and float(row['distance']) <= 100) or (float(row['event_level']) >=5 and float(row['event_level']) <6 and float(row['distance']) <= 150) or (float(row['event_level']) >=6 and float(row['event_level']) <7 and float(row['distance']) <= 200) or (float(row['event_level']) >=7 and float(row['distance']) <= 250):
        impacted_flag.append('Y')
    else:
        impacted_flag.append('N')

tmp_df['impacted_flag'] = impacted_flag


# In[32]:


#因vendor經緯度由ven_city產生，產生很多經緯度相同的不同venders，這邊left join後筆數將膨脹 
usgs_df = pd.merge(tmp_df, ven_df, left_on=['ven_lat','ven_long'], right_on=['ven_lat','ven_long'])


# In[33]:


usgs_df


# #### Concat USGS, TW, JP data

# In[34]:


###vix_df column: magType > earthquake postal code, eventType > source
viz_df = pd.concat([usgs_df, jp_tw_df], ignore_index=True)


# In[35]:


# eliminate \n, \r (mostly appears within vendor data)
viz_df = viz_df.replace('\n',' ',regex=True).replace('\r',' ',regex=True)


# In[36]:


#NaN > null
viz_df = viz_df.where(pd.notnull(viz_df), None)


# In[37]:


viz_df


# In[39]:


earth_df.shape


# In[40]:


jp_tw_df.shape


# In[38]:


viz_df.shape


# ### insert data to DB

# In[24]:


conn = pymssql.connect(server='labdb01', user='sys_DB40', password='Winb0nd1@dB40', database='WEC_EDW_DEV')  
cursor = conn.cursor()

#只保留7天內資料
#刪除一小時內資料


sql5 = '''
delete
from
[WEC_EDW_DEV].[APL].[tbl_eas_report]
where event_time  >= DATEADD(HH, -1, GETDATE()) 
or event_time <= DATEADD(day, -7, GETDATE())
'''

cursor.execute(sql5)


sql6 = '''
INSERT INTO [WEC_EDW_DEV].[APL].[tbl_eas_report] (event_id,ven_id,event_time,event_source,event_type,event_place,event_lat,event_long,event_level,impacted_flag,distance,ven_code,ven_name,ven_lat,ven_long,ven_city,ven_cntry,ven_addr,ven_port,ven_type,ven_owner,ven_contact) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''



for index, row in viz_df.iterrows():
    value = (row.event_id,row.ven_id,row.event_time,row.event_source,row.event_type,row.event_place,row.event_lat,row.event_long,row.event_level,row.impacted_flag,row.distance,row.ven_code,row.ven_name,row.ven_lat,row.ven_long,row.ven_city,row.ven_cntry,row.ven_addr,row.ven_port,row.ven_type,row.ven_owner,row.ven_contact)
    cursor.execute(sql6, value)
    
conn.commit()
conn.close()


# ### e-mail notification
# #### when there's impacted vendor

# In[12]:


impacted_df = viz_df[viz_df.impacted_flag == 'Y'][['event_time','event_place','event_level','event_source','ven_name','ven_type','ven_owner','ven_contact']]
impacted_df = impacted_df.rename(columns={'event_time':'地震時間', 'event_place':'地點','event_level':'震度','event_source':'資料來源','ven_name':'供應商','ven_type':'類別','ven_owner':'Owner','ven_contact':'聯絡窗口'})
impacted_df = impacted_df.reset_index(drop=True)


# In[13]:


from email.mime.text import MIMEText
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
import sys


# In[14]:


def Send_Mail(impacted_df) :
    recipients = ['cychen48@winbond.com', 'MCHsu1@winbond.com','HYLIANG@winbond.com','CTWeng3@winbond.com','KCTseng@winbond.com','CYWang17@winbond.com','MYTsai8@winbond.com']
    emaillist = [elem.strip().split(',') for elem in recipients]

    msg = MIMEMultipart()
    msg['Subject'] = "受地震影響的供應商-一小時內"
    msg['From'] = 'noreply@winbond.com'


    html = """    <html>
      <head></head>
      <body>
        {0}
      </body>
    </html>
    """.format(impacted_df.to_html())

    part1 = MIMEText(html, 'html')
    msg.attach(part1)

    try:
        server = smtplib.SMTP('10.6.10.125',25)
        server.sendmail(msg['From'], emaillist , msg.as_string())
    except smtplib.SMTPException as e:
        print ("Error: 无法发送邮件")


# In[15]:


if impacted_df.empty is False:
    Send_Mail(impacted_df)
else:
    print('no impacted vendor')


# In[ ]:




