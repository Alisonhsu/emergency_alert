"""
Created on 2020.07
Author: MCHsu1
"""

import pandas as pd
import pandas.io.sql
import pymssql
import xlrd

# Create Connection and Cursor objects
conn = pymssql.connect(server='labdb01', user='sys_DB40', password='Winb0nd1@dB40', database='WEC_EDW_DEV')  
cursor = conn.cursor()  

# Read data from excel
data = pd.read_excel (r'\\LABAP01\Emergency\User_upload\Vendors.xlsx') 
#df = pd.DataFrame(data, columns= ['venCity', 'venCntry'])

# Open the workbook and define the worksheet
xlsx_data = xlrd.open_workbook(r'\\LABAP01\Emergency\User_upload\Vendors.xlsx')
sheet = xlsx_data.sheet_by_name("vendors")

# Insert vendor's data into EDW
Query1 = '''
INSERT INTO WEC_EDW_DEV.MDL.tblEAS_Vendor_EQ (
venID, venType, venName, venAddr, venCity, venCntry, owner, venLat, venLong, venAprt_Prt, venContact
) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '''

# Delete dupicated rows
Query2 = '''
WITH cte AS (
SELECT *
FROM (select *, ROW_NUMBER() over (
partition by  venID order by venID asc) row_num
from [WEC_EDW_DEV].[MDL].[tblEAS_Vendor_EQ] ) a
where row_num not in ('1')
) DELETE FROM cte
'''

# grab existing row count in the database for validation later
cursor.execute("SELECT count(*) FROM WEC_EDW_DEV.MDL.tblEAS_Vendor_EQ")
before_import = cursor.fetchone()


for r in range(1, sheet.nrows):
    venID       = sheet.cell(r,0).value
    venType     = sheet.cell(r,1).value
    venName     = sheet.cell(r,2).value
    venAddr     = sheet.cell(r,3).value
    venCity     = sheet.cell(r,4).value
    venCntry    = sheet.cell(r,5).value
    owner       = sheet.cell(r,6).value
    venLat      = sheet.cell(r,7).value
    venLong     = sheet.cell(r,8).value
    venAprt_Prt = sheet.cell(r,9).value
    venContact  = sheet.cell(r,10).value  
    
    # Assign values from each row    
    values = (venID, venType, venName, venAddr, venCity, venCntry, owner, venLat, venLong, venAprt_Prt, venContact)
    
    # Execute SQL Query
    cursor.execute(Query1, values)
    cursor.execute(Query2)


# Commit the transaction
conn.commit()

# If we want to check if all rows are imported
# Successful notification    
cursor.execute("SELECT count(*) FROM WEC_EDW_DEV.MDL.tblEAS_Vendor_EQ")
result = cursor.fetchone()

print((result[0] - before_import[0]) == len(data.index))  # should be True
print('Insert successfully!')
print('Delete duplicated rows!')

# Close the database connection
cursor.close()
conn.close()
