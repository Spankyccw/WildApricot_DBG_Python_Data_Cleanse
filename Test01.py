#Sample to help debug oother Python script
#import Python built in modules
from datetime import datetime
import os

#import third-party modules
#Panda install location
#c:\users\charl\appdata\local\packages\pythonsoftwarefoundation.python.3.9_qbz5n2kfra8p0\localcache\local-packages\python39\site-packages
# Python install location
#cd 'c:\Users\Charl\OneDrive\Documents\Development\Python\Code'; & 'c:\Users\Charl\AppData\Local\Programs\Python\Python313\python.exe' 'c:\Users\Charl\.vscode\extensions\ms-python.debugpy-2025.10.0-win32-x64\bundled\libs\debugpy\launcher' '63133' '--' 'c:\Users\Charl\OneDrive\Documents\Development\Python\Code\import sys.py'
import pandas as pd
import re
# Use raw string (r'') to handle spaces in the file path
#input
file_path1 = r'C:\Users\Charl\OneDrive\Documents\Development\Python\DBG\GoT 2024 Attendance ccw.xlsx'
#output, temp name
file_path2 = r'C:\Users\Charl\OneDrive\Documents\Development\Python\DBG\GoT 2024 Attendance ccw clean.xlsx'

# Reset output filename to be dynamic.
# Original file name (without extension)
base_name2 = "GoT_2024_Attendance_ccw_clean"

# Get current datetime in YYYYMMDD_HHMM format
datetime_stamp = datetime.now().strftime('%Y%m%d_%H%M')

# Append datetime to base name
new_file_name2 = f"{base_name2}_{datetime_stamp}.xlsx"

# Optional: build full path
output_dir2 = r'C:\Users\Charl\OneDrive\Documents\Development\Python\DBG'
full_path2 = os.path.join(output_dir2, new_file_name2)

print("Generated dynamic ouput file name:", full_path2)

file_path2 = full_path2

#Report on data cleansing in data to be merged, ID columns are phone and email
print (file_path1)
df1 = pd.read_excel(file_path1)
print("Original DataFrame:")
print (df1)

#NOT IN MAIN CODE
import re

pattern = r'^\d{3}-\d{3}-\d{4}$'
test = '123-456-7890'

if re.match(pattern, test):
    print("Valid phone number format")
else:
    print("Invalid format")

#SAVE THIS CODE SNIPPET FOR LOGIC FOR DBG_GOT_2024_DATA_CLEANSE.PY
"""
# Define column names — update these if they differ in your file
first_name_col = 'First name'
last_name_col = 'Last name'
email_col = 'email'
phone_col = 'Phone'

#!! WONT THIS CAUSE LOSS OF DATA??!!
# Drop rows with missing or blank phone numbers
df1 = df1[df1[phone_col].notna() & (df1[phone_col].astype(str).str.strip() != '')]

# Define regex pattern for valid phone number format: 999-999-9999
pattern = r'^\d{3}-\d{3}-\d{4}$'

# Flag malformed phone numbers
df1['Malformed'] = ~df1[phone_col].astype(str).str.match(pattern)

# Filter and display malformed entries
malformed_df1 = df1[df1['Malformed']][[first_name_col, last_name_col, email_col, phone_col]]
print("Malformed phone numbers:")
print(malformed_df1)
""""""