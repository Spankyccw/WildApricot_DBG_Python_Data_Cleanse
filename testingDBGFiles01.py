#Title: TestDBG.py
#Purpose: Analyze Durango Botanic Gardens data files: contacts & members
#Change: ccwilliams 20240917 Initial version
print("Analyzing Durango Botanic Gardens data files: contacts & members")
#print("Input Data files are located C:\Users\Charl\OneDrive\Documents\DBG\Data Files")
print("Contacts: 2024-09-17 Contacts Durango Botanic Gardens.csv")
print("Members: 2024-09-17 Members Durango Botanic Gardens.csv")

import csv
import pandas as pd
import os

#Display default path for file access
print(os.path.dirname(os.path.abspath(__file__)))

#Display the contents of the input data files, checking file access, read only
#with open(r"C:\Users\Charl\OneDrive\Documents\DBG\Data Files\Contacts Durango Botanic Gardens.csv", mode='r') as file:
#with open(r"C:\Users\Charl\OneDrive\Documents\DBG\Data Files\Members Durango Botanic Gardens.csv", mode='r') as file:
#    csv_reader = csv.reader(file)
#    for row in csv_reader:
#        print(row)

#Create Panda data frames for file analysis
contact_df = pd.read_csv(r"C:\Users\Charl\OneDrive\Documents\DBG\Data Files\Contacts Durango Botanic Gardens.csv")
print (contact_df.head)
members_df = pd.read_csv(r"C:\Users\Charl\OneDrive\Documents\DBG\Data Files\Members Durango Botanic Gardens.csv")
print (members_df.head)