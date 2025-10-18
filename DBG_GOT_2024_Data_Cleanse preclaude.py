# Title: DBG_GOT_2024_Data_Cleanse
# Author: cwilliams & MS Copilot
# Date: 2025/09/08
# Purpose: Clean Durango Botanic Gardens "Garden on Tour" 2024 data clean before loading with Wild Apricot CMS
# Remove columns that are not needed for the data import/merge of Contacts "GOT 2024" column.
# Add column to set Contacts "GOT 2024" column.
# Dependencies: pandas
# Date/Name/Change
# 09/08/2025 Initial version
# 09/10/2025 Trying to add dynamic output filename with datetime, when executed ask if input file should be the last output filename
# as we loop to refine while retaining all the files along the way. Edit python to refine files formatting etc.
# 09/11/2025 Modularizing and testing as functions are added to main. Need to version control this work in GitHub.

#import Python built in modules
from datetime import datetime
import os

#import third-party modules
import pandas as pd
#import openpyxl
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

def clean_contact_fields_with_logging(df):
    """
    Identifies and removes leading/trailing spaces from 'email' and 'Phone' columns.
    Prints rows where corrections were made.

    Parameters:
        df (pd.DataFrame): The DataFrame containing 'email' and 'Phone' columns.

    Returns:
        pd.DataFrame: A copy of the DataFrame with cleaned 'email' and 'Phone' fields.
    """

    df_cleaned = df.copy()
    print("\n🔍 Identifying and fixing rows with leading/trailing spaces in 'email' and 'Phone':")

    for col in ['email', 'Phone']:
        if col in df_cleaned.columns:
            # Convert to string for safe string operations
            df_cleaned[col] = df_cleaned[col].astype(str)

            # Identify rows with leading/trailing spaces
            leading_spaces = df_cleaned[df_cleaned[col].str.startswith(' ', na=False)]
            trailing_spaces = df_cleaned[df_cleaned[col].str.endswith(' ', na=False)]

            # Log and clean
            if not leading_spaces.empty:
                print(f"\nColumn '{col}' had leading spaces in these rows:\n{leading_spaces}")
            else:
                print(f"\nColumn '{col}' had no leading spaces.")

            if not trailing_spaces.empty:
                print(f"\nColumn '{col}' had trailing spaces in these rows:\n{trailing_spaces}")
            else:
                print(f"\nColumn '{col}' had no trailing spaces.")

            # Strip spaces
            df_cleaned[col] = df_cleaned[col].str.strip()

            print(df_cleaned)

    return df_cleaned

# 🧼 Clean phone numbers: remove "1-" prefix, spaces, dashes, parentheses
def clean_phone_number(phone_value):
    if pd.isna(phone_value):
        return ''
    
    cleaned = str(phone_value).strip()
    
    if cleaned.startswith("1-"):
        cleaned = cleaned[2:]
    
    # Remove spaces, dashes, parentheses
    cleaned = re.sub(r'[\s\-\(\)]', '', cleaned)
    
    return cleaned

# 🔍 Identify phone numbers that are NOT exactly 10 digits after cleaning
def get_invalid_phone_number(df1, first_name_col='First name', last_name_col='Last name', email_col='email', phone_col='Phone'):
    # Apply cleaning function once
    df1['CleanPhone'] = df1[phone_col].apply(clean_phone_number)

    # Count digits in cleaned phone number
    df1['DigitCount'] = df1['CleanPhone'].apply(lambda x: len(re.sub(r'\D', '', str(x))))

    # Flag entries that are NOT exactly 10 digits
    df1['BadLength'] = df1['DigitCount'] != 10

    # Display ONLY entries with incorrect digit count (this was the fix needed)
    bad_length_df = df1[df1['BadLength']][[
        first_name_col, last_name_col, email_col, phone_col, 'CleanPhone', 'DigitCount'
    ]]

    print("\n🚫 Phone numbers with incorrect digit count (not 10 digits):")
    if not bad_length_df.empty:
        print(bad_length_df)
    else:
        print("All phone numbers have exactly 10 digits after cleaning.")
    
    return bad_length_df  # Return the invalid entries for further processing if needed

# Extra noncritical data clean up
# States that are not in state abbreviation list
def flag_invalid_states(df, state_col='State'):
    # List of official U.S. state abbreviations (uppercase)
    valid_states = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    }

    # Normalize column: strip whitespace and convert to uppercase
    df[state_col] = df[state_col].astype(str).str.strip().str.upper()

    # Filter rows where state is not blank and not in valid list
    invalid_states_df = df[
        (df[state_col] != '') & (~df[state_col].isin(valid_states))
    ]

    print(f"\nRows with non-blank but invalid state abbreviations in '{state_col}':")
    print(invalid_states_df[[state_col]])

    return invalid_states_df

#MAIN#############################################################

if __name__ == "__main__":

    script_name = os.path.basename(__file__)
    print(f"Running script: {script_name}")

    #print("Generated dynamic ouput file name:", full_path2)

    file_path2 = full_path2

    #Report on data cleansing in data to be merged, ID columns are phone and email
    #print (file_path1)
    df1 = pd.read_excel(file_path1)
    print("Original DataFrame:")
    print (df1)

    # Get the column names as a list
    column_names = df1.columns.tolist()
    #print(column_names)

    # Call the function to flag invalid state abbreviations
    invalid_states_df = flag_invalid_states(df1)

    # Optional: take action if any invalid states are found
    if not invalid_states_df.empty:
        print(f"\nFound {len(invalid_states_df)} rows with invalid state entries.")
    # You could log, export, or prompt for correction here
    else:
        print("\nAll state entries are valid.")
   
    #call phone numbers <> 10 digits
    get_invalid_phone_number(df1)
    #print(df1)
"""
    #call to remove leading and trailing spaces and return the data frame and print out invalids
    df1 = clean_contact_fields_with_logging(df1)
"""
"""
    #call to clean phone numbers
    invalid_phones_df = clean_phone_number(df1)
"""
"""
    # Optional: take action if any invalid states are found
    if not invalid_phones_df.empty:
        print(f"\nFound {len(invalid_phones_df)} rows with invalid phone entries.")
    # You could log, export, or prompt for correction here
    else:
        print("\nAll phone entries are valid.")
"""
"""
# Write cleaned DataFrame to dynamically named Excel file
    try:
        df1.to_excel(full_path2, index=False)
        print(f"\nCleaned data successfully written to:\n{full_path2}")
    except Exception as e:
        print(f"\nError writing to file: {e}")
"""