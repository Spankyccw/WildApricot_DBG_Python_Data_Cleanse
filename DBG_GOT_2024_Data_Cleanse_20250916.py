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
# 09/11/2025 Code improvements and debugging assistance provided by Claude (Anthropic AI) - enhanced error handling,
# data validation functions, phone number formatting for Wild Apricot compatibility, conditional output logic,
# function definition order fixes, interactive data cleansing workflow optimization, and comprehensive reporting
# with identifying fields for efficient source data correction.
# 09/11/2025 Claude petered out on me so using more piece meal AI assist with MSCopilot. Added some detail so see which phone numbers were modified.
# 09/13/2025 Experimented with using module usaddress for standardization of address information.

from datetime import datetime
import os
import sys
import pandas as pd
import re
import glob

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
    
    total_changes = 0

    for col in ['email', 'Phone']:
        if col in df_cleaned.columns:
            # Convert to string for safe string operations
            df_cleaned[col] = df_cleaned[col].astype(str)

            # Identify rows with leading/trailing spaces
            leading_spaces = df_cleaned[df_cleaned[col].str.startswith(' ', na=False)]
            trailing_spaces = df_cleaned[df_cleaned[col].str.endswith(' ', na=False)]
            
            # Combine leading and trailing space rows (remove duplicates)
            spaces_mask = df_cleaned[col].str.startswith(' ', na=False) | df_cleaned[col].str.endswith(' ', na=False)
            rows_with_spaces = df_cleaned[spaces_mask]

            # Log issues found
            if not leading_spaces.empty:
                print(f"\n📋 Column '{col}' had leading spaces in these rows:")
                # Show key identifying fields plus the problematic column
                key_cols = ['First name', 'Last name', 'email', 'Phone']
                display_cols = [c for c in key_cols if c in df_cleaned.columns] + [col]
                # Remove duplicates while preserving order
                display_cols = list(dict.fromkeys(display_cols))
                print(leading_spaces[display_cols])
            else:
                print(f"\n✅ Column '{col}' had no leading spaces.")

            if not trailing_spaces.empty:
                print(f"\n📋 Column '{col}' had trailing spaces in these rows:")
                # Show key identifying fields plus the problematic column
                key_cols = ['First name', 'Last name', 'email', 'Phone']
                display_cols = [c for c in key_cols if c in df_cleaned.columns] + [col]
                # Remove duplicates while preserving order
                display_cols = list(dict.fromkeys(display_cols))
                print(trailing_spaces[display_cols])
            else:
                print(f"\n✅ Column '{col}' had no trailing spaces.")

            # Only strip spaces from rows that actually have them
            if spaces_mask.any():
                df_cleaned.loc[spaces_mask, col] = df_cleaned.loc[spaces_mask, col].str.strip()
                changes_count = spaces_mask.sum()
                total_changes += changes_count
                print(f"🧽 Cleaned {changes_count} entries in '{col}' column.")
            else:
                print(f"✅ No cleaning needed for '{col}' column.")

    print(f"\n🧽 Contact field cleaning completed: {total_changes} total changes made across {len(df_cleaned)} rows.")
    
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

# 🔧 Format clean phone numbers into 999-999-9999 format
def format_phone_number(clean_phone):
    """
    Format a clean 10-digit phone number into 999-999-9999 format.
    
    Parameters:
        clean_phone (str): Clean phone number (digits only)
    
    Returns:
        str: Formatted phone number or original if invalid
    """
    if len(clean_phone) == 10 and clean_phone.isdigit():
        return f"{clean_phone[:3]}-{clean_phone[3:6]}-{clean_phone[6:]}"
    return clean_phone  # Return original if not valid 10 digits

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
        print(f"\nFound {len(bad_length_df)} phone numbers with incorrect length.")
    else:
        print("✅ All phone numbers have exactly 10 digits after cleaning.")
    
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

    # Create normalized version for comparison without modifying original
    df['NormalizedState'] = df[state_col].astype(str).str.strip().str.upper()
    
    # Filter rows where state is not blank and not in valid list
    invalid_states_df = df[
        (df['NormalizedState'] != '') & (~df['NormalizedState'].isin(valid_states))
    ]

    print(f"\n🏛️ Rows with non-blank but invalid state abbreviations in '{state_col}':")
    if not invalid_states_df.empty:
        # Show key identifying fields plus the problematic state column
        key_cols = ['First name', 'Last name', 'email', 'Phone', state_col]
        display_cols = [c for c in key_cols if c in df.columns]
        print(invalid_states_df[display_cols])
        print(f"\nFound {len(invalid_states_df)} rows with invalid state entries.")
        
        # Only normalize states that actually need it
        needs_normalization = df[state_col] != df['NormalizedState']
        if needs_normalization.any():
            df.loc[needs_normalization, state_col] = df.loc[needs_normalization, 'NormalizedState']
            normalized_count = needs_normalization.sum()
            print(f"🧽 Normalized {normalized_count} state entries (whitespace/case fixes).")
        else:
            print("✅ All state entries already properly formatted.")
    else:
        print("✅ All state entries are valid.")
        # Still check if normalization is needed for valid states
        needs_normalization = df[state_col] != df['NormalizedState']
        if needs_normalization.any():
            df.loc[needs_normalization, state_col] = df.loc[needs_normalization, 'NormalizedState']
            normalized_count = needs_normalization.sum()
            print(f"🧽 Normalized {normalized_count} valid state entries (whitespace/case fixes).")

    # Clean up temporary column
    df.drop(['NormalizedState'], axis=1, inplace=True)
    
    return invalid_states_df

def validate_record_count(df_before, df_after, stage_label=""):
    """
    Validates that the number of records in the DataFrame has not changed during processing.

    Parameters:
        df_before (pd.DataFrame): Original DataFrame before modification.
        df_after (pd.DataFrame): Modified DataFrame after processing.
        stage_label (str): Optional label to identify the processing stage in logs.

    Returns:
        bool: True if record counts match, False otherwise.
    """
    original_count = len(df_before)
    modified_count = len(df_after)

    print(f"\n🔍 Record Count Validation ({stage_label}):")
    print(f"   📄 Original record count: {original_count}")
    print(f"   📄 Modified record count: {modified_count}")

    if original_count != modified_count:
        print(f"🚫 Record count mismatch detected during '{stage_label}' stage.")
        print(f"💡 Difference: {original_count - modified_count} records lost.")
        return False
    else:
        print(f"✅ Record count validated: No data loss during '{stage_label}' stage.")
        return True

def get_latest_cleaned_file(output_dir, base_name):
    """
    Finds the most recent cleaned Excel file from a previous run based on naming convention.

    Parameters:
        output_dir (str): Directory where cleaned files are saved.
        base_name (str): Base name prefix used in cleaned file naming.

    Returns:
        str or None: Full path to the most recent cleaned file, or None if none found.
    """
    pattern = os.path.join(output_dir, f"{base_name}_*.xlsx")
    matching_files = glob.glob(pattern)

    if not matching_files:
        return None

    # Sort by modified time, descending
    matching_files.sort(key=os.path.getmtime, reverse=True)
    return matching_files[0]
#MAIN#############################################################

def get_latest_cleaned_file(output_dir, base_name):
    """
    Finds the most recent cleaned Excel file from a previous run based on naming convention.

    Parameters:
        output_dir (str): Directory where cleaned files are saved.
        base_name (str): Base name prefix used in cleaned file naming.

    Returns:
        str or None: Full path to the most recent cleaned file, or None if none found.
    """
    import glob

    pattern = os.path.join(output_dir, f"{base_name}_*.xlsx")
    matching_files = glob.glob(pattern)

    if not matching_files:
        return None

    matching_files.sort(key=os.path.getmtime, reverse=True)
    return matching_files[0]


if __name__ == "__main__":

    script_name = os.path.basename(__file__)
    print(f"Running script: {script_name}")

    # 🔍 Check for most recent cleaned file from previous run
    latest_cleaned_file = get_latest_cleaned_file(output_dir2, base_name2)

    if latest_cleaned_file:
        print("\n🔄 Do you want to use the last cleaned file as the new input?")
        print(f"   Last cleaned file found:\n{latest_cleaned_file}")
        user_choice = input("   Type 'Y' to substitute original input file with the last cleaned file, or press Enter to continue with the original: ").strip().lower()

        if user_choice == 'y':
            file_path1 = latest_cleaned_file
            print(f"\n📁 Substituted input file with:\n{file_path1}")
        else:
            print(f"\n📁 Proceeding with original input file:\n{file_path1}")
    else:
        print("\n⚠️ No previously cleaned file found. Proceeding with original input file.")

    file_path2 = full_path2

    # 🧾 Load input Excel file
    try:
        df1 = pd.read_excel(file_path1, engine='openpyxl')
        print(f'\n✅ Input File Loaded: {file_path1}')
    except Exception as e:
        print(f"\n❌ Failed to load input file: {e}")
        exit(1)

    print("\n📊 Input DataFrame Preview:")
    print(df1.head())

    # Store original DataFrame for comparison BEFORE any modifications
    df_original = df1.copy()

    # Call the function to flag invalid state abbreviations
    invalid_states_df = flag_invalid_states(df1)

    # Call phone numbers <> 10 digits
    invalid_phone_df = get_invalid_phone_number(df1)

    # Call to remove leading and trailing spaces and return the cleaned data frame
    df1 = clean_contact_fields_with_logging(df1)

    # Modularize this code into a function
    # Apply phone number cleaning and formatting to the actual DataFrame
    if 'Phone' in df1.columns:
        print(f"\n📞 Processing phone number cleaning and formatting...")

        df1['CleanPhone'] = df1['Phone'].apply(clean_phone_number)
        df1['DigitCount'] = df1['CleanPhone'].apply(lambda x: len(re.sub(r'\D', '', str(x))))
        df1['IsValidPhone'] = df1['DigitCount'] == 10

        df1['FormattedPhone'] = df1.apply(
            lambda row: format_phone_number(row['CleanPhone']) if row['IsValidPhone'] else row['Phone'],
            axis=1
        )

        phone_changes = df1['Phone'] != df1['FormattedPhone']
        changed_count = phone_changes.sum()
        valid_count = df1['IsValidPhone'].sum()
        invalid_count = len(df1) - valid_count

        df1.loc[phone_changes, 'Phone'] = df1.loc[phone_changes, 'FormattedPhone']
        print(df1.loc[phone_changes, ['First name', 'Last name', 'email', 'Phone']])

        df1.drop(['CleanPhone', 'DigitCount', 'IsValidPhone', 'FormattedPhone'], axis=1, inplace=True)

        print(f"✅ Phone processing completed:")
        print(f"   📞 {changed_count} phone numbers changed to 999-999-9999 format")
        print(f"   ✅ {valid_count - changed_count} phone numbers already in correct format")
        print(f"   ⚠️  {invalid_count} phone numbers left unchanged (invalid length)")

    # Drop calculation columns
    temp_cols = ['CleanPhone', 'DigitCount', 'BadLength']
    for col in temp_cols:
        if col in df1.columns:
            df1.drop([col], axis=1, inplace=True)

    # Check to see if there were changes between the original and potentially modified data as a condition of writing the file
    data_changed = not df1.equals(df_original)
    # Validation check for before and after record count, expecting the same everytime
    record_count_valid = validate_record_count(df_original, df1, "data cleaning")

    if not record_count_valid:
        print(f"\n🚫 STOPPING: Record count validation failed. No output file will be created.")
        print(f"💡 Please review the cleaning process - data may have been accidentally deleted.")
    elif data_changed:
        print(f"\n📝 Data has been modified and validation passed. Writing cleaned data to output file...")
        try:
            df1.to_excel(full_path2, index=False)
            print(f"\n✅ Cleaned data successfully written to:\n{full_path2}")
        except Exception as e:
            print(f"\n❌ Error writing to file: {e}")
    else:
        print(f"\n✅ No changes detected in data. Skipping output file creation.")
        print(f"💡 Original file is already clean - no output file needed.")