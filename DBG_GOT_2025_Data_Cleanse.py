# Title: DBG_GOT_2025_Data_Cleanse
# Author: cwilliams & MS Copilot
# Date: 2025/09/017
# Purpose: Clean Durango Botanic Gardens "Garden on Tour" 2024 data clean before loading with Wild Apricot CMS
# Remove columns that are not needed for the data import/merge of Contacts "GOT 2024" column.
# Add column to set Contacts "GOT 2024" column.
# Dependencies: pandas
# Date/Name/Change
# 09/08/2025 Initial version copied from BG_GOT_2024_Data_Cleanse
# 09/18/2025 Fixed NaN handling to prevent "nan" strings in output

from datetime import datetime
import os
import sys
import pandas as pd
import re
import glob
import logging

# Use raw string (r'') to handle spaces in the file path
#input
file_path1 = r'C:\Users\Charl\OneDrive\Documents\Development\Python\DBG\2025 Gardens on Tour ccw.xlsx'
#output, temp name
file_path2 = r'C:\Users\Charl\OneDrive\Documents\Development\Python\DBG\GoT 2025 Attendance ccw clean.xlsx'

# Reset output filename to be dynamic.
# Original file name (without extension)
base_name2 = "GoT_2025_Attendance_ccw_clean"

# Get current datetime in YYYYMMDD_HHMM format
datetime_stamp = datetime.now().strftime('%Y%m%d_%H%M')

# Append datetime to base name
new_file_name2 = f"{base_name2}_{datetime_stamp}.xlsx"

# Optional: build full path
output_dir2 = r'C:\Users\Charl\OneDrive\Documents\Development\Python\DBG'
full_path2 = os.path.join(output_dir2, new_file_name2)

# Setup logging configuration
log_filename = f"DBG_GoT_2025_data_cleanse_{datetime_stamp}.log"
log_filepath = os.path.join(output_dir2, log_filename)

def setup_logging(log_filepath):
    """
    Setup logging configuration for data cleaning operations.
    
    Parameters:
        log_filepath (str): Full path for the log file
    
    Returns:
        logging.Logger: Configured logger instance
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filepath, mode='w'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger(__name__)
    return logger

def safe_str_conversion(value):
    """
    Safely convert values to string, replacing NaN/None with empty string.
    
    Parameters:
        value: Value to convert to string
    
    Returns:
        str: String representation, empty string for NaN/None
    """
    if pd.isna(value) or value is None:
        return ''
    return str(value).strip()

def log_correction(logger, correction_type, row_data, old_value, new_value, field_name):
    """
    Log individual corrections with identifying information.
    
    Parameters:
        logger: Logger instance
        correction_type (str): Type of correction made
        row_data (pd.Series): Row data containing identifying fields
        old_value (str): Original value before correction
        new_value (str): New value after correction
        field_name (str): Name of the field being corrected
    """
    # Extract identifying fields safely
    first_name = safe_str_conversion(row_data.get('First name', 'N/A'))
    last_name = safe_str_conversion(row_data.get('Last name', 'N/A'))
    email = safe_str_conversion(row_data.get('email', 'N/A'))
    phone = safe_str_conversion(row_data.get('Phone', 'N/A'))
    
    # Replace empty strings with 'N/A' for logging clarity
    first_name = first_name if first_name else 'N/A'
    last_name = last_name if last_name else 'N/A'
    email = email if email else 'N/A'
    phone = phone if phone else 'N/A'
    
    logger.info(f"{correction_type} - {field_name}: '{old_value}' -> '{new_value}' | "
                f"Name: {first_name} {last_name} | Email: {email} | Phone: {phone}")

def clean_contact_fields_with_logging(df, logger):
    """
    Identifies and removes leading/trailing spaces from 'email' and 'Phone' columns.
    Logs individual corrections and provides summary.

    Parameters:
        df (pd.DataFrame): The DataFrame containing 'email' and 'Phone' columns.
        logger: Logger instance for recording corrections

    Returns:
        pd.DataFrame: A copy of the DataFrame with cleaned 'email' and 'Phone' fields.
    """
    df_cleaned = df.copy()
    logger.info("Starting contact field cleaning (email and Phone columns)")
    
    total_changes = 0
    changes_by_column = {}

    for col in ['email', 'Phone']:
        if col in df_cleaned.columns:
            column_changes = 0
            
            # Convert to string safely, preserving NaN as empty strings
            df_cleaned[col] = df_cleaned[col].apply(safe_str_conversion)

            # Identify rows with leading/trailing spaces (but not empty strings)
            spaces_mask = (df_cleaned[col] != '') & (
                df_cleaned[col].str.startswith(' ') | df_cleaned[col].str.endswith(' ')
            )
            
            # Log individual corrections
            for idx in df_cleaned[spaces_mask].index:
                row = df_cleaned.loc[idx]
                old_value = row[col]
                new_value = row[col].strip()
                
                log_correction(logger, "SPACE_CLEANUP", row, old_value, new_value, col)
                df_cleaned.loc[idx, col] = new_value
                column_changes += 1

            total_changes += column_changes
            changes_by_column[col] = column_changes

    # Summary logging
    logger.info(f"Contact field cleaning summary:")
    for col, changes in changes_by_column.items():
        logger.info(f"   - {col}: {changes} corrections made")
    logger.info(f"Contact field cleaning completed: {total_changes} total changes across {len(df_cleaned)} rows")
    
    return df_cleaned

def clean_phone_number(phone_value):
    """Clean phone numbers: remove "1-" prefix, spaces, dashes, parentheses"""
    if pd.isna(phone_value):
        return ''
    
    cleaned = safe_str_conversion(phone_value)
    
    if cleaned.startswith("1-"):
        cleaned = cleaned[2:]
    
    # Remove spaces, dashes, parentheses
    cleaned = re.sub(r'[\s\-\(\)]', '', cleaned)
    
    return cleaned

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

def get_invalid_phone_number(df1, logger, first_name_col='First name', last_name_col='Last name', email_col='email', phone_col='Phone'):
    """
    Identify phone numbers that are NOT exactly 10 digits after cleaning.
    Logs invalid entries and provides summary.
    
    Parameters:
        df1 (pd.DataFrame): DataFrame to analyze
        logger: Logger instance
        first_name_col, last_name_col, email_col, phone_col (str): Column names
    
    Returns:
        pd.DataFrame: DataFrame containing invalid phone number entries
    """
    logger.info("Starting phone number validation")
    
    # Apply cleaning function once
    df1['CleanPhone'] = df1[phone_col].apply(clean_phone_number)

    # Count digits in cleaned phone number
    df1['DigitCount'] = df1['CleanPhone'].apply(lambda x: len(re.sub(r'\D', '', safe_str_conversion(x))))

    # Flag entries that are NOT exactly 10 digits
    df1['BadLength'] = df1['DigitCount'] != 10

    # Get entries with incorrect digit count
    bad_length_df = df1[df1['BadLength']]
    
    # Log each invalid phone number
    for idx in bad_length_df.index:
        row = bad_length_df.loc[idx]
        logger.warning(f"INVALID_PHONE - Original: '{safe_str_conversion(row[phone_col])}' | Clean: '{row['CleanPhone']}' | "
                      f"Digits: {row['DigitCount']} | Name: {safe_str_conversion(row.get(first_name_col, 'N/A'))} {safe_str_conversion(row.get(last_name_col, 'N/A'))} | "
                      f"Email: {safe_str_conversion(row.get(email_col, 'N/A'))}")

    # Summary logging
    if not bad_length_df.empty:
        logger.warning(f"Found {len(bad_length_df)} phone numbers with incorrect length (not 10 digits)")
    else:
        logger.info("All phone numbers have exactly 10 digits after cleaning")
    
    return bad_length_df

def flag_invalid_states(df, logger, state_col='State'):
    """
    Flag and normalize invalid state abbreviations.
    Logs corrections and provides summary.
    
    Parameters:
        df (pd.DataFrame): DataFrame to process
        logger: Logger instance
        state_col (str): Name of the state column
    
    Returns:
        pd.DataFrame: DataFrame containing invalid state entries (before correction)
    """
    logger.info(f"Starting state validation for column '{state_col}'")
    
    # List of official U.S. state abbreviations (uppercase)
    valid_states = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    }

    # Create normalized version for comparison without modifying original
    df['NormalizedState'] = df[state_col].apply(lambda x: safe_str_conversion(x).upper())
    
    # Filter rows where state is not blank and not in valid list
    invalid_states_df = df[
        (df['NormalizedState'] != '') & (~df['NormalizedState'].isin(valid_states))
    ]

    # Log invalid state entries
    for idx in invalid_states_df.index:
        row = invalid_states_df.loc[idx]
        logger.warning(f"INVALID_STATE - Original: '{safe_str_conversion(row[state_col])}' | "
                      f"Name: {safe_str_conversion(row.get('First name', 'N/A'))} {safe_str_conversion(row.get('Last name', 'N/A'))} | "
                      f"Email: {safe_str_conversion(row.get('email', 'N/A'))} | Phone: {safe_str_conversion(row.get('Phone', 'N/A'))}")

    # Log and apply normalization corrections
    normalization_count = 0
    for idx in df.index:
        original_state = safe_str_conversion(df.loc[idx, state_col])
        normalized_state = df.loc[idx, 'NormalizedState']
        
        if original_state != normalized_state:
            row = df.loc[idx]
            log_correction(logger, "STATE_NORMALIZATION", row, original_state, normalized_state, state_col)
            df.loc[idx, state_col] = normalized_state if normalized_state else ''
            normalization_count += 1

    # Clean up temporary column
    df.drop(['NormalizedState'], axis=1, inplace=True)
    
    # Summary logging
    if not invalid_states_df.empty:
        logger.warning(f"Found {len(invalid_states_df)} rows with invalid state abbreviations")
    else:
        logger.info("All state entries are valid")
        
    if normalization_count > 0:
        logger.info(f"Normalized {normalization_count} state entries (whitespace/case fixes)")
    
    return invalid_states_df

def process_phone_formatting(df1, logger):
    """
    Apply phone number cleaning and formatting to the DataFrame.
    Logs individual changes and provides summary.
    
    Parameters:
        df1 (pd.DataFrame): DataFrame to process
        logger: Logger instance
    
    Returns:
        dict: Summary statistics of phone processing
    """
    if 'Phone' not in df1.columns:
        logger.warning("Phone column not found - skipping phone formatting")
        return {'changed_count': 0, 'valid_count': 0, 'invalid_count': 0}
    
    logger.info("Starting phone number cleaning and formatting")

    df1['CleanPhone'] = df1['Phone'].apply(clean_phone_number)
    df1['DigitCount'] = df1['CleanPhone'].apply(lambda x: len(re.sub(r'\D', '', safe_str_conversion(x))))
    df1['IsValidPhone'] = df1['DigitCount'] == 10

    df1['FormattedPhone'] = df1.apply(
        lambda row: format_phone_number(row['CleanPhone']) if row['IsValidPhone'] else safe_str_conversion(row['Phone']),
        axis=1
    )

    phone_changes = df1['Phone'].apply(safe_str_conversion) != df1['FormattedPhone']
    
    # Log individual phone number changes
    for idx in df1[phone_changes].index:
        row = df1.loc[idx]
        log_correction(logger, "PHONE_FORMAT", row, safe_str_conversion(row['Phone']), row['FormattedPhone'], 'Phone')

    # Apply changes
    df1.loc[phone_changes, 'Phone'] = df1.loc[phone_changes, 'FormattedPhone']

    # Calculate statistics
    changed_count = phone_changes.sum()
    valid_count = df1['IsValidPhone'].sum()
    invalid_count = len(df1) - valid_count

    # Clean up temporary columns
    df1.drop(['CleanPhone', 'DigitCount', 'IsValidPhone', 'FormattedPhone'], axis=1, inplace=True)

    # Summary logging
    logger.info(f"Phone processing summary:")
    logger.info(f"   - {changed_count} phone numbers changed to 999-999-9999 format")
    logger.info(f"   - {valid_count - changed_count} phone numbers already in correct format")
    logger.info(f"   - {invalid_count} phone numbers left unchanged (invalid length)")
    logger.info(f"Phone processing completed")

    return {
        'changed_count': changed_count,
        'valid_count': valid_count,
        'invalid_count': invalid_count
    }

def validate_record_count(df_before, df_after, logger, stage_label=""):
    """
    Validates that the number of records in the DataFrame has not changed during processing.

    Parameters:
        df_before (pd.DataFrame): Original DataFrame before modification.
        df_after (pd.DataFrame): Modified DataFrame after processing.
        logger: Logger instance
        stage_label (str): Optional label to identify the processing stage in logs.

    Returns:
        bool: True if record counts match, False otherwise.
    """
    original_count = len(df_before)
    modified_count = len(df_after)

    logger.info(f"Record Count Validation ({stage_label}):")
    logger.info(f"   Original record count: {original_count}")
    logger.info(f"   Modified record count: {modified_count}")

    if original_count != modified_count:
        logger.error(f"Record count mismatch detected during '{stage_label}' stage.")
        logger.error(f"Difference: {original_count - modified_count} records lost.")
        return False
    else:
        logger.info(f"Record count validated: No data loss during '{stage_label}' stage.")
        return True

def standardize_street_types(address_value):
    """
    Standardize street types and directional indicators in address strings.
    
    Parameters:
        address_value (str): Address string to standardize
    
    Returns:
        str: Address with standardized abbreviations
    """
    if pd.isna(address_value) or address_value == '':
        return ''
    
    address = safe_str_conversion(address_value)
    
    # Dictionary of street type standardizations (case insensitive)
    street_types = {
        r'\bStreet\b': 'St',
        r'\bAvenue\b': 'Ave',
        r'\bBoulevard\b': 'Blvd',
        r'\bDrive\b': 'Dr',
        r'\bLane\b': 'Ln',
        r'\bRoad\b': 'Rd',
        r'\bCircle\b': 'Cir',
        r'\bCourt\b': 'Ct',
        r'\bPlace\b': 'Pl',
        r'\bTrail\b': 'Trl',
        r'\bParkway\b': 'Pkwy',
        r'\bHighway\b': 'Hwy',
        r'\bWay\b': 'Way',
        r'\bSquare\b': 'Sq',
        r'\bTerrace\b': 'Ter',
        r'\bAlley\b': 'Aly',
        r'\bCounty Road\b': 'CR',
        r'\bCounty road\b': 'CR',
        r'\bCounty Rd\b': 'CR',
        r'\bC.R.\b': 'CR',
        r'\bState Route\b': 'SR',
        r'\bState Highway\b': 'SH',
        r'\bFarm Road\b': 'FM',
        r'\bRanch Road\b': 'RR',
        r'\bGarden\b': 'Gdn',
        r'\bGardens\b': 'Gdns',
        r'\bCrescent\b': 'Cres',
        r'\bHeights\b': 'Hts',
        r'\bCreek\b': 'Crk'
    }
    
    # Dictionary of directional standardizations
    directional_types = {
        r'\bNorth\b': 'N',
        r'\bSouth\b': 'S',
        r'\bEast\b': 'E',
        r'\bWest\b': 'W',
        r'\bNortheast\b': 'NE',
        r'\bNorthwest\b': 'NW',
        r'\bSoutheast\b': 'SE',
        r'\bSouthwest\b': 'SW'
    }
    
    # Apply street type standardizations
    for pattern, replacement in street_types.items():
        address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)
    
    # Apply directional standardizations
    for pattern, replacement in directional_types.items():
        address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)
    
    return address.strip()

def standardize_unit_types(address_value):
    """
    Standardize unit types in addresses (Apartment, Suite, Unit, etc.).
    
    Parameters:
        address_value (str): Address string to standardize
    
    Returns:
        str: Address with standardized unit types
    """
    if pd.isna(address_value) or address_value == '':
        return ''
    
    address = safe_str_conversion(address_value)
    
    # Dictionary of unit type standardizations
    unit_types = {
        r'\bApartment\b': 'Apt',
        r'\bSuite\b': 'Ste',
        r'\bUnit\b': 'Unit',
        r'\bBuilding\b': 'Bldg',
        r'\bFloor\b': 'Fl',
        r'\bRoom\b': 'Rm',
        r'\bOffice\b': 'Ofc',
        r'\bDepartment\b': 'Dept',
        r'\bTrailer\b': 'Trlr',
        r'\bSpace\b': 'Spc',
        r'\bLot\b': 'Lot'
    }
    
    # Apply unit type standardizations
    for pattern, replacement in unit_types.items():
        address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)
    
    return address.strip()

def format_address_standardization(df, logger, address_col='Address'):
    """
    Apply address standardization to the specified address column.
    Logs individual changes and provides summary.
    
    Parameters:
        df (pd.DataFrame): DataFrame to process
        logger: Logger instance
        address_col (str): Name of the address column to standardize
    
    Returns:
        dict: Summary statistics of address standardization
    """
    if address_col not in df.columns:
        logger.warning(f"Address column '{address_col}' not found - skipping address standardization")
        return {'street_changes': 0, 'unit_changes': 0, 'total_processed': 0}
    
    logger.info(f"Starting address standardization for column '{address_col}'")
    
    street_changes = 0
    unit_changes = 0
    total_processed = 0
    
    # Create working copies to track changes
    df['StandardizedStreet'] = df[address_col].apply(standardize_street_types)
    df['StandardizedUnit'] = df['StandardizedStreet'].apply(standardize_unit_types)
    
    # Process each row and log changes
    for idx in df.index:
        row = df.loc[idx]
        original_address = safe_str_conversion(row[address_col])
        street_standardized = safe_str_conversion(row['StandardizedStreet'])
        final_standardized = safe_str_conversion(row['StandardizedUnit'])
        
        # Skip empty addresses
        if original_address.strip() == '':
            continue
            
        total_processed += 1
        
        # Log street type changes
        if original_address != street_standardized:
            log_correction(logger, "ADDRESS_STREET_TYPE", row, original_address, street_standardized, address_col)
            street_changes += 1
        
        # Log unit type changes (compare final to street-only standardized)
        if street_standardized != final_standardized:
            log_correction(logger, "ADDRESS_UNIT_TYPE", row, street_standardized, final_standardized, address_col)
            unit_changes += 1
        
        # Apply the final standardized address
        df.loc[idx, address_col] = final_standardized
    
    # Clean up temporary columns
    df.drop(['StandardizedStreet', 'StandardizedUnit'], axis=1, inplace=True)
    
    # Summary logging
    logger.info(f"Address standardization summary for '{address_col}':")
    logger.info(f"   - {street_changes} street type standardizations")
    logger.info(f"   - {unit_changes} unit type standardizations")
    logger.info(f"   - {total_processed} addresses processed")
    logger.info(f"Address standardization completed for '{address_col}'")
    
    return {
        'street_changes': street_changes,
        'unit_changes': unit_changes,
        'total_processed': total_processed
    }

def clean_address_spacing_formatting(df, logger, address_col='Address'):
    """
    Clean up spacing and basic formatting issues in addresses.
    Removes extra spaces, standardizes spacing around punctuation.
    
    Parameters:
        df (pd.DataFrame): DataFrame to process
        logger: Logger instance
        address_col (str): Name of the address column to clean
    
    Returns:
        dict: Summary statistics of address cleaning
    """
    if address_col not in df.columns:
        logger.warning(f"Address column '{address_col}' not found - skipping address spacing cleanup")
        return {'spacing_changes': 0, 'total_processed': 0}
    
    logger.info(f"Starting address spacing cleanup for column '{address_col}'")
    
    spacing_changes = 0
    total_processed = 0
    
    for idx in df.index:
        row = df.loc[idx]
        original_address = safe_str_conversion(row[address_col])
        
        if original_address.strip() == '':
            continue
            
        total_processed += 1
        
        # Apply spacing cleanup
        cleaned_address = original_address.strip()
        
        # Remove multiple spaces
        cleaned_address = re.sub(r'\s+', ' ', cleaned_address)
        
        # Standardize spacing around commas
        cleaned_address = re.sub(r'\s*,\s*', ', ', cleaned_address)
        
        # Standardize spacing around periods, but preserve P.O. Box format
        # First, temporarily replace P.O. Box patterns to protect them
        cleaned_address = re.sub(r'\bP\.O\.\s*Box\b', 'TEMP_PO_BOX', cleaned_address, flags=re.IGNORECASE)
        cleaned_address = re.sub(r'\s*\.\s*', '. ', cleaned_address)
        # Restore P.O. Box format
        cleaned_address = re.sub(r'\bTEMP_PO_BOX\b', 'P.O. Box', cleaned_address)
        
        # Remove trailing comma or period followed by space
        cleaned_address = re.sub(r'[,.]$', '', cleaned_address).strip()
        
        # Log changes
        if original_address != cleaned_address:
            log_correction(logger, "ADDRESS_SPACING", row, original_address, cleaned_address, address_col)
            df.loc[idx, address_col] = cleaned_address
            spacing_changes += 1
    
    # Summary logging
    logger.info(f"Address spacing cleanup summary for '{address_col}':")
    logger.info(f"   - {spacing_changes} spacing corrections")
    logger.info(f"   - {total_processed} addresses processed")
    logger.info(f"Address spacing cleanup completed for '{address_col}'")
    
    return {
        'spacing_changes': spacing_changes,
        'total_processed': total_processed
    }

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

def clean_nan_values_before_export(df, logger):
    """
    Clean up any remaining NaN values before exporting to Excel.
    Replace NaN values with appropriate empty values based on column type.
    
    Parameters:
        df (pd.DataFrame): DataFrame to clean
        logger: Logger instance
    
    Returns:
        pd.DataFrame: DataFrame with NaN values cleaned
    """
    logger.info("Cleaning NaN values before export")
    
    df_cleaned = df.copy()
    nan_replacements = 0
    
    for col in df_cleaned.columns:
        # Count NaN values in this column
        nan_count = df_cleaned[col].isna().sum()
        if nan_count > 0:
            # Replace NaN with empty string for all columns
            df_cleaned[col] = df_cleaned[col].fillna('')
            nan_replacements += nan_count
            logger.info(f"   - {col}: {nan_count} NaN values replaced with empty strings")
    
    if nan_replacements > 0:
        logger.info(f"Total NaN values cleaned: {nan_replacements}")
    else:
        logger.info("No NaN values found")
    
    return df_cleaned

#MAIN#############################################################

if __name__ == "__main__":
    # Setup logging
    logger = setup_logging(log_filepath)
    logger.info(f"Starting DBG data cleaning process")

    script_name = os.path.basename(__file__)
    logger.info(f"Running script: {script_name}")
    logger.info(f"Log file: {log_filepath}")

    # Check for most recent cleaned file from previous run
    latest_cleaned_file = get_latest_cleaned_file(output_dir2, base_name2)

    if latest_cleaned_file:
        logger.info("Previous cleaned file found")
        logger.info(f"   Last cleaned file: {latest_cleaned_file}")
        print("\nDo you want to use the last cleaned file as the new input?")
        print(f"   Last cleaned file found:\n{latest_cleaned_file}")
        user_choice = input("   Type 'Y' to substitute original input file with the last cleaned file, or press Enter to continue with the original: ").strip().lower()

        if user_choice == 'y':
            file_path1 = latest_cleaned_file
            logger.info(f"Input file substituted with: {file_path1}")
        else:
            logger.info(f"Proceeding with original input file: {file_path1}")
    else:
        logger.info("No previously cleaned file found. Proceeding with original input file.")

    file_path2 = full_path2

    # Load input Excel file
    try:
        df1 = pd.read_excel(file_path1, engine='openpyxl')
        logger.info(f'Input File Loaded: {file_path1}')
        logger.info(f'Input DataFrame shape: {df1.shape}')
    except Exception as e:
        logger.error(f"Failed to load input file: {e}")
        exit(1)

    # Store original DataFrame for comparison BEFORE any modifications
    df_original = df1.copy()

    # Process invalid states
    invalid_states_df = flag_invalid_states(df1, logger)

    # Process invalid phone numbers
    invalid_phone_df = get_invalid_phone_number(df1, logger)

    # Clean contact fields (email and Phone spaces)
    df1 = clean_contact_fields_with_logging(df1, logger)

    # Process address spacing cleanup
    address_spacing_stats = clean_address_spacing_formatting(df1, logger, 'Address')

    # Process address standardization
    address_standard_stats = format_address_standardization(df1, logger, 'Address')

    # Process phone number formatting
    phone_stats = process_phone_formatting(df1, logger)

    # Drop any remaining temporary calculation columns
    temp_cols = ['CleanPhone', 'DigitCount', 'BadLength']
    for col in temp_cols:
        if col in df1.columns:
            df1.drop([col], axis=1, inplace=True)

    # Clean NaN values before final comparison and export
    df1 = clean_nan_values_before_export(df1, logger)

    # Check to see if there were changes between the original and potentially modified data
    data_changed = not df1.equals(df_original)
    logger.info(f"Data modification check: {'Changes detected' if data_changed else 'No changes detected'}")

    # Validation check for before and after record count
    record_count_valid = validate_record_count(df_original, df1, logger, "data cleaning")

    if not record_count_valid:
        logger.error("STOPPING: Record count validation failed. No output file will be created.")
        logger.error("Please review the cleaning process - data may have been accidentally deleted.")
    elif data_changed:
        logger.info("Data has been modified and validation passed. Writing cleaned data to output file...")
        try:
            df1.to_excel(full_path2, index=False)
            logger.info(f"Cleaned data successfully written to: {full_path2}")
        except Exception as e:
            logger.error(f"Error writing to file: {e}")
    else:
        logger.info("No changes detected in data. Skipping output file creation.")
        logger.info("Original file is already clean - no output file needed.")

    # Log final summary statistics
    logger.info("Final Processing Summary:")
    logger.info(f"   - Invalid states found: {len(invalid_states_df)}")
    logger.info(f"   - Invalid phone numbers found: {len(invalid_phone_df)}")
    logger.info(f"   - Phone formatting changes: {phone_stats.get('changed_count', 0)}")
    logger.info(f"   - Address spacing corrections: {address_spacing_stats.get('spacing_changes', 0)}")
    logger.info(f"   - Address street standardizations: {address_standard_stats.get('street_changes', 0)}")
    logger.info(f"   - Address unit standardizations: {address_standard_stats.get('unit_changes', 0)}")

    logger.info("DBG data cleaning process completed")
    logger.info(f"Detailed log saved to: {log_filepath}")