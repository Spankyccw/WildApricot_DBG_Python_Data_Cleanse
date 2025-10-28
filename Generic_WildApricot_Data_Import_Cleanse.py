# Title: Generic_WildApricot_Data_Cleanse
# Author: cwilliams
# Date: 2025/10/14
# Purpose: Clean event contact data before using the Import functionality into Wild Apricot CMS contacts table
# Dependencies: argparse, datetime, glob, logging, openpyxl, pandas, xlrd, os, re, sys
# Usage: python Generic_WildApricot_Data_Import_Cleanse.py "C:\Users\Charl\OneDrive\Documents\Development\Python\DBG\Bulb Sale 2024 ccw.xlsx" --event-column BulbSale2024 --event-value Yes --use-last-cleaned 
# Date/Name/Change
# 10/14/2025 cwilliams - Refactored to be generic with parameterized input via Claude
# 10/28/2025 cwilliams - Modified description slightly and added usage section to document how to call the code, add a -help next?

from datetime import datetime
import os
import sys
import pandas as pd
import re
import glob
import logging
import argparse

def setup_logging(log_filepath):
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
    if pd.isna(value) or value is None:
        return ''
    return str(value).strip()

def log_correction(logger, correction_type, row_data, old_value, new_value, field_name):
    first_name = safe_str_conversion(row_data.get('First name', 'N/A'))
    last_name = safe_str_conversion(row_data.get('Last name', 'N/A'))
    email = safe_str_conversion(row_data.get('email', 'N/A'))
    phone = safe_str_conversion(row_data.get('Phone', 'N/A'))
    
    first_name = first_name if first_name else 'N/A'
    last_name = last_name if last_name else 'N/A'
    email = email if email else 'N/A'
    phone = phone if phone else 'N/A'
    
    logger.info(f"{correction_type} - {field_name}: '{old_value}' -> '{new_value}' | "
                f"Name: {first_name} {last_name} | Email: {email} | Phone: {phone}")

def clean_contact_fields_with_logging(df, logger):
    df_cleaned = df.copy()
    logger.info("Starting contact field cleaning (email and Phone columns)")
    
    total_changes = 0
    changes_by_column = {}

    for col in ['email', 'Phone']:
        if col in df_cleaned.columns:
            column_changes = 0
            df_cleaned[col] = df_cleaned[col].apply(safe_str_conversion)
            spaces_mask = (df_cleaned[col] != '') & (
                df_cleaned[col].str.startswith(' ') | df_cleaned[col].str.endswith(' ')
            )
            
            for idx in df_cleaned[spaces_mask].index:
                row = df_cleaned.loc[idx]
                old_value = row[col]
                new_value = row[col].strip()
                log_correction(logger, "SPACE_CLEANUP", row, old_value, new_value, col)
                df_cleaned.loc[idx, col] = new_value
                column_changes += 1

            total_changes += column_changes
            changes_by_column[col] = column_changes

    logger.info(f"Contact field cleaning summary:")
    for col, changes in changes_by_column.items():
        logger.info(f"   - {col}: {changes} corrections made")
    logger.info(f"Contact field cleaning completed: {total_changes} total changes across {len(df_cleaned)} rows")
    
    return df_cleaned

def clean_phone_number(phone_value):
    if pd.isna(phone_value):
        return ''
    cleaned = safe_str_conversion(phone_value)
    if cleaned.startswith("1-"):
        cleaned = cleaned[2:]
    cleaned = re.sub(r'[\s\-\(\)]', '', cleaned)
    return cleaned

def format_phone_number(clean_phone):
    if len(clean_phone) == 10 and clean_phone.isdigit():
        return f"{clean_phone[:3]}-{clean_phone[3:6]}-{clean_phone[6:]}"
    return clean_phone

def get_invalid_phone_number(df1, logger, first_name_col='First name', last_name_col='Last name', email_col='email', phone_col='Phone'):
    logger.info("Starting phone number validation")
    df1['CleanPhone'] = df1[phone_col].apply(clean_phone_number)
    df1['DigitCount'] = df1['CleanPhone'].apply(lambda x: len(re.sub(r'\D', '', safe_str_conversion(x))))
    df1['BadLength'] = df1['DigitCount'] != 10
    bad_length_df = df1[df1['BadLength']]
    
    for idx in bad_length_df.index:
        row = bad_length_df.loc[idx]
        logger.warning(f"INVALID_PHONE - Original: '{safe_str_conversion(row[phone_col])}' | Clean: '{row['CleanPhone']}' | "
                      f"Digits: {row['DigitCount']} | Name: {safe_str_conversion(row.get(first_name_col, 'N/A'))} {safe_str_conversion(row.get(last_name_col, 'N/A'))} | "
                      f"Email: {safe_str_conversion(row.get(email_col, 'N/A'))}")

    if not bad_length_df.empty:
        logger.warning(f"Found {len(bad_length_df)} phone numbers with incorrect length (not 10 digits)")
    else:
        logger.info("All phone numbers have exactly 10 digits after cleaning")
    
    return bad_length_df

def flag_invalid_states(df, logger, state_col='State'):
    logger.info(f"Starting state validation for column '{state_col}'")
    
    valid_states = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    }

    df['NormalizedState'] = df[state_col].apply(lambda x: safe_str_conversion(x).upper())
    invalid_states_df = df[
        (df['NormalizedState'] != '') & (~df['NormalizedState'].isin(valid_states))
    ]

    for idx in invalid_states_df.index:
        row = invalid_states_df.loc[idx]
        logger.warning(f"INVALID_STATE - Original: '{safe_str_conversion(row[state_col])}' | "
                      f"Name: {safe_str_conversion(row.get('First name', 'N/A'))} {safe_str_conversion(row.get('Last name', 'N/A'))} | "
                      f"Email: {safe_str_conversion(row.get('email', 'N/A'))} | Phone: {safe_str_conversion(row.get('Phone', 'N/A'))}")

    normalization_count = 0
    for idx in df.index:
        original_state = safe_str_conversion(df.loc[idx, state_col])
        normalized_state = df.loc[idx, 'NormalizedState']
        
        if original_state != normalized_state:
            row = df.loc[idx]
            log_correction(logger, "STATE_NORMALIZATION", row, original_state, normalized_state, state_col)
            df.loc[idx, state_col] = normalized_state if normalized_state else ''
            normalization_count += 1

    df.drop(['NormalizedState'], axis=1, inplace=True)
    
    if not invalid_states_df.empty:
        logger.warning(f"Found {len(invalid_states_df)} rows with invalid state abbreviations")
    else:
        logger.info("All state entries are valid")
        
    if normalization_count > 0:
        logger.info(f"Normalized {normalization_count} state entries (whitespace/case fixes)")
    
    return invalid_states_df

def process_phone_formatting(df1, logger):
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
    
    for idx in df1[phone_changes].index:
        row = df1.loc[idx]
        log_correction(logger, "PHONE_FORMAT", row, safe_str_conversion(row['Phone']), row['FormattedPhone'], 'Phone')

    df1.loc[phone_changes, 'Phone'] = df1.loc[phone_changes, 'FormattedPhone']

    changed_count = phone_changes.sum()
    valid_count = df1['IsValidPhone'].sum()
    invalid_count = len(df1) - valid_count

    df1.drop(['CleanPhone', 'DigitCount', 'IsValidPhone', 'FormattedPhone'], axis=1, inplace=True)

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

def convert_address_to_title_case(address_value):
    if pd.isna(address_value) or address_value == '':
        return ''
    
    address = safe_str_conversion(address_value)
    letters_only = ''.join([c for c in address if c.isalpha()])
    if letters_only and letters_only.isupper():
        address = address.title()
        uppercase_fixes = {
            r'\bPo\b': 'PO',
            r'\bP\.o\.\b': 'P.O.',
            r'\bCr\b': 'CR',
            r'\bCr(\d)': r'CR\1',
            r'\bSr\b': 'SR',
            r'\bSr(\d)': r'SR\1',
            r'\bUs\b': 'US',
            r'\bUs(\d)': r'US\1',
            r'\bNe\b': 'NE',
            r'\bNw\b': 'NW',
            r'\bSe\b': 'SE',
            r'\bSw\b': 'SW',
        }
        for pattern, replacement in uppercase_fixes.items():
            address = re.sub(pattern, replacement, address)
    return address

def standardize_street_types(address_value):
    if pd.isna(address_value) or address_value == '':
        return ''
    
    address = safe_str_conversion(address_value)
    street_types = {
        r'\bStreet\b': 'St', r'\bAvenue\b': 'Ave', r'\bBoulevard\b': 'Blvd',
        r'\bDrive\b': 'Dr', r'\bLane\b': 'Ln', r'\bRoad\b': 'Rd',
        r'\bCircle\b': 'Cir', r'\bCourt\b': 'Ct', r'\bPlace\b': 'Pl',
        r'\bTrail\b': 'Trl', r'\bParkway\b': 'Pkwy', r'\bHighway\b': 'Hwy',
        r'\bWay\b': 'Way', r'\bSquare\b': 'Sq', r'\bTerrace\b': 'Ter',
        r'\bAlley\b': 'Aly', r'\bCounty Road\b': 'CR', r'\bCounty road\b': 'CR',
        r'\bCounty Rd\b': 'CR', r'\bC.R.\b': 'CR', r'\bState Route\b': 'SR',
        r'\bState Highway\b': 'SH', r'\bFarm Road\b': 'FM', r'\bRanch Road\b': 'RR',
        r'\bGarden\b': 'Gdn', r'\bGardens\b': 'Gdns', r'\bCrescent\b': 'Cres',
        r'\bHeights\b': 'Hts', r'\bCreek\b': 'Crk'
    }
    directional_types = {
        r'\bNorth\b': 'N', r'\bSouth\b': 'S', r'\bEast\b': 'E', r'\bWest\b': 'W',
        r'\bNortheast\b': 'NE', r'\bNorthwest\b': 'NW',
        r'\bSoutheast\b': 'SE', r'\bSouthwest\b': 'SW'
    }
    for pattern, replacement in street_types.items():
        address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)
    for pattern, replacement in directional_types.items():
        address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)
    return address.strip()

def standardize_unit_types(address_value):
    if pd.isna(address_value) or address_value == '':
        return ''
    
    address = safe_str_conversion(address_value)
    unit_types = {
        r'\bApartment\b': 'Apt', r'\bSuite\b': 'Ste', r'\bUnit\b': 'Unit',
        r'\bBuilding\b': 'Bldg', r'\bFloor\b': 'Fl', r'\bRoom\b': 'Rm',
        r'\bOffice\b': 'Ofc', r'\bDepartment\b': 'Dept', r'\bTrailer\b': 'Trlr',
        r'\bSpace\b': 'Spc', r'\bLot\b': 'Lot'
    }
    for pattern, replacement in unit_types.items():
        address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)
    return address.strip()

def clean_address_spacing_formatting(df, logger, address_col='Address'):
    if address_col not in df.columns:
        logger.warning(f"Address column '{address_col}' not found")
        return {'spacing_changes': 0, 'case_changes': 0, 'total_processed': 0}
    
    logger.info(f"Starting address spacing and case cleanup for column '{address_col}'")
    spacing_changes = 0
    case_changes = 0
    total_processed = 0
    
    for idx in df.index:
        row = df.loc[idx]
        original_address = safe_str_conversion(row[address_col])
        
        if original_address.strip() == '':
            continue
            
        total_processed += 1
        cleaned_address = original_address.strip()
        cleaned_address = re.sub(r'\s+', ' ', cleaned_address)
        cleaned_address = re.sub(r'\s*,\s*', ', ', cleaned_address)
        cleaned_address = re.sub(r'\bP\.O\.\s*Box\b', 'TEMP_PO_BOX', cleaned_address, flags=re.IGNORECASE)
        cleaned_address = re.sub(r'\s*\.\s*', '. ', cleaned_address)
        cleaned_address = re.sub(r'\bTEMP_PO_BOX\b', 'P.O. Box', cleaned_address)
        cleaned_address = re.sub(r'[,.]$', '', cleaned_address).strip()
        
        if original_address != cleaned_address:
            log_correction(logger, "ADDRESS_SPACING", row, original_address, cleaned_address, address_col)
            spacing_changes += 1
        
        case_converted = convert_address_to_title_case(cleaned_address)
        if cleaned_address != case_converted:
            log_correction(logger, "ADDRESS_CASE_CONVERSION", row, cleaned_address, case_converted, address_col)
            case_changes += 1
            cleaned_address = case_converted
        
        df.loc[idx, address_col] = cleaned_address
    
    logger.info(f"Address spacing and case cleanup summary:")
    logger.info(f"   - {spacing_changes} spacing corrections")
    logger.info(f"   - {case_changes} case conversions")
    logger.info(f"   - {total_processed} addresses processed")
    
    return {'spacing_changes': spacing_changes, 'case_changes': case_changes, 'total_processed': total_processed}

def format_address_standardization(df, logger, address_col='Address'):
    if address_col not in df.columns:
        logger.warning(f"Address column '{address_col}' not found")
        return {'street_changes': 0, 'unit_changes': 0, 'total_processed': 0}
    
    logger.info(f"Starting address standardization for column '{address_col}'")
    street_changes = 0
    unit_changes = 0
    total_processed = 0
    
    df['StandardizedStreet'] = df[address_col].apply(standardize_street_types)
    df['StandardizedUnit'] = df['StandardizedStreet'].apply(standardize_unit_types)
    
    for idx in df.index:
        row = df.loc[idx]
        original_address = safe_str_conversion(row[address_col])
        street_standardized = safe_str_conversion(row['StandardizedStreet'])
        final_standardized = safe_str_conversion(row['StandardizedUnit'])
        
        if original_address.strip() == '':
            continue
            
        total_processed += 1
        
        if original_address != street_standardized:
            log_correction(logger, "ADDRESS_STREET_TYPE", row, original_address, street_standardized, address_col)
            street_changes += 1
        
        if street_standardized != final_standardized:
            log_correction(logger, "ADDRESS_UNIT_TYPE", row, street_standardized, final_standardized, address_col)
            unit_changes += 1
        
        df.loc[idx, address_col] = final_standardized
    
    df.drop(['StandardizedStreet', 'StandardizedUnit'], axis=1, inplace=True)
    
    logger.info(f"Address standardization summary:")
    logger.info(f"   - {street_changes} street type standardizations")
    logger.info(f"   - {unit_changes} unit type standardizations")
    logger.info(f"   - {total_processed} addresses processed")
    
    return {'street_changes': street_changes, 'unit_changes': unit_changes, 'total_processed': total_processed}

def validate_event_column(df, logger, column_name=None, expected_value='Yes'):
    """
    Generic validation for event participation columns.
    If column_name is None, skip validation.
    """
    if column_name is None:
        logger.info("No event column specified for validation - skipping")
        return {'valid_count': 0, 'empty_count': 0, 'invalid_count': 0}
    
    if column_name not in df.columns:
        logger.warning(f"Column '{column_name}' not found - skipping validation")
        return {'valid_count': 0, 'empty_count': 0, 'invalid_count': 0}
    
    logger.info(f"Starting validation for column '{column_name}'")
    logger.info(f"   Expected value: '{expected_value}'")
    
    invalid_count = 0
    empty_count = 0
    valid_count = 0
    
    for idx in df.index:
        row = df.loc[idx]
        cell_value = safe_str_conversion(row[column_name]).strip()
        
        if cell_value == '':
            empty_count += 1
            logger.warning(f"EMPTY_VALUE - {column_name} is empty | "
                          f"Name: {safe_str_conversion(row.get('First name', 'N/A'))} {safe_str_conversion(row.get('Last name', 'N/A'))} | "
                          f"Email: {safe_str_conversion(row.get('email', 'N/A'))}")
        elif cell_value != expected_value:
            invalid_count += 1
            logger.warning(f"INVALID_VALUE - {column_name}: '{cell_value}' | "
                          f"Name: {safe_str_conversion(row.get('First name', 'N/A'))} {safe_str_conversion(row.get('Last name', 'N/A'))} | "
                          f"Email: {safe_str_conversion(row.get('email', 'N/A'))}")
        else:
            valid_count += 1
    
    logger.info(f"Validation summary for '{column_name}':")
    logger.info(f"   - Valid entries: {valid_count}")
    logger.info(f"   - Empty entries: {empty_count}")
    logger.info(f"   - Invalid entries: {invalid_count}")
    
    return {'valid_count': valid_count, 'empty_count': empty_count, 'invalid_count': invalid_count}

def get_latest_cleaned_file(output_dir, base_name):
    pattern = os.path.join(output_dir, f"{base_name}_clean_*.xlsx")
    matching_files = glob.glob(pattern)
    if not matching_files:
        return None
    matching_files.sort(key=os.path.getmtime, reverse=True)
    return matching_files[0]

def clean_nan_values_before_export(df, logger):
    logger.info("Cleaning NaN values before export")
    df_cleaned = df.copy()
    nan_replacements = 0
    
    for col in df_cleaned.columns:
        nan_count = df_cleaned[col].isna().sum()
        if nan_count > 0:
            df_cleaned[col] = df_cleaned[col].fillna('')
            nan_replacements += nan_count
            logger.info(f"   - {col}: {nan_count} NaN values replaced")
    
    if nan_replacements > 0:
        logger.info(f"Total NaN values cleaned: {nan_replacements}")
    else:
        logger.info("No NaN values found")
    
    return df_cleaned

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Clean contact data for Wild Apricot import',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python %(prog)s input_file.xlsx
  python %(prog)s input_file.xls --event-column "DurangoScape 2025"
  python %(prog)s input_file.xlsx --use-last-cleaned
        '''
    )
    
    parser.add_argument(
        'input_file',
        help='Path to input Excel file (.xls or .xlsx)'
    )
    
    parser.add_argument(
        '--event-column',
        default=None,
        help='Optional event column name to validate (e.g., "DurangoScape 2025")'
    )
    
    parser.add_argument(
        '--event-value',
        default='Yes',
        help='Expected value in event column (default: "Yes")'
    )
    
    parser.add_argument(
        '--use-last-cleaned',
        action='store_true',
        help='Automatically use the most recent cleaned file without prompting'
    )
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    
    # Validate input file exists
    if not os.path.exists(args.input_file):
        print(f"Error: Input file not found: {args.input_file}")
        sys.exit(1)
    
    # Extract file information
    input_path = os.path.abspath(args.input_file)
    input_dir = os.path.dirname(input_path)
    input_filename = os.path.basename(input_path)
    input_basename = os.path.splitext(input_filename)[0]
    input_ext = os.path.splitext(input_filename)[1]
    
    # Generate output filenames
    datetime_stamp = datetime.now().strftime('%Y%m%d_%H%M')
    output_filename = f"{input_basename}_clean_{datetime_stamp}.xlsx"
    output_path = os.path.join(input_dir, output_filename)
    
    log_filename = f"{input_basename}_cleanse_{datetime_stamp}.log"
    log_filepath = os.path.join(input_dir, log_filename)
    
    # Setup logging
    logger = setup_logging(log_filepath)
    logger.info(f"Starting Wild Apricot data cleaning process")
    logger.info(f"Script: {os.path.basename(__file__)}")
    logger.info(f"Input file: {input_path}")
    logger.info(f"Output file: {output_path}")
    logger.info(f"Log file: {log_filepath}")
    
    if args.event_column:
        logger.info(f"Event column validation: '{args.event_column}' (expected: '{args.event_value}')")
    
    # Check for previous cleaned files
    latest_cleaned_file = get_latest_cleaned_file(input_dir, input_basename)
    
    if latest_cleaned_file and not args.use_last_cleaned:
        logger.info("Previous cleaned file found")
        logger.info(f"   Last cleaned file: {latest_cleaned_file}")
        print("\nDo you want to use the last cleaned file as the new input?")
        print(f"   Last cleaned file found:\n{latest_cleaned_file}")
        user_choice = input("   Type 'Y' to substitute, or press Enter to continue with original: ").strip().lower()

        if user_choice == 'y':
            input_path = latest_cleaned_file
            logger.info(f"Input file substituted with: {input_path}")
        else:
            logger.info(f"Proceeding with original input file: {input_path}")
    elif latest_cleaned_file and args.use_last_cleaned:
        input_path = latest_cleaned_file
        logger.info(f"Using last cleaned file (auto-selected): {input_path}")
    else:
        logger.info("No previously cleaned file found")

    # Load input file
    try:
        if input_ext.lower() == '.xls':
            df1 = pd.read_excel(input_path, engine='xlrd')
            logger.info(f'Input file loaded (.xls format): {input_path}')
        else:
            df1 = pd.read_excel(input_path, engine='openpyxl')
            logger.info(f'Input file loaded (.xlsx format): {input_path}')
        
        logger.info(f'Input DataFrame shape: {df1.shape}')
        logger.info(f'Columns found: {list(df1.columns)}')
    except ImportError as e:
        logger.error(f"Missing required library: {e}")
        logger.error("For .xls files, install xlrd with: pip install xlrd")
        logger.error("For .xlsx files, install openpyxl with: pip install openpyxl")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to load input file: {e}")
        sys.exit(1)

    # Validate required columns
    required_columns = ['Last name', 'First name', 'email', 'Phone', 'Address', 'City', 'State', 'Zip']
    missing_columns = [col for col in required_columns if col not in df1.columns]
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        logger.error(f"Available columns: {list(df1.columns)}")
        sys.exit(1)
    else:
        logger.info("All required columns present in input file")

    # Preserve original for comparison
    df_original = df1.copy()

    # Execute cleaning operations
    invalid_states_df = flag_invalid_states(df1, logger)
    invalid_phone_df = get_invalid_phone_number(df1, logger)
    df1 = clean_contact_fields_with_logging(df1, logger)
    address_spacing_stats = clean_address_spacing_formatting(df1, logger, 'Address')
    address_standard_stats = format_address_standardization(df1, logger, 'Address')
    phone_stats = process_phone_formatting(df1, logger)
    event_stats = validate_event_column(df1, logger, args.event_column, args.event_value)

    # Clean up temporary columns
    temp_cols = ['CleanPhone', 'DigitCount', 'BadLength']
    for col in temp_cols:
        if col in df1.columns:
            df1.drop([col], axis=1, inplace=True)

    # Clean NaN values before export
    df1 = clean_nan_values_before_export(df1, logger)

    # Check if data was modified
    data_changed = not df1.equals(df_original)
    logger.info(f"Data modification check: {'Changes detected' if data_changed else 'No changes detected'}")

    # Validate record count
    record_count_valid = validate_record_count(df_original, df1, logger, "data cleaning")

    # Write output or skip if no changes
    if not record_count_valid:
        logger.error("STOPPING: Record count validation failed")
        sys.exit(1)
    elif data_changed:
        logger.info("Data has been modified and validation passed")
        try:
            df1.to_excel(output_path, index=False, engine='openpyxl')
            logger.info(f"Cleaned data successfully written to: {output_path}")
        except Exception as e:
            logger.error(f"Error writing to file: {e}")
            sys.exit(1)
    else:
        logger.info("No changes detected - skipping output file creation")

    # Final summary
    logger.info("Final Processing Summary:")
    logger.info(f"   - Total records processed: {len(df1)}")
    logger.info(f"   - Invalid states found: {len(invalid_states_df)}")
    logger.info(f"   - Invalid phone numbers found: {len(invalid_phone_df)}")
    logger.info(f"   - Phone formatting changes: {phone_stats.get('changed_count', 0)}")
    logger.info(f"   - Address spacing corrections: {address_spacing_stats.get('spacing_changes', 0)}")
    logger.info(f"   - Address case conversions: {address_spacing_stats.get('case_changes', 0)}")
    logger.info(f"   - Address street standardizations: {address_standard_stats.get('street_changes', 0)}")
    logger.info(f"   - Address unit standardizations: {address_standard_stats.get('unit_changes', 0)}")
    
    if args.event_column:
        logger.info(f"   - {args.event_column} valid entries: {event_stats.get('valid_count', 0)}")
        logger.info(f"   - {args.event_column} invalid/empty: {event_stats.get('invalid_count', 0) + event_stats.get('empty_count', 0)}")

    logger.info("Wild Apricot data cleaning process completed")
    logger.info(f"Detailed log saved to: {log_filepath}")