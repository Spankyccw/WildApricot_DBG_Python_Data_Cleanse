# Generic Wild Apricot Data Cleanse - Requirements Document

## Document Information

**Document Version:** 1.0  
**Date:** December 10, 2025  
**Author:** C. Williams  
**System Name:** Generic_WildApricot_Data_Cleanse  
**Original Script Date:** October 14, 2025

---

## 1. Executive Summary

This requirements document specifies the functionality and technical requirements for a Python-based data cleansing utility designed to prepare event contact data for import into the Wild Apricot CMS contacts table. The system processes Excel files containing contact information from various events, standardizes the data format, and produces cleaned output files ready for Wild Apricot's import functionality.

---

## 2. Project Overview

### 2.1 Purpose

The Generic_WildApricot_Data_Cleanse script automates the cleaning and standardization of event contact data before importing into the Wild Apricot CMS. The system ensures data consistency, validates contact information, and prepares files in the format required by Wild Apricot's import process.

### 2.2 Scope

**In Scope:**
- Processing Excel files (.xlsx and .xls formats) containing event contact data
- Data validation and cleansing of contact information
- Event column marking with customizable field names
- Address standardization and formatting
- Duplicate detection and handling
- Generation of cleaned output files ready for Wild Apricot import
- Comprehensive logging of all processing activities
- Command-line interface with flexible parameters

**Out of Scope:**
- Direct integration with Wild Apricot API
- Automated upload to Wild Apricot CMS
- Database storage of processed records
- Real-time data validation
- Web-based user interface

### 2.3 Intended Users

- Event coordinators managing contact lists
- Database administrators preparing data for CMS import
- Staff members handling event registrations
- Volunteers processing event attendee information

---

## 3. Business Requirements

### 3.1 Business Objectives

1. **Data Quality Assurance:** Ensure all contact data meets Wild Apricot's format requirements before import
2. **Time Efficiency:** Reduce manual data cleaning effort from hours to minutes
3. **Error Reduction:** Eliminate common data entry errors and inconsistencies
4. **Process Standardization:** Provide consistent data cleaning methodology across different events
5. **Audit Trail:** Maintain detailed logs of all data transformations for compliance and troubleshooting

### 3.2 Success Criteria

- 100% of processed files successfully import into Wild Apricot without errors
- Processing time under 5 minutes for files containing up to 1000 records
- All data transformations logged with sufficient detail for audit requirements
- Zero data loss during processing
- Duplicate detection accuracy of 95% or higher

---

## 4. Functional Requirements

### 4.1 Input Processing

**REQ-4.1.1:** The system shall accept Excel files in both .xlsx and .xls formats as input  
**REQ-4.1.2:** The system shall support command-line specification of the input file path  
**REQ-4.1.3:** The system shall validate that the input file exists before processing  
**REQ-4.1.4:** The system shall read all worksheets within the input Excel file  
**REQ-4.1.5:** The system shall handle files with multiple data sheets appropriately

### 4.2 Event Column Management

**REQ-4.2.1:** The system shall accept an event column name parameter via command line (--event-column)  
**REQ-4.2.2:** The system shall accept an event value parameter via command line (--event-value)  
**REQ-4.2.3:** The system shall create or update the specified event column for all records  
**REQ-4.2.4:** The system shall default event value to "Yes" if not specified  
**REQ-4.2.5:** The system shall preserve existing event column data if present

### 4.3 Data Cleansing Operations

**REQ-4.3.1:** The system shall implement safe string conversion for all data fields  
**REQ-4.3.2:** The system shall standardize address formats according to Wild Apricot requirements  
**REQ-4.3.3:** The system shall normalize phone number formats  
**REQ-4.3.4:** The system shall validate and standardize email addresses  
**REQ-4.3.5:** The system shall convert data to proper case where appropriate  
**REQ-4.3.6:** The system shall handle null and empty values gracefully  
**REQ-4.3.7:** The system shall preserve data that is already in correct format

### 4.4 File Output Management

**REQ-4.4.1:** The system shall generate output files in the same directory as the input file  
**REQ-4.4.2:** The system shall append timestamp to output filenames in format YYYYMMDD_HHMMSS  
**REQ-4.4.3:** The system shall include "_cleaned" suffix in output filenames  
**REQ-4.4.4:** The system shall save output files in .xlsx format compatible with Wild Apricot import  
**REQ-4.4.5:** The system shall preserve the original input file without modification  
**REQ-4.4.6:** The system shall support --use-last-cleaned flag to process most recent cleaned file

### 4.5 Logging and Audit Trail

**REQ-4.5.1:** The system shall create a log file for each processing run  
**REQ-4.5.2:** The system shall use timestamp-based naming for log files (format: YYYYMMDD_HHMMSS)  
**REQ-4.5.3:** The system shall log all significant processing events with timestamp  
**REQ-4.5.4:** The system shall output log messages to both file and console (stdout)  
**REQ-4.5.5:** The system shall log information level messages by default  
**REQ-4.5.6:** The system shall include input file path, output file path, and processing statistics in logs  
**REQ-4.5.7:** The system shall log all data transformation operations performed  
**REQ-4.5.8:** The system shall record any errors or warnings encountered during processing

### 4.6 Command-Line Interface

**REQ-4.6.1:** The system shall accept the input file path as the first positional argument  
**REQ-4.6.2:** The system shall support --event-column parameter for specifying event field name  
**REQ-4.6.3:** The system shall support --event-value parameter for specifying event field value  
**REQ-4.6.4:** The system shall support --use-last-cleaned flag to process previous output  
**REQ-4.6.5:** The system shall provide help documentation via standard -h or --help flag

---

## 5. Technical Requirements

### 5.1 Programming Language and Version

**REQ-5.1.1:** The system shall be implemented in Python 3.8 or higher  
**REQ-5.1.2:** The system shall use type hints where appropriate for code clarity

### 5.2 Dependencies

**REQ-5.2.1:** The system shall use the following Python libraries:
- argparse: Command-line argument parsing
- datetime: Timestamp generation
- glob: File pattern matching
- logging: Logging functionality
- openpyxl: Excel .xlsx file operations
- pandas: Data manipulation and analysis
- xlrd: Excel .xls file reading
- os: Operating system interface
- re: Regular expression operations
- sys: System-specific parameters

**REQ-5.2.2:** All dependencies shall be standard library or commonly available via pip

### 5.3 Performance Requirements

**REQ-5.3.1:** The system shall process files containing 1000 records in under 5 minutes  
**REQ-5.3.2:** The system shall handle files up to 50MB in size  
**REQ-5.3.3:** Memory usage shall not exceed 500MB for typical operations  
**REQ-5.3.4:** The system shall provide progress indicators for long-running operations

### 5.4 Data Validation

**REQ-5.4.1:** The system shall validate input file format before processing  
**REQ-5.4.2:** The system shall check for required columns in input data  
**REQ-5.4.3:** The system shall identify and report malformed data  
**REQ-5.4.4:** The system shall provide clear error messages for validation failures

### 5.5 Error Handling

**REQ-5.5.1:** The system shall handle file not found errors gracefully  
**REQ-5.5.2:** The system shall handle permission errors for file access  
**REQ-5.5.3:** The system shall catch and log all exceptions with meaningful messages  
**REQ-5.5.4:** The system shall exit with appropriate error codes on failure  
**REQ-5.5.5:** The system shall continue processing when non-critical errors occur

---

## 6. Data Requirements

### 6.1 Input Data Format

**REQ-6.1.1:** Input files shall be Excel workbooks (.xlsx or .xls)  
**REQ-6.1.2:** Input data shall contain contact information fields  
**REQ-6.1.3:** Input data may contain multiple rows per contact  
**REQ-6.1.4:** Input data shall include standard contact fields (name, address, email, phone)

### 6.2 Output Data Format

**REQ-6.2.1:** Output files shall be Excel workbooks in .xlsx format  
**REQ-6.2.2:** Output data shall conform to Wild Apricot import specifications  
**REQ-6.2.3:** Output data shall include all required fields for Wild Apricot contacts  
**REQ-6.2.4:** Output data shall preserve original data structure where possible  
**REQ-6.2.5:** Output data shall include the populated event column

### 6.3 Wild Apricot Column Standards

**REQ-6.3.1:** The system shall maintain compatibility with Wild Apricot CMS column naming conventions  
**REQ-6.3.2:** The system shall format data according to Wild Apricot field type requirements  
**REQ-6.3.3:** The system shall handle Wild Apricot-specific data constraints

---

## 7. User Interface Requirements

### 7.1 Command-Line Interface

**REQ-7.1.1:** The system shall provide a clear usage message when invoked with --help  
**REQ-7.1.2:** The system shall display progress information during processing  
**REQ-7.1.3:** The system shall show summary statistics upon completion  
**REQ-7.1.4:** The system shall use consistent formatting for all console output

### 7.2 Example Usage

The system shall support the following usage pattern:

```
python Generic_WildApricot_Data_Import_Cleanse.py "C:\path\to\file.xlsx" --event-column BulbSale2024 --event-value Yes --use-last-cleaned
```

---

## 8. Security Requirements

**REQ-8.1:** The system shall not transmit data over networks  
**REQ-8.2:** The system shall validate all input parameters to prevent injection attacks  
**REQ-8.3:** The system shall handle file paths securely to prevent directory traversal  
**REQ-8.4:** The system shall not store sensitive data in log files  
**REQ-8.5:** The system shall set appropriate file permissions on output files

---

## 9. Quality Assurance Requirements

### 9.1 Code Quality

**REQ-9.1.1:** Code shall follow PEP 8 Python style guidelines  
**REQ-9.1.2:** Functions shall include docstrings describing purpose and parameters  
**REQ-9.1.3:** Code shall include inline comments for complex logic  
**REQ-9.1.4:** Variable and function names shall be descriptive and meaningful

### 9.2 Testing Requirements

**REQ-9.2.1:** The system shall be tested with sample data from actual events  
**REQ-9.2.2:** The system shall be tested with edge cases (empty files, malformed data)  
**REQ-9.2.3:** The system shall be tested with both .xlsx and .xls file formats  
**REQ-9.2.4:** Output files shall be validated for successful import into Wild Apricot

---

## 10. Documentation Requirements

**REQ-10.1:** Source code shall include header comments with author, date, and purpose  
**REQ-10.2:** Source code shall document all dependencies and versions  
**REQ-10.3:** Source code shall include usage examples in comments  
**REQ-10.4:** Source code shall maintain a change log with dates, author, and descriptions  
**REQ-10.5:** A separate user guide shall be provided for end users

---

## 11. Maintenance and Support Requirements

**REQ-11.1:** The system shall be designed for easy modification by SQL/PL/SQL developers expanding into Python  
**REQ-11.2:** The code structure shall support addition of new data cleansing rules  
**REQ-11.3:** The system shall support addition of new event types without code changes  
**REQ-11.4:** Configuration parameters shall be easily modifiable  
**REQ-11.5:** The system shall be compatible with future Wild Apricot schema changes through parameter updates

---

## 12. Assumptions and Constraints

### 12.1 Assumptions

1. Users have Python 3.8 or higher installed on their systems
2. Users have necessary permissions to read input files and write output files
3. Input files are from trusted sources (event registrations, attendee lists)
4. Wild Apricot CMS import functionality remains consistent with current version
5. Excel file formats (.xlsx, .xls) will remain supported

### 12.2 Constraints

1. Processing must be completed locally without cloud services
2. System must work on Windows operating systems primarily
3. No database server dependencies
4. Must maintain backward compatibility with existing event data formats
5. Cannot modify Wild Apricot CMS schema or import process

---

## 13. Future Enhancements

The following enhancements may be considered for future versions:

1. Direct API integration with Wild Apricot CMS
2. Web-based user interface for non-technical users
3. Automated scheduling for batch processing
4. Advanced duplicate detection using fuzzy matching
5. Export to additional formats (CSV, JSON)
6. Configuration file support for frequently used parameters
7. Email notification upon processing completion
8. Data quality scoring and reporting
9. Integration with other CMS platforms
10. Machine learning for intelligent data standardization

---

## 14. Revision History

| Version | Date | Author | Description |
|---------|------|--------|-------------|
| 1.0 | 12/10/2025 | C. Williams | Initial requirements document based on version dated 10/28/2025 |

---

## 15. Approval

This requirements document shall be reviewed and approved by:

- **Technical Lead:** _____________________ Date: _________
- **Product Owner:** _____________________ Date: _________
- **Quality Assurance:** _____________________ Date: _________

---

## Appendix A: Example Command-Line Invocations

### Basic Usage
```bash
python Generic_WildApricot_Data_Import_Cleanse.py "input_file.xlsx"
```

### With Event Column Specification
```bash
python Generic_WildApricot_Data_Import_Cleanse.py "Bulb Sale 2024.xlsx" --event-column BulbSale2024 --event-value Yes
```

### Processing Previous Output
```bash
python Generic_WildApricot_Data_Import_Cleanse.py "original_file.xlsx" --use-last-cleaned
```

### Full Parameter Example
```bash
python Generic_WildApricot_Data_Import_Cleanse.py "C:\Users\Charl\OneDrive\Documents\Development\Python\DBG\Bulb Sale 2024 ccw.xlsx" --event-column BulbSale2024 --event-value Yes --use-last-cleaned
```

---

## Appendix B: Glossary

**CMS:** Content Management System  
**Wild Apricot:** Cloud-based membership management and website platform  
**XLSX:** Microsoft Excel Open XML Spreadsheet format  
**XLS:** Microsoft Excel 97-2003 Worksheet format  
**PEP 8:** Python Enhancement Proposal 8, Python's style guide  
**Pandas:** Python data analysis library  
**OpenPyxl:** Python library for reading/writing Excel 2010 xlsx/xlsm files  
**XLRD:** Python library for reading Excel files  
**Argparse:** Python module for command-line option and argument parsing  
**Event Column:** Custom field in Wild Apricot to track event participation  

---

*End of Requirements Document*
