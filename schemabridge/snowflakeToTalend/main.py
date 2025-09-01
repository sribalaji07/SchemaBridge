import pandas as pd

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import snowflake_to_talend_type

import re
from xml.etree.ElementTree import Element, SubElement, tostring, ElementTree
import xml.dom.minidom

def read_schema(file_path, log_list=None):
    if log_list is None:
        log_list = []
    try:
        if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
            log_list.append(f"[INFO] Reading Excel file: {file_path}")
            with pd.ExcelFile(file_path, engine='openpyxl') as xls:
                df = pd.read_excel(xls)
        elif file_path.endswith('.csv'):
            log_list.append(f"[INFO] Reading CSV file: {file_path}")
            df = pd.read_csv(file_path)
        else:
            log_list.append(f"[ERROR] Unsupported file format: {file_path}")
            return None, log_list
    except FileNotFoundError:
        log_list.append(f"[ERROR] File not found: {file_path}")
        return None, log_list
    except Exception as e:
        log_list.append(f"[ERROR] Error reading the file: {e}")
        return None, log_list

    required_columns = ['Column_Name', 'Column_Type']
    if not all(col in df.columns for col in required_columns):
        log_list.append(f"[ERROR] Missing required columns: {required_columns}")
        return None, log_list

    # Remove records where both required columns are missing
    df = df.dropna(subset=required_columns, how='all')
    log_list.append(f"[INFO] Length of dataframe after removing records with both columns missing: {len(df)}")

    # Trim white spaces for both columns
    df['Column_Name'] = df['Column_Name'].str.strip()
    df['Column_Type'] = df['Column_Type'].str.strip()

    # Check for rows where any of the required columns is empty
    for _, row in df.iterrows():
        if pd.isna(row['Column_Name']) or pd.isna(row['Column_Type']):
            log_list.append(f"[WARNING] Incomplete record found: {row.to_dict()}")
            log_list.append("[WARNING] Please correct the input file and try again.")
            return None, log_list

    log_list.append(f"[INFO] Schema read successfully with {len(df)} records.")
    return df[required_columns], log_list

def parse_type(type_str, log_list=None):
    if log_list is None:
        log_list = []
    # Example type_str: 'VARCHAR(20,2)' or 'NUMBER' or 'BOOLEAN'
    match = re.match(r'(\w+)(?:\((\d+)?(?:,(\d+))?\))?', type_str)
    if match:
        snowflake_type = match.group(1).upper()
        length = match.group(2) if match.group(2) else '-1'
        precision = match.group(3) if match.group(3) else '-1'
        talend_type = snowflake_to_talend_type.get(snowflake_type, 'id_String')  # Default to 'id_String' if not found
        return talend_type, length, precision, log_list
    log_list.append(f"[WARNING] Could not parse type string: '{type_str}', defaulting to 'id_String'.")
    return 'id_String', '-1', '-1', log_list


def generate_xml(df, log_list=None):
    if log_list is None:
        log_list = []
    log_list.append(f"[INFO] Generating XML from DataFrame...")
    root = Element('schema')
    for _, row in df.iterrows():
        col_name = row['Column_Name']
        col_type = row['Column_Type']
        talend_type, length, precision, log_list = parse_type(col_type, log_list)
        attribs = {
            'comment': '',
            'default': '',
            'key': 'false',
            'label': col_name,
            'length': length,
            'nullable': 'true',
            'originalDbColumnName': col_name,
            'originalLength': '-1',
            'pattern': '',
            'precision': precision,
            'talendType': talend_type
        }
        SubElement(root, 'column', attrib=attribs)
    xml_str = tostring(root, encoding='utf-8')
    pretty_xml = xml.dom.minidom.parseString(xml_str).toprettyxml(indent="    ")
    log_list.append(f"[INFO] XML generation complete. {len(df)} columns processed.")
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + '\n'.join(pretty_xml.splitlines()[1:]), log_list

def process_excel_schema(input_path, output_path=None):
    log_list = []
    log_list.append(f"[INFO] Starting schema processing for: {input_path}")
    df, log_list = read_schema(input_path, log_list)
    if df is None:
        log_list.append(f"[ERROR] Schema processing failed for: {input_path}")
        return None, log_list
    xml_content, log_list = generate_xml(df, log_list)
    if output_path:
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(xml_content)
            log_list.append(f"[INFO] XML written to: {output_path}")
        except Exception as e:
            log_list.append(f"[ERROR] Failed to write XML to file: {e}")
    log_list.append(f"[INFO] Schema processing complete for: {input_path}")
    return xml_content, log_list

"""
This module is intended to be imported and used by other code (e.g., Django views).
Always pass the file path of the uploaded file to process_excel_schema(input_path, output_path).
No hardcoded file paths should be used here.
"""