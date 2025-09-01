import pandas as pd

from config.config import snowflake_to_talend_type

import re
from xml.etree.ElementTree import Element, SubElement, tostring, ElementTree
import xml.dom.minidom

def read_schema(file_path):
    try:
        if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
            with pd.ExcelFile(file_path, engine='openpyxl') as xls:
                df = pd.read_excel(xls)
        elif file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            print(f"[ERROR] Unsupported file format: {file_path}")
            return None
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return None
    except Exception as e:
        print(f"Error reading the file: {e}")
        return None

    required_columns = ['Column_Name', 'Column_Type']
    if not all(col in df.columns for col in required_columns):
        print(f"[ERROR] Missing required columns: {required_columns}")
        return None

    # Remove records where both required columns are missing
    df = df.dropna(subset=required_columns, how='all')
    print(f"Length of dataframe after removing records with both columns missing: {len(df)}")

    # Trim white spaces for both columns
    df['Column_Name'] = df['Column_Name'].str.strip()
    df['Column_Type'] = df['Column_Type'].str.strip()

    # Check for rows where any of the required columns is empty
    for _, row in df.iterrows():
        if pd.isna(row['Column_Name']) or pd.isna(row['Column_Type']):
            print(f"[WARNING] Incomplete record found: {row.to_dict()}")
            print("[WARNING] Please correct the input file and try again.")
            return None

    return df[required_columns]

def parse_type(type_str):
    # Example type_str: 'VARCHAR(20,2)' or 'NUMBER' or 'BOOLEAN'
    match = re.match(r'(\w+)(?:\((\d+)?(?:,(\d+))?\))?', type_str)
    if match:
        snowflake_type = match.group(1).upper()
        length = match.group(2) if match.group(2) else '-1'
        precision = match.group(3) if match.group(3) else '-1'
        talend_type = snowflake_to_talend_type.get(snowflake_type, 'id_String')  # Default to 'id_String' if not found
        return talend_type, length, precision
    return 'id_String', '-1', '-1'

def generate_xml(df, output_file):
    root = Element('schema')
    for _, row in df.iterrows():
        col_name = row['Column_Name']
        col_type = row['Column_Type']
        talend_type, length, precision = parse_type(col_type)
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
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        for line in pretty_xml.splitlines()[1:]:
            f.write(line + '\n')

if __name__ == "__main__":
    print("--"*50)
    schema_file = "data/real_example.xlsx"
    schema_df = read_schema(schema_file)

    if schema_df is not None:
        print(schema_df.head())

    # Generate XML from schema
    generate_xml(schema_df, 'xml_schema2.xml')
    print("XML schema generated as xml_schema2.xml")
    print("--"*50)