# Basic imports - nothing fancy here
import gzip
import json
import os

# File paths - hardcoded for simplicity
SOURCE_FILE = "data/source/nhtsa_file.jsonl.gz"
BRONZE_FILE = "data/bronze/complete_nhtsa_data.json"
SILVER_FILE = "data/silver/filtered_nhtsa_data.json"

# Default record structure
DEFAULT_RECORD = {
    "Sent_VIN": "",
    "Manufacturer_Name": "",
    "Make": "",
    "Model": "",
    "Model_Year": "",
    "TRIM": "",
    "Vehicle_Type_Id": "",
    "Body_Class_Id": "",
    "Base_Price": "",
    "NCSA_Make": "",
    "NCSA_Model": "",
}

def read_source_data():
    """Read the compressed file - had to figure out the format first"""
    records = []
    try:
        with gzip.open(SOURCE_FILE, 'rt', encoding='utf-8') as file:
            for line_num, line in enumerate(file, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    data = json.loads(line)
                    # Sometimes it's a list, sometimes just one record
                    if isinstance(data, list):
                        records.extend(data)
                    else:
                        records.append(data)
                except json.JSONDecodeError as e:
                    # Skip bad lines - there might be some
                    continue
                    
    except FileNotFoundError:
        print(f"Error: Could not find input file {SOURCE_FILE}")
        raise
    except Exception as e:
        print(f"Unexpected error reading source: {e}")
        raise
        
    return records

def save_to_file(data, file_path, description):
    """Save data to file with directory creation"""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"{description}: {len(data)} records written to {file_path}")

def write_to_bronze():
    """Write complete raw data to bronze layer"""
    # Just dump everything from source - no processing yet
    records = read_source_data()
    save_to_file(records, BRONZE_FILE, "Bronze stage completed")

def write_to_silver():
    """Write processed and deduplicated data to silver layer"""
    # Read from bronze layer - this is the data lake pattern
    try:
        with open(BRONZE_FILE, 'r', encoding='utf-8') as f:
            records = json.load(f)
    except FileNotFoundError:
        print(f"Error: Bronze file not found at {BRONZE_FILE}")
        print("Run write_to_bronze() first to create the bronze layer")
        raise
    
    processed_records = []
    seen_vins = set()
    
    for record in records:
        processed = extract_vehicle_data(record)
        vin = processed["Sent_VIN"]
        
        # Only keep unique VINs - this is the deduplication part
        if not vin or vin not in seen_vins:
            if vin:
                seen_vins.add(vin)
            processed_records.append(processed)
    
    save_to_file(processed_records, SILVER_FILE, "Silver stage completed")

def extract_vehicle_data(data):
    """Extract the required vehicle data fields from a single NHTSA record"""
    record = DEFAULT_RECORD.copy()
    
    # Extract VIN from SearchCriteria - this was tricky to figure out
    search_criteria = data.get("SearchCriteria", "")
    if search_criteria and "VIN:" in search_criteria:
        vin_part = search_criteria.split("VIN:")[-1].strip()
        if len(vin_part) >= 11:
            record["Sent_VIN"] = vin_part[:11]
    
    # Map the field names to what we need
    field_mapping = {
        "Manufacturer Name": "Manufacturer_Name",
        "Make": "Make", 
        "Model": "Model",
        "Model Year": "Model_Year",
        "Trim": "TRIM",
        "Vehicle Type": "Vehicle_Type_Id",
        "Body Class": "Body_Class_Id",
        "Base Price ($)": "Base_Price",
        "NCSA Make": "NCSA_Make",
        "NCSA Model": "NCSA_Model"
    }
    
    # Go through all the results and extract what we need
    for result in data.get("Results", []):
        variable = result.get("Variable", "")
        if variable in field_mapping:
            # For ID fields, use ValueId; for text fields, use Value
            # Took me a while to figure this out
            if variable in ["Vehicle Type", "Body Class"]:
                record[field_mapping[variable]] = result.get("ValueId", "") or ""
            else:
                record[field_mapping[variable]] = result.get("Value", "") or ""
    
    return record

# Backward compatibility functions for Airflow DAG
def parse_nhtsa_file():
    """Legacy function - redirects to write_to_silver for backward compatibility"""
    write_to_silver()

# Main execution
if __name__ == "__main__":
    print("Starting NHTSA data pipeline...")
    
    try:
        write_to_bronze()
        write_to_silver()
        print("All pipeline stages completed!")
    except Exception as e:
        print(f"Pipeline failed: {e}")

