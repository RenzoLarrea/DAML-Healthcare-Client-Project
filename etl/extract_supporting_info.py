import json
import csv
import os
from typing import Any, List, Dict, Union


def extract_column_data(data: Any, column_name: str, path: str = "") -> List[Dict[str, str]]:
    """
    Recursively extract all values for a given column name from JSON data.
    
    Args:
        data: JSON data (dict, list, or primitive)
        column_name: The column/key name to search for
        path: Current path in the JSON structure
    
    Returns:
        List of dictionaries containing the path and value for each match
    """
    results = []
    
    if isinstance(data, dict):
        for key, value in data.items():
            current_path = f"{path}.{key}" if path else key
            
            # If we found the column we're looking for
            if key == column_name:
                results.append({
                    'path': current_path,
                    'value': str(value) if value is not None else ''
                })
            
            # Recursively search in nested structures
            results.extend(extract_column_data(value, column_name, current_path))
    
    elif isinstance(data, list):
        for i, item in enumerate(data):
            current_path = f"{path}[{i}]" if path else f"[{i}]"
            results.extend(extract_column_data(item, column_name, current_path))
    
    return results


def json_to_csv(json_filename: str, column_name: str, output_filename: str = None):
    """
    Extract data from a JSON file for a specific column and save to CSV.
    
    Args:
        json_filename: Path to the JSON file
        column_name: The column/key name to extract
        output_filename: Optional output CSV filename (defaults to column_name.csv)
    """
    try:
        # Read JSON file
        print(f"Reading JSON file: {json_filename}")
        with open(json_filename, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # Extract column data
        print(f"Extracting data for column: {column_name}")
        extracted_data = extract_column_data(data, column_name)
        
        if not extracted_data:
            print(f"No data found for column '{column_name}'")
            return
        
        # Generate output filename if not provided
        if output_filename is None:
            base_name = os.path.splitext(os.path.basename(json_filename))[0]
            output_filename = f"{base_name}_{column_name}.csv"
        
        # Write to CSV
        print(f"Writing {len(extracted_data)} records to CSV: {output_filename}")
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['path', 'value']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for row in extracted_data:
                writer.writerow(row)
        
        print(f"Successfully created CSV file: {output_filename}")
        print(f"Found {len(extracted_data)} instances of '{column_name}'")
        
        # Show first few examples
        if extracted_data:
            print("\nFirst few examples:")
            for i, item in enumerate(extracted_data[:5]):
                print(f"  {i+1}. Path: {item['path']}")
                print(f"     Value: {item['value'][:100]}{'...' if len(item['value']) > 100 else ''}")
                print()
    
    except FileNotFoundError:
        print(f"Error: File '{json_filename}' not found")
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format - {e}")
    except Exception as e:
        print(f"Error: {e}")


def main():
    """
    Main function with hardcoded parameters for JSON to CSV extraction.
    Modify the filename and column_name variables below as needed.
    """
    # Hardcoded parameters - modify these as needed
    filename = "formatted_EOB_response.json"  # Change this to your JSON filename
    column_name = "supportingInfo"  # Change this to the column you want to extract
    
    # Optional: specify output filename (leave as None for auto-generation)
    output_filename = None
    
    print("=" * 60)
    print("JSON to CSV Extraction Tool")
    print("=" * 60)
    print(f"Input file: {filename}")
    print(f"Column to extract: {column_name}")
    print("=" * 60)
    
    # Perform the extraction
    json_to_csv(filename, column_name, output_filename)


if __name__ == "__main__":
    main()
