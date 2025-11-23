
import json
import pandas as pd
import os

def main():
    json_file_path = 'street_data.json'

    if not os.path.exists(json_file_path):
        print(f"Error: {json_file_path} not found.")
        return

    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not data:
        print("The JSON file is empty or contains no data.")
        return

    df = pd.DataFrame(data)

    print("--- Summary Statistics of Extracted Street Data ---")
    print(f"Total number of street entries: {len(df)}")
    print(f"Number of unique street names: {df['street_name'].nunique()}")
    print(f"Number of unique settlements: {df['settlement'].nunique()}")

    print("\nStatus Distribution:")
    if 'status' in df.columns:
        status_counts = df['status'].value_counts(dropna=False)
        print(status_counts.to_string())
    else:
        print("No 'status' column found.")

    print("\nScore Statistics:")
    if 'score' in df.columns and pd.api.types.is_numeric_dtype(df['score']):
        print(df['score'].describe().to_string())
    else:
        print("No numeric 'score' column found or all values are missing.")

if __name__ == '__main__':
    main()
