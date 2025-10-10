import boto3
import pandas as pd
from io import BytesIO

def analyze_s3_data():
    s3 = boto3.client('s3')
    bucket = 'charlie-muni-pg-backup'
    prefix = 'vehicle_records/2025/'
    
    # List all parquet files
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    if 'Contents' not in response:
        print("No files found!")
        return
    
    # Read all parquet files into a list
    dfs = []
    print("Reading parquet files...\n")
    
    for obj in response['Contents']:
        file_key = obj['Key']
        if not file_key.endswith('.parquet'):
            continue
            
        print(f"Reading {file_key}...")
        print(f"  Size: {obj['Size'] / 1024:.2f} KB")
        
        obj_data = s3.get_object(Bucket=bucket, Key=file_key)
        parquet_bytes = obj_data['Body'].read()
        df = pd.read_parquet(BytesIO(parquet_bytes))
        
        print(f"  Rows: {len(df)}")
        print(f"  Columns: {list(df.columns)}")
        
        if len(df) == 0:
            print(f"  WARNING: This file is EMPTY!")
        else:
            print(f"  Sample data:\n{df.head(2)}")
            
        dfs.append(df)
        print()
    
    if not dfs or all(len(df) == 0 for df in dfs):
        print("\n❌ ERROR: All dataframes are empty! No data to analyze.")
        return
    
    # Combine all dataframes
    print(f"Combining {len(dfs)} files...")
    combined_df = pd.concat(dfs, ignore_index=True)
    
    if len(combined_df) == 0:
        print("\n❌ ERROR: Combined dataframe is empty!")
        return
    
    print("\n" + "="*80)
    print("ANALYSIS RESULTS")
    print("="*80 + "\n")
    
    # Total rows
    print(f"Total rows: {len(combined_df):,}")
    print(f"Columns: {list(combined_df.columns)}")
    
    # Try to find timestamp column
    timestamp_cols = [col for col in combined_df.columns if 'time' in col.lower()]
    
    if not timestamp_cols:
        print("\n⚠️  No timestamp column found. Available columns:")
        print(combined_df.dtypes)
        return
    
    timestamp_col = timestamp_cols[0]
    print(f"\nUsing timestamp column: '{timestamp_col}'")
    
    # Convert to datetime if not already
    combined_df[timestamp_col] = pd.to_datetime(combined_df[timestamp_col])
    
    # Min and Max timestamp
    min_timestamp = combined_df[timestamp_col].min()
    max_timestamp = combined_df[timestamp_col].max()
    
    print(f"\nMin timestamp: {min_timestamp}")
    print(f"Max timestamp: {max_timestamp}")
    print(f"Time range: {max_timestamp - min_timestamp}")
    
    # Average rows per unique timestamp
    rows_per_timestamp = combined_df.groupby(timestamp_col).size()
    avg_rows_per_timestamp = rows_per_timestamp.mean()
    
    print(f"\nUnique timestamps: {len(rows_per_timestamp):,}")
    print(f"Average rows per timestamp: {avg_rows_per_timestamp:.2f}")
    print(f"Min rows per timestamp: {rows_per_timestamp.min()}")
    print(f"Max rows per timestamp: {rows_per_timestamp.max()}")
    
    # Distribution
    print(f"\nRows per timestamp distribution:")
    print(rows_per_timestamp.describe())
    
    print("\n" + "="*80)

if __name__ == "__main__":
    analyze_s3_data()
