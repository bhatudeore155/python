import pandas as pd
import numpy as np
from datetime import datetime

# Extract
def extract_data(file_path):
    """Load data from CSV"""
    return pd.read_csv(file_path)

# Transform
def transform_data(df):
    """Clean and validate data"""
    df = df.copy()
    
    # Remove duplicates
    df = df.drop_duplicates()
    
    # Handle missing values
    df['amount'] = df['amount'].fillna(df['amount'].median())
    df['quantity'] = df['quantity'].fillna(0).astype(int)
    df['date'] = df['date'].fillna(datetime.now().date())
    
    # Remove invalid rows
    df = df[df['name'] != 'unknown_user']
    df = df[df['amount'] > 0]
    
    # Convert data types
    df['amount'] = df['amount'].astype(float)
    df['date'] = pd.to_datetime(df['date'])
    df1 = df.filter(items=['name', 'amount', 'quantity', 'date'])
    # Reorder columns
    df1 = df1[['name', 'date', 'quantity', 'amount']]
    
    return df1

# Load
def load_data(df, output_path):
    """Save cleaned data"""
    df.to_csv(output_path, index=False)
    print(f"Data loaded to {output_path}")

# Main ETL Pipeline
if __name__ == "__main__":
    input_file = "data/input.csv"
    output_file = "data/output.csv"
    
    print("Starting ETL pipeline...")
    
    # Extract
    raw_data = extract_data(input_file)
    print(f"Extracted {len(raw_data)} rows")
    
    # Transform
    clean_data = transform_data(raw_data)
    print(f"Transformed to {len(clean_data)} clean rows")
    
    # Load
    load_data(clean_data, output_file)
    
    print("ETL pipeline completed!")


