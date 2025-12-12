"""
Utility functions for the service call processing pipeline
"""
import os
import json
import pandas as pd
from datetime import datetime


def load_json(filepath):
    """Load JSON file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(data, filepath):
    """Save data to JSON file"""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_excel(filepath):
    """Load Excel file with error handling"""
    try:
        df = pd.read_excel(filepath)
        print(f"✓ Loaded {filepath}")
        print(f"  Rows: {len(df):,}")
        print(f"  Columns: {len(df.columns)}")
        return df
    except FileNotFoundError:
        print(f"✗ Error: File '{filepath}' not found!")
        return None
    except Exception as e:
        print(f"✗ Error loading file: {e}")
        return None


def save_excel(df, filepath, create_backup=True):
    """Save DataFrame to Excel with backup and versioning"""
    # Create backup if file exists
    if create_backup and os.path.exists(filepath):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = filepath.replace('.xlsx', f'_backup_{timestamp}.xlsx')
        os.rename(filepath, backup_path)
        print(f"📦 Backup created: {backup_path}")
    
    # Handle file versioning if it already exists
    counter = 1
    original_filepath = filepath
    while os.path.exists(filepath):
        base = original_filepath.replace('.xlsx', '')
        filepath = f"{base}_{counter}.xlsx"
        counter += 1
    
    try:
        df.to_excel(filepath, index=False)
        print(f"✓ Saved: {filepath}")
        return filepath
    except Exception as e:
        # Fallback to CSV
        csv_path = filepath.replace('.xlsx', '.csv')
        print(f"✗ Could not save Excel: {e}")
        print(f"  Attempting CSV: {csv_path}")
        df.to_csv(csv_path, index=False)
        print(f"✓ Saved as CSV: {csv_path}")
        return csv_path


def find_new_service_orders(new_df, processed_df, id_column='SERVICE_ORDER'):
    """
    Find SERVICE_ORDERs in new_df that don't exist in processed_df
    
    Args:
        new_df: DataFrame with all service calls (including new ones)
        processed_df: DataFrame with already processed calls
        id_column: Column name to use for comparison (default: 'SERVICE_ORDER')
    
    Returns:
        DataFrame containing only new/unprocessed service calls
    """
    if processed_df is None or processed_df.empty:
        print("ℹ No processed file found. Processing all records.")
        return new_df
    
    # Get existing SERVICE_ORDERs
    existing_orders = set(processed_df[id_column].unique())
    new_orders = set(new_df[id_column].unique())
    
    # Find orders that don't exist in processed file
    orders_to_process = new_orders - existing_orders
    
    print(f"\n📊 Incremental Processing Analysis:")
    print(f"  Existing orders: {len(existing_orders):,}")
    print(f"  New orders in file: {len(new_orders):,}")
    print(f"  Orders to process: {len(orders_to_process):,}")
    
    if not orders_to_process:
        print("✓ All service orders already processed!")
        return pd.DataFrame()
    
    # Filter to only new orders
    new_records_df = new_df[new_df[id_column].isin(orders_to_process)].copy()
    
    print(f"  New records to process: {len(new_records_df):,}")
    
    return new_records_df


def merge_processed_data(existing_df, new_df):
    """
    Merge newly processed data with existing processed data
    
    Args:
        existing_df: DataFrame with previously processed data
        new_df: DataFrame with newly processed data
    
    Returns:
        Combined DataFrame
    """
    if existing_df is None or existing_df.empty:
        return new_df
    
    # Concatenate
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    
    print(f"\n📊 Merge Results:")
    print(f"  Existing records: {len(existing_df):,}")
    print(f"  New records: {len(new_df):,}")
    print(f"  Combined records: {len(combined_df):,}")
    
    return combined_df


def validate_dataframe(df, required_columns):
    """
    Validate that DataFrame has required columns
    
    Args:
        df: DataFrame to validate
        required_columns: List of column names that must exist
    
    Returns:
        Tuple (is_valid, missing_columns)
    """
    missing = [col for col in required_columns if col not in df.columns]
    
    if missing:
        print(f"\n⚠ Warning: Missing required columns:")
        for col in missing:
            print(f"  ✗ {col}")
        return False, missing
    
    print(f"✓ All required columns present")
    return True, []


def print_summary(df, title="Data Summary"):
    """Print a nice summary of the DataFrame"""
    print(f"\n{'='*60}")
    print(title.center(60))
    print(f"{'='*60}")
    print(f"Total Rows: {len(df):,}")
    print(f"Total Columns: {len(df.columns)}")
    
    # Show sample columns
    if len(df.columns) <= 10:
        print(f"Columns: {', '.join(df.columns)}")
    else:
        print(f"First 5 Columns: {', '.join(df.columns[:5])}")
        print(f"Last 5 Columns: {', '.join(df.columns[-5:])}")
    
    print(f"{'='*60}\n")


def create_output_filename(base_name, suffix="", timestamp=True):
    """
    Create output filename with optional timestamp
    
    Args:
        base_name: Base name for the file (without extension)
        suffix: Optional suffix to add
        timestamp: Whether to add timestamp
    
    Returns:
        Filename string
    """
    parts = [base_name]
    
    if suffix:
        parts.append(suffix)
    
    if timestamp:
        parts.append(datetime.now().strftime("%Y%m%d_%H%M%S"))
    
    return "_".join(parts) + ".xlsx"
