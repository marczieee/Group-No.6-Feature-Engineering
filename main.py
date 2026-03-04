import os
import sys
import pandas as pd
import hashlib
import json
from datetime import datetime

from derive_computed_columns      import derive_computed_columns
from encode_categorical_features  import encode_categorical_features
from bin_numeric_ranges           import bin_numeric_ranges
from time_based_feature_extraction import time_based_feature_extraction
from flag_anomalies_column        import flag_anomalies_column

INPUT_FILE = "input/data.csv"
HASH_FILE = "input/.data_hash.json"

OUTPUT_FILES = {
    "derived_computed_columns"     : "output/derived_computed_columns.csv",
    "encoded_categorical_features" : "output/encoded_categorical_features.csv",
    "binned_numeric_ranges"        : "output/binned_numeric_ranges.csv",
    "time_based_features"          : "output/time_based_features.csv",
    "flagged_anomalies"            : "output/flagged_anomalies.csv",
    "consolidated_all_features"    : "output/consolidated_all_features.csv",
}

def get_input_hash():
    """Calculate hash of input file to detect changes"""
    if not os.path.exists(INPUT_FILE):
        return None
    df = pd.read_csv(INPUT_FILE)
    return hashlib.md5(df.to_string().encode()).hexdigest()

def has_input_changed():
    """Check if input file has changed since last run"""
    current_hash = get_input_hash()
    if not current_hash:
        return True
    
    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, 'r') as f:
            stored_hash = json.load(f).get('hash')
        return current_hash != stored_hash
    return True

def save_input_hash():
    """Save current input hash for next run"""
    current_hash = get_input_hash()
    if current_hash:
        with open(HASH_FILE, 'w') as f:
            json.dump({'hash': current_hash, 'timestamp': datetime.now().isoformat()}, f)

def run_pipeline():
    print("=" * 55)
    print("  Group 6 — Feature Engineering CSV Pipeline")
    print("=" * 55)

    if not os.path.exists(INPUT_FILE):
        print(f"\n❌ ERROR: Input file '{INPUT_FILE}' not found!")
        print("   Please place your CSV file in the 'input/' folder.")
        sys.exit(1)

    
    input_changed = has_input_changed()
    if not input_changed:
        print("\n⚠️  Input file hasn't changed since last run.")
        response = input("Do you still want to process? (y/n): ")
        if response.lower() != 'y':
            print("❌ Pipeline cancelled.")
            return

    print(f"\n📂 Input  : {INPUT_FILE}")
    print(f"📁 Output : output/\n")

    os.makedirs("output", exist_ok=True)

    print("\n" + "-" * 55)
    print("📊 Running Function 1: Derive Computed Columns")
    derive_computed_columns(
        INPUT_FILE,
        OUTPUT_FILES["derived_computed_columns"]
    )

    print("\n" + "-" * 55)
    print("📊 Running Function 2: Encode Categorical Features")
    encode_categorical_features(
        INPUT_FILE,
        OUTPUT_FILES["encoded_categorical_features"]
    )

    print("\n" + "-" * 55)
    print("📊 Running Function 3: Bin Numeric Ranges")
    bin_numeric_ranges(
        INPUT_FILE,
        OUTPUT_FILES["binned_numeric_ranges"]
    )

    print("\n" + "-" * 55)
    print("📊 Running Function 4: Time-Based Feature Extraction")
    time_based_feature_extraction(
        INPUT_FILE,
        OUTPUT_FILES["time_based_features"]
    )

    print("\n" + "-" * 55)
    print("📊 Running Function 5: Flag Anomalies Column")
    flag_anomalies_column(
        INPUT_FILE,
        OUTPUT_FILES["flagged_anomalies"]
    )

    print("\n" + "=" * 55)
    print("  ✅ Pipeline complete! All output files saved.")
    print("=" * 55)

    
    save_input_hash()

    print("\n📄 Output files generated:")
    for name, path in OUTPUT_FILES.items():
        if name != "consolidated_all_features":
            size = os.path.getsize(path) if os.path.exists(path) else 0
            print(f"   • {path}  ({size} bytes)")
    
    create_consolidated_csv()

def create_consolidated_csv():
    print("\n" + "=" * 55)
    print("  🔄 Creating Consolidated CSV File")
    print("=" * 55)
    
    output_files = [
        'output/derived_computed_columns.csv',
        'output/encoded_categorical_features.csv',
        'output/binned_numeric_ranges.csv',
        'output/time_based_features.csv',
        'output/flagged_anomalies.csv'
    ]
    
    all_files_exist = True
    for file in output_files:
        if not os.path.exists(file):
            print(f"❌ Cannot find {file}")
            all_files_exist = False
    
    if not all_files_exist:
        print("❌ Cannot create consolidated file - missing output files")
        return
    
    try:
        dataframes = []
        file_names = []
        
        for file in output_files:
            df = pd.read_csv(file)
            
            df = df.loc[:, ~df.columns.str.startswith('_')]
            dataframes.append(df)
            file_names.append(os.path.basename(file))
            print(f"✅ Read {os.path.basename(file)} - {len(df.columns)} columns")
        
        first_df = dataframes[0]
        merge_key = 'id' if 'id' in first_df.columns else first_df.columns[0]
        print(f"📊 Using '{merge_key}' as merge key")
        
        columns_to_drop = ['name', 'age', 'salary', 'department', 'join_date', 'score', 'category']
        
        consolidated = first_df.copy()
        print(f"📌 Base dataframe: {file_names[0]} - kept all {len(consolidated.columns)} columns")
        
        for i, df in enumerate(dataframes[1:], 2):
            columns_to_keep = []
            for col in df.columns:
                if col == merge_key:
                    columns_to_keep.append(col)
                elif col not in columns_to_drop:
                    columns_to_keep.append(col)
                else:
                    print(f"  ⏩ Removing duplicate column: '{col}' from {file_names[i-1]}")
            
            if len(columns_to_keep) > 1:
                consolidated = pd.merge(
                    consolidated, 
                    df[columns_to_keep], 
                    on=merge_key, 
                    how='outer'
                )
                print(f"  ✓ Merged {file_names[i-1]} - added {len(columns_to_keep)-1} new columns")
            else:
                print(f"  ⚠️ No new columns in {file_names[i-1]}, skipping merge")
        
        
        consolidated['_report_generated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        output_path = OUTPUT_FILES["consolidated_all_features"]
        consolidated.to_csv(output_path, index=False)
        
        print("\n" + "-" * 55)
        print(f"✅ CONSOLIDATED CSV GENERATED SUCCESSFULLY!")
        print(f"📁 Location: {output_path}")
        print(f"📊 Total columns: {len(consolidated.columns)}")
        print(f"📊 Total rows: {len(consolidated)}")
        
        file_size = os.path.getsize(output_path)
        print(f"💾 File size: {file_size} bytes ({file_size/1024:.2f} KB)")
        
        all_columns = list(consolidated.columns)
        print(f"\n📋 First 15 columns: {', '.join(all_columns[:15])}")
        if len(all_columns) > 15:
            print(f"   ... and {len(all_columns) - 15} more columns")
        print(f"\n🕒 Report generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"❌ Error creating consolidated CSV: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_pipeline()
