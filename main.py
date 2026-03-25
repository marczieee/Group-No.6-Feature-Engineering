import os
import sys
import pandas as pd
import hashlib
import json
from datetime import datetime
import glob

from derive_computed_columns      import derive_computed_columns
from encode_categorical_features  import encode_categorical_features
from bin_numeric_ranges           import bin_numeric_ranges
from time_based_feature_extraction import time_based_feature_extraction
from flag_anomalies_column        import flag_anomalies_column

INPUT_FOLDER = "input"
HASH_FILE = "input/.data_hash.json"
OUTPUT_FOLDER = "output"

def get_all_input_files():
    """Get all CSV files from input folder"""
    csv_files = glob.glob(os.path.join(INPUT_FOLDER, "*.csv"))
    # Filter out hidden files and hash file
    csv_files = [f for f in csv_files if not os.path.basename(f).startswith('.')]
    return sorted(csv_files)

def get_file_hash(filepath):
    """Calculate hash of a specific file"""
    if not os.path.exists(filepath):
        return None
    try:
        df = pd.read_csv(filepath)
        return hashlib.md5(df.to_string().encode()).hexdigest()
    except Exception as e:
        print(f"⚠️  Warning: Could not hash {filepath}: {e}")
        return None

def get_all_hashes():
    """Calculate hashes for all input files"""
    files = get_all_input_files()
    hashes = {}
    for filepath in files:
        file_hash = get_file_hash(filepath)
        if file_hash:
            hashes[filepath] = file_hash
    return hashes

def load_stored_hashes():
    """Load previously stored hashes"""
    if os.path.exists(HASH_FILE):
        try:
            with open(HASH_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_hashes(hashes):
    """Save current hashes with timestamp"""
    data = {
        'hashes': hashes,
        'timestamp': datetime.now().isoformat()
    }
    with open(HASH_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def get_files_to_process():
    """Determine which files need processing"""
    current_hashes = get_all_hashes()
    stored_data = load_stored_hashes()
    stored_hashes = stored_data.get('hashes', {})
    
    files_to_process = []
    
    for filepath, current_hash in current_hashes.items():
        if filepath not in stored_hashes or stored_hashes[filepath] != current_hash:
            files_to_process.append(filepath)
    
    return files_to_process, current_hashes

def is_ci_environment():
    """Detect if running in CI environment"""
    return os.getenv('CI') == 'true' or os.getenv('GITHUB_ACTIONS') == 'true'

def process_single_file(input_file, file_index=1, total_files=1):
    """Process a single CSV file through the pipeline"""
    
    filename = os.path.basename(input_file)
    file_prefix = os.path.splitext(filename)[0]
    
    print("\n" + "=" * 70)
    print(f"  Processing File {file_index}/{total_files}: {filename}")
    print("=" * 70)
    
    # Create output subdirectory for this file
    file_output_dir = os.path.join(OUTPUT_FOLDER, file_prefix)
    os.makedirs(file_output_dir, exist_ok=True)
    
    output_files = {
        "derived_computed_columns"     : os.path.join(file_output_dir, "derived_computed_columns.csv"),
        "encoded_categorical_features" : os.path.join(file_output_dir, "encoded_categorical_features.csv"),
        "binned_numeric_ranges"        : os.path.join(file_output_dir, "binned_numeric_ranges.csv"),
        "time_based_features"          : os.path.join(file_output_dir, "time_based_features.csv"),
        "flagged_anomalies"            : os.path.join(file_output_dir, "flagged_anomalies.csv"),
        "consolidated_all_features"    : os.path.join(file_output_dir, "consolidated_all_features.csv"),
    }
    
    print(f"\n📂 Input  : {input_file}")
    print(f"📁 Output : {file_output_dir}/\n")
    
    try:
        print("\n" + "-" * 70)
        print("📊 Running Function 1: Derive Computed Columns")
        derive_computed_columns(input_file, output_files["derived_computed_columns"])
        
        print("\n" + "-" * 70)
        print("📊 Running Function 2: Encode Categorical Features")
        encode_categorical_features(input_file, output_files["encoded_categorical_features"])
        
        print("\n" + "-" * 70)
        print("📊 Running Function 3: Bin Numeric Ranges")
        bin_numeric_ranges(input_file, output_files["binned_numeric_ranges"])
        
        print("\n" + "-" * 70)
        print("📊 Running Function 4: Time-Based Feature Extraction")
        time_based_feature_extraction(input_file, output_files["time_based_features"])
        
        print("\n" + "-" * 70)
        print("📊 Running Function 5: Flag Anomalies Column")
        flag_anomalies_column(input_file, output_files["flagged_anomalies"])
        
        print("\n" + "=" * 70)
        print(f"  ✅ Pipeline complete for {filename}! All output files saved.")
        print("=" * 70)
        
        print("\n📄 Output files generated:")
        for name, path in output_files.items():
            if name != "consolidated_all_features" and os.path.exists(path):
                size = os.path.getsize(path)
                print(f"   • {os.path.relpath(path)}  ({size:,} bytes)")
        
        create_consolidated_csv(output_files, file_output_dir)
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR processing {filename}: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_consolidated_csv(output_files, file_output_dir):
    """Create consolidated CSV from all feature outputs"""
    print("\n" + "=" * 70)
    print("  🔄 Creating Consolidated CSV File")
    print("=" * 70)
    
    required_files = [
        output_files["derived_computed_columns"],
        output_files["encoded_categorical_features"],
        output_files["binned_numeric_ranges"],
        output_files["time_based_features"],
        output_files["flagged_anomalies"]
    ]
    
    all_files_exist = True
    for file in required_files:
        if not os.path.exists(file):
            print(f"❌ Cannot find {file}")
            all_files_exist = False
    
    if not all_files_exist:
        print("❌ Cannot create consolidated file - missing output files")
        return
    
    try:
        dataframes = []
        file_names = []
        
        for file in required_files:
            df = pd.read_csv(file)
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
        
        output_path = output_files["consolidated_all_features"]
        consolidated.to_csv(output_path, index=False)
        
        print("\n" + "-" * 70)
        print(f"✅ CONSOLIDATED CSV GENERATED SUCCESSFULLY!")
        print(f"📁 Location: {output_path}")
        print(f"📊 Total columns: {len(consolidated.columns)}")
        print(f"📊 Total rows: {len(consolidated)}")
        
        file_size = os.path.getsize(output_path)
        print(f"💾 File size: {file_size:,} bytes ({file_size/1024:.2f} KB)")
        
        all_columns = list(consolidated.columns)
        print(f"\n📋 First 15 columns: {', '.join(all_columns[:15])}")
        if len(all_columns) > 15:
            print(f"   ... and {len(all_columns) - 15} more columns")
        
    except Exception as e:
        print(f"❌ Error creating consolidated CSV: {e}")
        import traceback
        traceback.print_exc()

def run_pipeline():
    """Main pipeline orchestrator"""
    print("\n" + "=" * 70)
    print("  🚀 Group 6 — Feature Engineering CSV Pipeline (Multi-File)")
    print("=" * 70)
    
    # Ensure folders exist
    os.makedirs(INPUT_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Get all input files
    input_files = get_all_input_files()
    
    if not input_files:
        print(f"\n❌ ERROR: No CSV files found in '{INPUT_FOLDER}/' folder!")
        print("   Please place your CSV file(s) in the 'input/' folder.")
        sys.exit(1)
    
    print(f"\n📋 Found {len(input_files)} CSV file(s) in input folder:")
    for f in input_files:
        size = os.path.getsize(f)
        print(f"   • {os.path.basename(f)} ({size:,} bytes)")
    
    # Check which files need processing
    files_to_process, current_hashes = get_files_to_process()
    
    if not files_to_process and not is_ci_environment():
        print("\n⚠️  No files have changed since last run.")
        response = input("Do you still want to process all files? (y/n): ")
        if response.lower() != 'y':
            print("❌ Pipeline cancelled.")
            return
        files_to_process = input_files
    elif not files_to_process:
        # In CI, always process if no changes (first run or forced)
        print("\n✅ Running in CI mode - processing all files")
        files_to_process = input_files
    else:
        print(f"\n🔄 Detected {len(files_to_process)} changed/new file(s) to process")
    
    # Process each file
    success_count = 0
    failed_count = 0
    
    for idx, input_file in enumerate(files_to_process, 1):
        if process_single_file(input_file, idx, len(files_to_process)):
            success_count += 1
        else:
            failed_count += 1
    
    # Save hashes for next run
    save_hashes(current_hashes)
    
    # Final summary
    print("\n" + "=" * 70)
    print("  📊 PIPELINE EXECUTION SUMMARY")
    print("=" * 70)
    print(f"✅ Successfully processed: {success_count} file(s)")
    if failed_count > 0:
        print(f"❌ Failed: {failed_count} file(s)")
    print(f"📁 Output directory: {OUTPUT_FOLDER}/")
    print("=" * 70)
    
    if failed_count > 0:
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
