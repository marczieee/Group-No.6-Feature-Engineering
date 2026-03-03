"""
Group 6 - Feature Engineering
main.py - Orchestrates all 5 CSV processing functions
Run this file to process the input CSV through all feature engineering steps.
"""

import os
import sys
import pandas as pd

from derive_computed_columns      import derive_computed_columns
from encode_categorical_features  import encode_categorical_features
from bin_numeric_ranges           import bin_numeric_ranges
from time_based_feature_extraction import time_based_feature_extraction
from flag_anomalies_column        import flag_anomalies_column

# ─── Configuration ────────────────────────────────────────────────────────────
INPUT_FILE = "input/data.csv"

OUTPUT_FILES = {
    "derived_computed_columns"     : "output/derived_computed_columns.csv",
    "encoded_categorical_features" : "output/encoded_categorical_features.csv",
    "binned_numeric_ranges"        : "output/binned_numeric_ranges.csv",
    "time_based_features"          : "output/time_based_features.csv",
    "flagged_anomalies"            : "output/flagged_anomalies.csv",
    "consolidated_all_features"    : "output/consolidated_all_features.csv",
}
# ──────────────────────────────────────────────────────────────────────────────


def run_pipeline():
    print("=" * 55)
    print("  Group 6 — Feature Engineering CSV Pipeline")
    print("=" * 55)

    # Validate input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"\n❌ ERROR: Input file '{INPUT_FILE}' not found!")
        print("   Please place your CSV file in the 'input/' folder.")
        sys.exit(1)

    print(f"\n📂 Input  : {INPUT_FILE}")
    print(f"📁 Output : output/\n")

    # Create output directory if it doesn't exist
    os.makedirs("output", exist_ok=True)

    # Run all 5 feature engineering functions
    print("\n" + "-" * 55)
    print("📊 Running Function 1: Derive Computed Columns")
    derive_computed_columns(
        INPUT_FILE,
        OUTPUT_FILES["derived_computed_columns"]
    )
    print("[derive_computed_columns] ✅ Saved to: output/derived_computed_columns.csv")

    print("\n" + "-" * 55)
    print("📊 Running Function 2: Encode Categorical Features")
    encode_categorical_features(
        INPUT_FILE,
        OUTPUT_FILES["encoded_categorical_features"]
    )
    print("[encode_categorical_features] ✅ Saved to: output/encoded_categorical_features.csv")

    print("\n" + "-" * 55)
    print("📊 Running Function 3: Bin Numeric Ranges")
    bin_numeric_ranges(
        INPUT_FILE,
        OUTPUT_FILES["binned_numeric_ranges"]
    )
    print("[bin_numeric_ranges] ✅ Saved to: output/binned_numeric_ranges.csv")

    print("\n" + "-" * 55)
    print("📊 Running Function 4: Time-Based Feature Extraction")
    time_based_feature_extraction(
        INPUT_FILE,
        OUTPUT_FILES["time_based_features"]
    )
    print("[time_based_feature_extraction] ✅ Saved to: output/time_based_features.csv")

    print("\n" + "-" * 55)
    print("📊 Running Function 5: Flag Anomalies Column")
    flag_anomalies_column(
        INPUT_FILE,
        OUTPUT_FILES["flagged_anomalies"]
    )
    print("[flag_anomalies_column] ✅ Saved to: output/flagged_anomalies.csv")

    print("\n" + "=" * 55)
    print("  ✅ Pipeline complete! All output files saved.")
    print("=" * 55)

    # Print summary of output files
    print("\n📄 Output files generated:")
    for name, path in OUTPUT_FILES.items():
        if name != "consolidated_all_features":
            size = os.path.getsize(path) if os.path.exists(path) else 0
            print(f"   • {path}  ({size} bytes)")
    
    # Create consolidated CSV
    create_consolidated_csv()


def create_consolidated_csv():
    """
    Pagsamahin ang lahat ng 5 output CSV files sa iisang consolidated file
    """
    print("\n" + "=" * 55)
    print("  🔄 Creating Consolidated CSV File")
    print("=" * 55)
    
    # List of output files
    output_files = [
        'output/derived_computed_columns.csv',
        'output/encoded_categorical_features.csv',
        'output/binned_numeric_ranges.csv',
        'output/time_based_features.csv',
        'output/flagged_anomalies.csv'
    ]
    
    # I-verify na lahat ng files ay existing
    all_files_exist = True
    for file in output_files:
        if not os.path.exists(file):
            print(f"❌ Hindi mahanap ang {file}")
            all_files_exist = False
    
    if not all_files_exist:
        print("❌ Hindi magagawa ang consolidated file - may missing na output files")
        return
    
    try:
        # Basahin ang bawat CSV file
        dfs = []
        file_names = []
        
        for file in output_files:
            df = pd.read_csv(file)
            dfs.append(df)
            file_names.append(os.path.basename(file))
            print(f"✅ Nabasa ang {os.path.basename(file)} - {len(df.columns)} columns")
        
        # Gamitin ang 'id' column para i-merge
        first_df = dfs[0]
        merge_col = 'id' if 'id' in first_df.columns else first_df.columns[0]
        print(f"📊 Ginagamit ang '{merge_col}' bilang key para pagsamahin")
        
        # Listahan ng mga columns na dapat i-drop (original columns)
        columns_to_drop = ['name', 'age', 'salary', 'department', 'join_date', 'score', 'category']
        
        # Magsimula sa unang dataframe (ito ang may original columns)
        consolidated = first_df.copy()
        print(f"📌 Base dataframe: {file_names[0]} - kept all {len(consolidated.columns)} columns")
        
        # I-merge ang natitirang dataframes - pero i-drop ang duplicate columns
        for i, df in enumerate(dfs[1:], 2):
            # I-drop ang merge column at ang mga original columns mula sa kasalukuyang dataframe
            cols_to_keep = []
            for col in df.columns:
                if col == merge_col:
                    cols_to_keep.append(col)
                elif col not in columns_to_drop:
                    cols_to_keep.append(col)
                else:
                    print(f"  ⏩ Inalis ang duplicate column: '{col}' from {file_names[i-1]}")
            
            # I-merge gamit ang selected columns lang
            if len(cols_to_keep) > 1:  # May ibang columns aside from merge_col
                consolidated = pd.merge(
                    consolidated, 
                    df[cols_to_keep], 
                    on=merge_col, 
                    how='outer'
                )
                print(f"  ✓ Na-merge ang {file_names[i-1]} - {len(cols_to_keep)-1} bagong columns idinagdag")
            else:
                print(f"  ⚠️ Walang bagong columns sa {file_names[i-1]}, skip merge")
        
        # I-save ang consolidated file
        output_path = OUTPUT_FILES["consolidated_all_features"]
        consolidated.to_csv(output_path, index=False)
        
        print("\n" + "-" * 55)
        print(f"✅ CONSOLIDATED CSV GENERATED!")
        print(f"📁 Lokasyon: {output_path}")
        print(f"📊 Kabuuang columns: {len(consolidated.columns)}")
        print(f"📊 Kabuuang rows: {len(consolidated)}")
        
        size = os.path.getsize(output_path)
        print(f"💾 File size: {size} bytes ({size/1024:.2f} KB)")
        
        all_cols = list(consolidated.columns)
        print(f"\n📋 First 15 columns: {', '.join(all_cols[:15])}")
        
    except Exception as e:
        print(f"❌ Error sa paggawa ng consolidated CSV: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_pipeline()