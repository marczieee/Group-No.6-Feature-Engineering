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
from time_based_feature_extraction import time_based_feature_extraction  # ✅ TAMA na ito
from flag_anomalies_column        import flag_anomalies_column

# ─── Configuration ────────────────────────────────────────────────────────────
INPUT_FILE = "input/data.csv"

OUTPUT_FILES = {
    "derived_computed_columns"     : "output/derived_computed_columns.csv",
    "encoded_categorical_features" : "output/encoded_categorical_features.csv",
    "binned_numeric_ranges"        : "output/binned_numeric_ranges.csv",
    "time_based_features"          : "output/time_based_features.csv",
    "flagged_anomalies"            : "output/flagged_anomalies.csv",
    "consolidated_all_features"    : "output/consolidated_all_features.csv",  # 🆕 BAGO
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
    derive_computed_columns(
        INPUT_FILE,
        OUTPUT_FILES["derived_computed_columns"]
    )

    encode_categorical_features(
        INPUT_FILE,
        OUTPUT_FILES["encoded_categorical_features"]
    )

    bin_numeric_ranges(
        INPUT_FILE,
        OUTPUT_FILES["binned_numeric_ranges"]
    )

    time_based_feature_extraction(
        INPUT_FILE,
        OUTPUT_FILES["time_based_features"]
    )

    flag_anomalies_column(
        INPUT_FILE,
        OUTPUT_FILES["flagged_anomalies"]
    )

    print("\n" + "=" * 55)
    print("  ✅ Pipeline complete! All output files saved.")
    print("=" * 55)

    # Print summary of output files
    print("\n📄 Output files generated:")
    for name, path in OUTPUT_FILES.items():
        if name != "consolidated_all_features":  # Skip muna ito, gagawin later
            size = os.path.getsize(path) if os.path.exists(path) else 0
            print(f"   • {path}  ({size} bytes)")
    
    # 🆕 Create consolidated CSV
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
        
        # Magsimula sa unang dataframe
        consolidated = first_df.copy()
        
        # I-merge ang natitirang dataframes
        for i, df in enumerate(dfs[1:], 2):
            # I-drop ang merge column mula sa right dataframe para maiwasan ang duplicate
            cols_to_merge = [col for col in df.columns if col != merge_col]
            
            # I-merge
            consolidated = pd.merge(
                consolidated, 
                df[[merge_col] + cols_to_merge], 
                on=merge_col, 
                how='outer'
            )
            print(f"  ✓ Na-merge ang {file_names[i-1]} - {len(cols_to_merge)} columns idinagdag")
        
        # I-save ang consolidated file
        output_path = OUTPUT_FILES["consolidated_all_features"]
        consolidated.to_csv(output_path, index=False)
        
        # I-display ang resulta
        print("\n" + "-" * 55)
        print(f"✅ CONSOLIDATED CSV GENERATED!")
        print(f"📁 Lokasyon: {output_path}")
        print(f"📊 Kabuuang columns: {len(consolidated.columns)}")
        print(f"📊 Kabuuang rows: {len(consolidated)}")
        
        # Ipakita ang file size
        size = os.path.getsize(output_path)
        print(f"💾 File size: {size} bytes ({size/1024:.2f} KB)")
        
    except Exception as e:
        print(f"❌ Error sa paggawa ng consolidated CSV: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_pipeline()