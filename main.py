import pandas as pd
import os
from derive_computed_columns import derive_computed_columns
from encode_categorical_features import encode_categorical_features
from bin_numeric_ranges import bin_numeric_ranges
from time_based_feature_extraction import extract_time_features
from flag_anomalies_column import flag_anomalies

def main():
    print("="*55)
    print("  Group 6 — Feature Engineering CSV Pipeline")
    print("="*55)
    
    # Define file paths
    input_file = "input/data.csv"
    output_dir = "output/"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        print("Please place your data.csv in the input folder.")
        return
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"📂 Input  : {input_file}")
    print(f"📁 Output : {output_dir}\n")
    
    # Load the data
    df = pd.read_csv(input_file)
    print(f"✅ Loaded {len(df)} rows from {input_file}")
    
    # Apply function 1: Derive computed columns
    print("\n" + "-"*55)
    df = derive_computed_columns(df)
    df.to_csv(output_dir + "derived_computed_columns.csv", index=False)
    print("[derive_computed_columns] ✅ Saved to: output/derived_computed_columns.csv")
    
    # Apply function 2: Encode categorical features
    df = encode_categorical_features(df)
    df.to_csv(output_dir + "encoded_categorical_features.csv", index=False)
    print("[encode_categorical_features] ✅ Saved to: output/encoded_categorical_features.csv")
    
    # Apply function 3: Bin numeric ranges
    df = bin_numeric_ranges(df)
    df.to_csv(output_dir + "binned_numeric_ranges.csv", index=False)
    print("[bin_numeric_ranges] ✅ Saved to: output/binned_numeric_ranges.csv")
    
    # Apply function 4: Extract time-based features
    df = extract_time_features(df)
    df.to_csv(output_dir + "time_based_features.csv", index=False)
    print("[time_based_feature_extraction] ✅ Saved to: output/time_based_features.csv")
    
    # Apply function 5: Flag anomalies
    df = flag_anomalies(df)
    df.to_csv(output_dir + "flagged_anomalies.csv", index=False)
    print("[flag_anomalies_column] ✅ Saved to: output/flagged_anomalies.csv")
    
    print("\n" + "="*55)
    print("  ✅ Pipeline complete! All output files saved.")
    print("="*55)
    
    # Display file sizes
    print("\n📄 Output files generated:")
    for filename in os.listdir(output_dir):
        if filename.endswith('.csv'):
            filepath = os.path.join(output_dir, filename)
            size = os.path.getsize(filepath)
            print(f"   • {filename}  ({size} bytes)")
    
    # ===== NEW: Create consolidated CSV =====
    create_consolidated_csv(output_dir)

def create_consolidated_csv(output_dir):
    """
    Pagsamahin ang lahat ng 5 output CSV files sa iisang consolidated file
    """
    print("\n" + "="*55)
    print("  🔄 Creating Consolidated CSV File")
    print("="*55)
    
    # List of output files
    output_files = [
        'derived_computed_columns.csv',
        'encoded_categorical_features.csv',
        'binned_numeric_ranges.csv',
        'time_based_features.csv',
        'flagged_anomalies.csv'
    ]
    
    # I-verify na lahat ng files ay existing
    all_files_exist = True
    for file in output_files:
        filepath = os.path.join(output_dir, file)
        if not os.path.exists(filepath):
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
            filepath = os.path.join(output_dir, file)
            df = pd.read_csv(filepath)
            dfs.append(df)
            file_names.append(file)
            print(f"✅ Nabasa ang {file} - {len(df.columns)} columns")
        
        # Gamitin ang 'id' column para i-merge (kung walang 'id', gamitin ang unang column)
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
        output_path = os.path.join(output_dir, 'consolidated_all_features.csv')
        consolidated.to_csv(output_path, index=False)
        
        # I-display ang resulta
        print("\n" + "-"*55)
        print(f"✅ CONSOLIDATED CSV GENERATED!")
        print(f"📁 Lokasyon: {output_path}")
        print(f"📊 Kabuuang columns: {len(consolidated.columns)}")
        print(f"📊 Kabuuang rows: {len(consolidated)}")
        
        # Ipakita ang unang 10 columns
        all_cols = list(consolidated.columns)
        print(f"📋 Sample columns: {', '.join(all_cols[:10])}")
        if len(all_cols) > 10:
            print(f"   ... at {len(all_cols) - 10} pang columns")
        
        # Ipakita ang file size
        size = os.path.getsize(output_path)
        print(f"💾 File size: {size} bytes ({size/1024:.2f} KB)")
        
    except Exception as e:
        print(f"❌ Error sa paggawa ng consolidated CSV: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()