#!/usr/bin/env python3
"""
Feature Engineering Pipeline - CI/CD Compatible Version
NO INTERACTIVE PROMPTS - Fully automated for GitHub Actions
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import json
import hashlib

def create_output_directory():
    """Create output directory if it doesn't exist"""
    os.makedirs('output', exist_ok=True)
    os.makedirs('logs', exist_ok=True)

def log_message(msg, level="INFO"):
    """Log message to console and file"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {level}: {msg}"
    print(log_entry)
    
    try:
        with open('logs/pipeline.log', 'a') as f:
            f.write(log_entry + '\n')
    except:
        pass

def get_file_hash(file_path):
    """Get hash of file to check for changes"""
    if not os.path.exists(file_path):
        return None
    
    with open(file_path, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()

def check_if_processed(file_path, force=False):
    """Check if file has been processed before"""
    if force:
        return False
        
    hash_file = f'output/.{os.path.basename(file_path)}.hash'
    
    current_hash = get_file_hash(file_path)
    
    if os.path.exists(hash_file):
        try:
            with open(hash_file, 'r') as f:
                previous_hash = f.read().strip()
            
            if previous_hash == current_hash:
                return True
        except:
            pass
    
    try:
        with open(hash_file, 'w') as f:
            f.write(current_hash)
    except:
        pass
    
    return False

def load_csv(file_path):
    """Load CSV file with flexible parsing"""
    try:
        for encoding in ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']:
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                log_message(f"✅ Loaded CSV with {encoding} encoding")
                return df
            except UnicodeDecodeError:
                continue
        
        df = pd.read_csv(file_path, engine='python')
        log_message(f"✅ Loaded CSV with Python engine")
        return df
        
    except Exception as e:
        log_message(f"❌ Error loading CSV: {e}", "ERROR")
        raise

def analyze_dataframe(df, file_name):
    """Analyze and provide information about the dataframe"""
    log_message("=" * 60)
    log_message("📊 DATA ANALYSIS")
    log_message("=" * 60)
    log_message(f"File: {file_name}")
    log_message(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns")
    log_message(f"Columns: {list(df.columns)}")
    
    analysis = {
        'file_name': file_name,
        'shape': list(df.shape),
        'columns': list(df.columns),
        'dtypes': {col: str(df[col].dtype) for col in df.columns},
        'null_counts': df.isnull().sum().to_dict(),
        'numeric_columns': list(df.select_dtypes(include=[np.number]).columns),
        'categorical_columns': list(df.select_dtypes(include=['object']).columns)
    }
    
    try:
        with open('output/data_analysis.json', 'w') as f:
            json.dump(analysis, f, indent=2, default=str)
        log_message(f"✅ Analysis saved")
    except Exception as e:
        log_message(f"⚠️ Could not save analysis: {e}")
    
    return analysis

def perform_feature_engineering(df, analysis):
    """Perform feature engineering based on data types"""
    log_message("=" * 60)
    log_message("🔧 FEATURE ENGINEERING")
    log_message("=" * 60)
    
    df_processed = df.copy()
    transformations = []
    
    # 1. Handle missing values
    if df_processed.isnull().any().any():
        log_message("Handling missing values...")
        for col in df_processed.columns:
            null_count = df_processed[col].isnull().sum()
            if null_count > 0:
                if df_processed[col].dtype in ['int64', 'float64']:
                    df_processed[col].fillna(df_processed[col].median(), inplace=True)
                    transformations.append(f"Filled {null_count} missing values in {col} with median")
                else:
                    df_processed[col].fillna('Unknown', inplace=True)
                    transformations.append(f"Filled {null_count} missing values in {col} with 'Unknown'")
        log_message(f"✅ Handled missing values")
    
    # 2. Create derived features for numeric columns
    numeric_cols = analysis['numeric_columns']
    if len(numeric_cols) >= 1:
        log_message("Creating derived numeric features...")
        for i in range(min(len(numeric_cols), 3)):
            col = numeric_cols[i]
            try:
                df_processed[f'{col}_binned'] = pd.cut(df_processed[col], bins=5, labels=False)
                transformations.append(f"Created binned feature for {col}")
            except:
                pass
            
            try:
                if df_processed[col].std() > 0:
                    df_processed[f'{col}_normalized'] = (df_processed[col] - df_processed[col].mean()) / df_processed[col].std()
                    transformations.append(f"Created normalized feature for {col}")
            except:
                pass
    
    # 3. Process categorical columns
    cat_cols = analysis['categorical_columns']
    if cat_cols:
        log_message("Processing categorical features...")
        for col in cat_cols[:5]:
            try:
                if df_processed[col].nunique() <= 10:
                    dummies = pd.get_dummies(df_processed[col], prefix=col, drop_first=True)
                    df_processed = pd.concat([df_processed, dummies], axis=1)
                    transformations.append(f"One-hot encoded {col}")
                else:
                    df_processed[f'{col}_encoded'] = df_processed[col].astype('category').cat.codes
                    transformations.append(f"Label encoded {col}")
            except Exception as e:
                log_message(f"⚠️ Could not process column {col}: {e}")
    
    # 4. Add metadata features
    df_processed['_record_id'] = range(len(df_processed))
    transformations.append("Added record IDs")
    
    # 5. Save transformations log
    try:
        with open('output/transformations.log', 'w') as f:
            f.write("\n".join(transformations))
        log_message(f"✅ Saved transformations log")
    except:
        pass
    
    log_message(f"✅ Performed {len(transformations)} transformations")
    
    return df_processed

def process_data(input_file, force=False):
    """Main processing function - NO INTERACTIVE PROMPTS"""
    print("\n" + "="*60)
    print("  Group 6 — Feature Engineering Pipeline (CI/CD Mode)")
    print("="*60 + "\n")
    
    # Check if file exists
    if not os.path.exists(input_file):
        print(f"❌ Input file does not exist: {input_file}")
        return False
    
    # Check if file has been processed before
    if not force and check_if_processed(input_file):
        print(f"\n⚠️  File {os.path.basename(input_file)} hasn't changed since last run.")
        print("   Use --force flag to process anyway.")
        print("   Skipping processing...\n")
        return True  # Not a failure, just skipped
    
    # Create directories
    create_output_directory()
    
    try:
        # Load data
        log_message(f"Loading data from: {input_file}")
        df = load_csv(input_file)
        log_message(f"✅ Loaded {len(df)} records")
        
        # Save original data
        original_file = f"output/original_data.csv"
        df.to_csv(original_file, index=False)
        log_message(f"✅ Original data saved to {original_file}")
        
        # Analyze data
        analysis = analyze_dataframe(df, os.path.basename(input_file))
        
        # Perform feature engineering
        df_processed = perform_feature_engineering(df, analysis)
        
        # Save processed data
        output_file = f"output/processed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df_processed.to_csv(output_file, index=False)
        log_message(f"✅ Processed data saved to {output_file}")
        
        # Save summary report
        summary = {
            'timestamp': datetime.now().isoformat(),
            'input_file': input_file,
            'original_shape': list(df.shape),
            'processed_shape': list(df_processed.shape),
            'features_created': df_processed.shape[1] - df.shape[1],
            'success': True
        }
        
        try:
            with open('output/pipeline_summary.json', 'w') as f:
                json.dump(summary, f, indent=2, default=str)
        except:
            pass
        
        # Final report
        print("\n" + "="*60)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY")
        print("="*60)
        print(f"📊 Original shape: {df.shape}")
        print(f"📈 Processed shape: {df_processed.shape}")
        print(f"✨ Features added: {df_processed.shape[1] - df.shape[1]}")
        print(f"📁 Output files in 'output/' directory")
        print("="*60 + "\n")
        
        return True
        
    except Exception as e:
        log_message(f"❌ Pipeline failed: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main entry point"""
    # Parse command line arguments
    force_process = '--force' in sys.argv
    
    # Get input file from command line argument
    input_file = None
    for arg in sys.argv[1:]:
        if not arg.startswith('--'):
            input_file = arg
            break
    
    if not input_file:
        # Look for CSV files in input directory
        input_dir = 'input'
        if os.path.exists(input_dir):
            csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
            if csv_files:
                input_file = os.path.join(input_dir, csv_files[0])
                print(f"📁 Auto-detected input file: {input_file}")
            else:
                print("❌ No CSV files found in 'input/' directory!")
                print("   Please add a CSV file to the 'input/' folder.")
                sys.exit(1)
        else:
            print("❌ No input file specified and 'input/' directory not found!")
            print("   Please create an 'input/' folder and add your CSV file.")
            print("   Usage: python main.py <path_to_csv_file> [--force]")
            sys.exit(1)
    else:
        print(f"📁 Using input file: {input_file}")
    
    # Process the data
    success = process_data(input_file, force_process)
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()