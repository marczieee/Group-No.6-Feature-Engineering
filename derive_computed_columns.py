"""
Group 6 - Feature Engineering
Function 1: derive_computed_columns
Adds new computed/derived columns from existing numeric data.
"""

import pandas as pd
import os
import hashlib


def derive_computed_columns(input_file: str, output_file: str) -> pd.DataFrame:
    """
    Derives new computed columns from existing data in a CSV file.
    """
    
    df = pd.read_csv(input_file)

    # Create a hash of the input data to  
    input_hash = hashlib.md5(df.to_string().encode()).hexdigest()
    
    # Derived columns
    df['salary_per_age'] = (df['salary'] / df['age']).round(2)
    df['annual_bonus']   = (df['salary'] * 0.10).round(2)
    df['is_senior']      = (df['age'] >= 40).astype(int)
    df['salary_level']   = df['salary'].apply(
        lambda x: 'High' if x > 90000 else ('Mid' if x > 55000 else 'Low')
    )
    df['score_rank'] = (df['score'] / 10).round(1)
    
    # Add metadata
    df['_input_hash'] = input_hash
    df['_generated_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

    # Save output
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"[derive_computed_columns] ✅ Saved to: {output_file}")
    return df


if __name__ == "__main__":
    derive_computed_columns(
        input_file="input/data.csv",
        output_file="output/derived_computed_columns.csv"
    )
