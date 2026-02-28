"""
Group 6 - Feature Engineering
Function 1: derive_computed_columns
Adds new computed/derived columns from existing numeric data.
"""

import pandas as pd
import os


def derive_computed_columns(input_file: str, output_file: str) -> pd.DataFrame:
    """
    Derives new computed columns from existing data in a CSV file.

    New columns added:
    - salary_per_age     : salary divided by age (productivity ratio)
    - annual_bonus       : 10% of salary as estimated bonus
    - is_senior          : 1 if age >= 40, else 0
    - salary_level       : 'High', 'Mid', or 'Low' based on salary range
    - score_rank         : normalized score out of 10

    Args:
        input_file  (str): Path to the input CSV file.
        output_file (str): Path where the processed CSV will be saved.

    Returns:
        pd.DataFrame: The processed dataframe with new columns.
    """
    # Load the CSV
    df = pd.read_csv(input_file)

    # Derived columns
    df['salary_per_age'] = (df['salary'] / df['age']).round(2)
    df['annual_bonus']   = (df['salary'] * 0.10).round(2)
    df['is_senior']      = df['age'].apply(lambda x: 1 if x >= 40 else 0)
    df['salary_level']   = df['salary'].apply(
        lambda x: 'High' if x > 90000 else ('Mid' if x > 55000 else 'Low')
    )
    df['score_rank'] = (df['score'] / 10).round(1)

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
