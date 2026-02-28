"""
Group 6 - Feature Engineering
Function 3: bin_numeric_ranges
Bins continuous numeric columns into labeled categorical range groups.
"""

import pandas as pd
import os


def bin_numeric_ranges(input_file: str, output_file: str) -> pd.DataFrame:
    """
    Bins numeric columns into labeled range groups (categorical buckets).

    Bins created:
    - age_group    : Young (<=25), Adult (26-35), Mid-Age (36-45), Senior (46+)
    - salary_range : Entry, Mid, Senior, Executive
    - score_grade  : Fail (<50), Pass (50-70), Good (70-85), Excellent (85+)

    Args:
        input_file  (str): Path to the input CSV file.
        output_file (str): Path where the processed CSV will be saved.

    Returns:
        pd.DataFrame: The processed dataframe with new bin columns.
    """
    # Load the CSV
    df = pd.read_csv(input_file)

    # Bin age into labeled groups
    df['age_group'] = pd.cut(
        df['age'],
        bins=[0, 25, 35, 45, 100],
        labels=['Young', 'Adult', 'Mid-Age', 'Senior'],
        right=True
    )

    # Bin salary into pay grade ranges
    df['salary_range'] = pd.cut(
        df['salary'],
        bins=[0, 50000, 80000, 120000, float('inf')],
        labels=['Entry', 'Mid', 'Senior', 'Executive'],
        right=True
    )

    # Bin score into performance grades
    df['score_grade'] = pd.cut(
        df['score'],
        bins=[0, 49, 70, 85, 100],
        labels=['Fail', 'Pass', 'Good', 'Excellent'],
        right=True
    )

    # Save output
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"[bin_numeric_ranges] ✅ Saved to: {output_file}")
    return df


if __name__ == "__main__":
    bin_numeric_ranges(
        input_file="input/data.csv",
        output_file="output/binned_numeric_ranges.csv"
    )
