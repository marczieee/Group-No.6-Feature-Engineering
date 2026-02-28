"""
Group 6 - Feature Engineering
Function 4: time_based_feature_extraction
Extracts useful time-based features from date columns.
"""

import pandas as pd
import os
from datetime import datetime


def time_based_feature_extraction(input_file: str, output_file: str) -> pd.DataFrame:
    """
    Extracts time-based features from date columns in a CSV file.

    New columns added:
    - join_year        : Year extracted from join_date
    - join_month       : Month number (1-12) extracted from join_date
    - join_quarter     : Quarter (1-4) extracted from join_date
    - join_day_of_week : Day of week (0=Monday, 6=Sunday)
    - years_in_company : Approximate years since joining (rounded to 1 decimal)
    - is_recent_hire   : 1 if joined in 2021 or later, else 0

    Args:
        input_file  (str): Path to the input CSV file.
        output_file (str): Path where the processed CSV will be saved.

    Returns:
        pd.DataFrame: The processed dataframe with new time-based columns.
    """
    # Load the CSV
    df = pd.read_csv(input_file)

    # Parse the date column
    df['join_date'] = pd.to_datetime(df['join_date'])
    today = pd.Timestamp(datetime.today().date())

    # Extract time components
    df['join_year']        = df['join_date'].dt.year
    df['join_month']       = df['join_date'].dt.month
    df['join_quarter']     = df['join_date'].dt.quarter
    df['join_day_of_week'] = df['join_date'].dt.dayofweek  # 0 = Monday

    # Calculate tenure
    df['years_in_company'] = ((today - df['join_date']).dt.days / 365).round(1)

    # Flag recent hires (2021 onward)
    df['is_recent_hire'] = df['join_year'].apply(lambda y: 1 if y >= 2021 else 0)

    # Save output
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"[time_based_feature_extraction] ✅ Saved to: {output_file}")
    return df


if __name__ == "__main__":
    time_based_feature_extraction(
        input_file="input/data.csv",
        output_file="output/time_based_features.csv"
    )
