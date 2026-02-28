"""
Group 6 - Feature Engineering
Function 2: encode_categorical_features
Encodes categorical/text columns into numeric representations.
"""

import pandas as pd
import os


def encode_categorical_features(input_file: str, output_file: str) -> pd.DataFrame:
    """
    Encodes categorical features in a CSV file into numeric values.

    Transformations applied:
    - department : One-hot encoded into separate binary columns (dept_HR, dept_IT, etc.)
    - category   : Label encoded using a mapping (A=1, B=2, C=3)

    Args:
        input_file  (str): Path to the input CSV file.
        output_file (str): Path where the processed CSV will be saved.

    Returns:
        pd.DataFrame: The processed dataframe with encoded columns.
    """
    # Load the CSV
    df = pd.read_csv(input_file)

    # One-hot encode 'department' column
    df = pd.get_dummies(df, columns=['department'], prefix='dept')

    # Convert boolean columns (True/False) to int (1/0)
    bool_cols = df.select_dtypes(include='bool').columns
    df[bool_cols] = df[bool_cols].astype(int)

    # Label encode 'category' column
    category_map = {'A': 1, 'B': 2, 'C': 3}
    df['category_encoded'] = df['category'].map(category_map)

    # Save output
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"[encode_categorical_features] ✅ Saved to: {output_file}")
    return df


if __name__ == "__main__":
    encode_categorical_features(
        input_file="input/data.csv",
        output_file="output/encoded_categorical_features.csv"
    )
