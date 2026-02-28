"""
Group 6 - Feature Engineering
Function 5: flag_anomalies_column
Detects and flags statistical outliers/anomalies in numeric columns.
"""

import pandas as pd
import os


def flag_anomalies_column(input_file: str, output_file: str) -> pd.DataFrame:
    """
    Flags anomalies/outliers in numeric columns using IQR and Z-score methods.

    Detection methods:
    - salary_anomaly : IQR method (1.5x IQR rule) applied to 'salary'
    - score_anomaly  : Z-score method (±2 standard deviations) applied to 'score'
    - age_anomaly    : IQR method applied to 'age'
    - is_anomaly     : 1 if ANY of the above flags are triggered, else 0

    Args:
        input_file  (str): Path to the input CSV file.
        output_file (str): Path where the processed CSV will be saved.

    Returns:
        pd.DataFrame: The processed dataframe with anomaly flag columns.
    """
    # Load the CSV
    df = pd.read_csv(input_file)

    # --- Salary anomaly: IQR Method ---
    Q1_sal = df['salary'].quantile(0.25)
    Q3_sal = df['salary'].quantile(0.75)
    IQR_sal = Q3_sal - Q1_sal
    df['salary_anomaly'] = (
        (df['salary'] < Q1_sal - 1.5 * IQR_sal) |
        (df['salary'] > Q3_sal + 1.5 * IQR_sal)
    ).astype(int)

    # --- Score anomaly: Z-score Method ---
    score_mean = df['score'].mean()
    score_std  = df['score'].std()
    df['score_anomaly'] = (
        (df['score'] < score_mean - 2 * score_std) |
        (df['score'] > score_mean + 2 * score_std)
    ).astype(int)

    # --- Age anomaly: IQR Method ---
    Q1_age = df['age'].quantile(0.25)
    Q3_age = df['age'].quantile(0.75)
    IQR_age = Q3_age - Q1_age
    df['age_anomaly'] = (
        (df['age'] < Q1_age - 1.5 * IQR_age) |
        (df['age'] > Q3_age + 1.5 * IQR_age)
    ).astype(int)

    # --- Combined anomaly flag ---
    df['is_anomaly'] = (
        (df['salary_anomaly'] == 1) |
        (df['score_anomaly']  == 1) |
        (df['age_anomaly']    == 1)
    ).astype(int)

    # Save output
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"[flag_anomalies_column] ✅ Saved to: {output_file}")
    return df


if __name__ == "__main__":
    flag_anomalies_column(
        input_file="input/data.csv",
        output_file="output/flagged_anomalies.csv"
    )
