"""
Group 6 - Feature Engineering
Function 5: flag_anomalies_column
Detects statistical outliers and flags them in the dataset.
"""

import pandas as pd
import numpy as np
import os

def flag_anomalies_column(input_file: str, output_file: str) -> pd.DataFrame:
    """
    Detects anomalies in numeric columns using IQR and Z-score methods.
    
    New columns added:
    - salary_anomaly: 1 if salary is outlier (IQR method)
    - score_anomaly: 1 if score is outlier (Z-score method, |z| > 2)
    - age_anomaly: 1 if age >= 60 (custom rule)
    - is_anomaly: 1 if ANY of the above is 1
    """

    df = pd.read_csv(input_file)

    df_copy = df.copy()
    

    Q1_salary = df_copy['salary'].quantile(0.25)
    Q3_salary = df_copy['salary'].quantile(0.75)
    IQR_salary = Q3_salary - Q1_salary
    lower_bound_salary = Q1_salary - 1.5 * IQR_salary
    upper_bound_salary = Q3_salary + 1.5 * IQR_salary
    
    df_copy['salary_anomaly'] = ((df_copy['salary'] < lower_bound_salary) | 
                                   (df_copy['salary'] > upper_bound_salary)).astype(int)
    

    mean_score = df_copy['score'].mean()
    std_score = df_copy['score'].std()
    
    if std_score > 0:
        z_scores = (df_copy['score'] - mean_score) / std_score
        df_copy['score_anomaly'] = (np.abs(z_scores) > 2).astype(int)
    else:
        df_copy['score_anomaly'] = 0
    

    df_copy['age_anomaly'] = (df_copy['age'] >= 60).astype(int)
    

    df_copy['is_anomaly'] = ((df_copy['salary_anomaly'] == 1) | 
                              (df_copy['score_anomaly'] == 1) | 
                              (df_copy['age_anomaly'] == 1)).astype(int)
    

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df_copy.to_csv(output_file, index=False)
    

    print(f"[flag_anomalies_column] ✅ Saved to: {output_file}")
    print(f"   📊 Anomalies detected: {df_copy['is_anomaly'].sum()} out of {len(df_copy)} rows")
    print(f"      - Salary anomalies: {df_copy['salary_anomaly'].sum()}")
    print(f"      - Score anomalies: {df_copy['score_anomaly'].sum()}")
    print(f"      - Age anomalies (age >= 60): {df_copy['age_anomaly'].sum()}")
    

    anomalies = df_copy[df_copy['is_anomaly'] == 1]
    if len(anomalies) > 0:
        print(f"\n   🔍 Anomaly details:")
        for idx, row in anomalies.iterrows():
            reasons = []
            if row['salary_anomaly'] == 1:
                reasons.append(f"Salary={row['salary']}")
            if row['score_anomaly'] == 1:
                reasons.append(f"Score={row['score']}")
            if row['age_anomaly'] == 1:
                reasons.append(f"Age={row['age']} (>=60)")
            print(f"      - {row['name']}: {', '.join(reasons)}")
    
    return df_copy

if __name__ == "__main__":
    flag_anomalies_column(
        input_file="input/data.csv",
        output_file="output/flagged_anomalies.csv"
    )