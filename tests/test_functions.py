"""
Group 6 - Feature Engineering
Test functions for all 5 CSV processing functions
"""

import pytest
import pandas as pd
import os
import numpy as np

from derive_computed_columns import derive_computed_columns
from encode_categorical_features import encode_categorical_features
from bin_numeric_ranges import bin_numeric_ranges
from time_based_feature_extraction import time_based_feature_extraction
from flag_anomalies_column import flag_anomalies

# Fixture to provide test dataframe
@pytest.fixture
def df():
    """Create a sample dataframe for testing"""
    data = {
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'name': ['Alice', 'Bob', 'Charlie', 'Mac', 'Mia', 'Frank', 'Yuki', 'Hank', 'Iris', 'Jake'],
        'age': [30, 32, 28, 45, 22, 38, 29, 52, 55, 30],
        'salary': [50000, 75000, 60000, 120000, 30000, 95000, 62000, 150000, 80000, 47000],
        'department': ['HR', 'IT', 'Finance', 'IT', 'HR', 'Finance', 'IT', 'HR', 'Finance', 'IT'],
        'join_date': ['2021-03-15', '2019-07-22', '2020-11-01', '2015-05-30', '2023-01-10', 
                      '2017-08-19', '2020-03-25', '2010-12-05', '2018-06-14', '2022-09-30'],
        'score': [88, 45, 92, 10, 77, 55, 88, 5, 66, 99],
        'category': ['A', 'B', 'A', 'C', 'B', 'A', 'B', 'C', 'A', 'B']
    }
    return pd.DataFrame(data)


# ============================================================================
# TESTS FOR FUNCTION 1: derive_computed_columns
# ============================================================================

class TestDeriveComputedColumns:
    """Test cases for derive_computed_columns function"""
    
    def test_output_file_created(self, df, tmp_path):
        """Test that output file is created"""
        output_file = tmp_path / "test_output.csv"
        derive_computed_columns(df, str(output_file))
        assert os.path.exists(output_file)
    
    def test_salary_per_age_column_exists(self, df):
        """Test that salary_per_age column is added"""
        result = derive_computed_columns(df, "output/test_derived.csv")
        assert 'salary_per_age' in result.columns
    
    def test_annual_bonus_column_exists(self, df):
        """Test that annual_bonus column is added"""
        result = derive_computed_columns(df, "output/test_derived.csv")
        assert 'annual_bonus' in result.columns
    
    def test_is_senior_column_exists(self, df):
        """Test that is_senior column is added"""
        result = derive_computed_columns(df, "output/test_derived.csv")
        assert 'is_senior' in result.columns
    
    def test_salary_level_column_exists(self, df):
        """Test that salary_level column is added"""
        result = derive_computed_columns(df, "output/test_derived.csv")
        assert 'salary_level' in result.columns
    
    def test_score_rank_column_exists(self, df):
        """Test that score_rank column is added"""
        result = derive_computed_columns(df, "output/test_derived.csv")
        assert 'score_rank' in result.columns
    
    def test_is_senior_binary(self, df):
        """Test that is_senior contains only 0 or 1"""
        result = derive_computed_columns(df, "output/test_derived.csv")
        assert result['is_senior'].isin([0, 1]).all()
    
    # UPDATED TEST FOR 60+ SENIOR
    def test_is_senior_correct_for_age_60_plus(self, df):
        """Test that age 60 and above are marked as senior (1)"""
        # Add a person aged 60+ for testing
        test_df = df.copy()
        senior_row = pd.DataFrame({
            'id': [99],
            'name': ['TestSenior'],
            'age': [65],
            'salary': [100000],
            'department': ['IT'],
            'join_date': ['2020-01-01'],
            'score': [80],
            'category': ['A']
        })
        test_df = pd.concat([test_df, senior_row], ignore_index=True)
        
        result = derive_computed_columns(test_df, "output/test_derived.csv")
        senior_rows = result[result['age'] >= 60]
        assert (senior_rows['is_senior'] == 1).all()
    
    # NEW TEST FOR AGES 40-59 (should be 0)
    def test_is_senior_correct_for_age_40_to_59(self, df):
        """Test that age 40-59 are NOT marked as senior (0)"""
        result = derive_computed_columns(df, "output/test_derived.csv")
        middle_rows = result[(result['age'] >= 40) & (result['age'] < 60)]
        if len(middle_rows) > 0:
            assert (middle_rows['is_senior'] == 0).all()
    
    # UPDATED TEST FOR UNDER 40 (should be 0)
    def test_is_senior_correct_for_under_40(self, df):
        """Test that age under 40 are NOT marked as senior (0)"""
        result = derive_computed_columns(df, "output/test_derived.csv")
        junior_rows = result[result['age'] < 40]
        if len(junior_rows) > 0:
            assert (junior_rows['is_senior'] == 0).all()
    
    def test_salary_per_age_positive(self, df):
        """Test that salary_per_age is positive"""
        result = derive_computed_columns(df, "output/test_derived.csv")
        assert (result['salary_per_age'] > 0).all()
    
    def test_annual_bonus_is_10_percent(self, df):
        """Test that annual_bonus is exactly 10% of salary"""
        result = derive_computed_columns(df, "output/test_derived.csv")
        expected_bonus = (result['salary'] * 0.10).round(2)
        pd.testing.assert_series_equal(result['annual_bonus'], expected_bonus, check_names=False)
    
    def test_salary_level_valid_values(self, df):
        """Test that salary_level contains only valid categories"""
        result = derive_computed_columns(df, "output/test_derived.csv")
        valid_levels = ['High', 'Mid', 'Low']
        assert result['salary_level'].isin(valid_levels).all()
    
    def test_no_null_values_in_derived(self, df):
        """Test that derived columns have no null values"""
        result = derive_computed_columns(df, "output/test_derived.csv")
        derived_cols = ['salary_per_age', 'annual_bonus', 'is_senior', 'salary_level', 'score_rank']
        for col in derived_cols:
            assert result[col].notna().all()
    
    def test_row_count_preserved(self, df):
        """Test that number of rows remains the same"""
        original_count = len(df)
        result = derive_computed_columns(df, "output/test_derived.csv")
        assert len(result) == original_count


# ============================================================================
# TESTS FOR FUNCTION 2: encode_categorical_features
# ============================================================================

class TestEncodeCategoricalFeatures:
    """Test cases for encode_categorical_features function"""
    
    def test_output_file_created(self, df, tmp_path):
        """Test that output file is created"""
        output_file = tmp_path / "test_output.csv"
        encode_categorical_features(df, str(output_file))
        assert os.path.exists(output_file)
    
    def test_department_one_hot_columns_exist(self, df):
        """Test that one-hot encoded department columns are created"""
        result = encode_categorical_features(df, "output/test_encoded.csv")
        expected_cols = ['dept_HR', 'dept_IT', 'dept_Finance']
        for col in expected_cols:
            assert col in result.columns
    
    def test_original_department_column_removed(self, df):
        """Test that original department column is removed"""
        result = encode_categorical_features(df, "output/test_encoded.csv")
        assert 'department' not in result.columns
    
    def test_category_encoded_column_exists(self, df):
        """Test that category_encoded column is created"""
        result = encode_categorical_features(df, "output/test_encoded.csv")
        assert 'category_encoded' in result.columns
    
    def test_category_encoded_valid_values(self, df):
        """Test that category_encoded contains only 1, 2, or 3"""
        result = encode_categorical_features(df, "output/test_encoded.csv")
        assert result['category_encoded'].isin([1, 2, 3]).all()
    
    def test_category_encoded_no_nulls(self, df):
        """Test that category_encoded has no null values"""
        result = encode_categorical_features(df, "output/test_encoded.csv")
        assert result['category_encoded'].notna().all()
    
    def test_dept_columns_are_binary(self, df):
        """Test that department columns contain only 0 or 1"""
        result = encode_categorical_features(df, "output/test_encoded.csv")
        dept_cols = ['dept_HR', 'dept_IT', 'dept_Finance']
        for col in dept_cols:
            assert result[col].isin([0, 1]).all()
    
    def test_each_row_has_exactly_one_dept(self, df):
        """Test that each row has exactly one department column with value 1"""
        result = encode_categorical_features(df, "output/test_encoded.csv")
        dept_cols = ['dept_HR', 'dept_IT', 'dept_Finance']
        dept_sum = result[dept_cols].sum(axis=1)
        assert (dept_sum == 1).all()
    
    def test_row_count_preserved(self, df):
        """Test that number of rows remains the same"""
        original_count = len(df)
        result = encode_categorical_features(df, "output/test_encoded.csv")
        assert len(result) == original_count


# ============================================================================
# TESTS FOR FUNCTION 3: bin_numeric_ranges
# ============================================================================

class TestBinNumericRanges:
    """Test cases for bin_numeric_ranges function"""
    
    def test_output_file_created(self, df, tmp_path):
        """Test that output file is created"""
        output_file = tmp_path / "test_output.csv"
        bin_numeric_ranges(df, str(output_file))
        assert os.path.exists(output_file)
    
    def test_age_group_column_exists(self, df):
        """Test that age_group column is added"""
        result = bin_numeric_ranges(df, "output/test_binned.csv")
        assert 'age_group' in result.columns
    
    def test_salary_range_column_exists(self, df):
        """Test that salary_range column is added"""
        result = bin_numeric_ranges(df, "output/test_binned.csv")
        assert 'salary_range' in result.columns
    
    def test_score_grade_column_exists(self, df):
        """Test that score_grade column is added"""
        result = bin_numeric_ranges(df, "output/test_binned.csv")
        assert 'score_grade' in result.columns
    
    def test_age_group_valid_labels(self, df):
        """Test that age_group contains only valid labels"""
        result = bin_numeric_ranges(df, "output/test_binned.csv")
        valid_labels = ['Young', 'Adult', 'Mid-Age', 'Senior']
        assert result['age_group'].isin(valid_labels).all()
    
    def test_salary_range_valid_labels(self, df):
        """Test that salary_range contains only valid labels"""
        result = bin_numeric_ranges(df, "output/test_binned.csv")
        valid_labels = ['Entry', 'Mid', 'Senior', 'Executive']
        assert result['salary_range'].isin(valid_labels).all()
    
    def test_score_grade_valid_labels(self, df):
        """Test that score_grade contains only valid labels"""
        result = bin_numeric_ranges(df, "output/test_binned.csv")
        valid_labels = ['Fail', 'Pass', 'Good', 'Excellent']
        assert result['score_grade'].isin(valid_labels).all()
    
    def test_original_columns_preserved(self, df):
        """Test that original columns are still present"""
        result = bin_numeric_ranges(df, "output/test_binned.csv")
        original_cols = ['id', 'name', 'age', 'salary', 'department', 'join_date', 'score', 'category']
        for col in original_cols:
            assert col in result.columns
    
    def test_row_count_preserved(self, df):
        """Test that number of rows remains the same"""
        original_count = len(df)
        result = bin_numeric_ranges(df, "output/test_binned.csv")
        assert len(result) == original_count


# ============================================================================
# TESTS FOR FUNCTION 4: time_based_feature_extraction
# ============================================================================

class TestTimeBasedFeatureExtraction:
    """Test cases for time_based_feature_extraction function"""
    
    def test_output_file_created(self, df, tmp_path):
        """Test that output file is created"""
        output_file = tmp_path / "test_output.csv"
        time_based_feature_extraction(df, str(output_file))
        assert os.path.exists(output_file)
    
    def test_join_year_column_exists(self, df):
        """Test that join_year column is added"""
        result = time_based_feature_extraction(df, "output/test_time.csv")
        assert 'join_year' in result.columns
    
    def test_join_month_column_exists(self, df):
        """Test that join_month column is added"""
        result = time_based_feature_extraction(df, "output/test_time.csv")
        assert 'join_month' in result.columns
    
    def test_join_quarter_column_exists(self, df):
        """Test that join_quarter column is added"""
        result = time_based_feature_extraction(df, "output/test_time.csv")
        assert 'join_quarter' in result.columns
    
    def test_join_day_of_week_column_exists(self, df):
        """Test that join_day_of_week column is added"""
        result = time_based_feature_extraction(df, "output/test_time.csv")
        assert 'join_day_of_week' in result.columns
    
    def test_years_in_company_column_exists(self, df):
        """Test that years_in_company column is added"""
        result = time_based_feature_extraction(df, "output/test_time.csv")
        assert 'years_in_company' in result.columns
    
    def test_is_recent_hire_column_exists(self, df):
        """Test that is_recent_hire column is added"""
        result = time_based_feature_extraction(df, "output/test_time.csv")
        assert 'is_recent_hire' in result.columns
    
    def test_join_year_valid_range(self, df):
        """Test that join_year is within reasonable range"""
        result = time_based_feature_extraction(df, "output/test_time.csv")
        current_year = pd.Timestamp.now().year
        assert (result['join_year'] >= 2000).all()
        assert (result['join_year'] <= current_year).all()
    
    def test_join_month_valid_range(self, df):
        """Test that join_month is between 1 and 12"""
        result = time_based_feature_extraction(df, "output/test_time.csv")
        assert (result['join_month'] >= 1).all()
        assert (result['join_month'] <= 12).all()
    
    def test_join_quarter_valid_range(self, df):
        """Test that join_quarter is between 1 and 4"""
        result = time_based_feature_extraction(df, "output/test_time.csv")
        assert (result['join_quarter'] >= 1).all()
        assert (result['join_quarter'] <= 4).all()
    
    def test_years_in_company_positive(self, df):
        """Test that years_in_company is positive"""
        result = time_based_feature_extraction(df, "output/test_time.csv")
        assert (result['years_in_company'] >= 0).all()
    
    def test_is_recent_hire_binary(self, df):
        """Test that is_recent_hire contains only 0 or 1"""
        result = time_based_feature_extraction(df, "output/test_time.csv")
        assert result['is_recent_hire'].isin([0, 1]).all()
    
    def test_row_count_preserved(self, df):
        """Test that number of rows remains the same"""
        original_count = len(df)
        result = time_based_feature_extraction(df, "output/test_time.csv")
        assert len(result) == original_count


# ============================================================================
# TESTS FOR FUNCTION 5: flag_anomalies_column
# ============================================================================

class TestFlagAnomaliesColumn:
    """Test cases for flag_anomalies_column function"""
    
    def test_output_file_created(self, df, tmp_path):
        """Test that output file is created"""
        output_file = tmp_path / "test_output.csv"
        flag_anomalies(df, str(output_file))
        assert os.path.exists(output_file)
    
    def test_salary_anomaly_column_exists(self, df):
        """Test that salary_anomaly column is added"""
        result = flag_anomalies(df, "output/test_flagged.csv")
        assert 'salary_anomaly' in result.columns
    
    def test_score_anomaly_column_exists(self, df):
        """Test that score_anomaly column is added"""
        result = flag_anomalies(df, "output/test_flagged.csv")
        assert 'score_anomaly' in result.columns
    
    def test_age_anomaly_column_exists(self, df):
        """Test that age_anomaly column is added"""
        result = flag_anomalies(df, "output/test_flagged.csv")
        assert 'age_anomaly' in result.columns
    
    def test_is_anomaly_column_exists(self, df):
        """Test that is_anomaly column is added"""
        result = flag_anomalies(df, "output/test_flagged.csv")
        assert 'is_anomaly' in result.columns
    
    def test_salary_anomaly_binary(self, df):
        """Test that salary_anomaly contains only 0 or 1"""
        result = flag_anomalies(df, "output/test_flagged.csv")
        assert result['salary_anomaly'].isin([0, 1]).all()
    
    def test_score_anomaly_binary(self, df):
        """Test that score_anomaly contains only 0 or 1"""
        result = flag_anomalies(df, "output/test_flagged.csv")
        assert result['score_anomaly'].isin([0, 1]).all()
    
    def test_age_anomaly_binary(self, df):
        """Test that age_anomaly contains only 0 or 1"""
        result = flag_anomalies(df, "output/test_flagged.csv")
        assert result['age_anomaly'].isin([0, 1]).all()
    
    def test_is_anomaly_binary(self, df):
        """Test that is_anomaly contains only 0 or 1"""
        result = flag_anomalies(df, "output/test_flagged.csv")
        assert result['is_anomaly'].isin([0, 1]).all()
    
    def test_is_anomaly_is_union_of_flags(self, df):
        """Test that is_anomaly is 1 if any flag is 1"""
        result = flag_anomalies(df, "output/test_flagged.csv")
        expected_union = ((result['salary_anomaly'] == 1) | 
                          (result['score_anomaly'] == 1) | 
                          (result['age_anomaly'] == 1)).astype(int)
        pd.testing.assert_series_equal(result['is_anomaly'], expected_union, check_names=False)
    
    def test_no_null_anomaly_flags(self, df):
        """Test that anomaly flag columns have no null values"""
        result = flag_anomalies(df, "output/test_flagged.csv")
        anomaly_cols = ['salary_anomaly', 'score_anomaly', 'age_anomaly', 'is_anomaly']
        for col in anomaly_cols:
            assert result[col].notna().all()
    
    def test_row_count_preserved(self, df):
        """Test that number of rows remains the same"""
        original_count = len(df)
        result = flag_anomalies(df, "output/test_flagged.csv")
        assert len(result) == original_count