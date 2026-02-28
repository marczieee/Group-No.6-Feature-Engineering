"""
Group 6 - Feature Engineering
tests/test_functions.py — PyTest test cases for all 5 processing functions
Run with: pytest tests/test_functions.py -v
"""

import pytest
import pandas as pd
import os
import sys

# Add parent directory to path so imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from derive_computed_columns       import derive_computed_columns
from encode_categorical_features   import encode_categorical_features
from bin_numeric_ranges            import bin_numeric_ranges
from time_based_feature_extraction import time_based_feature_extraction
from flag_anomalies_column         import flag_anomalies_column

# ─── Shared config ────────────────────────────────────────────────────────────
INPUT  = "input/data.csv"
OUTPUT = "output"

@pytest.fixture(scope="session", autouse=True)
def ensure_dirs():
    """Make sure input and output folders exist before any tests run."""
    os.makedirs(OUTPUT, exist_ok=True)
    os.makedirs("input", exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTION 1: derive_computed_columns
# ═══════════════════════════════════════════════════════════════════════════════

class TestDeriveComputedColumns:

    @pytest.fixture(scope="class")
    def df(self):
        out = os.path.join(OUTPUT, "test_derived.csv")
        return derive_computed_columns(INPUT, out)

    def test_output_file_created(self, df):
        assert os.path.exists(os.path.join(OUTPUT, "test_derived.csv"))

    def test_salary_per_age_column_exists(self, df):
        assert 'salary_per_age' in df.columns

    def test_annual_bonus_column_exists(self, df):
        assert 'annual_bonus' in df.columns

    def test_is_senior_column_exists(self, df):
        assert 'is_senior' in df.columns

    def test_salary_level_column_exists(self, df):
        assert 'salary_level' in df.columns

    def test_score_rank_column_exists(self, df):
        assert 'score_rank' in df.columns

    def test_is_senior_binary(self, df):
        assert set(df['is_senior'].unique()).issubset({0, 1})

    def test_is_senior_correct_for_age_40_plus(self, df):
        senior_rows = df[df['age'] >= 40]
        assert (senior_rows['is_senior'] == 1).all()

    def test_is_senior_correct_for_under_40(self, df):
        junior_rows = df[df['age'] < 40]
        assert (junior_rows['is_senior'] == 0).all()

    def test_salary_per_age_positive(self, df):
        assert (df['salary_per_age'] > 0).all()

    def test_annual_bonus_is_10_percent(self, df):
        original = pd.read_csv(INPUT)
        expected = (original['salary'] * 0.10).round(2)
        pd.testing.assert_series_equal(
            df['annual_bonus'].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False
        )

    def test_salary_level_valid_values(self, df):
        assert set(df['salary_level'].unique()).issubset({'High', 'Mid', 'Low'})

    def test_no_null_values_in_derived(self, df):
        new_cols = ['salary_per_age', 'annual_bonus', 'is_senior', 'salary_level', 'score_rank']
        assert df[new_cols].isnull().sum().sum() == 0

    def test_row_count_preserved(self, df):
        original = pd.read_csv(INPUT)
        assert len(df) == len(original)


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTION 2: encode_categorical_features
# ═══════════════════════════════════════════════════════════════════════════════

class TestEncodeCategoricalFeatures:

    @pytest.fixture(scope="class")
    def df(self):
        out = os.path.join(OUTPUT, "test_encoded.csv")
        return encode_categorical_features(INPUT, out)

    def test_output_file_created(self, df):
        assert os.path.exists(os.path.join(OUTPUT, "test_encoded.csv"))

    def test_department_one_hot_columns_exist(self, df):
        assert any(col.startswith('dept_') for col in df.columns)

    def test_original_department_column_removed(self, df):
        assert 'department' not in df.columns

    def test_category_encoded_column_exists(self, df):
        assert 'category_encoded' in df.columns

    def test_category_encoded_valid_values(self, df):
        assert set(df['category_encoded'].unique()).issubset({1, 2, 3})

    def test_category_encoded_no_nulls(self, df):
        assert df['category_encoded'].isnull().sum() == 0

    def test_dept_columns_are_binary(self, df):
        dept_cols = [col for col in df.columns if col.startswith('dept_')]
        for col in dept_cols:
            assert set(df[col].unique()).issubset({0, 1})

    def test_each_row_has_exactly_one_dept(self, df):
        dept_cols = [col for col in df.columns if col.startswith('dept_')]
        assert (df[dept_cols].sum(axis=1) == 1).all()

    def test_row_count_preserved(self, df):
        original = pd.read_csv(INPUT)
        assert len(df) == len(original)


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTION 3: bin_numeric_ranges
# ═══════════════════════════════════════════════════════════════════════════════

class TestBinNumericRanges:

    @pytest.fixture(scope="class")
    def df(self):
        out = os.path.join(OUTPUT, "test_binned.csv")
        return bin_numeric_ranges(INPUT, out)

    def test_output_file_created(self, df):
        assert os.path.exists(os.path.join(OUTPUT, "test_binned.csv"))

    def test_age_group_column_exists(self, df):
        assert 'age_group' in df.columns

    def test_salary_range_column_exists(self, df):
        assert 'salary_range' in df.columns

    def test_score_grade_column_exists(self, df):
        assert 'score_grade' in df.columns

    def test_age_group_valid_labels(self, df):
        valid = {'Young', 'Adult', 'Mid-Age', 'Senior'}
        actual = set(df['age_group'].dropna().unique())
        assert actual.issubset(valid)

    def test_salary_range_valid_labels(self, df):
        valid = {'Entry', 'Mid', 'Senior', 'Executive'}
        actual = set(df['salary_range'].dropna().unique())
        assert actual.issubset(valid)

    def test_score_grade_valid_labels(self, df):
        valid = {'Fail', 'Pass', 'Good', 'Excellent'}
        actual = set(df['score_grade'].dropna().unique())
        assert actual.issubset(valid)

    def test_original_columns_preserved(self, df):
        original = pd.read_csv(INPUT)
        for col in original.columns:
            assert col in df.columns

    def test_row_count_preserved(self, df):
        original = pd.read_csv(INPUT)
        assert len(df) == len(original)


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTION 4: time_based_feature_extraction
# ═══════════════════════════════════════════════════════════════════════════════

class TestTimeBasedFeatureExtraction:

    @pytest.fixture(scope="class")
    def df(self):
        out = os.path.join(OUTPUT, "test_time.csv")
        return time_based_feature_extraction(INPUT, out)

    def test_output_file_created(self, df):
        assert os.path.exists(os.path.join(OUTPUT, "test_time.csv"))

    def test_join_year_column_exists(self, df):
        assert 'join_year' in df.columns

    def test_join_month_column_exists(self, df):
        assert 'join_month' in df.columns

    def test_join_quarter_column_exists(self, df):
        assert 'join_quarter' in df.columns

    def test_years_in_company_column_exists(self, df):
        assert 'years_in_company' in df.columns

    def test_is_recent_hire_column_exists(self, df):
        assert 'is_recent_hire' in df.columns

    def test_join_year_valid_range(self, df):
        import datetime
        current_year = datetime.datetime.today().year
        assert df['join_year'].between(2000, current_year).all()

    def test_join_month_valid_range(self, df):
        assert df['join_month'].between(1, 12).all()

    def test_join_quarter_valid_range(self, df):
        assert df['join_quarter'].between(1, 4).all()

    def test_years_in_company_positive(self, df):
        assert (df['years_in_company'] > 0).all()

    def test_is_recent_hire_binary(self, df):
        assert set(df['is_recent_hire'].unique()).issubset({0, 1})

    def test_row_count_preserved(self, df):
        original = pd.read_csv(INPUT)
        assert len(df) == len(original)


# ═══════════════════════════════════════════════════════════════════════════════
# FUNCTION 5: flag_anomalies_column
# ═══════════════════════════════════════════════════════════════════════════════

class TestFlagAnomaliesColumn:

    @pytest.fixture(scope="class")
    def df(self):
        out = os.path.join(OUTPUT, "test_flagged.csv")
        return flag_anomalies_column(INPUT, out)

    def test_output_file_created(self, df):
        assert os.path.exists(os.path.join(OUTPUT, "test_flagged.csv"))

    def test_salary_anomaly_column_exists(self, df):
        assert 'salary_anomaly' in df.columns

    def test_score_anomaly_column_exists(self, df):
        assert 'score_anomaly' in df.columns

    def test_age_anomaly_column_exists(self, df):
        assert 'age_anomaly' in df.columns

    def test_is_anomaly_column_exists(self, df):
        assert 'is_anomaly' in df.columns

    def test_salary_anomaly_binary(self, df):
        assert set(df['salary_anomaly'].unique()).issubset({0, 1})

    def test_score_anomaly_binary(self, df):
        assert set(df['score_anomaly'].unique()).issubset({0, 1})

    def test_is_anomaly_binary(self, df):
        assert set(df['is_anomaly'].unique()).issubset({0, 1})

    def test_is_anomaly_is_union_of_flags(self, df):
        expected = (
            (df['salary_anomaly'] == 1) |
            (df['score_anomaly']  == 1) |
            (df['age_anomaly']    == 1)
        ).astype(int)
        pd.testing.assert_series_equal(df['is_anomaly'], expected, check_names=False)

    def test_no_null_anomaly_flags(self, df):
        flag_cols = ['salary_anomaly', 'score_anomaly', 'age_anomaly', 'is_anomaly']
        assert df[flag_cols].isnull().sum().sum() == 0

    def test_row_count_preserved(self, df):
        original = pd.read_csv(INPUT)
        assert len(df) == len(original)
