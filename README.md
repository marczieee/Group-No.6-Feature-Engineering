# 📊 Group 6 — Feature Engineering CSV Pipeline

> **DEV OPS Midterm Activity** | Automated CSV Data Processing using GitHub Actions CI Pipeline

---

## 👥 Group Members

| Role                    | Responsibility                                      |
|-------------------------|-----------------------------------------------------|
| Data Processing Lead    | Implements the 5 CSV processing Python functions    |
| DevOps Engineer         | Configures the GitHub Actions CI pipeline           |
| Tester                  | Writes and validates PyTest test cases              |
| Documenter / Presenter  | Prepares README and presentation slides             |

---

## 📁 Project Structure

```
group6-feature-engineering/
│
├── .github/
│   └── workflows/
│       └── ci.yml                        ← GitHub Actions CI pipeline
│
├── input/
│   └── data.csv                          ← Input CSV file (place yours here)
│
├── output/                               ← Auto-generated processed CSV files
│   ├── derived_computed_columns.csv
│   ├── encoded_categorical_features.csv
│   ├── binned_numeric_ranges.csv
│   ├── time_based_features.csv
│   └── flagged_anomalies.csv
│
├── derive_computed_columns.py            ← Function 1
├── encode_categorical_features.py        ← Function 2
├── bin_numeric_ranges.py                 ← Function 3
├── time_based_feature_extraction.py      ← Function 4
├── flag_anomalies_column.py              ← Function 5
├── main.py                               ← Runs all 5 functions
│
├── tests/
│   └── test_functions.py                 ← PyTest test cases
│
├── requirements.txt                      ← Python dependencies
└── README.md                             ← This file
```

---

## ⚙️ Functions Implemented

### 1. `derive_computed_columns.py`
Adds new computed columns derived from existing numeric data.

| New Column       | Description                              |
|------------------|------------------------------------------|
| `salary_per_age` | Salary divided by age (efficiency ratio) |
| `annual_bonus`   | 10% of salary as estimated bonus         |
| `is_senior`      | 1 if age ≥ 40, else 0                    |
| `salary_level`   | High / Mid / Low based on salary range   |
| `score_rank`     | Score normalized out of 10               |

---

### 2. `encode_categorical_features.py`
Converts categorical/text columns into numeric form for ML compatibility.

| Transformation         | Description                                            |
|------------------------|--------------------------------------------------------|
| One-hot encode dept    | `department` → `dept_HR`, `dept_IT`, `dept_Finance`   |
| Label encode category  | `category` A=1, B=2, C=3 → `category_encoded`         |

---

### 3. `bin_numeric_ranges.py`
Groups continuous numeric values into meaningful labeled buckets.

| New Column      | Bins                                              |
|-----------------|---------------------------------------------------|
| `age_group`     | Young / Adult / Mid-Age / Senior                  |
| `salary_range`  | Entry / Mid / Senior / Executive                  |
| `score_grade`   | Fail / Pass / Good / Excellent                    |

---

### 4. `time_based_feature_extraction.py`
Extracts useful date/time-based features from date columns.

| New Column          | Description                                    |
|---------------------|------------------------------------------------|
| `join_year`         | Year extracted from join_date                  |
| `join_month`        | Month number (1–12)                            |
| `join_quarter`      | Quarter (1–4)                                  |
| `join_day_of_week`  | Day of week (0=Monday, 6=Sunday)               |
| `years_in_company`  | Total years since joining (rounded to 1 decimal)|
| `is_recent_hire`    | 1 if joined 2021 or later, else 0              |

---

### 5. `flag_anomalies_column.py`
Detects statistical outliers and flags them in the dataset.

| New Column        | Method Used                                    |
|-------------------|------------------------------------------------|
| `salary_anomaly`  | IQR method (1.5× IQR rule)                    |
| `score_anomaly`   | Z-score method (±2 standard deviations)        |
| `age_anomaly`     | IQR method                                     |
| `is_anomaly`      | 1 if ANY individual flag is triggered          |

---

## 🚀 How to Run Locally

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/group6-feature-engineering.git
cd group6-feature-engineering
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Place your CSV file in the input folder
```bash
# Your file should be at: input/data.csv
```

### 4. Run the full pipeline
```bash
python main.py
```

### 5. Run all tests
```bash
pytest tests/test_functions.py -v
```

---

## 🔄 CI/CD Workflow

Every `git push` or `pull request` to the `main` branch automatically triggers the GitHub Actions pipeline:

```
Push to GitHub
     │
     ▼
GitHub Actions Triggered
     │
     ├─ 1. Checkout repository code
     ├─ 2. Set up Python 3.10
     ├─ 3. Install requirements (pandas, numpy, pytest)
     ├─ 4. Run main.py (all 5 functions process input/data.csv)
     ├─ 5. Run pytest (validates all 5 functions pass tests)
     └─ 6. Commit & push output/ CSV files back to repository
```

The pipeline is defined in: `.github/workflows/ci.yml`

---

## 🧪 Testing Strategy

All 5 functions are tested using **PyTest**. Tests cover:

- Output file is created successfully
- New columns are added with correct names
- Column values are within expected ranges / valid labels
- Binary flag columns only contain 0 or 1
- No null values in critical columns
- Row count is preserved (no data loss)
- Business logic correctness (e.g., `is_senior` correct for age ≥ 40)

Run tests with:
```bash
pytest tests/test_functions.py -v
```

---

## 📦 Dependencies

| Package  | Version   | Purpose                        |
|----------|-----------|--------------------------------|
| pandas   | ≥ 1.5.0   | CSV loading and data processing|
| numpy    | ≥ 1.23.0  | Numeric computations           |
| pytest   | ≥ 7.0.0   | Automated testing              |

---

## 📋 Input CSV Format

Your input CSV (`input/data.csv`) should contain these columns:

| Column       | Type    | Example        |
|--------------|---------|----------------|
| `id`         | integer | 1              |
| `name`       | string  | Alice          |
| `age`        | integer | 25             |
| `salary`     | float   | 50000          |
| `department` | string  | HR / IT / Finance |
| `join_date`  | date    | 2021-03-15     |
| `score`      | integer | 88             |
| `category`   | string  | A / B / C      |
