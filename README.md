# 🚀 Group 6 - Feature Engineering Pipeline

## ✨ Key Features

✅ **Automatic Processing** - Processes ALL CSV files in the `input/` folder automatically  
✅ **CI/CD Ready** - Runs seamlessly in GitHub Actions without manual input  
✅ **Multi-File Support** - Handle multiple datasets simultaneously  
✅ **Smart Caching** - Only reprocesses files that have changed  
✅ **Organized Outputs** - Each input file gets its own output subdirectory  

---

## 📁 Project Structure

```
Group-No.6-Feature-Engineering/
│
├── input/                          # 📥 Place your CSV files here
│   ├── data.csv                    # Your input files
│   ├── sales_data.csv              # Can have multiple files
│   └── customer_data.csv           # All will be processed
│
├── output/                         # 📤 Generated outputs
│   ├── data/                       # Outputs for data.csv
│   │   ├── derived_computed_columns.csv
│   │   ├── encoded_categorical_features.csv
│   │   ├── binned_numeric_ranges.csv
│   │   ├── time_based_features.csv
│   │   ├── flagged_anomalies.csv
│   │   └── consolidated_all_features.csv
│   │
│   ├── sales_data/                 # Outputs for sales_data.csv
│   │   └── ... (same structure)
│   │
│   └── customer_data/              # Outputs for customer_data.csv
│       └── ... (same structure)
│
├── main.py                         # 🎯 Main pipeline script
├── derive_computed_columns.py
├── encode_categorical_features.py
├── bin_numeric_ranges.py
├── time_based_feature_extraction.py
├── flag_anomalies_column.py
└── .github/
    └── workflows/
        └── ci.yml                  # GitHub Actions configuration
```

---

## 🎯 How to Use

### **Method 1: Local Execution**

1. **Add your CSV files** to the `input/` folder:
   ```bash
   cp your_data.csv input/
   ```

2. **Run the pipeline**:
   ```bash
   python main.py
   ```

3. **Check outputs** in `output/your_data/` folder

### **Method 2: GitHub Actions (Automatic)**

1. **Add CSV files** to the `input/` folder

2. **Commit and push**:
   ```bash
   git add input/your_data.csv
   git commit -m "Add new dataset"
   git push origin main
   ```

3. **CI automatically runs** and:
   - ✅ Processes all files in `input/`
   - ✅ Generates outputs in `output/`
   - ✅ Commits results back to repository
   - ✅ Creates downloadable artifacts

4. **View results**:
   - Check the `output/` folder in your repository
   - Download artifacts from GitHub Actions tab
   - View job summary for processing details

---

## 🔧 What Changed from Original

### ✅ **Fixed Issues**

| Issue | Solution |
|-------|----------|
| ❌ Hardcoded `input/data.csv` | ✅ Now scans entire `input/` folder |
| ❌ Interactive prompts block CI | ✅ Auto-detects CI environment |
| ❌ Single file only | ✅ Processes multiple files automatically |
| ❌ No output organization | ✅ Separate folders per input file |

### 🆕 **New Features**

1. **Multi-File Processing**: Drop multiple CSV files in `input/`, all get processed
2. **Smart Change Detection**: Only reprocesses files that have changed (hash-based)
3. **CI Environment Detection**: Skips interactive prompts when running in GitHub Actions
4. **Better Output Organization**: Each input file gets its own output subdirectory
5. **Comprehensive Logging**: Detailed processing information and summaries
6. **Artifact Upload**: Outputs available as downloadable GitHub artifacts

---

## 🎓 For Your Professor

### **Requirements Met:**

✅ **"Any files can be input in the repository"**  
   - Place any CSV file in `input/` folder → automatically processed

✅ **"Can input any files in the input folder"**  
   - Supports unlimited number of CSV files
   - No hardcoded filenames

✅ **"Automated using our project"**  
   - GitHub Actions CI runs automatically on push
   - No manual intervention needed

✅ **"Output can be seen or handled"**  
   - Outputs committed back to repository in `output/` folder
   - Available as downloadable artifacts
   - Job summary shows processing details

### **Testing the System:**

```bash
# Test 1: Single file
echo "id,name,age,salary,department,join_date,score,category
1,Alice,25,50000,IT,2020-01-15,85,A" > input/test1.csv
git add input/test1.csv
git commit -m "Test: Single file"
git push

# Test 2: Multiple files
cp input/test1.csv input/test2.csv
cp input/test1.csv input/test3.csv
git add input/
git commit -m "Test: Multiple files"
git push

# Check output/ folder after CI completes
```

---

## 🐛 Troubleshooting

### **"No CSV files found"**
- Ensure files are in `input/` folder
- Files must have `.csv` extension
- Files must not start with `.` (hidden)

### **"CI fails to commit"**
- Check GitHub Actions has write permissions
- Verify `permissions: contents: write` in workflow

### **"Pipeline runs but no outputs"**
- Check CI logs for error messages
- Verify input CSV has proper format
- Check individual function files are present

---

## 📊 CI/CD Pipeline Details

The GitHub Actions workflow:

1. **Triggers on**:
   - Push to `main` branch
   - Changes to `input/` folder
   - Changes to Python files
   - Manual trigger via workflow_dispatch

2. **Runs**:
   - Sets up Python 3.10
   - Installs dependencies (pandas, numpy, pytest)
   - Processes all CSV files in `input/`
   - Runs tests (if available)
   - Commits outputs back to repository
   - Uploads artifacts for download

3. **Outputs**:
   - Generated CSV files in `output/`
   - Job summary with file counts
   - Downloadable artifacts (30-day retention)

---

## 💡 Tips

- **Large files**: CI works best with files < 100MB
- **Multiple datasets**: All files processed in parallel
- **Change detection**: Only changed files reprocess (saves time)
- **Manual override**: In local mode, can force reprocess all files

---

## 📝 Example Output

```
📋 Found 3 CSV file(s) in input folder:
   • data.csv (1,234 bytes)
   • sales.csv (5,678 bytes)
   • customers.csv (3,456 bytes)

======================================================================
  Processing File 1/3: data.csv
======================================================================
📊 Running Function 1: Derive Computed Columns
✅ Derived columns saved to output/data/derived_computed_columns.csv

... (continues for all functions)

✅ Pipeline complete for data.csv!

📁 Output directory: output/
```

---

## 🎉 Ready to Use!

Just push your CSV files to `input/` and watch the magic happen! 🚀
