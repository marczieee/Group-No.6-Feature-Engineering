"""
Group 6 - Feature Engineering
main.py - Orchestrates all 5 CSV processing functions
Run this file to process the input CSV through all feature engineering steps.
"""

import os
import sys

from derive_computed_columns      import derive_computed_columns
from encode_categorical_features  import encode_categorical_features
from bin_numeric_ranges           import bin_numeric_ranges
from time_based_feature_extraction import time_based_feature_extraction
from flag_anomalies_column        import flag_anomalies_column

# ─── Configuration ────────────────────────────────────────────────────────────
INPUT_FILE = "input/data.csv"

OUTPUT_FILES = {
    "derived_computed_columns"     : "output/derived_computed_columns.csv",
    "encoded_categorical_features" : "output/encoded_categorical_features.csv",
    "binned_numeric_ranges"        : "output/binned_numeric_ranges.csv",
    "time_based_features"          : "output/time_based_features.csv",
    "flagged_anomalies"            : "output/flagged_anomalies.csv",
}
# ──────────────────────────────────────────────────────────────────────────────


def run_pipeline():
    print("=" * 55)
    print("  Group 6 — Feature Engineering CSV Pipeline")
    print("=" * 55)

    # Validate input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"\n❌ ERROR: Input file '{INPUT_FILE}' not found!")
        print("   Please place your CSV file in the 'input/' folder.")
        sys.exit(1)

    print(f"\n📂 Input  : {INPUT_FILE}")
    print(f"📁 Output : output/\n")

    # Run all 5 feature engineering functions
    derive_computed_columns(
        INPUT_FILE,
        OUTPUT_FILES["derived_computed_columns"]
    )

    encode_categorical_features(
        INPUT_FILE,
        OUTPUT_FILES["encoded_categorical_features"]
    )

    bin_numeric_ranges(
        INPUT_FILE,
        OUTPUT_FILES["binned_numeric_ranges"]
    )

    time_based_feature_extraction(
        INPUT_FILE,
        OUTPUT_FILES["time_based_features"]
    )

    flag_anomalies_column(
        INPUT_FILE,
        OUTPUT_FILES["flagged_anomalies"]
    )

    print("\n" + "=" * 55)
    print("  ✅ Pipeline complete! All output files saved.")
    print("=" * 55)

    # Print summary of output files
    print("\n📄 Output files generated:")
    for name, path in OUTPUT_FILES.items():
        size = os.path.getsize(path) if os.path.exists(path) else 0
        print(f"   • {path}  ({size} bytes)")


if __name__ == "__main__":
    run_pipeline()
