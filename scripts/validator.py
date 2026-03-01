import pandas as pd
import great_expectations as ge
import os
import json

def run_quality_firewall(input_path="data/raw/sales_data.csv"):
    """
    Run Data Quality checks using Great Expectations.
    It evaluates the data against defined business rules and outputs a summary.
    """
    print(f"Starting Data Quality Firewall for: {input_path}\n")
    
    # 1. Ensure the input file exists
    if not os.path.exists(input_path):
        print(f"Error: File not found at {input_path}")
        return

    # 2. Load data into Great Expectations DataFrame
    df = pd.read_csv(input_path)
    ge_df = ge.from_pandas(df)
    
    # 3. Defining the Expectations (The Business Rules)
    results = []
    
    # Rule 1: Product ID must not be null
    print("Running Rule 1: Checking for missing Product IDs...")
    res_prod = ge_df.expect_column_values_to_not_be_null("product_id")
    results.append({"rule": "No Null Product ID", "success": res_prod.success, "unexpected_percent": res_prod.result.get('unexpected_percent', 0)})
    
    # Rule 2: Unit Price must be >= 0
    print("Running Rule 2: Checking for negative Unit Prices...")
    res_price = ge_df.expect_column_values_to_be_between("unit_price", min_value=0.0)
    results.append({"rule": "Positive Unit Price", "success": res_price.success, "unexpected_percent": res_price.result.get('unexpected_percent', 0)})
    
    # Rule 3: Category must be within the approved list
    print("Running Rule 3: Checking for invalid Categories...")
    approved_categories = ['Electronics', 'Home & Garden', 'Fashion', 'Beauty', 'Sports']
    res_category = ge_df.expect_column_values_to_be_in_set("category", approved_categories)
    results.append({"rule": "Valid Categories", "success": res_category.success, "unexpected_percent": res_category.result.get('unexpected_percent', 0)})

    # 4. Summary & Decision (The Firewall Gate)
    print("\n --- Data Quality Summary ---")
    all_passed = True
    for r in results:
        status = "PASSED" if r["success"] else "FAILED"
        if not r["success"]:
            all_passed = False
        print(f"{status} | {r['rule']} | Error Rate: {r['unexpected_percent']:.2f}%")
        
    print("-" * 30)
    if all_passed:
        print("ACTION: Data is CLEAN. Ready to load into Data Warehouse.")
    else:
        print("ACTION: Data is DIRTY. Sending to Quarantine and Alerting Data Engineer!")
    
    return all_passed

if __name__ == "__main__":
    run_quality_firewall()