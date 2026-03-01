import pandas as pd
import great_expectations as ge
import os
import psycopg2
from datetime import datetime

def log_metrics_to_postgres(total_rows, failed_rules_count, is_clean):
    """
    Logs data quality validation metrics into the PostgreSQL database.
    Creates the 'data_quality_metrics' table if it does not already exist.

    Args:
        total_rows (int): The total number of rows validated.
        failed_rules_count (int): The number of validation rules that failed.
        is_clean (bool): The final status of the dataset (True if passed all rules).
    """
    # Determine database host based on environment (Docker vs Local)
    db_host = os.getenv("DB_HOST", "localhost")
    
    try:
        # Establish connection to the PostgreSQL database
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host=db_host,
            port="5432"
        )
        cursor = conn.cursor()

        # Ensure the metrics table exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS data_quality_metrics (
            id SERIAL PRIMARY KEY,
            execution_time TIMESTAMP NOT NULL,
            pipeline_name VARCHAR(255) NOT NULL,
            total_rows INT NOT NULL,
            failed_rules_count INT NOT NULL,
            status VARCHAR(50) NOT NULL
        );
        """
        cursor.execute(create_table_query)

        # Insert the validation results
        insert_query = """
        INSERT INTO data_quality_metrics (execution_time, pipeline_name, total_rows, failed_rules_count, status)
        VALUES (%s, %s, %s, %s, %s);
        """
        status_text = "PASSED" if is_clean else "FAILED"
        cursor.execute(insert_query, (datetime.now(), "ecommerce_sales_pipeline", total_rows, failed_rules_count, status_text))

        # Commit transaction and close connection
        conn.commit()
        cursor.close()
        conn.close()
        print("Successfully logged validation metrics to PostgreSQL.")

    except Exception as e:
        print(f"Failed to log metrics to database: {e}")

def run_quality_firewall(input_path="data/raw/sales_data.csv"):
    """
    Executes the data quality firewall against the specified CSV file.
    Evaluates the data against defined business rules and logs the results.

    Args:
        input_path (str): The file path to the raw data CSV.

    Returns:
        bool: True if all data quality checks pass, False otherwise.
    """
    print(f"Starting Data Quality Firewall for: {input_path}\n")
    
    if not os.path.exists(input_path):
        print(f"Error: Source file not found at {input_path}")
        return False

    # Load data into Great Expectations DataFrame
    df = pd.read_csv(input_path)
    total_rows = len(df)
    ge_df = ge.from_pandas(df)
    
    results = []
    
    # Rule 1: Product ID must not be null
    print("Executing Rule 1: Validating Product ID completeness...")
    res_prod = ge_df.expect_column_values_to_not_be_null("product_id")
    results.append({
        "rule": "No Null Product ID", 
        "success": res_prod.success, 
        "unexpected_percent": res_prod.result.get('unexpected_percent', 0)
    })
    
    # Rule 2: Unit Price must be >= 0
    print("Executing Rule 2: Validating Unit Price logic...")
    res_price = ge_df.expect_column_values_to_be_between("unit_price", min_value=0.0)
    results.append({
        "rule": "Positive Unit Price", 
        "success": res_price.success, 
        "unexpected_percent": res_price.result.get('unexpected_percent', 0)
    })
    
    # Rule 3: Category must be within the approved list
    print("Executing Rule 3: Validating Category adherence...")
    approved_categories = ['Electronics', 'Home & Garden', 'Fashion', 'Beauty', 'Sports']
    res_category = ge_df.expect_column_values_to_be_in_set("category", approved_categories)
    results.append({
        "rule": "Valid Categories", 
        "success": res_category.success, 
        "unexpected_percent": res_category.result.get('unexpected_percent', 0)
    })

    # Summarize Results
    print("\n--- Validation Summary ---")
    all_passed = True
    failed_rules_count = 0
    
    for r in results:
        status = "PASSED" if r["success"] else "❌ FAILED"
        if not r["success"]:
            all_passed = False
            failed_rules_count += 1
        print(f"{status} | {r['rule']} | Error Rate: {r['unexpected_percent']:.2f}%")
        
    print("-" * 30)
    
    # Log metrics to PostgreSQL
    log_metrics_to_postgres(total_rows, failed_rules_count, all_passed)

    if all_passed:
        print("ACTION: Data validation successful. Proceeding to Data Warehouse.")
    else:
        print("ACTION: Data validation failed. Initiating quarantine protocols.")
        
    return all_passed

if __name__ == "__main__":
    run_quality_firewall()