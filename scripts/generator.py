import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import random

def generate_sales_data(num_rows=1000, output_path="data/raw/sales_data.csv"):
    """
    Generate realistic e-commerce sales data with intentional quality issues 
    to test the Data Quality Firewall.
    """
    # 1. Ensure the target directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # 2. Base Data Generation (Healthy Data)
    start_date = datetime(2026, 1, 1)
    categories = ['Electronics', 'Home & Garden', 'Fashion', 'Beauty', 'Sports']
    
    data = {
        'order_id': [f"ORD-{10000 + i}" for i in range(num_rows)],
        'order_date': [(start_date + timedelta(days=random.randint(0, 58))).strftime('%Y-%m-%d') for i in range(num_rows)],
        'product_id': [f"PROD-{random.randint(100, 999)}" for i in range(num_rows)],
        'category': [random.choice(categories) for i in range(num_rows)],
        'unit_price': [round(random.uniform(10.0, 500.0), 2) for i in range(num_rows)],
        'quantity': [random.randint(1, 5) for i in range(num_rows)]
    }
    
    df = pd.DataFrame(data)
    df['total_price'] = df['unit_price'] * df['quantity']

    # 3. Injecting "Dirty Data" (The Chaos Factor)
    
    # Issue 1: Negative Prices (Logic Error) - 3% of data
    neg_indices = random.sample(range(num_rows), int(num_rows * 0.03))
    df.loc[neg_indices, 'unit_price'] *= -1
    
    # Issue 2: Missing Values (Data Entry Error) - 4% of data
    null_indices = random.sample(range(num_rows), int(num_rows * 0.04))
    df.loc[null_indices, 'product_id'] = np.nan
    
    # Issue 3: Future Dates (System Clock Error) - 2% of data
    future_indices = random.sample(range(num_rows), int(num_rows * 0.02))
    df.loc[future_indices, 'order_date'] = '2099-12-31'
    
    # Issue 4: Invalid Categories (Typo/Formatting Error) - 3% of data
    typo_indices = random.sample(range(num_rows), int(num_rows * 0.03))
    df.loc[typo_indices, 'category'] = 'Elec-Invalid-123'

    # 4. Save to CSV
    df.to_csv(output_path, index=False)
    print(f"Successfully generated {num_rows} rows of data at: {output_path}")
    print(f"Notice: Dirty data has been injected for validation testing.")

if __name__ == "__main__":
    generate_sales_data()