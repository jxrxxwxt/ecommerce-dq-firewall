# E-commerce Data Quality Firewall

An automated, end-to-end Data Engineering pipeline designed to validate, quarantine, and monitor the quality of simulated E-commerce sales data.

## Project Overview
This project simulates a real-world scenario where raw data from an E-commerce platform needs to be rigorously validated before being loaded into a Data Warehouse. It utilizes **Great Expectations** as the core validation engine to filter out invalid records, ensuring downstream systems only process high-quality data. 

When dirty data is detected, the pipeline automatically routes the file to a quarantine zone, alerts the Data Engineering team via a **Discord Webhook**, and logs the execution metrics into a **PostgreSQL** database. A **Streamlit** dashboard is provided to monitor pipeline health and success rates.

## Architecture & Workflow
1. **Data Generator**: A Python script generates simulated E-commerce sales data, intentionally injecting anomalies (e.g., negative prices, null IDs, invalid categories).
2. **Quality Gate (Great Expectations)**: Validates the incoming CSV file against predefined business rules.
3. **Apache Airflow DAG**: Orchestrates the pipeline with branching logic:
   - **Pass**: Data proceeds to the simulated Data Warehouse.
   - **Fail**: Data is moved to a quarantine folder, and an alert is dispatched.
4. **Metrics Logger**: Execution results are stored in a PostgreSQL database.
5. **Dashboard**: A Streamlit web application visualizes the validation metrics and pipeline trends.

## Technology Stack
* **Orchestration**: Apache Airflow
* **Data Quality / Testing**: Great Expectations, Pandas
* **Database**: PostgreSQL
* **Frontend / Dashboard**: Streamlit
* **Infrastructure**: Docker & Docker Compose
* **Alerting**: Discord Webhook Integration

## Key Features
* **File-level Quarantine (All-or-Nothing)**: Prevents contaminated datasets from entering the Data Warehouse.
* **Automated Alerting**: Sends structured Embed messages to Discord immediately upon validation failure.
* **Metrics Tracking**: Persists historical validation results to track data quality trends over time.
* **Containerized Environment**: Completely reproducible setup using Docker Compose.

## How to Run Locally

### Prerequisites
* Docker and Docker Compose installed
* Python 3.9+ (if running scripts outside Docker)

### Setup Instructions
1. Clone the repository:
   ```
   git clone [https://github.com/your-username/ecommerce-dq-firewall.git](https://github.com/your-username/ecommerce-dq-firewall.git)
   cd ecommerce-dq-firewall
2. Configure environment variables (create a .env file):
    ```
    DISCORD_WEBHOOK_URL=your_webhook_url_here
    ```

3. Start the infrastructure:
    ```
    docker-compose up -d
    ```

4. Access the services:

    Airflow UI: http://localhost:8080 (or 8085 based on config) | User: admin, Pass: adminpassword
   
    Streamlit Dashboard: http://localhost:8501

<img width="1919" height="1006" alt="image" src="https://github.com/user-attachments/assets/d68ec2bf-12a0-424a-9e4b-ffa338d3887c" />

<img width="1919" height="1007" alt="image" src="https://github.com/user-attachments/assets/7bb70add-9e59-4cc1-80f7-551ab364ea47" />
