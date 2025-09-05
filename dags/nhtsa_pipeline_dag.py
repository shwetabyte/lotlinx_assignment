"""
NHTSA Data Pipeline DAG
Simple 3-stage pipeline using functions from src/code/nhtsa_file_parser.py
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import os
import sys
import pandas as pd
from sqlalchemy import text

# Add the src/code directory to Python path
sys.path.append('/opt/airflow/src/code')

# Import our parser functions directly
from nhtsa_file_parser import write_to_bronze, write_to_silver

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'nhtsa_data_pipeline',
    default_args=default_args,
    description='NHTSA automotive data processing pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['nhtsa', 'automotive', 'data-pipeline'],
)

def task_bronze(**context):
    """Task 1: Load to Bronze layer using our parser function"""
    write_to_bronze()

def task_silver(**context):
    """Task 2: Load to Silver layer using our parser function"""
    write_to_silver()

def task_database(**context):
    """Task 3: Load to Database"""
    # Get database connection
    postgres_hook = PostgresHook(postgres_conn_id='nhtsa_postgres')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        with conn.begin():
            # Clear existing data
            conn.execute(text("DELETE FROM processed_nhtsa_data"))
            conn.execute(text("DELETE FROM nhtsa_lookup_table"))
            
            # Load processed data from silver layer
            with open('/opt/airflow/data/silver/filtered_nhtsa_data.json', 'r') as f:
                data = json.load(f)
            
            for record in data:
                # Convert data types
                try:
                    model_year = int(record['Model_Year']) if record['Model_Year'] else None
                except (ValueError, TypeError):
                    model_year = None
                    
                try:
                    vehicle_type_id = int(record['Vehicle_Type_Id']) if record['Vehicle_Type_Id'] else None
                except (ValueError, TypeError):
                    vehicle_type_id = None
                    
                try:
                    body_class_id = int(record['Body_Class_Id']) if record['Body_Class_Id'] else None
                except (ValueError, TypeError):
                    body_class_id = None
                    
                try:
                    base_price = float(record['Base_Price']) if record['Base_Price'] else None
                except (ValueError, TypeError):
                    base_price = None
                
                # Insert record
                insert_query = text("""
                    INSERT INTO processed_nhtsa_data 
                    (sent_vin, manufacturer_name, make, model, model_year, trim, 
                     vehicle_type_id, body_class_id, base_price, ncsa_make, ncsa_model)
                    VALUES (:sent_vin, :manufacturer_name, :make, :model, :model_year, :trim,
                            :vehicle_type_id, :body_class_id, :base_price, :ncsa_make, :ncsa_model)
                """)
                
                conn.execute(insert_query, {
                    'sent_vin': record['Sent_VIN'],
                    'manufacturer_name': record['Manufacturer_Name'],
                    'make': record['Make'],
                    'model': record['Model'],
                    'model_year': model_year,
                    'trim': record['TRIM'],
                    'vehicle_type_id': vehicle_type_id,
                    'body_class_id': body_class_id,
                    'base_price': base_price,
                    'ncsa_make': record['NCSA_Make'],
                    'ncsa_model': record['NCSA_Model']
                })
            
            # Load lookup table
            lookup_df = pd.read_csv('/opt/airflow/data/lookup/nhtsa_lookup_file.csv')
            
            for _, row in lookup_df.iterrows():
                insert_lookup = text("""
                    INSERT INTO nhtsa_lookup_table 
                    (vehicle_type_id, vehicle_type, body_class_id, body_class, 
                     lx_bodyclass_lvl1, lx_bodyclass_lvl2, incomplete_chassis)
                    VALUES (:vehicle_type_id, :vehicle_type, :body_class_id, :body_class,
                            :lx_bodyclass_lvl1, :lx_bodyclass_lvl2, :incomplete_chassis)
                """)
                
                conn.execute(insert_lookup, {
                    'vehicle_type_id': int(row['Vehicle_Type_ID']) if pd.notna(row['Vehicle_Type_ID']) else None,
                    'vehicle_type': str(row['Vehicle_Type']) if pd.notna(row['Vehicle_Type']) else None,
                    'body_class_id': int(row['Body_Class_ID']) if pd.notna(row['Body_Class_ID']) else None,
                    'body_class': str(row['Body_Class']) if pd.notna(row['Body_Class']) else None,
                    'lx_bodyclass_lvl1': str(row['LX_BodyClass_lvl1']) if pd.notna(row['LX_BodyClass_lvl1']) else None,
                    'lx_bodyclass_lvl2': str(row['LX_BodyClass_lvl2']) if pd.notna(row['LX_BodyClass_lvl2']) else None,
                    'incomplete_chassis': bool(row['Incomplete_Chassis']) if pd.notna(row['Incomplete_Chassis']) else False
                })
    
    print("Database loading completed")

def task_load_to_gold(**context):
    """Task 4: Load analytical results to Gold layer tables"""
    
    postgres_hook = PostgresHook(postgres_conn_id='nhtsa_postgres')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        with conn.begin():
            # Create Gold layer tables
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS gold_top_vehicle_models (
                    model_year INTEGER,
                    make VARCHAR(50),
                    model VARCHAR(100),
                    vehicle_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS gold_body_class_distribution (
                    lx_bodyclass_lvl1 VARCHAR(50),
                    bodysegment VARCHAR(50),
                    vehicle_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            
            # Clear existing data
            conn.execute(text("DELETE FROM gold_top_vehicle_models"))
            conn.execute(text("DELETE FROM gold_body_class_distribution"))
            
            # Load Gold Table 1: Top 10 most common vehicles
            gold_query1 = """
                INSERT INTO gold_top_vehicle_models (model_year, make, model, vehicle_count)
                SELECT model_year, make, model, COUNT(DISTINCT sent_vin) as vehicle_count
                FROM processed_nhtsa_data
                WHERE sent_vin IS NOT NULL AND sent_vin != ''
                GROUP BY model_year, make, model
                ORDER BY vehicle_count DESC
                LIMIT 10;
            """
            conn.execute(text(gold_query1))
            
            # Load Gold Table 2: Body class distribution
            gold_query2 = """
                INSERT INTO gold_body_class_distribution (lx_bodyclass_lvl1, bodysegment, vehicle_count)
                SELECT 
                    l.lx_bodyclass_lvl1,
                    l.lx_bodyclass_lvl2 as bodysegment,
                    COUNT(DISTINCT p.sent_vin) as vehicle_count
                FROM processed_nhtsa_data p
                JOIN nhtsa_lookup_table l 
                    ON p.vehicle_type_id = l.vehicle_type_id 
                    AND p.body_class_id = l.body_class_id
                WHERE p.sent_vin IS NOT NULL AND p.sent_vin != ''
                    AND l.lx_bodyclass_lvl1 NOT IN ('MOTORCYCLE', 'BUS')
                    AND NOT (l.lx_bodyclass_lvl1 = 'PASSENGER CAR' AND l.lx_bodyclass_lvl2 = 'CONVERTIBLE')
                GROUP BY l.lx_bodyclass_lvl1, l.lx_bodyclass_lvl2
                ORDER BY l.lx_bodyclass_lvl1, vehicle_count DESC;
            """
            conn.execute(text(gold_query2))
            
            # Show some results - just for verification
            result1 = conn.execute(text("SELECT * FROM gold_top_vehicle_models ORDER BY vehicle_count DESC LIMIT 5"))
            print("Top 5 vehicle models:")
            for row in result1:
                print(f"Year: {row[0]}, Make: {row[1]}, Model: {row[2]}, Count: {row[3]}")
    
    print("Gold layer loaded successfully")

# Define the tasks
task1_bronze = PythonOperator(
    task_id='load_to_bronze',
    python_callable=task_bronze,
    dag=dag,
)

task2_silver = PythonOperator(
    task_id='load_to_silver',
    python_callable=task_silver,
    dag=dag,
)

task3_database = PythonOperator(
    task_id='load_to_database',
    python_callable=task_database,
    dag=dag,
)

task4_gold = PythonOperator(
    task_id='load_to_gold',
    python_callable=task_load_to_gold,
    dag=dag,
)

# Set task dependencies
task1_bronze >> task2_silver >> task3_database >> task4_gold