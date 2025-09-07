from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'daily_analytics_pipeline',
    default_args=default_args,
    description='Daily e-commerce analytics ETL pipeline',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    catchup=False,
    tags=['analytics', 'daily']
)

def extract_daily_sales_data(**context):
    """Extract daily sales data from operational database"""
    execution_date = context['execution_date']
    
    query = f"""
    SELECT 
        DATE(o.created_at) as order_date,
        o.id as order_id,
        o.user_id,
        o.status,
        o.total_amount,
        o.subtotal,
        o.tax_amount,
        o.shipping_amount,
        oi.product_id,
        oi.quantity,
        oi.price as item_price,
        p.name as product_name,
        p.category_id,
        c.name as category_name,
        u.email as customer_email,
        EXTRACT(HOUR FROM o.created_at) as order_hour
    FROM orders o
    JOIN order_items oi ON o.id = oi.order_id
    JOIN products p ON oi.product_id = p.id
    JOIN categories c ON p.category_id = c.id
    JOIN users u ON o.user_id = u.id
    WHERE DATE(o.created_at) = '{execution_date.strftime('%Y-%m-%d')}'
    """
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    df = postgres_hook.get_pandas_df(query)
    
    # Store in temporary location for next task
    df.to_csv(f'/tmp/daily_sales_{execution_date.strftime("%Y%m%d")}.csv', index=False)
    
    return len(df)

def transform_sales_data(**context):
    """Transform and aggregate sales data"""
    execution_date = context['execution_date']
    file_path = f'/tmp/daily_sales_{execution_date.strftime("%Y%m%d")}.csv'
    
    df = pd.read_csv(file_path)
    
    # Create daily summary
    daily_summary = df.groupby(['order_date', 'category_name']).agg({
        'order_id': 'nunique',
        'user_id': 'nunique',
        'total_amount': 'sum',
        'quantity': 'sum'
    }).rename(columns={
        'order_id': 'total_orders',
        'user_id': 'unique_customers',
        'total_amount': 'total_revenue',
        'quantity': 'total_items_sold'
    }).reset_index()
    
    # Create hourly summary
    hourly_summary = df.groupby(['order_date', 'order_hour']).agg({
        'order_id': 'nunique',
        'total_amount': 'sum'
    }).rename(columns={
        'order_id': 'hourly_orders',
        'total_amount': 'hourly_revenue'
    }).reset_index()
    
    # Save transformed data
    daily_summary.to_csv(f'/tmp/daily_summary_{execution_date.strftime("%Y%m%d")}.csv', index=False)
    hourly_summary.to_csv(f'/tmp/hourly_summary_{execution_date.strftime("%Y%m%d")}.csv', index=False)
    
    return {'daily_rows': len(daily_summary), 'hourly_rows': len(hourly_summary)}

def load_analytics_data(**context):
    """Load transformed data into analytics tables"""
    execution_date = context['execution_date']
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Load daily summary
    daily_df = pd.read_csv(f'/tmp/daily_summary_{execution_date.strftime("%Y%m%d")}.csv')
    daily_df.to_sql('daily_sales_summary', postgres_hook.get_sqlalchemy_engine(), 
                   if_exists='append', index=False, method='multi')
    
    # Load hourly summary
    hourly_df = pd.read_csv(f'/tmp/hourly_summary_{execution_date.strftime("%Y%m%d")}.csv')
    hourly_df.to_sql('hourly_sales_summary', postgres_hook.get_sqlalchemy_engine(), 
                    if_exists='append', index=False, method='multi')
    
    return f"Loaded {len(daily_df)} daily records and {len(hourly_df)} hourly records"

# Create analytics tables
create_analytics_tables = PostgresOperator(
    task_id='create_analytics_tables',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS daily_sales_summary (
        order_date DATE,
        category_name VARCHAR(100),
        total_orders INTEGER,
        unique_customers INTEGER,
        total_revenue DECIMAL(10,2),
        total_items_sold INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS hourly_sales_summary (
        order_date DATE,
        order_hour INTEGER,
        hourly_orders INTEGER,
        hourly_revenue DECIMAL(10,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_daily_sales',
    python_callable=extract_daily_sales_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_sales_data',
    python_callable=transform_sales_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_analytics_data',
    python_callable=load_analytics_data,
    dag=dag
)

# Set task dependencies
create_analytics_tables >> extract_task >> transform_task >> load_task