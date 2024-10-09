from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import requests
import pandas as pd

# Default arguments for the DAG
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG using the context manager
with DAG(
    dag_id='stock_data_pipeline',              # Unique identifier for the DAG
    schedule_interval='@daily',                # DAG will run daily
    start_date=days_ago(1),                    # Start date is 1 day ago
    catchup=False,                             # No catchup for missed runs
    default_args=default_args,                 # Set retry and delay settings
    description='Daily stock data pipeline using Snowflake and Alpha Vantage'
) as dag:

    @task
    def fetch_stock_data():
        symbol = "AAPL"  # Apple stock
        api_key = Variable.get("alpha_vantage_api_key")  # Retrieve API key from Airflow Variable
        base_url = Variable.get("alpha_vantage_url")  # Retrieve URL from Airflow Variable

        # Construct the full URL
        url = f"{base_url}?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}&outputsize=compact"

        response = requests.get(url)
        data = response.json()

        # Extracting the time series data
        time_series = data.get('Time Series (Daily)', {})
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df = df.reset_index().rename(columns={
            'index': 'date',
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            '5. volume': 'volume'
        })

        # Adding stock symbol and formatting
        df['symbol'] = symbol
        df['date'] = pd.to_datetime(df['date'])

        # Filter data for the last 90 days
        ninety_days_ago = pd.Timestamp.today() - pd.Timedelta(days=90)
        df_last_90_days = df[df['date'] >= ninety_days_ago]

        # Convert the 'date' column to string to ensure it's JSON serializable
        df_last_90_days['date'] = df_last_90_days['date'].astype(str)

        return df_last_90_days.to_dict()

    @task
    def insert_to_snowflake(stock_data):
        # Convert stock_data dictionary back to DataFrame
        df = pd.DataFrame.from_dict(stock_data)

        # Use Airflow's Snowflake connection
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            # Use the correct database and schema
            cursor.execute("USE DATABASE stock_price")
            cursor.execute("USE SCHEMA raw_data")

            # Create or replace the table
            create_table_query = """
            CREATE OR REPLACE TABLE stock_prices (
                date DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                symbol VARCHAR,
                PRIMARY KEY (date, symbol)
            );
            """
            cursor.execute(create_table_query)

            # Insert data into the table
            for _, row in df.iterrows():
                cursor.execute(
                    """
                    INSERT INTO stock_prices (date, open, high, low, close, volume, symbol)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['symbol'])
                )
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    @task
    def query_snowflake():
        # Use Airflow's Snowflake connection
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            # Fetch records for the last 90 days
            ninety_days_ago = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
            query = f"SELECT * FROM stock_prices WHERE date >= '{ninety_days_ago}'"
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)

            # Display the DataFrame
            print(df)
        finally:
            cursor.close()
            conn.close()

    # Define task dependencies
    stock_data = fetch_stock_data()
    insert_to_snowflake(stock_data)
    query_snowflake()
