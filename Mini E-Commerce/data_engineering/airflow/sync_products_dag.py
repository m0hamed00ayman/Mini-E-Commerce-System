"""
Airflow DAG for syncing products from MongoDB to SQL Server
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from pymongo import MongoClient
import pandas as pd
import numpy as np
import logging

# Default arguments
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['admin@ecommerce.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# DAG definition
dag = DAG(
    'sync_products_dag',
    default_args=default_args,
    description='Sync user-submitted products from MongoDB to SQL Server',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['ecommerce', 'etl', 'sync'],
)

def extract_mongo_products(**context):
    """Extract products from MongoDB"""
    try:
        # MongoDB connection
        mongo_client = MongoClient('mongodb://localhost:27017/')
        db = mongo_client['ecommerce']
        collection = db['user_products']
        
        # Query for pending or approved products not yet synced
        query = {
            'status': {'$in': ['approved', 'pending']},
            'last_sync': {'$exists': False}
        }
        
        # Extract data
        products = list(collection.find(query))
        
        if not products:
            logging.info("No new products to sync")
            context['ti'].xcom_push(key='product_count', value=0)
            return []
        
        # Convert to DataFrame
        df = pd.DataFrame(products)
        
        # Clean data
        if '_id' in df.columns:
            df['mongo_id'] = df['_id'].astype(str)
            df = df.drop('_id', axis=1)
        
        # Handle missing values
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['stock_quantity'] = pd.to_numeric(df['stock_quantity'], errors='coerce').fillna(0).astype(int)
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(4.0)
        
        # Add timestamps
        df['extracted_at'] = datetime.now()
        df['batch_id'] = context['ds']
        
        logging.info(f"Extracted {len(df)} products from MongoDB")
        context['ti'].xcom_push(key='product_count', value=len(df))
        
        # Push DataFrame to XCom
        context['ti'].xcom_push(key='mongo_products', value=df.to_dict('records'))
        
        return df.to_dict('records')
        
    except Exception as e:
        logging.error(f"Error extracting from MongoDB: {e}")
        raise

def transform_products(**context):
    """Transform products data using Pandas and NumPy"""
    try:
        # Pull data from XCom
        products_data = context['ti'].xcom_pull(key='mongo_products', task_ids='extract_mongo_products')
        
        if not products_data:
            logging.info("No data to transform")
            context['ti'].xcom_push(key='transformed_count', value=0)
            return []
        
        df = pd.DataFrame(products_data)
        
        # Data cleaning and transformation
        logging.info("Starting data transformation...")
        
        # 1. Price validation and normalization
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        
        # Remove products with invalid prices
        initial_count = len(df)
        df = df[df['price'] > 0]
        df = df[df['price'] < 100000]  # Remove extreme outliers
        
        # Log removed records
        removed_count = initial_count - len(df)
        if removed_count > 0:
            logging.info(f"Removed {removed_count} products with invalid prices")
        
        # 2. Category standardization
        if 'category' in df.columns:
            # Convert to title case and strip whitespace
            df['category'] = df['category'].str.strip().str.title()
            
            # Map common variations
            category_mapping = {
                'Electronics': ['Electronics', 'Tech', 'Gadgets'],
                'Clothing': ['Clothing', 'Apparel', 'Fashion'],
                'Books': ['Books', 'Book', 'Literature'],
                'Home & Kitchen': ['Home', 'Kitchen', 'Home & Kitchen', 'Home Appliances'],
                'Sports': ['Sports', 'Fitness', 'Outdoors'],
            }
            
            def map_category(cat):
                for standard, variants in category_mapping.items():
                    if cat in variants:
                        return standard
                return 'Other'
            
            df['category'] = df['category'].apply(map_category)
        
        # 3. Text cleaning
        if 'name' in df.columns:
            df['name'] = df['name'].str.strip()
            df['name'] = df['name'].str[:200]  # Truncate to 200 chars
        
        if 'description' in df.columns:
            df['description'] = df['description'].str.strip()
            df['description'] = df['description'].fillna('')
        
        # 4. Vendor standardization
        if 'vendor' in df.columns:
            df['vendor'] = df['vendor'].str.strip().fillna('User Submitted')
        
        # 5. Rating normalization (0-5 scale)
        if 'rating' in df.columns:
            df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
            df['rating'] = df['rating'].clip(0, 5)
            df['rating'] = df['rating'].fillna(4.0)
        
        # 6. Generate SKU or product code
        df['product_code'] = 'USR-' + pd.Series(range(1, len(df) + 1)).astype(str).str.zfill(6)
        
        # 7. Calculate analytics metrics using NumPy
        prices = df['price'].values
        df['price_category'] = np.where(
            prices < 50, 'Budget',
            np.where(prices < 200, 'Mid-range', 'Premium')
        )
        
        # Calculate price percentiles
        if len(prices) > 0:
            df['price_percentile'] = pd.qcut(prices, q=4, labels=['Q1', 'Q2', 'Q3', 'Q4'], duplicates='drop')
        
        # 8. Add metadata
        df['transformed_at'] = datetime.now()
        df['data_source'] = 'mongodb_user_submitted'
        df['etl_batch'] = context['ds']
        
        # 9. Select final columns for SQL Server
        final_columns = [
            'product_code', 'name', 'description', 'price', 'category',
            'stock_quantity', 'vendor', 'rating', 'price_category',
            'mongo_id', 'data_source', 'extracted_at', 'transformed_at',
            'etl_batch'
        ]
        
        # Filter to available columns
        available_columns = [col for col in final_columns if col in df.columns]
        df_final = df[available_columns]
        
        logging.info(f"Transformed {len(df_final)} products")
        
        # Calculate transformation statistics
        stats = {
            'total_products': len(df_final),
            'avg_price': float(np.mean(df_final['price'])),
            'total_value': float(np.sum(df_final['price'] * df_final['stock_quantity'])),
            'category_distribution': df_final['category'].value_counts().to_dict(),
            'price_category_dist': df_final['price_category'].value_counts().to_dict()
        }
        
        context['ti'].xcom_push(key='transformed_count', value=len(df_final))
        context['ti'].xcom_push(key='transformation_stats', value=stats)
        context['ti'].xcom_push(key='transformed_products', value=df_final.to_dict('records'))
        
        return df_final.to_dict('records')
        
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise

def load_to_sql_server(**context):
    """Load transformed products to SQL Server"""
    try:
        # Pull transformed data
        products_data = context['ti'].xcom_pull(key='transformed_products', task_ids='transform_products')
        
        if not products_data:
            logging.info("No data to load")
            context['ti'].xcom_push(key='loaded_count', value=0)
            return
        
        df = pd.DataFrame(products_data)
        
        # SQL Server connection
        mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')
        conn = mssql_hook.get_conn()
        cursor = conn.cursor()
        
        # Create staging table if not exists
        create_staging_table = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='staging_products' AND xtype='U')
        CREATE TABLE staging_products (
            product_code NVARCHAR(50),
            name NVARCHAR(200),
            description NVARCHAR(MAX),
            price DECIMAL(10,2),
            category NVARCHAR(100),
            stock_quantity INT,
            vendor NVARCHAR(200),
            rating DECIMAL(3,2),
            price_category NVARCHAR(50),
            mongo_id NVARCHAR(100),
            data_source NVARCHAR(100),
            extracted_at DATETIME,
            transformed_at DATETIME,
            etl_batch NVARCHAR(50),
            load_timestamp DATETIME DEFAULT GETDATE()
        )
        """
        cursor.execute(create_staging_table)
        
        # Insert data using pandas to_sql (more efficient for bulk inserts)
        from sqlalchemy import create_engine
        
        # Create SQLAlchemy engine
        conn_str = mssql_hook.get_uri()
        engine = create_engine(conn_str)
        
        # Insert to staging
        df.to_sql('staging_products', engine, if_exists='append', index=False, chunksize=1000)
        
        # Merge staging into main products table
        merge_sql = """
        MERGE products AS target
        USING staging_products AS source
        ON target.product_code = source.product_code
        WHEN MATCHED THEN
            UPDATE SET 
                target.name = source.name,
                target.description = source.description,
                target.price = source.price,
                target.category = source.category,
                target.stock_quantity = source.stock_quantity,
                target.vendor = source.vendor,
                target.rating = source.rating,
                target.updated_at = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (product_code, name, description, price, category, 
                    stock_quantity, vendor, rating, created_at, updated_at)
            VALUES (source.product_code, source.name, source.description, 
                    source.price, source.category, source.stock_quantity,
                    source.vendor, source.rating, GETDATE(), GETDATE());
        """
        cursor.execute(merge_sql)
        
        # Update MongoDB sync status
        if 'mongo_id' in df.columns:
            mongo_ids = df['mongo_id'].tolist()
            if mongo_ids:
                # Update MongoDB documents
                mongo_client = MongoClient('mongodb://localhost:27017/')
                db = mongo_client['ecommerce']
                collection = db['user_products']
                
                # Update sync status
                collection.update_many(
                    {'_id': {'$in': [ObjectId(id) for id in mongo_ids if id]}},
                    {'$set': {
                        'last_sync': datetime.now(),
                        'sync_status': 'completed'
                    }}
                )
                mongo_client.close()
        
        # Clear staging table
        cursor.execute("TRUNCATE TABLE staging_products")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        loaded_count = len(df)
        logging.info(f"Successfully loaded {loaded_count} products to SQL Server")
        context['ti'].xcom_push(key='loaded_count', value=loaded_count)
        
    except Exception as e:
        logging.error(f"Error loading to SQL Server: {e}")
        raise

def generate_etl_report(**context):
    """Generate ETL execution report"""
    try:
        # Pull execution metrics
        extracted = context['ti'].xcom_pull(key='product_count', task_ids='extract_mongo_products') or 0
        transformed = context['ti'].xcom_pull(key='transformed_count', task_ids='transform_products') or 0
        loaded = context['ti'].xcom_pull(key='loaded_count', task_ids='load_to_sql_server') or 0
        
        stats = context['ti'].xcom_pull(key='transformation_stats', task_ids='transform_products') or {}
        
        # Calculate success rate
        success_rate = (loaded / extracted * 100) if extracted > 0 else 0
        
        # Create report
        report = f"""
        📊 ETL Execution Report - {context['ds']}
        ============================================
        
        📈 Execution Metrics:
        • Products Extracted: {extracted}
        • Products Transformed: {transformed}
        • Products Loaded: {loaded}
        • Success Rate: {success_rate:.1f}%
        
        📊 Transformation Statistics:
        • Average Price: ${stats.get('avg_price', 0):.2f}
        • Total Inventory Value: ${stats.get('total_value', 0):.2f}
        
        🏷️ Category Distribution:
        """
        
        if 'category_distribution' in stats:
            for category, count in stats['category_distribution'].items():
                report += f"  • {category}: {count} products\n"
        
        report += f"""
        
        💰 Price Categories:
        """
        
        if 'price_category_dist' in stats:
            for category, count in stats['price_category_dist'].items():
                report += f"  • {category}: {count} products\n"
        
        report += f"""
        
        ⏱️ Execution Time: {context['ts']}
        ============================================
        """
        
        logging.info(report)
        context['ti'].xcom_push(key='etl_report', value=report)
        
        return report
        
    except Exception as e:
        logging.error(f"Error generating report: {e}")
        raise

# Task definitions
extract_task = PythonOperator(
    task_id='extract_mongo_products',
    python_callable=extract_mongo_products,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_products',
    python_callable=transform_products,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_sql_server',
    python_callable=load_to_sql_server,
    provide_context=True,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_etl_report',
    python_callable=generate_etl_report,
    provide_context=True,
    dag=dag,
)

email_report_task = EmailOperator(
    task_id='email_etl_report',
    to=['data.team@ecommerce.com'],
    subject='Daily Product Sync ETL Report - {{ ds }}',
    html_content="""
    <h2>📊 Daily Product Sync ETL Report</h2>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    
    <h3>📈 Execution Summary:</h3>
    <ul>
        <li><strong>Products Extracted:</strong> {{ ti.xcom_pull(task_ids='extract_mongo_products', key='product_count') }}</li>
        <li><strong>Products Transformed:</strong> {{ ti.xcom_pull(task_ids='transform_products', key='transformed_count') }}</li>
        <li><strong>Products Loaded:</strong> {{ ti.xcom_pull(task_ids='load_to_sql_server', key='loaded_count') }}</li>
    </ul>
    
    <h3>🔧 ETL Pipeline Status: COMPLETED ✅</h3>
    
    <p>The daily sync of user-submitted products from MongoDB to SQL Server has completed successfully.</p>
    
    <hr>
    <p><em>This is an automated report from the ECommerce ETL System.</em></p>
    """,
    dag=dag,
)

# Task dependencies
extract_task >> transform_task >> load_task >> report_task >> email_report_task

# Log start and end
dag.doc_md = __doc__

if __name__ == "__main__":
    dag.test()