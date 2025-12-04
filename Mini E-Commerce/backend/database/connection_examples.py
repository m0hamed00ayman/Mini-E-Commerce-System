"""
Database Connection Examples
"""
import pyodbc
from pymongo import MongoClient
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import urllib.parse

class DatabaseConnections:
    """Examples of different database connection methods"""
    
    @staticmethod
    def sql_server_pyodbc():
        """SQL Server connection using pyodbc"""
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=ECommerceDB;"
            "UID=sa;"
            "PWD=YourPassword123;"
            "TrustServerCertificate=yes;"
        )
        
        try:
            conn = pyodbc.connect(connection_string)
            print("✓ SQL Server connected via pyODBC")
            
            # Example query using Pandas
            df = pd.read_sql("SELECT TOP 10 * FROM products", conn)
            print(f"Retrieved {len(df)} products")
            
            # Perform analysis with NumPy
            if not df.empty:
                prices = df['price'].values
                print(f"Average price: ${np.mean(prices):.2f}")
                print(f"Price range: ${np.min(prices):.2f} - ${np.max(prices):.2f}")
            
            conn.close()
            return df
            
        except Exception as e:
            print(f"✗ SQL Server connection error: {e}")
            return None
    
    @staticmethod
    def sql_server_sqlalchemy():
        """SQL Server connection using SQLAlchemy"""
        connection_string = (
            "mssql+pyodbc://sa:YourPassword123@localhost/ECommerceDB?"
            "driver=ODBC+Driver+17+for+SQL+Server&"
            "TrustServerCertificate=yes"
        )
        
        try:
            engine = create_engine(connection_string)
            print("✓ SQL Server connected via SQLAlchemy")
            
            # Example using Pandas with SQLAlchemy
            df = pd.read_sql_table('products', engine, schema=None)
            print(f"Retrieved {len(df)} products")
            
            return df
            
        except Exception as e:
            print(f"✗ SQLAlchemy connection error: {e}")
            return None
    
    @staticmethod
    def mongodb_connection():
        """MongoDB connection using pymongo"""
        try:
            # Local MongoDB
            client = MongoClient('localhost', 27017)
            
            # For MongoDB Atlas (cloud):
            # username = urllib.parse.quote_plus('your_username')
            # password = urllib.parse.quote_plus('your_password')
            # client = MongoClient(f'mongodb+srv://{username}:{password}@cluster.mongodb.net/ecommerce?retryWrites=true&w=majority')
            
            db = client['ecommerce']
            print("✓ MongoDB connected")
            
            # Example operations
            collection = db['user_products']
            
            # Insert sample document
            sample_product = {
                'name': 'Sample Product',
                'price': 29.99,
                'category': 'Sample',
                'status': 'active',
                'timestamp': pd.Timestamp.now()
            }
            
            # Insert
            result = collection.insert_one(sample_product)
            print(f"Inserted document ID: {result.inserted_id}")
            
            # Find
            documents = list(collection.find({'category': 'Sample'}))
            print(f"Found {len(documents)} documents")
            
            # Convert to Pandas DataFrame
            df = pd.DataFrame(documents)
            if '_id' in df.columns:
                df.drop('_id', axis=1, inplace=True)
            
            client.close()
            return df
            
        except Exception as e:
            print(f"✗ MongoDB connection error: {e}")
            return None
    
    @staticmethod
    def pandas_database_operations():
        """Examples of Pandas operations with databases"""
        
        # Read from SQL
        df_sql = pd.read_sql("""
            SELECT p.*, COALESCE(SUM(si.quantity), 0) as total_sold
            FROM products p
            LEFT JOIN sale_items si ON p.id = si.product_id
            GROUP BY p.id, p.name, p.description, p.price, p.category, 
                     p.stock_quantity, p.vendor, p.rating, p.created_at
            ORDER BY total_sold DESC
        """, DatabaseConnections.sql_server_pyodbc())
        
        if df_sql is not None and not df_sql.empty:
            # Data cleaning with Pandas
            df_sql['price'] = pd.to_numeric(df_sql['price'], errors='coerce')
            df_sql['rating'] = pd.to_numeric(df_sql['rating'], errors='coerce')
            
            # Fill missing values
            df_sql['rating'].fillna(df_sql['rating'].mean(), inplace=True)
            df_sql['stock_quantity'].fillna(0, inplace=True)
            
            # Calculate new columns
            df_sql['revenue'] = df_sql['price'] * df_sql['total_sold']
            df_sql['inventory_value'] = df_sql['price'] * df_sql['stock_quantity']
            
            # Statistical analysis with NumPy
            stats = {
                'total_products': len(df_sql),
                'avg_price': np.mean(df_sql['price']),
                'total_revenue': np.sum(df_sql['revenue']),
                'total_inventory_value': np.sum(df_sql['inventory_value']),
                'price_std': np.std(df_sql['price']),
                'price_skew': float(pd.Series(df_sql['price']).skew())
            }
            
            print("\n📊 Product Statistics:")
            for key, value in stats.items():
                if 'price' in key or 'revenue' in key or 'value' in key:
                    print(f"{key}: ${value:,.2f}")
                else:
                    print(f"{key}: {value}")
            
            return df_sql, stats
        
        return None, None
    
    @staticmethod
    def bulk_data_operations():
        """Example of bulk data operations"""
        try:
            conn = pyodbc.connect(
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=localhost;"
                "DATABASE=ECommerceDB;"
                "UID=sa;"
                "PWD=YourPassword123;"
                "TrustServerCertificate=yes;"
            )
            
            # Create sample bulk data with NumPy
            n_samples = 1000
            product_ids = np.arange(1, n_samples + 1)
            prices = np.random.uniform(10, 500, n_samples)
            quantities = np.random.randint(1, 100, n_samples)
            
            # Create DataFrame
            bulk_df = pd.DataFrame({
                'product_id': product_ids,
                'price': prices,
                'quantity': quantities,
                'total': prices * quantities
            })
            
            # Calculate discounts using vectorized operations
            bulk_df['discount_rate'] = np.where(
                bulk_df['quantity'] >= 50, 0.15,
                np.where(bulk_df['quantity'] >= 20, 0.10,
                        np.where(bulk_df['quantity'] >= 10, 0.05, 0))
            )
            
            bulk_df['discount_amount'] = bulk_df['total'] * bulk_df['discount_rate']
            bulk_df['final_total'] = bulk_df['total'] - bulk_df['discount_amount']
            
            # Summary statistics
            summary = {
                'total_items': np.sum(bulk_df['quantity']),
                'total_revenue': np.sum(bulk_df['final_total']),
                'avg_discount': np.mean(bulk_df['discount_rate']),
                'total_discount': np.sum(bulk_df['discount_amount'])
            }
            
            print("\n📦 Bulk Data Summary:")
            for key, value in summary.items():
                if 'revenue' in key or 'discount' in key:
                    print(f"{key}: ${value:,.2f}")
                else:
                    print(f"{key}: {value:,.0f}")
            
            conn.close()
            return bulk_df
            
        except Exception as e:
            print(f"Bulk operations error: {e}")
            return None
    
    @staticmethod
    def advanced_analytics():
        """Advanced analytics examples"""
        try:
            # Get data
            df, _ = DatabaseConnections.pandas_database_operations()
            
            if df is not None and not df.empty:
                # Time series analysis (if we had date data)
                if 'created_at' in df.columns:
                    df['created_at'] = pd.to_datetime(df['created_at'])
                    df['month'] = df['created_at'].dt.to_period('M')
                    
                    monthly_stats = df.groupby('month').agg({
                        'price': ['mean', 'std', 'count'],
                        'revenue': 'sum'
                    }).round(2)
                    
                    print("\n📅 Monthly Statistics:")
                    print(monthly_stats)
                
                # Correlation analysis
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) > 1:
                    correlation_matrix = df[numeric_cols].corr()
                    print("\n🔗 Correlation Matrix:")
                    print(correlation_matrix.round(3))
                
                # Outlier detection using z-score
                from scipy import stats
                z_scores = np.abs(stats.zscore(df['price'].dropna()))
                outliers = df['price'][z_scores > 3]
                
                if len(outliers) > 0:
                    print(f"\n⚠️  Found {len(outliers)} price outliers:")
                    print(outliers)
                
                return df
            
        except Exception as e:
            print(f"Advanced analytics error: {e}")
            return None

# Example usage
if __name__ == "__main__":
    print("=" * 60)
    print("Database Connection Examples")
    print("=" * 60)
    
    # Test SQL Server connection
    print("\n1. Testing SQL Server Connection:")
    DatabaseConnections.sql_server_pyodbc()
    
    # Test MongoDB connection
    print("\n2. Testing MongoDB Connection:")
    DatabaseConnections.mongodb_connection()
    
    # Test Pandas operations
    print("\n3. Testing Pandas Database Operations:")
    DatabaseConnections.pandas_database_operations()
    
    # Test bulk operations
    print("\n4. Testing Bulk Data Operations:")
    DatabaseConnections.bulk_data_operations()
    
    # Test advanced analytics
    print("\n5. Testing Advanced Analytics:")
    DatabaseConnections.advanced_analytics()
    
    print("\n" + "=" * 60)
    print("All connection tests completed!")
    print("=" * 60)