"""
Product Management Module
"""
import pyodbc
from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime

class ProductManager:
    def __init__(self, sql_config, mongo_config):
        self.sql_config = sql_config
        self.mongo_config = mongo_config
    
    def get_sql_connection(self):
        """Create SQL Server connection"""
        conn_str = f"DRIVER={self.sql_config['driver']};SERVER={self.sql_config['server']};DATABASE={self.sql_config['database']};UID={self.sql_config['username']};PWD={self.sql_config['password']}"
        return pyodbc.connect(conn_str)
    
    def get_mongo_client(self):
        """Create MongoDB client"""
        return MongoClient(self.mongo_config['host'], self.mongo_config['port'])
    
    def get_all_products(self):
        """Get all products from both databases"""
        products = []
        
        # Get from SQL Server
        try:
            conn = self.get_sql_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM products ORDER BY created_at DESC")
            
            columns = [column[0] for column in cursor.description]
            for row in cursor.fetchall():
                product = dict(zip(columns, row))
                product['source'] = 'sql_server'
                products.append(product)
            
            conn.close()
        except Exception as e:
            print(f"SQL Server error: {e}")
        
        # Get from MongoDB
        try:
            mongo_client = self.get_mongo_client()
            db = mongo_client[self.mongo_config['database']]
            collection = db[self.mongo_config['collection']]
            
            mongo_products = list(collection.find({}))
            for product in mongo_products:
                product['id'] = str(product.pop('_id'))
                product['source'] = 'mongodb'
                products.append(product)
            
            mongo_client.close()
        except Exception as e:
            print(f"MongoDB error: {e}")
        
        return products
    
    def add_product_sql(self, product_data):
        """Add product to SQL Server"""
        try:
            conn = self.get_sql_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO products (name, description, price, category, stock_quantity, vendor, rating)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, 
            product_data['name'], product_data['description'], product_data['price'],
            product_data['category'], product_data['stock_quantity'], 
            product_data.get('vendor', 'Unknown'), product_data.get('rating', 4.0))
            
            conn.commit()
            product_id = cursor.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
            conn.close()
            
            return {'success': True, 'product_id': product_id}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def add_product_mongo(self, product_data):
        """Add product to MongoDB"""
        try:
            mongo_client = self.get_mongo_client()
            db = mongo_client[self.mongo_config['database']]
            collection = db[self.mongo_config['collection']]
            
            # Add timestamp
            product_data['created_at'] = datetime.now()
            product_data['updated_at'] = datetime.now()
            
            result = collection.insert_one(product_data)
            mongo_client.close()
            
            return {'success': True, 'product_id': str(result.inserted_id)}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_product_analytics(self):
        """Get product analytics using Pandas"""
        try:
            conn = self.get_sql_connection()
            
            # Get products and sales data
            query = """
                SELECT 
                    p.category,
                    p.price,
                    p.stock_quantity,
                    COALESCE(SUM(si.quantity), 0) as total_sold,
                    COALESCE(SUM(si.total_price), 0) as total_revenue
                FROM products p
                LEFT JOIN sale_items si ON p.id = si.product_id
                GROUP BY p.category, p.price, p.stock_quantity
            """
            
            df = pd.read_sql(query, conn)
            conn.close()
            
            if df.empty:
                return {}
            
            # Calculate statistics using NumPy
            analytics = {
                'category_stats': df.groupby('category').agg({
                    'price': ['mean', 'min', 'max'],
                    'total_sold': 'sum',
                    'total_revenue': 'sum'
                }).to_dict(),
                
                'overall_stats': {
                    'avg_price': float(np.mean(df['price'])),
                    'total_inventory_value': float(np.sum(df['price'] * df['stock_quantity'])),
                    'total_potential_revenue': float(np.sum(df['price'] * df['stock_quantity'])),
                    'inventory_turnover': float(np.sum(df['total_sold']) / np.sum(df['stock_quantity'])) if np.sum(df['stock_quantity']) > 0 else 0
                },
                
                'price_distribution': {
                    'under_50': int(np.sum(df['price'] < 50)),
                    '50_100': int(np.sum((df['price'] >= 50) & (df['price'] < 100))),
                    '100_200': int(np.sum((df['price'] >= 100) & (df['price'] < 200))),
                    'over_200': int(np.sum(df['price'] >= 200))
                }
            }
            
            return analytics
            
        except Exception as e:
            print(f"Analytics error: {e}")
            return {}