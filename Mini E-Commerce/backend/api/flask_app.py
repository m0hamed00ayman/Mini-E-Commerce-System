"""
Flask API for Mini E-Commerce System
"""
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import pyodbc
from pymongo import MongoClient
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime
from decimal import Decimal
import matplotlib
matplotlib.use('Agg')  # For backend use
import matplotlib.pyplot as plt
from io import BytesIO
import base64

# Import models
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from models.products import ProductManager
from models.cart import CartManager
from models.users import UserManager

app = Flask(__name__, 
            template_folder='../../frontend/templates', 
            static_folder='../../frontend/static')
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'ecommerce-dev-key-2024')

# Database configurations
SQL_SERVER_CONFIG = {
    'server': os.getenv('SQL_SERVER', 'localhost'),
    'database': os.getenv('SQL_DATABASE', 'ECommerceDB'),
    'username': os.getenv('SQL_USERNAME', 'sa'),
    'password': os.getenv('SQL_PASSWORD', 'password'),
    'driver': os.getenv('SQL_DRIVER', '{ODBC Driver 17 for SQL Server}')
}

MONGODB_CONFIG = {
    'host': os.getenv('MONGODB_HOST', 'localhost'),
    'port': int(os.getenv('MONGODB_PORT', 27017)),
    'database': os.getenv('MONGODB_DATABASE', 'ecommerce'),
    'collection': os.getenv('MONGODB_COLLECTION', 'user_products')
}

# Initialize managers
product_manager = ProductManager(SQL_SERVER_CONFIG, MONGODB_CONFIG)
cart_manager = CartManager(SQL_SERVER_CONFIG)
user_manager = UserManager(SQL_SERVER_CONFIG)

def get_sql_connection():
    """Create SQL Server connection"""
    conn_str = f"DRIVER={SQL_SERVER_CONFIG['driver']};SERVER={SQL_SERVER_CONFIG['server']};DATABASE={SQL_SERVER_CONFIG['database']};UID={SQL_SERVER_CONFIG['username']};PWD={SQL_SERVER_CONFIG['password']}"
    return pyodbc.connect(conn_str)

def calculate_discount(price, quantity):
    """Calculate discount using NumPy"""
    base_price = np.array([price])
    quantities = np.array([quantity])
    
    # Volume discount: 10% for 5+ items, 20% for 10+ items
    if quantity >= 10:
        discount_rate = 0.20
    elif quantity >= 5:
        discount_rate = 0.10
    else:
        discount_rate = 0.0
    
    discounted_prices = base_price * (1 - discount_rate)
    total = discounted_prices * quantities
    
    return {
        'original_price': float(price),
        'discount_rate': discount_rate * 100,
        'discounted_price': float(discounted_prices[0]),
        'total': float(total[0]),
        'savings': float(price * quantity - total[0])
    }

@app.route('/')
def home():
    """Home page with dashboard"""
    try:
        conn = get_sql_connection()
        
        # Get product count using Pandas
        product_count = pd.read_sql("SELECT COUNT(*) as count FROM products", conn).iloc[0]['count']
        
        # Get total sales
        sales_df = pd.read_sql("""
            SELECT SUM(total_amount) as revenue, COUNT(*) as transactions 
            FROM sales 
            WHERE sale_date >= DATEADD(day, -30, GETDATE())
        """, conn)
        
        # Get top categories
        categories_df = pd.read_sql("""
            SELECT category, COUNT(*) as product_count 
            FROM products 
            GROUP BY category 
            ORDER BY product_count DESC 
            LIMIT 5
        """, conn)
        
        conn.close()
        
        # Convert to lists for template
        categories = categories_df.to_dict('records')
        
        return render_template('index.html', 
                             product_count=int(product_count),
                             revenue=float(sales_df.iloc[0]['revenue'] or 0),
                             transactions=int(sales_df.iloc[0]['transactions'] or 0),
                             top_categories=categories)
    except Exception as e:
        return render_template('index.html', 
                             product_count=0,
                             revenue=0,
                             transactions=0,
                             top_categories=[],
                             error=str(e))

@app.route('/products', methods=['GET'])
def get_products():
    """Get all products with analytical data"""
    try:
        conn = get_sql_connection()
        
        # Get SQL Server products using Pandas
        sql_products_df = pd.read_sql("""
            SELECT id, name, description, price, category, 
                   stock_quantity, created_at, rating,
                   (SELECT COUNT(*) FROM sales WHERE product_id = products.id) as sales_count
            FROM products
            ORDER BY created_at DESC
        """, conn)
        
        # Get MongoDB products
        mongo_client = MongoClient(MONGODB_CONFIG['host'], MONGODB_CONFIG['port'])
        db = mongo_client[MONGODB_CONFIG['database']]
        collection = db[MONGODB_CONFIG['collection']]
        
        # Convert MongoDB cursor to list and then to DataFrame
        mongo_products = list(collection.find({}, {'_id': 0, 'mongodb_id': 0}))
        mongo_products_df = pd.DataFrame(mongo_products) if mongo_products else pd.DataFrame()
        
        mongo_client.close()
        
        # Combine DataFrames
        if not mongo_products_df.empty:
            all_products_df = pd.concat([sql_products_df, mongo_products_df], ignore_index=True)
        else:
            all_products_df = sql_products_df
        
        # Calculate analytics
        if not all_products_df.empty:
            # Price statistics using NumPy
            prices = all_products_df['price'].dropna().values
            price_stats = {
                'avg_price': np.mean(prices) if len(prices) > 0 else 0,
                'min_price': np.min(prices) if len(prices) > 0 else 0,
                'max_price': np.max(prices) if len(prices) > 0 else 0,
                'price_std': np.std(prices) if len(prices) > 0 else 0
            }
            
            # Category distribution
            category_dist = all_products_df['category'].value_counts().to_dict()
            
            # Convert DataFrame to list of dictionaries for template
            products_list = all_products_df.to_dict('records')
        else:
            price_stats = {}
            category_dist = {}
            products_list = []
        
        conn.close()
        
        return render_template('products.html', 
                             products=products_list,
                             price_stats=price_stats,
                             category_dist=category_dist,
                             total_products=len(products_list))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/add-product', methods=['GET', 'POST'])
def add_product():
    """Add a new product (to MongoDB for user-submitted products)"""
    if request.method == 'GET':
        return render_template('add_product.html')
    
    try:
        data = request.form
        
        # Validate required fields
        required_fields = ['name', 'price', 'category']
        for field in required_fields:
            if field not in data or not data[field]:
                return jsonify({'error': f'{field} is required'}), 400
        
        # Create product object with NumPy validation
        price = float(data['price'])
        if np.isnan(price) or price <= 0:
            return jsonify({'error': 'Invalid price'}), 400
        
        product_data = {
            'name': data['name'],
            'description': data.get('description', ''),
            'price': price,
            'category': data['category'],
            'stock_quantity': int(data.get('stock_quantity', 10)),
            'vendor': data.get('vendor', 'User Submitted'),
            'rating': float(data.get('rating', 4.0)),
            'tags': data.get('tags', '').split(','),
            'created_at': datetime.now().isoformat(),
            'source': 'user_submitted'
        }
        
        # Save to MongoDB
        mongo_client = MongoClient(MONGODB_CONFIG['host'], MONGODB_CONFIG['port'])
        db = mongo_client[MONGODB_CONFIG['database']]
        collection = db[MONGODB_CONFIG['collection']]
        
        result = collection.insert_one(product_data)
        mongo_client.close()
        
        # Also add to SQL Server if it's a vendor product
        if data.get('vendor') and data['vendor'] != 'User Submitted':
            conn = get_sql_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO products (name, description, price, category, stock_quantity, vendor, rating)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, 
            product_data['name'], product_data['description'], product_data['price'],
            product_data['category'], product_data['stock_quantity'], 
            product_data['vendor'], product_data['rating'])
            conn.commit()
            conn.close()
        
        return redirect('/products')
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/cart/add', methods=['POST'])
def add_to_cart():
    """Add product to cart with discount calculation"""
    try:
        data = request.json
        product_id = data.get('product_id')
        quantity = int(data.get('quantity', 1))
        
        # Initialize session cart if not exists
        if 'cart' not in session:
            session['cart'] = {}
        
        # Get product details
        conn = get_sql_connection()
        cursor = conn.cursor()
        
        # Check if product exists in SQL
        cursor.execute("SELECT id, name, price FROM products WHERE id = ?", product_id)
        row = cursor.fetchone()
        
        if row:
            product = {
                'id': row[0],
                'name': row[1],
                'price': float(row[2])
            }
        else:
            # Check MongoDB
            mongo_client = MongoClient(MONGODB_CONFIG['host'], MONGODB_CONFIG['port'])
            db = mongo_client[MONGODB_CONFIG['database']]
            collection = db[MONGODB_CONFIG['collection']]
            
            # MongoDB documents might not have numeric IDs
            mongo_product = collection.find_one({'name': {'$regex': f'.*{product_id}.*', '$options': 'i'}})
            if mongo_product:
                product = {
                    'id': str(mongo_product.get('_id')),
                    'name': mongo_product.get('name'),
                    'price': float(mongo_product.get('price', 0))
                }
            else:
                return jsonify({'error': 'Product not found'}), 404
            
            mongo_client.close()
        
        conn.close()
        
        # Calculate discount using NumPy
        discount_info = calculate_discount(product['price'], quantity)
        
        # Add to cart
        cart_item = {
            'product_id': product['id'],
            'name': product['name'],
            'price': product['price'],
            'quantity': quantity,
            'discount_info': discount_info
        }
        
        session['cart'][product['id']] = cart_item
        session.modified = True
        
        return jsonify({
            'success': True,
            'message': 'Product added to cart',
            'cart_item': cart_item,
            'cart_total': calculate_cart_total()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def calculate_cart_total():
    """Calculate total cart value with discounts"""
    if 'cart' not in session:
        return 0
    
    total = 0
    for item in session['cart'].values():
        total += item['discount_info']['total']
    
    return total

@app.route('/cart', methods=['GET'])
def view_cart():
    """View cart with analytical insights"""
    cart_items = session.get('cart', {}).values()
    
    # Calculate cart analytics using Pandas
    if cart_items:
        cart_df = pd.DataFrame(list(cart_items))
        
        # Calculate totals
        subtotal = cart_df['discount_info'].apply(lambda x: x['original_price'] * cart_df.loc[cart_df['discount_info'] == x, 'quantity'].values[0]).sum()
        total_discount = cart_df['discount_info'].apply(lambda x: x['savings']).sum()
        final_total = subtotal - total_discount
        
        # Category distribution
        if 'category' in cart_df.columns:
            category_dist = cart_df.groupby('category')['quantity'].sum().to_dict()
        else:
            category_dist = {}
    else:
        subtotal = 0
        total_discount = 0
        final_total = 0
        category_dist = {}
    
    return render_template('cart.html',
                         cart_items=list(cart_items),
                         subtotal=subtotal,
                         total_discount=total_discount,
                         final_total=final_total,
                         category_dist=category_dist,
                         item_count=len(list(cart_items)))

@app.route('/checkout', methods=['GET', 'POST'])
def checkout():
    """Process checkout and create sale record"""
    if request.method == 'GET':
        cart_items = session.get('cart', {}).values()
        
        if not cart_items:
            return redirect('/cart')
        
        # Calculate totals
        cart_df = pd.DataFrame(list(cart_items))
        subtotal = cart_df['discount_info'].apply(lambda x: x['original_price'] * cart_df.loc[cart_df['discount_info'] == x, 'quantity'].values[0]).sum()
        total_discount = cart_df['discount_info'].apply(lambda x: x['savings']).sum()
        final_total = subtotal - total_discount
        
        return render_template('checkout.html',
                             cart_items=list(cart_items),
                             subtotal=subtotal,
                             total_discount=total_discount,
                             final_total=final_total)
    
    # POST request - process checkout
    try:
        data = request.form
        
        # Validate required fields
        required_fields = ['name', 'email', 'address', 'payment_method']
        for field in required_fields:
            if field not in data or not data[field]:
                return jsonify({'error': f'{field} is required'}), 400
        
        cart_items = session.get('cart', {})
        if not cart_items:
            return jsonify({'error': 'Cart is empty'}), 400
        
        # Calculate totals using Pandas
        cart_df = pd.DataFrame(list(cart_items.values()))
        final_total = cart_df['discount_info'].apply(lambda x: x['total']).sum()
        
        # Create sale record in SQL Server
        conn = get_sql_connection()
        cursor = conn.cursor()
        
        # Create sale record
        cursor.execute("""
            INSERT INTO sales (customer_name, customer_email, total_amount, payment_method, sale_date)
            VALUES (?, ?, ?, ?, GETDATE())
        """, data['name'], data['email'], final_total, data['payment_method'])
        
        sale_id = cursor.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
        
        # Create sale items
        for item in cart_items.values():
            cursor.execute("""
                INSERT INTO sale_items (sale_id, product_id, product_name, quantity, unit_price, discount_amount, total_price)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, 
            sale_id, item['product_id'], item['name'], item['quantity'],
            item['price'], item['discount_info']['savings'], 
            item['discount_info']['total'])
            
            # Update stock if product exists in SQL
            if str(item['product_id']).isdigit():
                cursor.execute("""
                    UPDATE products 
                    SET stock_quantity = stock_quantity - ? 
                    WHERE id = ?
                """, item['quantity'], item['product_id'])
        
        conn.commit()
        conn.close()
        
        # Clear cart
        session.pop('cart', None)
        
        return render_template('checkout.html',
                             success=True,
                             order_id=sale_id,
                             total_amount=final_total)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/analytics/sales')
def sales_analytics():
    """Generate sales analytics dashboard"""
    try:
        conn = get_sql_connection()
        
        # Get sales data using Pandas
        sales_df = pd.read_sql("""
            SELECT 
                s.sale_date,
                s.total_amount,
                s.payment_method,
                si.product_name,
                si.quantity,
                si.unit_price,
                si.discount_amount
            FROM sales s
            JOIN sale_items si ON s.id = si.sale_id
            WHERE s.sale_date >= DATEADD(month, -6, GETDATE())
            ORDER BY s.sale_date DESC
        """, conn)
        
        # Generate chart
        if not sales_df.empty:
            # Daily sales trend
            sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
            daily_sales = sales_df.groupby(sales_df['sale_date'].dt.date)['total_amount'].sum()
            
            plt.figure(figsize=(12, 6))
            daily_sales.plot(kind='line', marker='o', color='blue')
            plt.title('Daily Sales Trend (Last 6 Months)')
            plt.xlabel('Date')
            plt.ylabel('Sales Amount ($)')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            # Save chart to bytes
            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', dpi=100)
            img_buffer.seek(0)
            chart_base64 = base64.b64encode(img_buffer.getvalue()).decode('utf-8')
            plt.close()
        else:
            chart_base64 = None
        
        # Calculate KPIs using NumPy
        if not sales_df.empty:
            sales_amounts = sales_df['total_amount'].values
            
            kpis = {
                'total_sales': np.sum(sales_amounts),
                'avg_order_value': np.mean(sales_amounts),
                'max_order': np.max(sales_amounts),
                'min_order': np.min(sales_amounts),
                'order_count': len(sales_amounts),
                'std_dev': np.std(sales_amounts)
            }
            
            # Top products
            top_products = sales_df.groupby('product_name')['quantity'].sum().nlargest(10).to_dict()
            
            # Payment method distribution
            payment_dist = sales_df['payment_method'].value_counts().to_dict()
        else:
            kpis = {}
            top_products = {}
            payment_dist = {}
        
        conn.close()
        
        return render_template('analytics.html',
                             sales_data=sales_df.to_dict('records')[:50],  # Limit for display
                             kpis=kpis,
                             top_products=top_products,
                             payment_dist=payment_dist,
                             chart_base64=chart_base64)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/products/json')
def api_products_json():
    """API endpoint returning products as JSON with analytics"""
    try:
        conn = get_sql_connection()
        
        # Get products with sales data
        query = """
            SELECT 
                p.*,
                COALESCE(SUM(si.quantity), 0) as total_sold,
                COALESCE(SUM(si.total_price), 0) as total_revenue
            FROM products p
            LEFT JOIN sale_items si ON p.id = si.product_id
            GROUP BY p.id, p.name, p.description, p.price, p.category, 
                     p.stock_quantity, p.vendor, p.rating, p.created_at
            ORDER BY total_revenue DESC
        """
        
        products_df = pd.read_sql(query, conn)
        conn.close()
        
        # Convert to dictionary
        products_dict = products_df.to_dict('records')
        
        # Calculate overall statistics using NumPy
        prices = products_df['price'].values
        revenue = products_df['total_revenue'].values
        
        stats = {
            'total_products': len(products_df),
            'avg_price': float(np.mean(prices)),
            'total_revenue': float(np.sum(revenue)),
            'top_product': products_df.iloc[0]['name'] if len(products_df) > 0 else None,
            'inventory_value': float(np.sum(prices * products_df['stock_quantity'].values))
        }
        
        return jsonify({
            'success': True,
            'stats': stats,
            'products': products_dict,
            'count': len(products_dict)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)