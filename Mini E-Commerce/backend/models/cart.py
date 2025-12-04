"""
Shopping Cart Management
"""
import pyodbc
import numpy as np

class CartManager:
    def __init__(self, sql_config):
        self.sql_config = sql_config
    
    def get_sql_connection(self):
        """Create SQL Server connection"""
        conn_str = f"DRIVER={self.sql_config['driver']};SERVER={self.sql_config['server']};DATABASE={self.sql_config['database']};UID={self.sql_config['username']};PWD={self.sql_config['password']}"
        return pyodbc.connect(conn_str)
    
    def calculate_cart_totals(self, cart_items):
        """Calculate cart totals with discounts"""
        if not cart_items:
            return {
                'subtotal': 0,
                'discount': 0,
                'tax': 0,
                'total': 0
            }
        
        # Convert to NumPy arrays for vectorized calculations
        prices = np.array([item['price'] for item in cart_items])
        quantities = np.array([item['quantity'] for item in cart_items])
        
        # Calculate volume discounts
        discount_rates = np.where(quantities >= 10, 0.20,
                                 np.where(quantities >= 5, 0.10, 0.0))
        
        subtotal = np.sum(prices * quantities)
        discount_amount = np.sum(prices * quantities * discount_rates)
        
        # Calculate tax (8%)
        taxable_amount = subtotal - discount_amount
        tax = taxable_amount * 0.08
        
        total = subtotal - discount_amount + tax
        
        return {
            'subtotal': float(subtotal),
            'discount': float(discount_amount),
            'tax': float(tax),
            'total': float(total),
            'item_count': len(cart_items)
        }
    
    def save_cart_to_db(self, user_id, cart_items):
        """Save cart to database for persistence"""
        try:
            conn = self.get_sql_connection()
            cursor = conn.cursor()
            
            # Clear existing cart items
            cursor.execute("DELETE FROM cart WHERE user_id = ?", user_id)
            
            # Insert new cart items
            for item in cart_items:
                cursor.execute("""
                    INSERT INTO cart (user_id, product_id, quantity, added_at)
                    VALUES (?, ?, ?, GETDATE())
                """, user_id, item['product_id'], item['quantity'])
            
            conn.commit()
            conn.close()
            
            return True
        except Exception as e:
            print(f"Error saving cart: {e}")
            return False
    
    def get_user_cart(self, user_id):
        """Get user's cart from database"""
        try:
            conn = self.get_sql_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT c.id, c.product_id, p.name, p.price, c.quantity, c.added_at
                FROM cart c
                JOIN products p ON c.product_id = p.id
                WHERE c.user_id = ?
                ORDER BY c.added_at DESC
            """, user_id)
            
            cart_items = []
            for row in cursor.fetchall():
                cart_items.append({
                    'cart_id': row[0],
                    'product_id': row[1],
                    'name': row[2],
                    'price': float(row[3]),
                    'quantity': row[4],
                    'added_at': row[5]
                })
            
            conn.close()
            return cart_items
        except Exception as e:
            print(f"Error getting cart: {e}")
            return []