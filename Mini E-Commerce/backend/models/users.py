"""
User Management Module
"""
import pyodbc
import hashlib
import secrets
from datetime import datetime

class UserManager:
    def __init__(self, sql_config):
        self.sql_config = sql_config
    
    def get_sql_connection(self):
        """Create SQL Server connection"""
        conn_str = f"DRIVER={self.sql_config['driver']};SERVER={self.sql_config['server']};DATABASE={self.sql_config['database']};UID={self.sql_config['username']};PWD={self.sql_config['password']}"
        return pyodbc.connect(conn_str)
    
    def create_user(self, username, email, password):
        """Create new user"""
        try:
            # Hash password
            salt = secrets.token_hex(16)
            hashed_password = hashlib.sha256((password + salt).encode()).hexdigest()
            
            conn = self.get_sql_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO users (username, email, password_hash, password_salt, created_at)
                VALUES (?, ?, ?, ?, GETDATE())
            """, username, email, hashed_password, salt)
            
            conn.commit()
            user_id = cursor.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
            conn.close()
            
            return {'success': True, 'user_id': user_id}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def authenticate_user(self, email, password):
        """Authenticate user"""
        try:
            conn = self.get_sql_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, username, password_hash, password_salt 
                FROM users 
                WHERE email = ?
            """, email)
            
            row = cursor.fetchone()
            conn.close()
            
            if not row:
                return {'success': False, 'error': 'User not found'}
            
            user_id, username, stored_hash, salt = row
            
            # Verify password
            hashed_input = hashlib.sha256((password + salt).encode()).hexdigest()
            
            if hashed_input == stored_hash:
                return {
                    'success': True,
                    'user_id': user_id,
                    'username': username
                }
            else:
                return {'success': False, 'error': 'Invalid password'}
                
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_user_profile(self, user_id):
        """Get user profile with purchase history"""
        try:
            conn = self.get_sql_connection()
            cursor = conn.cursor()
            
            # Get user info
            cursor.execute("""
                SELECT username, email, created_at, last_login
                FROM users WHERE id = ?
            """, user_id)
            
            user_row = cursor.fetchone()
            if not user_row:
                return None
            
            user_info = {
                'username': user_row[0],
                'email': user_row[1],
                'created_at': user_row[2],
                'last_login': user_row[3]
            }
            
            # Get purchase history
            cursor.execute("""
                SELECT s.id, s.sale_date, s.total_amount, s.payment_method,
                       COUNT(si.id) as item_count
                FROM sales s
                JOIN sale_items si ON s.id = si.sale_id
                WHERE s.customer_email = ?
                GROUP BY s.id, s.sale_date, s.total_amount, s.payment_method
                ORDER BY s.sale_date DESC
            """, user_info['email'])
            
            purchases = []
            for row in cursor.fetchall():
                purchases.append({
                    'sale_id': row[0],
                    'sale_date': row[1],
                    'total_amount': float(row[2]),
                    'payment_method': row[3],
                    'item_count': row[4]
                })
            
            user_info['purchases'] = purchases
            user_info['total_spent'] = sum(p['total_amount'] for p in purchases)
            user_info['purchase_count'] = len(purchases)
            
            conn.close()
            return user_info
            
        except Exception as e:
            print(f"Error getting user profile: {e}")
            return None