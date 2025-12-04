-- Mini E-Commerce System Database Schema
-- SQL Server 2019+

CREATE DATABASE ECommerceDB;
GO

USE ECommerceDB;
GO

-- Users table
CREATE TABLE users (
    id INT PRIMARY KEY IDENTITY(1,1),
    username NVARCHAR(100) NOT NULL UNIQUE,
    email NVARCHAR(255) NOT NULL UNIQUE,
    password_hash NVARCHAR(255) NOT NULL,
    password_salt NVARCHAR(255) NOT NULL,
    created_at DATETIME DEFAULT GETDATE(),
    last_login DATETIME NULL,
    is_active BIT DEFAULT 1
);
GO

-- Categories table
CREATE TABLE categories (
    id INT PRIMARY KEY IDENTITY(1,1),
    name NVARCHAR(100) NOT NULL UNIQUE,
    description NVARCHAR(500),
    parent_id INT NULL,
    created_at DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (parent_id) REFERENCES categories(id)
);
GO

-- Products table
CREATE TABLE products (
    id INT PRIMARY KEY IDENTITY(1,1),
    name NVARCHAR(200) NOT NULL,
    description NVARCHAR(MAX),
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    category NVARCHAR(100) NOT NULL,
    stock_quantity INT NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    vendor NVARCHAR(200),
    rating DECIMAL(3,2) DEFAULT 0.0 CHECK (rating >= 0 AND rating <= 5),
    image_url NVARCHAR(500),
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
);
GO

-- Shopping cart table
CREATE TABLE cart (
    id INT PRIMARY KEY IDENTITY(1,1),
    user_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL DEFAULT 1 CHECK (quantity > 0),
    added_at DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);
GO

-- Sales table
CREATE TABLE sales (
    id INT PRIMARY KEY IDENTITY(1,1),
    customer_name NVARCHAR(200) NOT NULL,
    customer_email NVARCHAR(255) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0),
    payment_method NVARCHAR(50) NOT NULL,
    sale_date DATETIME DEFAULT GETDATE(),
    shipping_address NVARCHAR(500),
    status NVARCHAR(50) DEFAULT 'completed'
);
GO

-- Sale items table
CREATE TABLE sale_items (
    id INT PRIMARY KEY IDENTITY(1,1),
    sale_id INT NOT NULL,
    product_id INT NOT NULL,
    product_name NVARCHAR(200) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
    discount_amount DECIMAL(10,2) DEFAULT 0 CHECK (discount_amount >= 0),
    total_price DECIMAL(10,2) NOT NULL CHECK (total_price >= 0),
    FOREIGN KEY (sale_id) REFERENCES sales(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id)
);
GO

-- Product reviews table
CREATE TABLE product_reviews (
    id INT PRIMARY KEY IDENTITY(1,1),
    product_id INT NOT NULL,
    user_id INT NOT NULL,
    rating INT NOT NULL CHECK (rating >= 1 AND rating <= 5),
    review_text NVARCHAR(MAX),
    created_at DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (product_id) REFERENCES products(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
);
GO

-- Inventory log table
CREATE TABLE inventory_log (
    id INT PRIMARY KEY IDENTITY(1,1),
    product_id INT NOT NULL,
    change_type NVARCHAR(50) NOT NULL, -- 'restock', 'sale', 'adjustment', 'return'
    quantity_change INT NOT NULL,
    new_quantity INT NOT NULL,
    reason NVARCHAR(500),
    created_at DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (product_id) REFERENCES products(id)
);
GO

-- Price history table
CREATE TABLE price_history (
    id INT PRIMARY KEY IDENTITY(1,1),
    product_id INT NOT NULL,
    old_price DECIMAL(10,2) NOT NULL,
    new_price DECIMAL(10,2) NOT NULL,
    change_date DATETIME DEFAULT GETDATE(),
    changed_by NVARCHAR(100),
    reason NVARCHAR(500),
    FOREIGN KEY (product_id) REFERENCES products(id)
);
GO

-- Wishlist table
CREATE TABLE wishlist (
    id INT PRIMARY KEY IDENTITY(1,1),
    user_id INT NOT NULL,
    product_id INT NOT NULL,
    added_at DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (product_id) REFERENCES products(id),
    UNIQUE (user_id, product_id)
);
GO

-- Create indexes for performance
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_sales_date ON sales(sale_date);
CREATE INDEX idx_sales_email ON sales(customer_email);
CREATE INDEX idx_cart_user ON cart(user_id);
CREATE INDEX idx_sale_items_sale ON sale_items(sale_id);
GO

-- Insert sample data
INSERT INTO categories (name, description) VALUES
('Electronics', 'Electronic devices and accessories'),
('Clothing', 'Apparel and fashion items'),
('Books', 'Books and educational materials'),
('Home & Kitchen', 'Home appliances and kitchenware'),
('Sports', 'Sports equipment and accessories');
GO

INSERT INTO products (name, description, price, category, stock_quantity, vendor, rating) VALUES
('Laptop Pro', 'High-performance laptop with 16GB RAM, 512GB SSD', 1299.99, 'Electronics', 50, 'TechCorp', 4.5),
('Wireless Headphones', 'Noise-cancelling wireless headphones', 199.99, 'Electronics', 100, 'AudioTech', 4.2),
('Running Shoes', 'Lightweight running shoes for athletes', 89.99, 'Sports', 200, 'SportGear', 4.7),
('Coffee Maker', 'Programmable coffee maker with thermal carafe', 79.99, 'Home & Kitchen', 75, 'HomeEssentials', 4.3),
('Python Programming Book', 'Complete guide to Python programming', 39.99, 'Books', 150, 'TechBooks', 4.8),
('Smart Watch', 'Fitness tracker with heart rate monitor', 249.99, 'Electronics', 60, 'WearableTech', 4.4),
('Yoga Mat', 'Non-slip yoga mat for all levels', 29.99, 'Sports', 300, 'FitLife', 4.6),
('Blender', 'High-speed blender for smoothies and soups', 129.99, 'Home & Kitchen', 40, 'KitchenPro', 4.1);
GO

-- Create sample user (password: password123)
DECLARE @salt NVARCHAR(255) = 'samplesalt123';
DECLARE @password_hash NVARCHAR(255) = CONVERT(NVARCHAR(255), HASHBYTES('SHA2_256', 'password123' + @salt), 2);

INSERT INTO users (username, email, password_hash, password_salt) VALUES
('john_doe', 'john@example.com', @password_hash, @salt),
('jane_smith', 'jane@example.com', @password_hash, @salt);
GO

-- Create sample sales data
INSERT INTO sales (customer_name, customer_email, total_amount, payment_method) VALUES
('John Doe', 'john@example.com', 1299.99, 'Credit Card'),
('Jane Smith', 'jane@example.com', 289.98, 'PayPal');
GO

INSERT INTO sale_items (sale_id, product_id, product_name, quantity, unit_price, total_price) VALUES
(1, 1, 'Laptop Pro', 1, 1299.99, 1299.99),
(2, 2, 'Wireless Headphones', 1, 199.99, 199.99),
(2, 4, 'Coffee Maker', 1, 79.99, 79.99);
GO

-- Create views for reporting
CREATE VIEW vw_product_sales AS
SELECT 
    p.id,
    p.name,
    p.category,
    p.price,
    COALESCE(SUM(si.quantity), 0) as total_sold,
    COALESCE(SUM(si.total_price), 0) as total_revenue,
    p.stock_quantity
FROM products p
LEFT JOIN sale_items si ON p.id = si.product_id
GROUP BY p.id, p.name, p.category, p.price, p.stock_quantity;
GO

CREATE VIEW vw_daily_sales AS
SELECT 
    CAST(sale_date AS DATE) as sale_day,
    COUNT(*) as order_count,
    SUM(total_amount) as daily_revenue,
    AVG(total_amount) as avg_order_value
FROM sales
GROUP BY CAST(sale_date AS DATE);
GO

-- Create stored procedures
CREATE PROCEDURE sp_AddToCart
    @user_id INT,
    @product_id INT,
    @quantity INT = 1
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if item already in cart
    IF EXISTS (SELECT 1 FROM cart WHERE user_id = @user_id AND product_id = @product_id)
    BEGIN
        UPDATE cart 
        SET quantity = quantity + @quantity,
            added_at = GETDATE()
        WHERE user_id = @user_id AND product_id = @product_id;
    END
    ELSE
    BEGIN
        INSERT INTO cart (user_id, product_id, quantity)
        VALUES (@user_id, @product_id, @quantity);
    END
    
    -- Return updated cart
    SELECT * FROM cart WHERE user_id = @user_id;
END
GO

CREATE PROCEDURE sp_ProcessCheckout
    @customer_name NVARCHAR(200),
    @customer_email NVARCHAR(255),
    @payment_method NVARCHAR(50),
    @shipping_address NVARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @sale_id INT;
    
    BEGIN TRANSACTION;
    
    BEGIN TRY
        -- Create sale record
        INSERT INTO sales (customer_name, customer_email, payment_method, shipping_address, total_amount)
        VALUES (@customer_name, @customer_email, @payment_method, @shipping_address, 0);
        
        SET @sale_id = SCOPE_IDENTITY();
        
        -- Update total amount with cart items (simplified - would need cart parameter)
        UPDATE sales 
        SET total_amount = (
            SELECT SUM(p.price * c.quantity)
            FROM cart c
            JOIN products p ON c.product_id = p.id
            WHERE c.user_id = (SELECT id FROM users WHERE email = @customer_email)
        )
        WHERE id = @sale_id;
        
        -- Clear user's cart
        DELETE FROM cart 
        WHERE user_id = (SELECT id FROM users WHERE email = @customer_email);
        
        COMMIT TRANSACTION;
        
        SELECT @sale_id as sale_id, 'Checkout successful' as message;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END
GO

-- Create function for discount calculation
CREATE FUNCTION fn_CalculateDiscount
(
    @quantity INT,
    @price DECIMAL(10,2)
)
RETURNS DECIMAL(10,2)
AS
BEGIN
    DECLARE @discount_rate DECIMAL(3,2);
    
    SET @discount_rate = CASE
        WHEN @quantity >= 10 THEN 0.20
        WHEN @quantity >= 5 THEN 0.10
        ELSE 0.00
    END;
    
    RETURN @price * @quantity * @discount_rate;
END
GO

PRINT 'Database schema created successfully!';