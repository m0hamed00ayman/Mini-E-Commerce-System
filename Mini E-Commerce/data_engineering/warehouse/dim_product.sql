-- Data Warehouse: Product Dimension Table
-- Complete implementation with SCD Type 2

CREATE TABLE warehouse.dim_product (
    -- Surrogate key
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Business keys (natural keys)
    product_id INT NOT NULL,
    product_code NVARCHAR(50) NOT NULL,
    
    -- Product attributes
    product_name NVARCHAR(200) NOT NULL,
    product_description NVARCHAR(MAX),
    price DECIMAL(10,2) NOT NULL,
    category NVARCHAR(100) NOT NULL,
    sub_category NVARCHAR(100),
    brand NVARCHAR(100),
    vendor NVARCHAR(200),
    manufacturer NVARCHAR(200),
    
    -- Inventory attributes
    stock_quantity INT NOT NULL DEFAULT 0,
    reorder_level INT DEFAULT 10,
    reorder_quantity INT DEFAULT 50,
    weight DECIMAL(10,2),
    dimensions NVARCHAR(100),
    color NVARCHAR(50),
    size NVARCHAR(50),
    
    -- Quality attributes
    rating DECIMAL(3,2) DEFAULT 0.0,
    review_count INT DEFAULT 0,
    warranty_period INT,
    is_featured BIT DEFAULT 0,
    is_new_arrival BIT DEFAULT 0,
    is_bestseller BIT DEFAULT 0,
    
    -- Financial attributes
    cost_price DECIMAL(10,2),
    profit_margin DECIMAL(5,2),
    tax_rate DECIMAL(5,2) DEFAULT 0.0,
    
    -- Performance metrics (updated regularly)
    total_quantity_sold INT DEFAULT 0,
    total_revenue DECIMAL(15,2) DEFAULT 0.0,
    total_orders INT DEFAULT 0,
    avg_order_quantity DECIMAL(10,2) DEFAULT 0.0,
    last_sale_date DATETIME,
    
    -- Calculated fields
    inventory_value AS (price * stock_quantity),
    stock_status AS (
        CASE 
            WHEN stock_quantity = 0 THEN 'Out of Stock'
            WHEN stock_quantity <= reorder_level THEN 'Low Stock'
            ELSE 'In Stock'
        END
    ),
    price_category AS (
        CASE 
            WHEN price < 50 THEN 'Budget'
            WHEN price < 200 THEN 'Mid-range'
            ELSE 'Premium'
        END
    ),
    performance_category AS (
        CASE 
            WHEN total_revenue > 10000 THEN 'Top Performer'
            WHEN total_revenue > 1000 THEN 'Good Performer'
            WHEN total_revenue > 100 THEN 'Average'
            ELSE 'Low Performer'
        END
    ),
    
    -- SCD Type 2 fields
    is_current_version BIT DEFAULT 1,
    version_start_date DATETIME DEFAULT GETDATE(),
    version_end_date DATETIME DEFAULT '9999-12-31',
    version_number INT DEFAULT 1,
    
    -- Audit fields
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE(),
    source_system NVARCHAR(50) DEFAULT 'SQL Server',
    etl_batch_id NVARCHAR(100),
    
    -- Indexes for performance
    INDEX idx_product_code (product_code),
    INDEX idx_category (category),
    INDEX idx_price (price),
    INDEX idx_vendor (vendor),
    INDEX idx_stock_status (stock_status),
    INDEX idx_performance (performance_category),
    INDEX idx_current_version (is_current_version)
);

-- Create historical table for SCD Type 2 tracking
CREATE TABLE warehouse.dim_product_history (
    history_id INT IDENTITY(1,1) PRIMARY KEY,
    product_key INT NOT NULL,
    product_id INT NOT NULL,
    version_number INT NOT NULL,
    changed_column NVARCHAR(100),
    old_value NVARCHAR(MAX),
    new_value NVARCHAR(MAX),
    change_type NVARCHAR(50), -- INSERT, UPDATE, DELETE
    changed_by NVARCHAR(100),
    changed_at DATETIME DEFAULT GETDATE(),
    
    FOREIGN KEY (product_key) REFERENCES warehouse.dim_product(product_key)
);

-- Create product category dimension (snowflake schema)
CREATE TABLE warehouse.dim_product_category (
    category_key INT IDENTITY(1,1) PRIMARY KEY,
    category_name NVARCHAR(100) NOT NULL UNIQUE,
    category_description NVARCHAR(500),
    parent_category_key INT NULL,
    category_level INT DEFAULT 1,
    category_path NVARCHAR(500),
    product_count INT DEFAULT 0,
    total_revenue DECIMAL(15,2) DEFAULT 0.0,
    
    FOREIGN KEY (parent_category_key) REFERENCES warehouse.dim_product_category(category_key)
);

-- Create vendor dimension
CREATE TABLE warehouse.dim_vendor (
    vendor_key INT IDENTITY(1,1) PRIMARY KEY,
    vendor_name NVARCHAR(200) NOT NULL UNIQUE,
    vendor_type NVARCHAR(50),
    contact_person NVARCHAR(100),
    email NVARCHAR(255),
    phone NVARCHAR(50),
    address NVARCHAR(500),
    city NVARCHAR(100),
    state NVARCHAR(50),
    country NVARCHAR(100),
    rating DECIMAL(3,2) DEFAULT 0.0,
    total_products INT DEFAULT 0,
    total_sales DECIMAL(15,2) DEFAULT 0.0,
    payment_terms NVARCHAR(100),
    lead_time_days INT,
    
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
);

-- Create indexes for performance
CREATE INDEX idx_category_parent ON warehouse.dim_product_category(parent_category_key);
CREATE INDEX idx_vendor_type ON warehouse.dim_vendor(vendor_type);
CREATE INDEX idx_vendor_country ON warehouse.dim_vendor(country);

-- Insert default categories
INSERT INTO warehouse.dim_product_category (category_name, category_description, category_level) VALUES
('Electronics', 'Electronic devices and accessories', 1),
('Clothing', 'Apparel and fashion items', 1),
('Books', 'Books and educational materials', 1),
('Home & Kitchen', 'Home appliances and kitchenware', 1),
('Sports', 'Sports equipment and accessories', 1),
('Beauty', 'Beauty and personal care products', 1),
('Toys', 'Toys and games', 1),
('Automotive', 'Automotive parts and accessories', 1);

-- Insert sample vendors
INSERT INTO warehouse.dim_vendor (vendor_name, vendor_type, country) VALUES
('TechCorp', 'Manufacturer', 'USA'),
('AudioTech', 'Manufacturer', 'Germany'),
('SportGear', 'Distributor', 'China'),
('HomeEssentials', 'Retailer', 'USA'),
('TechBooks', 'Publisher', 'UK'),
('WearableTech', 'Manufacturer', 'South Korea'),
('FitLife', 'Brand', 'USA'),
('KitchenPro', 'Manufacturer', 'Italy');

-- Create view for current product versions
CREATE VIEW warehouse.vw_current_products AS
SELECT 
    dp.product_key,
    dp.product_id,
    dp.product_code,
    dp.product_name,
    dp.product_description,
    dp.price,
    dp.category,
    dp.sub_category,
    dp.brand,
    dp.vendor,
    dp.stock_quantity,
    dp.rating,
    dp.total_quantity_sold,
    dp.total_revenue,
    dp.total_orders,
    dp.inventory_value,
    dp.stock_status,
    dp.price_category,
    dp.performance_category,
    dp.created_at,
    dp.updated_at
FROM warehouse.dim_product dp
WHERE dp.is_current_version = 1;

-- Create view for product performance
CREATE VIEW warehouse.vw_product_performance AS
SELECT
    dp.product_key,
    dp.product_name,
    dp.category,
    dp.price,
    dp.stock_quantity,
    dp.total_quantity_sold,
    dp.total_revenue,
    dp.total_orders,
    dp.rating,
    dp.inventory_value,
    
    -- Key metrics
    dp.total_revenue / NULLIF(dp.total_orders, 0) as avg_revenue_per_order,
    dp.total_quantity_sold / NULLIF(dp.total_orders, 0) as avg_quantity_per_order,
    dp.total_revenue / NULLIF(dp.total_quantity_sold, 0) as avg_price_per_unit,
    
    -- Inventory metrics
    dp.stock_quantity / NULLIF(dp.total_quantity_sold, 0) * 30 as days_of_inventory,
    CASE 
        WHEN dp.total_quantity_sold > 0 
        THEN dp.stock_quantity / dp.total_quantity_sold 
        ELSE NULL 
    END as inventory_turnover_ratio,
    
    -- Ranking metrics
    ROW_NUMBER() OVER (ORDER BY dp.total_revenue DESC) as revenue_rank,
    ROW_NUMBER() OVER (ORDER BY dp.total_quantity_sold DESC) as quantity_rank,
    ROW_NUMBER() OVER (PARTITION BY dp.category ORDER BY dp.total_revenue DESC) as category_revenue_rank,
    
    -- Performance indicators
    CASE 
        WHEN dp.total_revenue > 10000 THEN 'AAA'
        WHEN dp.total_revenue > 5000 THEN 'AA'
        WHEN dp.total_revenue > 1000 THEN 'A'
        WHEN dp.total_revenue > 100 THEN 'B'
        ELSE 'C'
    END as performance_grade,
    
    dp.created_at,
    dp.updated_at
    
FROM warehouse.dim_product dp
WHERE dp.is_current_version = 1;

-- Create stored procedure for updating product dimension
CREATE PROCEDURE warehouse.sp_update_dim_product
    @product_id INT,
    @product_code NVARCHAR(50),
    @product_name NVARCHAR(200),
    @price DECIMAL(10,2),
    @category NVARCHAR(100),
    @stock_quantity INT,
    @vendor NVARCHAR(200) = NULL,
    @rating DECIMAL(3,2) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @existing_product_key INT;
    DECLARE @current_version INT;
    
    -- Check if product exists
    SELECT @existing_product_key = product_key,
           @current_version = version_number
    FROM warehouse.dim_product
    WHERE product_id = @product_id 
      AND is_current_version = 1;
    
    IF @existing_product_key IS NULL
    BEGIN
        -- Insert new product
        INSERT INTO warehouse.dim_product (
            product_id, product_code, product_name, price, 
            category, stock_quantity, vendor, rating
        ) VALUES (
            @product_id, @product_code, @product_name, @price,
            @category, @stock_quantity, @vendor, @rating
        );
    END
    ELSE
    BEGIN
        -- Check if any attributes changed
        IF EXISTS (
            SELECT 1 
            FROM warehouse.dim_product 
            WHERE product_key = @existing_product_key
              AND (
                product_name != @product_name
                OR price != @price
                OR category != @category
                OR stock_quantity != @stock_quantity
                OR ISNULL(vendor, '') != ISNULL(@vendor, '')
                OR ISNULL(rating, 0) != ISNULL(@rating, 0)
              )
        )
        BEGIN
            -- Expire current version
            UPDATE warehouse.dim_product
            SET is_current_version = 0,
                version_end_date = GETDATE(),
                updated_at = GETDATE()
            WHERE product_key = @existing_product_key;
            
            -- Insert new version
            INSERT INTO warehouse.dim_product (
                product_id, product_code, product_name, price,
                category, stock_quantity, vendor, rating,
                version_number, version_start_date
            )
            SELECT 
                product_id, product_code, @product_name, @price,
                @category, @stock_quantity, @vendor, @rating,
                @current_version + 1, GETDATE()
            FROM warehouse.dim_product
            WHERE product_key = @existing_product_key;
        END
        ELSE
        BEGIN
            -- Update existing version
            UPDATE warehouse.dim_product
            SET updated_at = GETDATE()
            WHERE product_key = @existing_product_key;
        END
    END
END;

-- Create function to get product hierarchy
CREATE FUNCTION warehouse.fn_get_product_hierarchy (@product_key INT)
RETURNS TABLE
AS
RETURN
(
    WITH product_hierarchy AS (
        SELECT 
            dp.product_key,
            dp.product_name,
            dp.category,
            dpc.parent_category_key,
            1 as level,
            CAST(dp.product_name AS NVARCHAR(MAX)) as hierarchy_path
        FROM warehouse.dim_product dp
        LEFT JOIN warehouse.dim_product_category dpc ON dp.category = dpc.category_name
        WHERE dp.product_key = @product_key
          AND dp.is_current_version = 1
        
        UNION ALL
        
        SELECT 
            NULL as product_key,
            dpc.category_name as product_name,
            dpc.category_name as category,
            dpc.parent_category_key,
            ph.level + 1,
            CAST(dpc.category_name + ' > ' + ph.hierarchy_path AS NVARCHAR(MAX))
        FROM warehouse.dim_product_category dpc
        INNER JOIN product_hierarchy ph ON dpc.category_key = ph.parent_category_key
    )
    SELECT * FROM product_hierarchy
);