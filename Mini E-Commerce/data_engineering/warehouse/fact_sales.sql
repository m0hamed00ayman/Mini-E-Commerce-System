-- Data Warehouse: Sales Fact Table
-- Complete implementation with measures and dimensions

CREATE TABLE warehouse.fact_sales (
    -- Surrogate keys for dimensions
    sales_fact_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_key INT NOT NULL,
    time_key INT NOT NULL,
    product_key INT NOT NULL,
    user_key INT NOT NULL,
    vendor_key INT,
    geography_key INT,
    payment_method_key INT,
    shipping_method_key INT,
    
    -- Business keys
    sale_id INT NOT NULL,
    transaction_id NVARCHAR(100) UNIQUE,
    
    -- Date and time components
    sale_date DATE NOT NULL,
    sale_time TIME NOT NULL,
    sale_timestamp DATETIME2 NOT NULL,
    sale_year INT NOT NULL,
    sale_month INT NOT NULL,
    sale_day INT NOT NULL,
    sale_quarter INT NOT NULL,
    sale_week INT NOT NULL,
    sale_dayofweek INT NOT NULL,
    sale_hour INT NOT NULL,
    sale_minute INT NOT NULL,
    
    -- Product measures
    product_quantity INT NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    cost_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2) DEFAULT 0.0,
    tax_amount DECIMAL(10,2) DEFAULT 0.0,
    shipping_cost DECIMAL(10,2) DEFAULT 0.0,
    handling_fee DECIMAL(10,2) DEFAULT 0.0,
    
    -- Financial measures
    gross_amount DECIMAL(12,2) NOT NULL,
    net_amount DECIMAL(12,2) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    profit_amount DECIMAL(12,2),
    profit_margin DECIMAL(5,2),
    
    -- Discount metrics
    discount_percentage DECIMAL(5,2) DEFAULT 0.0,
    discount_type NVARCHAR(50),
    coupon_code NVARCHAR(50),
    
    -- Order metrics
    order_quantity INT NOT NULL DEFAULT 1,
    order_item_count INT DEFAULT 1,
    unique_product_count INT DEFAULT 1,
    
    -- Customer metrics
    customer_age_at_purchase INT,
    days_since_customer_registration INT,
    customer_purchase_count INT,
    customer_total_spent DECIMAL(15,2),
    
    -- Product metrics at time of sale
    product_age_days INT,
    product_current_rating DECIMAL(3,2),
    product_stock_at_sale INT,
    
    -- Shipping information
    shipping_address NVARCHAR(500),
    shipping_city NVARCHAR(100),
    shipping_state NVARCHAR(100),
    shipping_country NVARCHAR(100),
    shipping_postal_code NVARCHAR(20),
    estimated_delivery_days INT,
    actual_delivery_days INT,
    delivery_status NVARCHAR(50),
    
    -- Payment information
    payment_status NVARCHAR(50) NOT NULL,
    payment_processor NVARCHAR(100),
    payment_transaction_id NVARCHAR(100),
    payment_authorization_code NVARCHAR(100),
    
    -- Order status
    order_status NVARCHAR(50) NOT NULL,
    fulfillment_status NVARCHAR(50),
    return_status NVARCHAR(50),
    refund_status NVARCHAR(50),
    refund_amount DECIMAL(10,2) DEFAULT 0.0,
    
    -- Channel information
    sales_channel NVARCHAR(50) DEFAULT 'Web',
    device_type NVARCHAR(50),
    browser_type NVARCHAR(50),
    operating_system NVARCHAR(50),
    ip_address NVARCHAR(50),
    
    -- Campaign information
    campaign_id NVARCHAR(100),
    campaign_name NVARCHAR(200),
    utm_source NVARCHAR(100),
    utm_medium NVARCHAR(100),
    utm_campaign NVARCHAR(100),
    utm_term NVARCHAR(100),
    utm_content NVARCHAR(100),
    
    -- Session information
    session_id NVARCHAR(100),
    page_views_before_purchase INT,
    time_to_purchase_seconds INT,
    
    -- Calculated fields
    is_first_purchase AS (
        CASE WHEN customer_purchase_count = 1 THEN 1 ELSE 0 END
    ),
    is_repeat_purchase AS (
        CASE WHEN customer_purchase_count > 1 THEN 1 ELSE 0 END
    ),
    is_large_order AS (
        CASE WHEN total_amount > 500 THEN 1 ELSE 0 END
    ),
    is_discounted_order AS (
        CASE WHEN discount_amount > 0 THEN 1 ELSE 0 END
    ),
    is_high_margin AS (
        CASE WHEN profit_margin > 50 THEN 1 ELSE 0 END
    ),
    order_size_category AS (
        CASE 
            WHEN order_item_count > 10 THEN 'XL'
            WHEN order_item_count > 5 THEN 'Large'
            WHEN order_item_count > 2 THEN 'Medium'
            ELSE 'Small'
        END
    ),
    
    -- Performance indicators
    customer_acquisition_cost DECIMAL(10,2),
    customer_lifetime_value DECIMAL(15,2),
    return_on_ad_spend DECIMAL(10,2),
    
    -- Audit fields
    created_at DATETIME2 DEFAULT SYSDATETIME(),
    updated_at DATETIME2 DEFAULT SYSDATETIME(),
    source_system NVARCHAR(50) DEFAULT 'SQL Server',
    etl_batch_id NVARCHAR(100),
    etl_load_timestamp DATETIME2 DEFAULT SYSDATETIME(),
    
    -- Foreign key constraints
    CONSTRAINT fk_fact_sales_date FOREIGN KEY (date_key) REFERENCES warehouse.dim_date(date_key),
    CONSTRAINT fk_fact_sales_product FOREIGN KEY (product_key) REFERENCES warehouse.dim_product(product_key),
    CONSTRAINT fk_fact_sales_user FOREIGN KEY (user_key) REFERENCES warehouse.dim_user(user_key),
    CONSTRAINT fk_fact_sales_vendor FOREIGN KEY (vendor_key) REFERENCES warehouse.dim_vendor(vendor_key),
    CONSTRAINT fk_fact_sales_geography FOREIGN KEY (geography_key) REFERENCES warehouse.dim_geography(geography_key),
    
    -- Indexes for performance
    INDEX idx_sale_date (sale_date),
    INDEX idx_sale_timestamp (sale_timestamp),
    INDEX idx_product_key (product_key),
    INDEX idx_user_key (user_key),
    INDEX idx_total_amount (total_amount),
    INDEX idx_order_status (order_status),
    INDEX idx_payment_status (payment_status),
    INDEX idx_sales_channel (sales_channel),
    INDEX idx_campaign_id (campaign_id),
    INDEX idx_date_product (date_key, product_key),
    INDEX idx_date_user (date_key, user_key),
    INDEX idx_product_user (product_key, user_key)
);

-- Create date dimension (if not exists)
CREATE TABLE warehouse.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_number INT NOT NULL,
    day_name NVARCHAR(20) NOT NULL,
    day_of_week INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_quarter INT NOT NULL,
    day_of_year INT NOT NULL,
    week_number INT NOT NULL,
    week_of_year INT NOT NULL,
    month_number INT NOT NULL,
    month_name NVARCHAR(20) NOT NULL,
    month_name_short NVARCHAR(3) NOT NULL,
    quarter_number INT NOT NULL,
    quarter_name NVARCHAR(10) NOT NULL,
    year_number INT NOT NULL,
    year_quarter NVARCHAR(10) NOT NULL,
    year_month NVARCHAR(10) NOT NULL,
    year_week NVARCHAR(10) NOT NULL,
    is_weekend BIT NOT NULL,
    is_holiday BIT DEFAULT 0,
    holiday_name NVARCHAR(100),
    is_business_day BIT DEFAULT 1,
    fiscal_year INT,
    fiscal_quarter INT,
    fiscal_month INT,
    
    created_at DATETIME DEFAULT GETDATE(),
    
    INDEX idx_full_date (full_date),
    INDEX idx_year_month (year_month),
    INDEX idx_year_quarter (year_quarter)
);

-- Create time dimension
CREATE TABLE warehouse.dim_time (
    time_key INT PRIMARY KEY,
    time_value TIME NOT NULL UNIQUE,
    hour_24 INT NOT NULL,
    hour_12 INT NOT NULL,
    minute INT NOT NULL,
    second INT NOT NULL,
    am_pm NVARCHAR(2) NOT NULL,
    hour_minute NVARCHAR(5) NOT NULL,
    time_period NVARCHAR(50) NOT NULL,
    is_business_hour BIT DEFAULT 0,
    is_peak_hour BIT DEFAULT 0,
    
    created_at DATETIME DEFAULT GETDATE()
);

-- Create payment method dimension
CREATE TABLE warehouse.dim_payment_method (
    payment_method_key INT IDENTITY(1,1) PRIMARY KEY,
    payment_method_code NVARCHAR(50) NOT NULL UNIQUE,
    payment_method_name NVARCHAR(100) NOT NULL,
    payment_type NVARCHAR(50) NOT NULL,
    payment_processor NVARCHAR(100),
    transaction_fee_percentage DECIMAL(5,2) DEFAULT 0.0,
    transaction_fee_fixed DECIMAL(10,2) DEFAULT 0.0,
    settlement_days INT DEFAULT 1,
    is_active BIT DEFAULT 1,
    
    total_transactions INT DEFAULT 0,
    total_amount DECIMAL(15,2) DEFAULT 0.0,
    avg_transaction_amount DECIMAL(10,2) DEFAULT 0.0,
    success_rate DECIMAL(5,2) DEFAULT 0.0,
    
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
);

-- Create shipping method dimension
CREATE TABLE warehouse.dim_shipping_method (
    shipping_method_key INT IDENTITY(1,1) PRIMARY KEY,
    shipping_method_code NVARCHAR(50) NOT NULL UNIQUE,
    shipping_method_name NVARCHAR(100) NOT NULL,
    carrier_name NVARCHAR(100),
    service_level NVARCHAR(50),
    estimated_days_min INT,
    estimated_days_max INT,
    base_cost DECIMAL(10,2) DEFAULT 0.0,
    cost_per_kg DECIMAL(10,2) DEFAULT 0.0,
    is_active BIT DEFAULT 1,
    is_trackable BIT DEFAULT 0,
    insurance_available BIT DEFAULT 0,
    
    total_shipments INT DEFAULT 0,
    on_time_rate DECIMAL(5,2) DEFAULT 0.0,
    damage_rate DECIMAL(5,2) DEFAULT 0.0,
    avg_delivery_days DECIMAL(10,2) DEFAULT 0.0,
    
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
);

-- Create indexes for dimensions
CREATE INDEX idx_date_year ON warehouse.dim_date(year_number);
CREATE INDEX idx_date_month ON warehouse.dim_date(month_number);
CREATE INDEX idx_time_hour ON warehouse.dim_time(hour_24);
CREATE INDEX idx_payment_type ON warehouse.dim_payment_method(payment_type);
CREATE INDEX idx_shipping_carrier ON warehouse.dim_shipping_method(carrier_name);

-- Insert default payment methods
INSERT INTO warehouse.dim_payment_method (payment_method_code, payment_method_name, payment_type) VALUES
('CC_VISA', 'Visa', 'Credit Card'),
('CC_MASTERCARD', 'MasterCard', 'Credit Card'),
('CC_AMEX', 'American Express', 'Credit Card'),
('CC_DISCOVER', 'Discover', 'Credit Card'),
('PAYPAL', 'PayPal', 'Digital Wallet'),
('APPLE_PAY', 'Apple Pay', 'Digital Wallet'),
('GOOGLE_PAY', 'Google Pay', 'Digital Wallet'),
('BANK_TRANSFER', 'Bank Transfer', 'Bank Transfer'),
('COD', 'Cash on Delivery', 'Cash'),
('GIFT_CARD', 'Gift Card', 'Store Credit');

-- Insert default shipping methods
INSERT INTO warehouse.dim_shipping_method (shipping_method_code, shipping_method_name, carrier_name, service_level, estimated_days_min, estimated_days_max) VALUES
('UPS_GROUND', 'UPS Ground', 'UPS', 'Standard', 3, 5),
('UPS_2DAY', 'UPS 2nd Day Air', 'UPS', 'Express', 2, 2),
('UPS_NEXTDAY', 'UPS Next Day Air', 'UPS', 'Overnight', 1, 1),
('FEDEX_GROUND', 'FedEx Ground', 'FedEx', 'Standard', 3, 5),
('FEDEX_2DAY', 'FedEx 2Day', 'FedEx', 'Express', 2, 2),
('FEDEX_OVERNIGHT', 'FedEx Overnight', 'FedEx', 'Overnight', 1, 1),
('USPS_PRIORITY', 'USPS Priority', 'USPS', 'Standard', 2, 3),
('USPS_EXPRESS', 'USPS Express', 'USPS', 'Express', 1, 2),
('DHL_EXPRESS', 'DHL Express', 'DHL', 'International', 3, 7),
('LOCAL_PICKUP', 'Local Pickup', 'Store', 'Pickup', 0, 0);

-- Create view for daily sales summary
CREATE VIEW warehouse.vw_daily_sales_summary AS
SELECT
    dd.full_date as sale_date,
    dd.year_number,
    dd.month_number,
    dd.month_name,
    dd.week_number,
    dd.day_of_week,
    dd.day_name,
    
    COUNT(DISTINCT fs.sale_id) as total_orders,
    COUNT(fs.sales_fact_key) as total_items,
    SUM(fs.total_amount) as total_revenue,
    SUM(fs.profit_amount) as total_profit,
    SUM(fs.discount_amount) as total_discount,
    SUM(fs.tax_amount) as total_tax,
    SUM(fs.shipping_cost) as total_shipping,
    
    AVG(fs.total_amount) as avg_order_value,
    AVG(fs.profit_margin) as avg_profit_margin,
    SUM(fs.discount_amount) / NULLIF(SUM(fs.gross_amount), 0) * 100 as discount_rate,
    
    COUNT(DISTINCT fs.user_key) as unique_customers,
    COUNT(DISTINCT fs.product_key) as unique_products,
    
    -- New vs returning customers
    SUM(fs.is_first_purchase) as new_customers,
    SUM(fs.is_repeat_purchase) as returning_customers,
    
    -- Order size analysis
    SUM(CASE WHEN fs.order_size_category = 'Small' THEN 1 ELSE 0 END) as small_orders,
    SUM(CASE WHEN fs.order_size_category = 'Medium' THEN 1 ELSE 0 END) as medium_orders,
    SUM(CASE WHEN fs.order_size_category = 'Large' THEN 1 ELSE 0 END) as large_orders,
    SUM(CASE WHEN fs.order_size_category = 'XL' THEN 1 ELSE 0 END) as xl_orders,
    
    -- Channel analysis
    SUM(CASE WHEN fs.sales_channel = 'Web' THEN 1 ELSE 0 END) as web_orders,
    SUM(CASE WHEN fs.sales_channel = 'Mobile' THEN 1 ELSE 0 END) as mobile_orders,
    SUM(CASE WHEN fs.sales_channel = 'Store' THEN 1 ELSE 0 END) as store_orders,
    
    -- Payment method analysis
    SUM(CASE WHEN fs.payment_method_key IN (1,2,3,4) THEN 1 ELSE 0 END) as credit_card_orders,
    SUM(CASE WHEN fs.payment_method_key IN (5,6,7) THEN 1 ELSE 0 END) as digital_wallet_orders,
    SUM(CASE WHEN fs.payment_method_key = 9 THEN 1 ELSE 0 END) as cod_orders
    
FROM warehouse.fact_sales fs
JOIN warehouse.dim_date dd ON fs.date_key = dd.date_key
GROUP BY 
    dd.full_date, dd.year_number, dd.month_number, dd.month_name,
    dd.week_number, dd.day_of_week, dd.day_name;

-- Create view for product sales performance
CREATE VIEW warehouse.vw_product_sales_performance AS
SELECT
    dp.product_key,
    dp.product_name,
    dp.category,
    dp.price_category,
    dp.performance_category,
    
    COUNT(DISTINCT fs.sale_id) as total_orders,
    SUM(fs.product_quantity) as total_quantity_sold,
    SUM(fs.total_amount) as total_revenue,
    SUM(fs.profit_amount) as total_profit,
    AVG(fs.profit_margin) as avg_profit_margin,
    
    COUNT(DISTINCT fs.user_key) as unique_customers,
    COUNT(DISTINCT fs.date_key) as days_with_sales,
    
    -- Time-based metrics
    MIN(fs.sale_date) as first_sale_date,
    MAX(fs.sale_date) as last_sale_date,
    DATEDIFF(day, MIN(fs.sale_date), MAX(fs.sale_date)) as sales_period_days,
    
    -- Average metrics
    SUM(fs.total_amount) / NULLIF(SUM(fs.product_quantity), 0) as avg_selling_price,
    SUM(fs.product_quantity) / NULLIF(COUNT(DISTINCT fs.sale_id), 0) as avg_quantity_per_order,
    SUM(fs.total_amount) / NULLIF(COUNT(DISTINCT fs.sale_id), 0) as avg_revenue_per_order,
    
    -- Growth metrics
    SUM(CASE WHEN fs.sale_date >= DATEADD(month, -1, GETDATE()) THEN fs.total_amount ELSE 0 END) as revenue_last_30_days,
    SUM(CASE WHEN fs.sale_date >= DATEADD(month, -2, GETDATE()) AND fs.sale_date < DATEADD(month, -1, GETDATE()) 
             THEN fs.total_amount ELSE 0 END) as revenue_previous_30_days,
    
    -- Ranking
    ROW_NUMBER() OVER (ORDER BY SUM(fs.total_amount) DESC) as revenue_rank,
    ROW_NUMBER() OVER (PARTITION BY dp.category ORDER BY SUM(fs.total_amount) DESC) as category_revenue_rank
    
FROM warehouse.fact_sales fs
JOIN warehouse.dim_product dp ON fs.product_key = dp.product_key
GROUP BY 
    dp.product_key, dp.product_name, dp.category, 
    dp.price_category, dp.performance_category;

-- Create view for customer purchasing behavior
CREATE VIEW warehouse.vw_customer_purchasing_behavior AS
SELECT
    du.user_key,
    du.email,
    du.username,
    du.customer_type,
    du.spending_category,
    du.rfm_segment,
    
    COUNT(DISTINCT fs.sale_id) as total_orders,
    COUNT(fs.sales_fact_key) as total_items_purchased,
    SUM(fs.total_amount) as total_spent,
    AVG(fs.total_amount) as avg_order_value,
    
    MIN(fs.sale_date) as first_purchase_date,
    MAX(fs.sale_date) as last_purchase_date,
    DATEDIFF(day, MIN(fs.sale_date), MAX(fs.sale_date)) as customer_lifetime_days,
    
    COUNT(DISTINCT fs.product_key) as unique_products_purchased,
    COUNT(DISTINCT dp.category) as unique_categories_purchased,
    
    -- Frequency metrics
    COUNT(DISTINCT fs.sale_id) / NULLIF(DATEDIFF(day, MIN(fs.sale_date), MAX(fs.sale_date)) / 30.0, 0) as purchases_per_month,
    DATEDIFF(day, MAX(fs.sale_date), GETDATE()) as days_since_last_purchase,
    
    -- Value metrics
    SUM(fs.total_amount) / NULLIF(DATEDIFF(day, MIN(fs.sale_date), MAX(fs.sale_date)) / 30.0, 0) as avg_monthly_spend,
    SUM(fs.profit_amount) as total_profit_generated,
    
    -- Product preferences
    STRING_AGG(DISTINCT dp.category, ', ') WITHIN GROUP (ORDER BY COUNT(fs.sales_fact_key) DESC) as top_categories,
    
    -- Payment preferences
    STRING_AGG(DISTINCT dpm.payment_method_name, ', ') WITHIN GROUP (ORDER BY COUNT(fs.sales_fact_key) DESC) as preferred_payment_methods,
    
    -- Seasonality
    COUNT(DISTINCT DATEPART(month, fs.sale_date)) as active_months,
    COUNT(DISTINCT DATEPART(quarter, fs.sale_date)) as active_quarters
    
FROM warehouse.fact_sales fs
JOIN warehouse.dim_user du ON fs.user_key = du.user_key
JOIN warehouse.dim_product dp ON fs.product_key = dp.product_key
JOIN warehouse.dim_payment_method dpm ON fs.payment_method_key = dpm.payment_method_key
GROUP BY 
    du.user_key, du.email, du.username, du.customer_type,
    du.spending_category, du.rfm_segment;

-- Create stored procedure for loading sales facts
CREATE PROCEDURE warehouse.sp_load_fact_sales
    @start_date DATE = NULL,
    @end_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @batch_id UNIQUEIDENTIFIER = NEWID();
    DECLARE @load_timestamp DATETIME2 = SYSDATETIME();
    
    -- Set default date range if not provided
    IF @start_date IS NULL
        SET @start_date = DATEADD(day, -30, GETDATE());
    
    IF @end_date IS NULL
        SET @end_date = GETDATE();
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Insert new sales facts
        INSERT INTO warehouse.fact_sales (
            date_key, time_key, product_key, user_key, vendor_key, geography_key,
            payment_method_key, shipping_method_key, sale_id, transaction_id,
            sale_date, sale_time, sale_timestamp, sale_year, sale_month, sale_day,
            sale_quarter, sale_week, sale_dayofweek, sale_hour, sale_minute,
            product_quantity, unit_price, cost_price, discount_amount, tax_amount,
            shipping_cost, handling_fee, gross_amount, net_amount, total_amount,
            profit_amount, profit_margin, discount_percentage, discount_type,
            coupon_code, order_quantity, order_item_count, unique_product_count,
            customer_age_at_purchase, days_since_customer_registration,
            customer_purchase_count, customer_total_spent, product_age_days,
            product_current_rating, product_stock_at_sale, shipping_address,
            shipping_city, shipping_state, shipping_country, shipping_postal_code,
            estimated_delivery_days, actual_delivery_days, delivery_status,
            payment_status, payment_processor, payment_transaction_id,
            payment_authorization_code, order_status, fulfillment_status,
            return_status, refund_status, refund_amount, sales_channel,
            device_type, browser_type, operating_system, ip_address,
            campaign_id, campaign_name, utm_source, utm_medium, utm_campaign,
            utm_term, utm_content, session_id, page_views_before_purchase,
            time_to_purchase_seconds, customer_acquisition_cost,
            customer_lifetime_value, return_on_ad_spend,
            etl_batch_id, etl_load_timestamp
        )
        SELECT
            -- Dimension keys (to be joined in ETL)
            dd.date_key,
            dt.time_key,
            dp.product_key,
            du.user_key,
            dv.vendor_key,
            dg.geography_key,
            dpm.payment_method_key,
            dsm.shipping_method_key,
            
            -- Business keys
            s.id as sale_id,
            CONCAT('TXN-', s.id, '-', FORMAT(s.sale_date, 'yyyyMMdd')) as transaction_id,
            
            -- Date and time components
            CAST(s.sale_date AS DATE) as sale_date,
            CAST(s.sale_date AS TIME) as sale_time,
            s.sale_date as sale_timestamp,
            YEAR(s.sale_date) as sale_year,
            MONTH(s.sale_date) as sale_month,
            DAY(s.sale_date) as sale_day,
            DATEPART(quarter, s.sale_date) as sale_quarter,
            DATEPART(week, s.sale_date) as sale_week,
            DATEPART(weekday, s.sale_date) as sale_dayofweek,
            DATEPART(hour, s.sale_date) as sale_hour,
            DATEPART(minute, s.sale_date) as sale_minute,
            
            -- Product measures
            si.quantity as product_quantity,
            si.unit_price,
            p.cost_price,
            si.discount_amount,
            si.unit_price * si.quantity * 0.08 as tax_amount, -- Assuming 8% tax
            0.0 as shipping_cost, -- Would come from shipping table
            0.0 as handling_fee,
            
            -- Financial measures
            si.unit_price * si.quantity as gross_amount,
            (si.unit_price * si.quantity) - si.discount_amount as net_amount,
            si.total_price as total_amount,
            (si.unit_price - p.cost_price) * si.quantity - si.discount_amount as profit_amount,
            CASE 
                WHEN si.unit_price > 0 
                THEN ((si.unit_price - p.cost_price) * si.quantity - si.discount_amount) / (si.unit_price * si.quantity) * 100
                ELSE 0
            END as profit_margin,
            
            -- Discount metrics
            CASE 
                WHEN si.unit_price * si.quantity > 0 
                THEN si.discount_amount / (si.unit_price * si.quantity) * 100
                ELSE 0
            END as discount_percentage,
            'Volume Discount' as discount_type,
            NULL as coupon_code,
            
            -- Order metrics (would come from order aggregation)
            1 as order_quantity,
            1 as order_item_count,
            1 as unique_product_count,
            
            -- Customer metrics
            DATEDIFF(year, u.date_of_birth, s.sale_date) as customer_age_at_purchase,
            DATEDIFF(day, u.created_at, s.sale_date) as days_since_customer_registration,
            (SELECT COUNT(*) FROM sales s2 WHERE s2.customer_email = s.customer_email AND s2.sale_date <= s.sale_date) as customer_purchase_count,
            (SELECT SUM(total_amount) FROM sales s2 WHERE s2.customer_email = s.customer_email AND s2.sale_date <= s.sale_date) as customer_total_spent,
            
            -- Product metrics at time of sale
            DATEDIFF(day, p.created_at, s.sale_date) as product_age_days,
            p.rating as product_current_rating,
            p.stock_quantity as product_stock_at_sale,
            
            -- Shipping information
            s.shipping_address,
            NULL as shipping_city,
            NULL as shipping_state,
            NULL as shipping_country,
            NULL as shipping_postal_code,
            3 as estimated_delivery_days,
            NULL as actual_delivery_days,
            'Delivered' as delivery_status,
            
            -- Payment information
            'Completed' as payment_status,
            s.payment_method as payment_processor,
            CONCAT('AUTH-', s.id) as payment_transaction_id,
            CONCAT('AUTHCODE-', s.id) as payment_authorization_code,
            
            -- Order status
            'Completed' as order_status,
            'Fulfilled' as fulfillment_status,
            'No Return' as return_status,
            'No Refund' as refund_status,
            0.0 as refund_amount,
            
            -- Channel information
            'Web' as sales_channel,
            'Desktop' as device_type,
            'Chrome' as browser_type,
            'Windows' as operating_system,
            '192.168.1.1' as ip_address,
            
            -- Campaign information
            NULL as campaign_id,
            NULL as campaign_name,
            NULL as utm_source,
            NULL as utm_medium,
            NULL as utm_campaign,
            NULL as utm_term,
            NULL as utm_content,
            
            -- Session information
            CONCAT('SESS-', s.id) as session_id,
            5 as page_views_before_purchase,
            300 as time_to_purchase_seconds,
            
            -- Performance indicators
            10.0 as customer_acquisition_cost,
            (SELECT SUM(total_amount) FROM sales s2 WHERE s2.customer_email = s.customer_email) as customer_lifetime_value,
            5.0 as return_on_ad_spend,
            
            -- ETL metadata
            @batch_id as etl_batch_id,
            @load_timestamp as etl_load_timestamp
            
        FROM ECommerceDB.dbo.sales s
        JOIN ECommerceDB.dbo.sale_items si ON s.id = si.sale_id
        JOIN ECommerceDB.dbo.products p ON si.product_id = p.id
        LEFT JOIN ECommerceDB.dbo.users u ON s.customer_email = u.email
        
        -- Join with warehouse dimensions
        LEFT JOIN warehouse.dim_date dd ON CAST(s.sale_date AS DATE) = dd.full_date
        LEFT JOIN warehouse.dim_time dt ON CAST(CAST(s.sale_date AS TIME) AS VARCHAR(8)) = CAST(dt.time_value AS VARCHAR(8))
        LEFT JOIN warehouse.dim_product dp ON p.id = dp.product_id AND dp.is_current_version = 1
        LEFT JOIN warehouse.dim_user du ON u.id = du.user_id AND du.is_current_version = 1
        LEFT JOIN warehouse.dim_vendor dv ON p.vendor = dv.vendor_name
        LEFT JOIN warehouse.dim_geography dg ON u.city = dg.city AND u.state = dg.state AND u.country = dg.country
        LEFT JOIN warehouse.dim_payment_method dpm ON s.payment_method = dpm.payment_method_name
        LEFT JOIN warehouse.dim_shipping_method dsm ON 'UPS_GROUND' = dsm.shipping_method_code
        
        WHERE s.sale_date BETWEEN @start_date AND @end_date
          AND NOT EXISTS (
              SELECT 1 FROM warehouse.fact_sales fs2 
              WHERE fs2.sale_id = s.id AND fs2.product_key = dp.product_key
          );
        
        -- Update statistics
        DECLARE @rows_inserted INT = @@ROWCOUNT;
        
        COMMIT TRANSACTION;
        
        -- Log successful load
        INSERT INTO warehouse.etl_log (
            batch_id, load_timestamp, table_name,
            rows_inserted, rows_updated, rows_deleted,
            status, error_message
        ) VALUES (
            @batch_id, @load_timestamp, 'fact_sales',
            @rows_inserted, 0, 0,
            'SUCCESS', NULL
        );
        
        PRINT 'Sales fact load completed successfully. Rows inserted: ' + CAST(@rows_inserted AS NVARCHAR(20));
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        
        -- Log error
        INSERT INTO warehouse.etl_log (
            batch_id, load_timestamp, table_name,
            rows_inserted, rows_updated, rows_deleted,
            status, error_message
        ) VALUES (
            @batch_id, @load_timestamp, 'fact_sales',
            0, 0, 0,
            'ERROR', ERROR_MESSAGE()
        );
        
        THROW;
    END CATCH;
END;

-- Create ETL log table
CREATE TABLE warehouse.etl_log (
    log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    batch_id UNIQUEIDENTIFIER NOT NULL,
    load_timestamp DATETIME2 NOT NULL,
    table_name NVARCHAR(100) NOT NULL,
    rows_inserted INT DEFAULT 0,
    rows_updated INT DEFAULT 0,
    rows_deleted INT DEFAULT 0,
    status NVARCHAR(20) NOT NULL,
    error_message NVARCHAR(MAX),
    duration_ms INT,
    
    created_at DATETIME2 DEFAULT SYSDATETIME(),
    
    INDEX idx_batch_id (batch_id),
    INDEX idx_load_timestamp (load_timestamp),
    INDEX idx_status (status)
);

-- Create index maintenance stored procedure
CREATE PROCEDURE warehouse.sp_maintain_indexes
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @table_name NVARCHAR(255);
    DECLARE @sql NVARCHAR(MAX);
    
    -- Reorganize indexes with fragmentation between 5% and 30%
    DECLARE index_cursor CURSOR FOR
    SELECT 
        QUOTENAME(s.name) + '.' + QUOTENAME(t.name) as table_name
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) ips
    JOIN sys.tables t ON ips.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE ips.avg_fragmentation_in_percent BETWEEN 5 AND 30
      AND ips.index_id > 0
      AND s.name = 'warehouse'
    GROUP BY s.name, t.name;
    
    OPEN index_cursor;
    FETCH NEXT FROM index_cursor INTO @table_name;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = 'ALTER INDEX ALL ON ' + @table_name + ' REORGANIZE;';
        EXEC sp_executesql @sql;
        
        FETCH NEXT FROM index_cursor INTO @table_name;
    END;
    
    CLOSE index_cursor;
    DEALLOCATE index_cursor;
    
    -- Rebuild indexes with fragmentation > 30%
    DECLARE rebuild_cursor CURSOR FOR
    SELECT 
        QUOTENAME(s.name) + '.' + QUOTENAME(t.name) as table_name
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) ips
    JOIN sys.tables t ON ips.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE ips.avg_fragmentation_in_percent > 30
      AND ips.index_id > 0
      AND s.name = 'warehouse'
    GROUP BY s.name, t.name;
    
    OPEN rebuild_cursor;
    FETCH NEXT FROM rebuild_cursor INTO @table_name;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = 'ALTER INDEX ALL ON ' + @table_name + ' REBUILD;';
        EXEC sp_executesql @sql;
        
        FETCH NEXT FROM rebuild_cursor INTO @table_name;
    END;
    
    CLOSE rebuild_cursor;
    DEALLOCATE rebuild_cursor;
    
    -- Update statistics
    EXEC sp_updatestats;
    
    PRINT 'Index maintenance completed successfully.';
END;