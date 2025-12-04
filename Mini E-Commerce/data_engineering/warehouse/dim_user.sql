-- Data Warehouse: User Dimension Table
-- Complete implementation with SCD Type 2

CREATE TABLE warehouse.dim_user (
    -- Surrogate key
    user_key INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Business keys (natural keys)
    user_id INT NOT NULL,
    email NVARCHAR(255) NOT NULL UNIQUE,
    
    -- User attributes
    username NVARCHAR(100) NOT NULL,
    first_name NVARCHAR(100),
    last_name NVARCHAR(100),
    display_name NVARCHAR(200),
    
    -- Demographics
    date_of_birth DATE,
    age AS (DATEDIFF(year, date_of_birth, GETDATE())),
    age_group AS (
        CASE 
            WHEN DATEDIFF(year, date_of_birth, GETDATE()) < 18 THEN 'Under 18'
            WHEN DATEDIFF(year, date_of_birth, GETDATE()) <= 25 THEN '18-25'
            WHEN DATEDIFF(year, date_of_birth, GETDATE()) <= 35 THEN '26-35'
            WHEN DATEDIFF(year, date_of_birth, GETDATE()) <= 50 THEN '36-50'
            ELSE 'Over 50'
        END
    ),
    gender NVARCHAR(20),
    
    -- Contact information
    phone_number NVARCHAR(50),
    address_line1 NVARCHAR(200),
    address_line2 NVARCHAR(200),
    city NVARCHAR(100),
    state NVARCHAR(100),
    country NVARCHAR(100),
    postal_code NVARCHAR(20),
    
    -- Account information
    registration_date DATETIME NOT NULL,
    registration_source NVARCHAR(100),
    account_type NVARCHAR(50) DEFAULT 'Regular',
    subscription_type NVARCHAR(50),
    subscription_status NVARCHAR(50),
    last_login_date DATETIME,
    email_verified BIT DEFAULT 0,
    phone_verified BIT DEFAULT 0,
    
    -- Preferences
    preferred_language NVARCHAR(50) DEFAULT 'English',
    currency_preference NVARCHAR(10) DEFAULT 'USD',
    notification_preferences NVARCHAR(MAX),
    marketing_consent BIT DEFAULT 1,
    
    -- Behavioral metrics
    total_orders INT DEFAULT 0,
    total_spent DECIMAL(15,2) DEFAULT 0.0,
    avg_order_value DECIMAL(10,2) DEFAULT 0.0,
    first_purchase_date DATETIME,
    last_purchase_date DATETIME,
    days_since_last_purchase AS (
        CASE 
            WHEN last_purchase_date IS NULL THEN NULL
            ELSE DATEDIFF(day, last_purchase_date, GETDATE())
        END
    ),
    purchase_frequency DECIMAL(10,2) DEFAULT 0.0,
    
    -- Product preferences
    favorite_category NVARCHAR(100),
    favorite_brand NVARCHAR(100),
    avg_product_rating DECIMAL(3,2) DEFAULT 0.0,
    
    -- Cart behavior
    avg_cart_value DECIMAL(10,2) DEFAULT 0.0,
    cart_abandonment_rate DECIMAL(5,2) DEFAULT 0.0,
    last_cart_activity DATETIME,
    
    -- Customer service metrics
    support_tickets INT DEFAULT 0,
    avg_resolution_time_hours DECIMAL(10,2),
    customer_satisfaction_score DECIMAL(3,2),
    
    -- RFM Analysis
    recency_score INT,
    frequency_score INT,
    monetary_score INT,
    rfm_segment NVARCHAR(50),
    rfm_score AS (recency_score * 100 + frequency_score * 10 + monetary_score),
    customer_segment NVARCHAR(50),
    customer_tier NVARCHAR(50),
    
    -- Lifetime value
    predicted_lifetime_value DECIMAL(15,2),
    customer_lifetime_value DECIMAL(15,2) DEFAULT 0.0,
    months_as_customer INT DEFAULT 0,
    
    -- Calculated fields
    is_active AS (
        CASE 
            WHEN days_since_last_purchase <= 30 THEN 1
            ELSE 0
        END
    ),
    customer_type AS (
        CASE 
            WHEN total_orders = 0 THEN 'Prospect'
            WHEN total_orders = 1 THEN 'First-Time Buyer'
            WHEN total_orders <= 5 THEN 'Repeat Customer'
            WHEN total_orders <= 20 THEN 'Frequent Buyer'
            ELSE 'VIP'
        END
    ),
    spending_category AS (
        CASE 
            WHEN total_spent > 10000 THEN 'Whale'
            WHEN total_spent > 1000 THEN 'High Spender'
            WHEN total_spent > 100 THEN 'Medium Spender'
            WHEN total_spent > 0 THEN 'Low Spender'
            ELSE 'Non-Spender'
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
    INDEX idx_email (email),
    INDEX idx_customer_segment (customer_segment),
    INDEX idx_customer_tier (customer_tier),
    INDEX idx_registration_date (registration_date),
    INDEX idx_total_spent (total_spent),
    INDEX idx_is_active (is_active),
    INDEX idx_current_version (is_current_version)
);

-- Create user segment dimension
CREATE TABLE warehouse.dim_user_segment (
    segment_key INT IDENTITY(1,1) PRIMARY KEY,
    segment_name NVARCHAR(100) NOT NULL UNIQUE,
    segment_description NVARCHAR(500),
    min_recency INT,
    max_recency INT,
    min_frequency INT,
    max_frequency INT,
    min_monetary DECIMAL(15,2),
    max_monetary DECIMAL(15,2),
    target_actions NVARCHAR(MAX),
    retention_strategy NVARCHAR(MAX),
    user_count INT DEFAULT 0,
    avg_lifetime_value DECIMAL(15,2) DEFAULT 0.0,
    
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
);

-- Create user geography dimension
CREATE TABLE warehouse.dim_geography (
    geography_key INT IDENTITY(1,1) PRIMARY KEY,
    country NVARCHAR(100) NOT NULL,
    region NVARCHAR(100),
    state NVARCHAR(100),
    city NVARCHAR(100),
    postal_code NVARCHAR(20),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    timezone NVARCHAR(50),
    population INT,
    avg_income DECIMAL(15,2),
    
    user_count INT DEFAULT 0,
    total_revenue DECIMAL(15,2) DEFAULT 0.0,
    avg_order_value DECIMAL(10,2) DEFAULT 0.0,
    
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE(),
    
    INDEX idx_country (country),
    INDEX idx_city (city),
    INDEX idx_postal_code (postal_code)
);

-- Create indexes for performance
CREATE INDEX idx_segment_recency ON warehouse.dim_user_segment(min_recency, max_recency);
CREATE INDEX idx_segment_monetary ON warehouse.dim_user_segment(min_monetary, max_monetary);
CREATE INDEX idx_geo_location ON warehouse.dim_geography(latitude, longitude);

-- Insert default user segments
INSERT INTO warehouse.dim_user_segment (segment_name, segment_description, min_recency, max_recency, min_frequency, max_frequency, min_monetary, max_monetary) VALUES
('Champions', 'Best customers who buy recently and often, spending the most', 1, 30, 5, 999, 1000, 999999),
('Loyal Customers', 'Recent customers with good purchase frequency', 1, 30, 3, 4, 500, 999),
('Potential Loyalists', 'Recent customers with average frequency', 1, 30, 2, 3, 200, 499),
('Recent Customers', 'Bought recently but not often', 1, 30, 1, 2, 0, 199),
('Promising', 'Recent customers but haven''t spent much yet', 1, 30, 1, 1, 0, 99),
('Need Attention', 'Average recency, frequency and monetary values', 31, 90, 2, 3, 200, 499),
('About To Sleep', 'Below average recency, frequency and monetary values', 31, 90, 1, 2, 0, 199),
('At Risk', 'Used to purchase often but haven''t returned recently', 91, 180, 3, 5, 500, 999),
('Can''t Lose Them', 'Used to purchase often and spent big, but haven''t returned recently', 91, 180, 5, 999, 1000, 999999),
('Hibernating', 'Last purchase was long ago, low number of orders', 181, 365, 1, 2, 0, 199),
('Lost', 'Last purchase was long ago, purchased few times', 366, 999, 1, 1, 0, 99);

-- Create view for current user versions
CREATE VIEW warehouse.vw_current_users AS
SELECT 
    du.user_key,
    du.user_id,
    du.email,
    du.username,
    du.first_name,
    du.last_name,
    du.age_group,
    du.gender,
    du.city,
    du.state,
    du.country,
    du.registration_date,
    du.account_type,
    du.total_orders,
    du.total_spent,
    du.avg_order_value,
    du.first_purchase_date,
    du.last_purchase_date,
    du.days_since_last_purchase,
    du.customer_type,
    du.spending_category,
    du.rfm_segment,
    du.customer_segment,
    du.is_active,
    du.created_at,
    du.updated_at
FROM warehouse.dim_user du
WHERE du.is_current_version = 1;

-- Create view for user performance analysis
CREATE VIEW warehouse.vw_user_performance AS
SELECT
    du.user_key,
    du.email,
    du.username,
    du.registration_date,
    du.total_orders,
    du.total_spent,
    du.avg_order_value,
    du.first_purchase_date,
    du.last_purchase_date,
    du.days_since_last_purchase,
    
    -- Key metrics
    du.total_spent / NULLIF(DATEDIFF(month, du.registration_date, GETDATE()), 0) as avg_monthly_spend,
    du.total_orders / NULLIF(DATEDIFF(month, du.registration_date, GETDATE()), 0) as avg_monthly_orders,
    
    -- Customer health indicators
    CASE 
        WHEN du.days_since_last_purchase <= 7 THEN 'Very Active'
        WHEN du.days_since_last_purchase <= 30 THEN 'Active'
        WHEN du.days_since_last_purchase <= 90 THEN 'Less Active'
        WHEN du.days_since_last_purchase <= 180 THEN 'Inactive'
        ELSE 'Churned'
    END as activity_status,
    
    -- Value metrics
    CASE 
        WHEN du.total_spent > 10000 THEN 'VIP'
        WHEN du.total_spent > 1000 THEN 'Premium'
        WHEN du.total_spent > 100 THEN 'Regular'
        WHEN du.total_spent > 0 THEN 'Occasional'
        ELSE 'Non-Spender'
    END as value_tier,
    
    -- Engagement metrics
    CASE 
        WHEN du.total_orders >= 10 THEN 'Highly Engaged'
        WHEN du.total_orders >= 5 THEN 'Engaged'
        WHEN du.total_orders >= 2 THEN 'Somewhat Engaged'
        WHEN du.total_orders = 1 THEN 'One-Time'
        ELSE 'Never Purchased'
    END as engagement_level,
    
    -- Predictions
    CASE 
        WHEN du.days_since_last_purchase > 180 THEN 'High Risk'
        WHEN du.days_since_last_purchase > 90 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as churn_risk,
    
    -- Segment analysis
    du.rfm_segment,
    du.customer_segment,
    du.is_active,
    
    du.created_at,
    du.updated_at
    
FROM warehouse.dim_user du
WHERE du.is_current_version = 1;

-- Create stored procedure for updating user dimension
CREATE PROCEDURE warehouse.sp_update_dim_user
    @user_id INT,
    @email NVARCHAR(255),
    @username NVARCHAR(100),
    @first_name NVARCHAR(100) = NULL,
    @last_name NVARCHAR(100) = NULL,
    @date_of_birth DATE = NULL,
    @gender NVARCHAR(20) = NULL,
    @phone_number NVARCHAR(50) = NULL,
    @address_line1 NVARCHAR(200) = NULL,
    @city NVARCHAR(100) = NULL,
    @state NVARCHAR(100) = NULL,
    @country NVARCHAR(100) = NULL,
    @postal_code NVARCHAR(20) = NULL,
    @last_login_date DATETIME = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @existing_user_key INT;
    DECLARE @current_version INT;
    
    -- Check if user exists
    SELECT @existing_user_key = user_key,
           @current_version = version_number
    FROM warehouse.dim_user
    WHERE user_id = @user_id 
      AND is_current_version = 1;
    
    IF @existing_user_key IS NULL
    BEGIN
        -- Insert new user
        INSERT INTO warehouse.dim_user (
            user_id, email, username, first_name, last_name,
            date_of_birth, gender, phone_number, address_line1,
            city, state, country, postal_code, last_login_date,
            registration_date
        ) VALUES (
            @user_id, @email, @username, @first_name, @last_name,
            @date_of_birth, @gender, @phone_number, @address_line1,
            @city, @state, @country, @postal_code, @last_login_date,
            GETDATE()
        );
    END
    ELSE
    BEGIN
        -- Check if any attributes changed
        IF EXISTS (
            SELECT 1 
            FROM warehouse.dim_user 
            WHERE user_key = @existing_user_key
              AND (
                email != @email
                OR username != @username
                OR ISNULL(first_name, '') != ISNULL(@first_name, '')
                OR ISNULL(last_name, '') != ISNULL(@last_name, '')
                OR ISNULL(date_of_birth, '1900-01-01') != ISNULL(@date_of_birth, '1900-01-01')
                OR ISNULL(gender, '') != ISNULL(@gender, '')
                OR ISNULL(phone_number, '') != ISNULL(@phone_number, '')
                OR ISNULL(address_line1, '') != ISNULL(@address_line1, '')
                OR ISNULL(city, '') != ISNULL(@city, '')
                OR ISNULL(state, '') != ISNULL(@state, '')
                OR ISNULL(country, '') != ISNULL(@country, '')
                OR ISNULL(postal_code, '') != ISNULL(@postal_code, '')
              )
        )
        BEGIN
            -- Expire current version
            UPDATE warehouse.dim_user
            SET is_current_version = 0,
                version_end_date = GETDATE(),
                updated_at = GETDATE()
            WHERE user_key = @existing_user_key;
            
            -- Insert new version
            INSERT INTO warehouse.dim_user (
                user_id, email, username, first_name, last_name,
                date_of_birth, gender, phone_number, address_line1,
                city, state, country, postal_code, last_login_date,
                registration_date, version_number, version_start_date
            )
            SELECT 
                user_id, @email, @username, @first_name, @last_name,
                @date_of_birth, @gender, @phone_number, @address_line1,
                @city, @state, @country, @postal_code, @last_login_date,
                registration_date, @current_version + 1, GETDATE()
            FROM warehouse.dim_user
            WHERE user_key = @existing_user_key;
        END
        ELSE
        BEGIN
            -- Update last login if provided
            IF @last_login_date IS NOT NULL
            BEGIN
                UPDATE warehouse.dim_user
                SET last_login_date = @last_login_date,
                    updated_at = GETDATE()
                WHERE user_key = @existing_user_key;
            END
        END
    END
END;

-- Create function to calculate RFM scores
CREATE FUNCTION warehouse.fn_calculate_rfm (
    @recency_days INT,
    @frequency INT,
    @monetary DECIMAL(15,2)
)
RETURNS @rfm_scores TABLE (
    recency_score INT,
    frequency_score INT,
    monetary_score INT,
    rfm_segment NVARCHAR(50)
)
AS
BEGIN
    DECLARE @r_score INT, @f_score INT, @m_score INT;
    
    -- Calculate recency score (1-5, 5 being most recent)
    SET @r_score = CASE 
        WHEN @recency_days <= 7 THEN 5
        WHEN @recency_days <= 30 THEN 4
        WHEN @recency_days <= 90 THEN 3
        WHEN @recency_days <= 180 THEN 2
        ELSE 1
    END;
    
    -- Calculate frequency score (1-5, 5 being most frequent)
    SET @f_score = CASE 
        WHEN @frequency >= 10 THEN 5
        WHEN @frequency >= 5 THEN 4
        WHEN @frequency >= 3 THEN 3
        WHEN @frequency >= 2 THEN 2
        ELSE 1
    END;
    
    -- Calculate monetary score (1-5, 5 being highest spending)
    SET @m_score = CASE 
        WHEN @monetary >= 1000 THEN 5
        WHEN @monetary >= 500 THEN 4
        WHEN @monetary >= 200 THEN 3
        WHEN @monetary >= 100 THEN 2
        ELSE 1
    END;
    
    -- Determine RFM segment
    DECLARE @segment NVARCHAR(50);
    SET @segment = CASE 
        WHEN @r_score >= 4 AND @f_score >= 4 AND @m_score >= 4 THEN 'Champions'
        WHEN @r_score >= 3 AND @f_score >= 3 AND @m_score >= 3 THEN 'Loyal Customers'
        WHEN @r_score >= 3 AND @f_score >= 2 THEN 'Potential Loyalists'
        WHEN @r_score >= 3 AND @f_score <= 2 THEN 'Recent Customers'
        WHEN @r_score <= 2 AND @f_score >= 3 THEN 'At Risk'
        WHEN @r_score <= 2 AND @f_score <= 2 AND @m_score >= 3 THEN 'Can''t Lose Them'
        WHEN @r_score <= 2 AND @f_score <= 2 AND @m_score <= 2 THEN 'Lost'
        ELSE 'Need Attention'
    END;
    
    INSERT INTO @rfm_scores VALUES (@r_score, @f_score, @m_score, @segment);
    RETURN;
END;