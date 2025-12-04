-- sales_fact.sql
-- Sales fact table for data warehouse

{{
    config(
        materialized='incremental',
        unique_key='sales_fact_key',
        incremental_strategy='merge',
        merge_update_columns=['total_amount', 'item_count', 'discount_amount',
                             'payment_method', 'shipping_address', 'status',
                             'updated_at']
    )
}}

WITH sales_staging AS (
    SELECT
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['s.id', 's.sale_date']) }} as sales_fact_key,
        
        -- Business keys
        s.id as sale_id,
        
        -- Date dimension key (assuming dim_date exists)
        -- In production, you'd join with a date dimension table
        CAST(CONVERT(VARCHAR, s.sale_date, 112) AS INT) as date_key,
        
        -- Time components
        DATEPART(HOUR, s.sale_date) as sale_hour,
        DATEPART(WEEKDAY, s.sale_date) as sale_weekday,
        DATENAME(MONTH, s.sale_date) as sale_month_name,
        DATEPART(QUARTER, s.sale_date) as sale_quarter,
        
        -- Sale attributes
        s.total_amount,
        s.payment_method,
        s.shipping_address,
        s.status,
        s.sale_date,
        
        -- Customer information (for dim_user join)
        s.customer_email,
        s.customer_name,
        
        -- Metadata
        CURRENT_TIMESTAMP as loaded_at,
        
        -- Source tracking
        'sql_server' as source_system
        
    FROM {{ source('ecommerce_raw', 'sales') }} s
    
    {% if is_incremental() %}
    WHERE s.sale_date > (SELECT DATEADD(day, -7, MAX(sale_date)) FROM {{ this }})
    {% endif %}
),

sale_items_agg AS (
    SELECT
        si.sale_id,
        COUNT(*) as item_count,
        SUM(si.quantity) as total_quantity,
        SUM(si.discount_amount) as total_discount_amount,
        SUM(si.total_price) as items_total_price,
        
        -- Product metrics
        COUNT(DISTINCT si.product_id) as unique_products,
        MAX(si.quantity) as max_quantity_per_item,
        MIN(si.quantity) as min_quantity_per_item,
        AVG(si.quantity) as avg_quantity_per_item,
        
        -- Price metrics
        AVG(si.unit_price) as avg_unit_price,
        MAX(si.unit_price) as max_unit_price,
        MIN(si.unit_price) as min_unit_price,
        
        -- Discount analysis
        SUM(si.discount_amount) / NULLIF(SUM(si.unit_price * si.quantity), 0) * 100 as discount_percentage
        
    FROM {{ source('ecommerce_raw', 'sale_items') }} si
    GROUP BY si.sale_id
),

product_category_agg AS (
    SELECT
        si.sale_id,
        p.category,
        SUM(si.quantity) as category_quantity,
        SUM(si.total_price) as category_total
    FROM {{ source('ecommerce_raw', 'sale_items') }} si
    JOIN {{ source('ecommerce_raw', 'products') }} p ON si.product_id = p.id
    GROUP BY si.sale_id, p.category
),

category_pivot AS (
    SELECT
        sale_id,
        MAX(CASE WHEN category = 'Electronics' THEN category_total END) as electronics_amount,
        MAX(CASE WHEN category = 'Clothing' THEN category_total END) as clothing_amount,
        MAX(CASE WHEN category = 'Books' THEN category_total END) as books_amount,
        MAX(CASE WHEN category = 'Home & Kitchen' THEN category_total END) as home_kitchen_amount,
        MAX(CASE WHEN category = 'Sports' THEN category_total END) as sports_amount
    FROM product_category_agg
    GROUP BY sale_id
),

time_analysis AS (
    SELECT
        sale_id,
        CASE 
            WHEN DATEPART(HOUR, sale_date) BETWEEN 6 AND 12 THEN 'Morning'
            WHEN DATEPART(HOUR, sale_date) BETWEEN 13 AND 18 THEN 'Afternoon'
            WHEN DATEPART(HOUR, sale_date) BETWEEN 19 AND 23 THEN 'Evening'
            ELSE 'Night'
        END as time_of_day,
        CASE 
            WHEN DATEPART(WEEKDAY, sale_date) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type
    FROM {{ source('ecommerce_raw', 'sales') }}
),

final AS (
    SELECT
        ss.sales_fact_key,
        ss.sale_id,
        ss.date_key,
        
        -- Time dimensions
        ss.sale_hour,
        ss.sale_weekday,
        ss.sale_month_name,
        ss.sale_quarter,
        ta.time_of_day,
        ta.day_type,
        
        -- Sale metrics
        ss.total_amount,
        COALESCE(sia.item_count, 0) as item_count,
        COALESCE(sia.total_quantity, 0) as total_quantity,
        COALESCE(sia.total_discount_amount, 0) as discount_amount,
        COALESCE(sia.discount_percentage, 0) as discount_percentage,
        
        -- Product metrics
        COALESCE(sia.unique_products, 0) as unique_products,
        COALESCE(sia.max_quantity_per_item, 0) as max_quantity_per_item,
        COALESCE(sia.min_quantity_per_item, 0) as min_quantity_per_item,
        COALESCE(sia.avg_quantity_per_item, 0) as avg_quantity_per_item,
        
        -- Price metrics
        COALESCE(sia.avg_unit_price, 0) as avg_unit_price,
        COALESCE(sia.max_unit_price, 0) as max_unit_price,
        COALESCE(sia.min_unit_price, 0) as min_unit_price,
        
        -- Category breakdown
        COALESCE(cp.electronics_amount, 0) as electronics_amount,
        COALESCE(cp.clothing_amount, 0) as clothing_amount,
        COALESCE(cp.books_amount, 0) as books_amount,
        COALESCE(cp.home_kitchen_amount, 0) as home_kitchen_amount,
        COALESCE(cp.sports_amount, 0) as sports_amount,
        
        -- Customer info (will be replaced by dim_user join in BI layer)
        ss.customer_email,
        ss.customer_name,
        
        -- Transaction info
        ss.payment_method,
        ss.shipping_address,
        ss.status,
        ss.sale_date,
        
        -- Derived metrics
        ss.total_amount / NULLIF(COALESCE(sia.item_count, 1), 0) as avg_amount_per_item,
        CASE 
            WHEN COALESCE(sia.item_count, 0) > 5 THEN 'Large Order'
            WHEN COALESCE(sia.item_count, 0) > 2 THEN 'Medium Order'
            ELSE 'Small Order'
        END as order_size_category,
        
        -- Performance indicators
        CASE 
            WHEN ss.total_amount > 500 THEN 'High Value'
            WHEN ss.total_amount > 100 THEN 'Medium Value'
            ELSE 'Low Value'
        END as order_value_category,
        
        -- Flags
        CASE WHEN COALESCE(sia.discount_percentage, 0) > 10 THEN 1 ELSE 0 END as has_significant_discount,
        CASE WHEN COALESCE(sia.unique_products, 0) > 3 THEN 1 ELSE 0 END as is_diverse_order,
        CASE WHEN ss.payment_method = 'Credit Card' THEN 1 ELSE 0 END as is_credit_card_payment,
        
        -- Timestamps
        ss.loaded_at,
        
        -- Source information
        ss.source_system
        
    FROM sales_staging ss
    LEFT JOIN sale_items_agg sia ON ss.sale_id = sia.sale_id
    LEFT JOIN category_pivot cp ON ss.sale_id = cp.sale_id
    LEFT JOIN time_analysis ta ON ss.sale_id = ta.sale_id
    
    WHERE ss.total_amount > 0
      AND ss.sale_date IS NOT NULL
)

SELECT * FROM final