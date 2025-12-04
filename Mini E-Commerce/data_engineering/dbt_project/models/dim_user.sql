-- dim_user.sql
-- User dimension table for data warehouse

{{
    config(
        materialized='incremental',
        unique_key='user_key',
        incremental_strategy='merge',
        merge_update_columns=['username', 'email', 'user_segment', 'total_spent',
                             'order_count', 'avg_order_value', 'last_purchase_date',
                             'days_since_last_purchase', 'is_active', 'updated_at']
    )
}}

WITH user_staging AS (
    SELECT
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['u.id', 'u.email']) }} as user_key,
        
        -- Business keys
        u.id as user_id,
        u.email,
        
        -- User attributes
        u.username,
        u.email,
        u.created_at as registration_date,
        u.last_login,
        
        -- Status flags
        CASE 
            WHEN u.is_active = 1 THEN 1
            ELSE 0
        END as is_active,
        
        -- Metadata
        u.created_at,
        u.updated_at,
        CURRENT_TIMESTAMP as loaded_at,
        
        -- SCD Type 2 fields
        1 as is_current_version,
        CAST('9999-12-31' AS DATETIME) as valid_to,
        CAST('1900-01-01' AS DATETIME) as valid_from,
        
        -- Source tracking
        'sql_server' as source_system
        
    FROM {{ source('ecommerce_raw', 'users') }} u
    
    {% if is_incremental() %}
    WHERE u.updated_at > (SELECT MAX(updated_at) FROM {{ this }})
       OR u.id NOT IN (SELECT user_id FROM {{ this }} WHERE is_current_version = 1)
    {% endif %}
),

user_sales AS (
    SELECT
        s.customer_email,
        COUNT(DISTINCT s.id) as order_count,
        SUM(s.total_amount) as total_spent,
        AVG(s.total_amount) as avg_order_value,
        MIN(s.sale_date) as first_purchase_date,
        MAX(s.sale_date) as last_purchase_date,
        SUM(si.quantity) as total_items_purchased,
        COUNT(DISTINCT si.product_id) as unique_products_purchased
    FROM {{ source('ecommerce_raw', 'sales') }} s
    JOIN {{ source('ecommerce_raw', 'sale_items') }} si ON s.id = si.sale_id
    GROUP BY s.customer_email
),

user_cart_activity AS (
    SELECT
        c.user_id,
        COUNT(DISTINCT c.product_id) as unique_cart_items,
        SUM(c.quantity) as total_cart_quantity,
        MAX(c.added_at) as last_cart_activity
    FROM {{ source('ecommerce_raw', 'cart') }} c
    GROUP BY c.user_id
),

user_engagement AS (
    SELECT
        u.id as user_id,
        DATEDIFF(day, u.created_at, GETDATE()) as days_since_registration,
        CASE 
            WHEN u.last_login IS NULL THEN 999
            ELSE DATEDIFF(day, u.last_login, GETDATE())
        END as days_since_last_login,
        CASE 
            WHEN us.last_purchase_date IS NULL THEN 999
            ELSE DATEDIFF(day, us.last_purchase_date, GETDATE())
        END as days_since_last_purchase
    FROM {{ source('ecommerce_raw', 'users') }} u
    LEFT JOIN user_sales us ON u.email = us.customer_email
),

rfm_calculation AS (
    SELECT
        u.email,
        -- Recency (days since last purchase)
        COALESCE(ue.days_since_last_purchase, 999) as recency,
        
        -- Frequency (order count)
        COALESCE(us.order_count, 0) as frequency,
        
        -- Monetary (total spent)
        COALESCE(us.total_spent, 0) as monetary,
        
        -- RFM Scores (1-5, 5 being best)
        NTILE(5) OVER (ORDER BY COALESCE(ue.days_since_last_purchase, 999) DESC) as recency_score,
        NTILE(5) OVER (ORDER BY COALESCE(us.order_count, 0)) as frequency_score,
        NTILE(5) OVER (ORDER BY COALESCE(us.total_spent, 0)) as monetary_score
    FROM {{ source('ecommerce_raw', 'users') }} u
    LEFT JOIN user_engagement ue ON u.id = ue.user_id
    LEFT JOIN user_sales us ON u.email = us.customer_email
),

rfm_segmentation AS (
    SELECT
        email,
        recency_score,
        frequency_score,
        monetary_score,
        (recency_score * 100 + frequency_score * 10 + monetary_score) as rfm_cell,
        
        -- RFM Segmentation
        CASE 
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 
            THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 
            THEN 'Loyal Customers'
            WHEN recency_score >= 3 AND frequency_score >= 2 
            THEN 'Potential Loyalists'
            WHEN recency_score >= 3 AND frequency_score <= 2 
            THEN 'Recent Customers'
            WHEN recency_score <= 2 AND frequency_score >= 3 
            THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 3 
            THEN 'Cant Lose Them'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2 
            THEN 'Lost'
            ELSE 'Need Attention'
        END as rfm_segment,
        
        -- Simple segmentation
        CASE 
            WHEN COALESCE(total_spent, 0) > 1000 THEN 'VIP'
            WHEN COALESCE(total_spent, 0) > 100 THEN 'Regular'
            WHEN COALESCE(order_count, 0) > 0 THEN 'Occasional'
            ELSE 'New/Inactive'
        END as user_segment
        
    FROM rfm_calculation rc
    LEFT JOIN user_sales us ON rc.email = us.customer_email
),

user_behavior AS (
    SELECT
        u.id as user_id,
        -- Purchase behavior
        COALESCE(us.order_count, 0) as order_count,
        COALESCE(us.total_spent, 0) as total_spent,
        COALESCE(us.avg_order_value, 0) as avg_order_value,
        COALESCE(us.total_items_purchased, 0) as total_items_purchased,
        COALESCE(us.unique_products_purchased, 0) as unique_products_purchased,
        
        -- Cart behavior
        COALESCE(uca.unique_cart_items, 0) as unique_cart_items,
        COALESCE(uca.total_cart_quantity, 0) as total_cart_quantity,
        uca.last_cart_activity,
        
        -- Engagement metrics
        ue.days_since_registration,
        ue.days_since_last_login,
        ue.days_since_last_purchase,
        
        -- RFM metrics
        rfm.recency_score,
        rfm.frequency_score,
        rfm.monetary_score,
        rfm.rfm_cell,
        rfm.rfm_segment,
        rfm.user_segment,
        
        -- Behavioral flags
        CASE WHEN COALESCE(us.order_count, 0) > 0 THEN 1 ELSE 0 END as has_made_purchase,
        CASE WHEN COALESCE(uca.unique_cart_items, 0) > 0 THEN 1 ELSE 0 END as has_cart_items,
        CASE WHEN ue.days_since_last_purchase <= 30 THEN 1 ELSE 0 END as is_active_buyer,
        CASE WHEN ue.days_since_last_login <= 7 THEN 1 ELSE 0 END as is_recently_active
        
    FROM {{ source('ecommerce_raw', 'users') }} u
    LEFT JOIN user_sales us ON u.email = us.customer_email
    LEFT JOIN user_cart_activity uca ON u.id = uca.user_id
    LEFT JOIN user_engagement ue ON u.id = ue.user_id
    LEFT JOIN rfm_segmentation rfm ON u.email = rfm.email
),

final AS (
    SELECT
        us.user_key,
        us.user_id,
        us.username,
        us.email,
        us.registration_date,
        us.last_login,
        us.is_active,
        
        -- Behavioral metrics
        ub.order_count,
        ub.total_spent,
        ub.avg_order_value,
        ub.total_items_purchased,
        ub.unique_products_purchased,
        ub.unique_cart_items,
        ub.total_cart_quantity,
        ub.last_cart_activity,
        
        -- Engagement metrics
        ub.days_since_registration,
        ub.days_since_last_login,
        ub.days_since_last_purchase,
        
        -- RFM analysis
        ub.recency_score,
        ub.frequency_score,
        ub.monetary_score,
        ub.rfm_cell,
        ub.rfm_segment,
        ub.user_segment,
        
        -- Behavioral flags
        ub.has_made_purchase,
        ub.has_cart_items,
        ub.is_active_buyer,
        ub.is_recently_active,
        
        -- Value metrics
        CASE 
            WHEN ub.total_spent > 0 
            THEN ub.total_spent / NULLIF(ub.days_since_registration, 0) * 30
            ELSE 0
        END as estimated_monthly_value,
        
        CASE 
            WHEN ub.order_count > 0 
            THEN ub.total_spent / NULLIF(ub.order_count, 0)
            ELSE 0
        END as lifetime_value_per_order,
        
        -- Timestamps
        us.created_at,
        us.updated_at,
        us.loaded_at,
        
        -- SCD fields
        us.is_current_version,
        us.valid_from,
        us.valid_to,
        
        -- Source information
        us.source_system
        
    FROM user_staging us
    LEFT JOIN user_behavior ub ON us.user_id = ub.user_id
    
    WHERE us.email IS NOT NULL
      AND us.username IS NOT NULL
)

SELECT * FROM final