-- dim_product.sql
-- Product dimension table for data warehouse

{{
    config(
        materialized='incremental',
        unique_key='product_key',
        incremental_strategy='merge',
        merge_update_columns=['product_name', 'product_description', 'price', 'category', 
                             'stock_quantity', 'vendor', 'rating', 'price_category',
                             'is_active', 'updated_at']
    )
}}

WITH product_staging AS (
    SELECT
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['p.id', 'p.product_code']) }} as product_key,
        
        -- Business keys
        p.id as product_id,
        p.product_code,
        
        -- Product attributes
        p.name as product_name,
        p.description as product_description,
        CAST(p.price AS DECIMAL(10,2)) as price,
        p.category,
        p.stock_quantity,
        COALESCE(p.vendor, 'Unknown') as vendor,
        CAST(COALESCE(p.rating, 4.0) AS DECIMAL(3,2)) as rating,
        
        -- Calculated fields
        CASE 
            WHEN p.price < 50 THEN 'Budget'
            WHEN p.price < 200 THEN 'Mid-range'
            ELSE 'Premium'
        END as price_category,
        
        -- Status flags
        CASE 
            WHEN p.stock_quantity > 0 THEN 1
            ELSE 0
        END as is_in_stock,
        
        CASE 
            WHEN p.stock_quantity <= 10 AND p.stock_quantity > 0 THEN 'Low Stock'
            WHEN p.stock_quantity = 0 THEN 'Out of Stock'
            ELSE 'In Stock'
        END as stock_status,
        
        -- Metadata
        p.created_at,
        p.updated_at,
        CURRENT_TIMESTAMP as loaded_at,
        
        -- Slowly Changing Dimension (SCD) Type 2 fields
        1 as is_current_version,
        CAST('9999-12-31' AS DATETIME) as valid_to,
        CAST('1900-01-01' AS DATETIME) as valid_from,
        
        -- Source tracking
        'sql_server' as source_system,
        p.data_source
        
    FROM {{ source('ecommerce_raw', 'products') }} p
    
    {% if is_incremental() %}
    WHERE p.updated_at > (SELECT MAX(updated_at) FROM {{ this }})
       OR p.id NOT IN (SELECT product_id FROM {{ this }} WHERE is_current_version = 1)
    {% endif %}
),

product_sales_agg AS (
    SELECT
        si.product_id,
        COUNT(DISTINCT si.sale_id) as total_orders,
        SUM(si.quantity) as total_quantity_sold,
        SUM(si.total_price) as total_revenue,
        AVG(si.quantity) as avg_order_quantity,
        MAX(s.sale_date) as last_sale_date
    FROM {{ source('ecommerce_raw', 'sale_items') }} si
    JOIN {{ source('ecommerce_raw', 'sales') }} s ON si.sale_id = s.id
    GROUP BY si.product_id
),

category_rankings AS (
    SELECT
        category,
        product_id,
        ROW_NUMBER() OVER (
            PARTITION BY category 
            ORDER BY COALESCE(psa.total_revenue, 0) DESC
        ) as category_rank
    FROM {{ source('ecommerce_raw', 'products') }} p
    LEFT JOIN product_sales_agg psa ON p.id = psa.product_id
),

price_analysis AS (
    SELECT
        p.id as product_id,
        p.price,
        PERCENT_RANK() OVER (ORDER BY p.price) as price_percentile,
        CASE 
            WHEN p.price < PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY p.price) OVER ()
            THEN 'Bottom 25%'
            WHEN p.price < PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY p.price) OVER ()
            THEN '25-50%'
            WHEN p.price < PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY p.price) OVER ()
            THEN '50-75%'
            ELSE 'Top 25%'
        END as price_quartile
    FROM {{ source('ecommerce_raw', 'products') }} p
),

final AS (
    SELECT
        ps.product_key,
        ps.product_id,
        ps.product_code,
        ps.product_name,
        ps.product_description,
        ps.price,
        ps.category,
        ps.stock_quantity,
        ps.vendor,
        ps.rating,
        ps.price_category,
        ps.is_in_stock,
        ps.stock_status,
        
        -- Sales metrics
        COALESCE(psa.total_orders, 0) as total_orders,
        COALESCE(psa.total_quantity_sold, 0) as total_quantity_sold,
        COALESCE(psa.total_revenue, 0) as total_revenue,
        COALESCE(psa.avg_order_quantity, 0) as avg_order_quantity,
        psa.last_sale_date,
        
        -- Inventory metrics
        ps.price * ps.stock_quantity as inventory_value,
        CASE 
            WHEN psa.total_quantity_sold > 0 
            THEN ps.stock_quantity / NULLIF(psa.total_quantity_sold, 0)
            ELSE NULL
        END as months_of_supply,
        
        -- Rankings
        cr.category_rank,
        pa.price_percentile,
        pa.price_quartile,
        
        -- Performance indicators
        CASE 
            WHEN COALESCE(psa.total_revenue, 0) > 1000 THEN 'High Performer'
            WHEN COALESCE(psa.total_revenue, 0) > 100 THEN 'Medium Performer'
            ELSE 'Low Performer'
        END as performance_category,
        
        -- Flags
        CASE WHEN ps.rating >= 4.5 THEN 1 ELSE 0 END as is_top_rated,
        CASE WHEN cr.category_rank <= 3 THEN 1 ELSE 0 END as is_category_top_3,
        
        -- Timestamps
        ps.created_at,
        ps.updated_at,
        ps.loaded_at,
        
        -- SCD fields
        ps.is_current_version,
        ps.valid_from,
        ps.valid_to,
        
        -- Source information
        ps.source_system,
        ps.data_source
        
    FROM product_staging ps
    LEFT JOIN product_sales_agg psa ON ps.product_id = psa.product_id
    LEFT JOIN category_rankings cr ON ps.product_id = cr.product_id AND ps.category = cr.category
    LEFT JOIN price_analysis pa ON ps.product_id = pa.product_id
    
    WHERE ps.product_name IS NOT NULL
      AND ps.price IS NOT NULL
      AND ps.price > 0
)

SELECT * FROM final