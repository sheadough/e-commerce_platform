-- Customer Lifetime Value (CLV) calculation
CREATE OR REPLACE VIEW customer_lifetime_value AS
WITH customer_stats AS (
    SELECT 
        u.id as customer_id,
        u.email,
        u.created_at as first_purchase_date,
        COUNT(DISTINCT o.id) as total_orders,
        SUM(o.total_amount) as total_spent,
        AVG(o.total_amount) as avg_order_value,
        MAX(o.created_at) as last_purchase_date,
        EXTRACT(DAYS FROM (MAX(o.created_at) - MIN(o.created_at))) as customer_lifespan_days
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE o.status = 'completed'
    GROUP BY u.id, u.email, u.created_at
)
SELECT 
    *,
    CASE 
        WHEN customer_lifespan_days > 0 THEN 
            total_spent * (365.0 / customer_lifespan_days)
        ELSE total_spent
    END as estimated_annual_value,
    CASE 
        WHEN total_orders = 1 THEN 'One-time buyer'
        WHEN total_orders BETWEEN 2 AND 5 THEN 'Regular customer'
        WHEN total_orders > 5 THEN 'Loyal customer'
    END as customer_segment
FROM customer_stats;

-- Product performance metrics
CREATE OR REPLACE VIEW product_performance AS
SELECT 
    p.id as product_id,
    p.name as product_name,
    p.sku,
    c.name as category_name,
    p.price as current_price,
    COUNT(DISTINCT oi.order_id) as total_orders,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.quantity * oi.price) as total_revenue,
    AVG(oi.price) as avg_selling_price,
    p.stock_quantity as current_stock,
    CASE 
        WHEN p.stock_quantity = 0 THEN 'Out of Stock'
        WHEN p.stock_quantity < 10 THEN 'Low Stock'
        ELSE 'In Stock'
    END as stock_status
FROM products p
LEFT JOIN categories c ON p.category_id = c.id
LEFT JOIN order_items oi ON p.id = oi.product_id
LEFT JOIN orders o ON oi.order_id = o.id
WHERE o.status = 'completed' OR o.status IS NULL
GROUP BY p.id, p.name, p.sku, c.name, p.price, p.stock_quantity;

-- Sales funnel analysis
CREATE OR REPLACE VIEW sales_funnel AS
WITH daily_metrics AS (
    SELECT 
        DATE(created_at) as metric_date,
        'page_views' as metric_type,
        COUNT(*) as metric_value
    FROM user_sessions 
    GROUP BY DATE(created_at)
    
    UNION ALL
    
    SELECT 
        DATE(created_at) as metric_date,
        'cart_additions' as metric_type,
        COUNT(*) as metric_value
    FROM cart_items ci
    JOIN carts c ON ci.cart_id = c.id
    GROUP BY DATE(ci.created_at)
    
    UNION ALL
    
    SELECT 
        DATE(created_at) as metric_date,
        'checkouts_started' as metric_type,
        COUNT(DISTINCT user_id) as metric_value
    FROM orders 
    WHERE status != 'pending'
    GROUP BY DATE(created_at)
    
    UNION ALL
    
    SELECT 
        DATE(created_at) as metric_date,
        'orders_completed' as metric_type,
        COUNT(*) as metric_value
    FROM orders 
    WHERE status = 'completed'
    GROUP BY DATE(created_at)
)
SELECT 
    metric_date,
    SUM(CASE WHEN metric_type = 'page_views' THEN metric_value END) as page_views,
    SUM(CASE WHEN metric_type = 'cart_additions' THEN metric_value END) as cart_additions,
    SUM(CASE WHEN metric_type = 'checkouts_started' THEN metric_value END) as checkouts_started,
    SUM(CASE WHEN metric_type = 'orders_completed' THEN metric_value END) as orders_completed,
    ROUND(
        100.0 * SUM(CASE WHEN metric_type = 'cart_additions' THEN metric_value END) / 
        NULLIF(SUM(CASE WHEN metric_type = 'page_views' THEN metric_value END), 0), 2
    ) as cart_conversion_rate,
    ROUND(
        100.0 * SUM(CASE WHEN metric_type = 'orders_completed' THEN metric_value END) / 
        NULLIF(SUM(CASE WHEN metric_type = 'checkouts_started' THEN metric_value END), 0), 2
    ) as checkout_conversion_rate
FROM daily_metrics 
GROUP BY metric_date
ORDER BY metric_date;