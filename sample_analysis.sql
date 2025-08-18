
    -- Simple data transformation
    CREATE VIEW customer_summary AS
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        COUNT(o.order_id) as total_orders,
        SUM(o.amount) as total_amount
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    WHERE c.status = 'ACTIVE'
    GROUP BY c.customer_id, c.first_name, c.last_name;
    
    INSERT INTO customer_analytics (customer_id, total_orders, total_amount, analysis_date)
    SELECT customer_id, total_orders, total_amount, CURRENT_DATE
    FROM customer_summary
    WHERE total_amount > 1000;
    