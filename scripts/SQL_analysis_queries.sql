-- ============================================================================
-- RETAIL DATABASE - ANALYSIS QUERIES
-- ============================================================================

USE RetailDB;
GO

-- ============================================================================
-- A) SALES ANALYSIS
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- A1. Top 10 Best-Selling Products (by quantity sold)
-- ────────────────────────────────────────────────────────────────────────────
SELECT TOP 10
    p.product_id,
    p.product_name,
    p.brand_name,
    p.category_name,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.total_price) AS total_revenue,
    AVG(oi.list_price) AS avg_selling_price,
    COUNT(DISTINCT oi.order_id) AS number_of_orders
FROM OrderItems oi
INNER JOIN Products p ON oi.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.brand_name, p.category_name
ORDER BY total_quantity_sold DESC;
GO

-- ────────────────────────────────────────────────────────────────────────────
-- A2. Top 5 Customers by Spending
-- ────────────────────────────────────────────────────────────────────────────
SELECT TOP 5
    c.customer_id,
    c.full_name,
    c.email,
    c.city,
    c.state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.order_total) AS total_spent,
    AVG(o.order_total) AS avg_order_value,
    MAX(o.order_date) AS last_order_date
FROM Customers c
INNER JOIN Orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.full_name, c.email, c.city, c.state
ORDER BY total_spent DESC;
GO

-- ────────────────────────────────────────────────────────────────────────────
-- A3. Revenue per Store
-- ────────────────────────────────────────────────────────────────────────────
SELECT 
    s.store_id,
    s.store_name,
    s.city,
    s.state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.order_total) AS total_revenue,
    AVG(o.order_total) AS avg_order_value,
    SUM(oi.quantity) AS total_items_sold
FROM Stores s
INNER JOIN Orders o ON s.store_id = o.store_id
INNER JOIN OrderItems oi ON o.order_id = oi.order_id
GROUP BY s.store_id, s.store_name, s.city, s.state
ORDER BY total_revenue DESC;
GO

-- ────────────────────────────────────────────────────────────────────────────
-- A4. Revenue per Category
-- ────────────────────────────────────────────────────────────────────────────
SELECT 
    c.category_id,
    c.category_name,
    COUNT(DISTINCT p.product_id) AS number_of_products,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.total_price) AS total_revenue,
    AVG(oi.total_price) AS avg_transaction_value,
    COUNT(DISTINCT oi.order_id) AS total_orders
FROM Categories c
INNER JOIN Products p ON c.category_id = p.category_id
INNER JOIN OrderItems oi ON p.product_id = oi.product_id
GROUP BY c.category_id, c.category_name
ORDER BY total_revenue DESC;
GO

-- ────────────────────────────────────────────────────────────────────────────
-- A5. Monthly Sales Trend
-- ────────────────────────────────────────────────────────────────────────────
SELECT 
    YEAR(o.order_date) AS order_year,
    MONTH(o.order_date) AS order_month,
    DATENAME(MONTH, o.order_date) AS month_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.order_total) AS total_revenue,
    AVG(o.order_total) AS avg_order_value,
    SUM(oi.quantity) AS total_items_sold,
    COUNT(DISTINCT o.customer_id) AS unique_customers
FROM Orders o
INNER JOIN OrderItems oi ON o.order_id = oi.order_id
WHERE o.order_date IS NOT NULL
GROUP BY YEAR(o.order_date), MONTH(o.order_date), DATENAME(MONTH, o.order_date)
ORDER BY order_year DESC, order_month DESC;
GO

-- ============================================================================
-- B) INVENTORY ANALYSIS
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- B1. Products with Low Stock (threshold: less than 10 units total)
-- ────────────────────────────────────────────────────────────────────────────
SELECT 
    p.product_id,
    p.product_name,
    p.brand_name,
    p.category_name,
    p.list_price,
    SUM(st.quantity) AS total_stock_across_stores,
    COUNT(st.store_id) AS number_of_stores_stocking,
    -- Show stock per store
    STRING_AGG(
        CONCAT(s.store_name, ': ', st.quantity), 
        ', '
    ) AS stock_by_store
FROM Products p
LEFT JOIN Stocks st ON p.product_id = st.product_id
LEFT JOIN Stores s ON st.store_id = s.store_id
GROUP BY p.product_id, p.product_name, p.brand_name, p.category_name, p.list_price
HAVING SUM(st.quantity) < 10 OR SUM(st.quantity) IS NULL
ORDER BY total_stock_across_stores ASC;
GO

-- ────────────────────────────────────────────────────────────────────────────
-- B2. Stores with Highest Inventory Levels
-- ────────────────────────────────────────────────────────────────────────────
SELECT 
    s.store_id,
    s.store_name,
    s.city,
    s.state,
    COUNT(DISTINCT st.product_id) AS unique_products_stocked,
    SUM(st.quantity) AS total_inventory_units,
    AVG(st.quantity) AS avg_stock_per_product,
    -- Calculate total inventory value
    SUM(st.quantity * p.list_price) AS total_inventory_value
FROM Stores s
INNER JOIN Stocks st ON s.store_id = st.store_id
INNER JOIN Products p ON st.product_id = p.product_id
GROUP BY s.store_id, s.store_name, s.city, s.state
ORDER BY total_inventory_units DESC;
GO

-- ============================================================================
-- C) STAFF PERFORMANCE
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- C1. Number of Orders Handled by Each Staff Member
-- ────────────────────────────────────────────────────────────────────────────
SELECT 
    st.staff_id,
    st.first_name + ' ' + st.last_name AS staff_name,
    st.email,
    s.store_name,
    s.city AS store_city,
    COUNT(DISTINCT o.order_id) AS total_orders_handled,
    SUM(o.order_total) AS total_sales_revenue,
    AVG(o.order_total) AS avg_order_value,
    COUNT(DISTINCT o.customer_id) AS unique_customers_served,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
FROM Staffs st
LEFT JOIN Orders o ON st.staff_id = o.staff_id
LEFT JOIN Stores s ON st.store_id = s.store_id
WHERE st.active = 1
GROUP BY st.staff_id, st.first_name, st.last_name, st.email, s.store_name, s.city
ORDER BY total_orders_handled DESC;
GO

-- ────────────────────────────────────────────────────────────────────────────
-- C2. Best-Performing Staff Member by Total Sales
-- ────────────────────────────────────────────────────────────────────────────
SELECT TOP 1
    st.staff_id,
    st.first_name + ' ' + st.last_name AS staff_name,
    st.email,
    s.store_name,
    COUNT(DISTINCT o.order_id) AS total_orders_handled,
    SUM(o.order_total) AS total_sales_revenue,
    AVG(o.order_total) AS avg_order_value,
    SUM(oi.quantity) AS total_items_sold,
    COUNT(DISTINCT o.customer_id) AS unique_customers_served,
    -- Performance metrics
    SUM(o.order_total) / NULLIF(COUNT(DISTINCT o.order_id), 0) AS revenue_per_order,
    COUNT(DISTINCT o.order_id) * 1.0 / 
        NULLIF(DATEDIFF(DAY, MIN(o.order_date), MAX(o.order_date)), 0) AS avg_orders_per_day
FROM Staffs st
INNER JOIN Orders o ON st.staff_id = o.staff_id
INNER JOIN OrderItems oi ON o.order_id = oi.order_id
LEFT JOIN Stores s ON st.store_id = s.store_id
WHERE st.active = 1
GROUP BY st.staff_id, st.first_name, st.last_name, st.email, s.store_name
ORDER BY total_sales_revenue DESC;
GO

-- ============================================================================
-- D) CUSTOMER INSIGHTS
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- D1. Customers with No Orders
-- ────────────────────────────────────────────────────────────────────────────
SELECT 
    c.customer_id,
    c.full_name,
    c.email,
    c.phone,
    c.city,
    c.state,
    c.zip_code
FROM Customers c
LEFT JOIN Orders o ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL
ORDER BY c.customer_id;
GO

-- ────────────────────────────────────────────────────────────────────────────
-- D2. Average Spending per Customer
-- ────────────────────────────────────────────────────────────────────────────
-- Overall average
SELECT 
    COUNT(DISTINCT c.customer_id) AS total_customers,
    COUNT(DISTINCT CASE WHEN o.order_id IS NOT NULL THEN c.customer_id END) AS customers_with_orders,
    COUNT(DISTINCT CASE WHEN o.order_id IS NULL THEN c.customer_id END) AS customers_without_orders,
    SUM(o.order_total) AS total_revenue,
    AVG(o.order_total) AS avg_order_value,
    SUM(o.order_total) / NULLIF(COUNT(DISTINCT CASE WHEN o.order_id IS NOT NULL THEN c.customer_id END), 0) AS avg_spending_per_customer
FROM Customers c
LEFT JOIN Orders o ON c.customer_id = o.customer_id;
GO

-- Detailed customer spending breakdown
SELECT 
    c.customer_id,
    c.full_name,
    c.email,
    c.city,
    c.state,
    COUNT(o.order_id) AS total_orders,
    ISNULL(SUM(o.order_total), 0) AS total_spent,
    ISNULL(AVG(o.order_total), 0) AS avg_order_value,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date,
    CASE 
        WHEN COUNT(o.order_id) = 0 THEN 'No Orders'
        WHEN SUM(o.order_total) < 500 THEN 'Low Spender'
        WHEN SUM(o.order_total) BETWEEN 500 AND 2000 THEN 'Medium Spender'
        ELSE 'High Spender'
    END AS customer_segment
FROM Customers c
LEFT JOIN Orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.full_name, c.email, c.city, c.state
ORDER BY total_spent DESC;
GO

-- ============================================================================
-- BONUS: ADDITIONAL USEFUL QUERIES
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- BONUS 1: Sales Performance by Brand
-- ────────────────────────────────────────────────────────────────────────────
SELECT 
    b.brand_id,
    b.brand_name,
    COUNT(DISTINCT p.product_id) AS number_of_products,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.total_price) AS total_revenue,
    AVG(oi.total_price) AS avg_transaction_value,
    COUNT(DISTINCT oi.order_id) AS total_orders
FROM Brands b
INNER JOIN Products p ON b.brand_id = p.brand_id
INNER JOIN OrderItems oi ON p.product_id = oi.product_id
GROUP BY b.brand_id, b.brand_name
ORDER BY total_revenue DESC;
GO

-- ────────────────────────────────────────────────────────────────────────────
-- BONUS 2: Customer Retention Analysis (Repeat vs One-time Customers)
-- ────────────────────────────────────────────────────────────────────────────
SELECT 
    customer_type,
    COUNT(*) AS number_of_customers,
    AVG(total_spent) AS avg_total_spent,
    AVG(total_orders) AS avg_orders_per_customer
FROM (
    SELECT 
        c.customer_id,
        c.full_name,
        COUNT(o.order_id) AS total_orders,
        SUM(o.order_total) AS total_spent,
        CASE 
            WHEN COUNT(o.order_id) = 1 THEN 'One-time Customer'
            WHEN COUNT(o.order_id) BETWEEN 2 AND 3 THEN 'Occasional Customer'
            ELSE 'Repeat Customer'
        END AS customer_type
    FROM Customers c
    LEFT JOIN Orders o ON c.customer_id = o.customer_id
    WHERE o.order_id IS NOT NULL
    GROUP BY c.customer_id, c.full_name
) AS customer_segments
GROUP BY customer_type
ORDER BY number_of_customers DESC;
GO

-- ────────────────────────────────────────────────────────────────────────────
-- BONUS 3: Order Status Distribution
-- ────────────────────────────────────────────────────────────────────────────
SELECT 
    order_status,
    CASE 
        WHEN order_status = 1 THEN 'Pending'
        WHEN order_status = 2 THEN 'Processing'
        WHEN order_status = 3 THEN 'Rejected'
        WHEN order_status = 4 THEN 'Completed'
        ELSE 'Unknown'
    END AS status_name,
    COUNT(*) AS order_count,
    SUM(order_total) AS total_revenue,
    AVG(order_total) AS avg_order_value,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS percentage_of_total
FROM Orders
GROUP BY order_status
ORDER BY order_status;
GO

-- ============================================================================
-- END OF ANALYSIS QUERIES
-- ============================================================================