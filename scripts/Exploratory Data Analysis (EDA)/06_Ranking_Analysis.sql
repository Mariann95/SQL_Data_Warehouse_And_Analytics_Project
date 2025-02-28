/*
===============================================================================
Ranking Analysis
===============================================================================
Purpose:
    - To rank items (e.g., products, customers) based on performance or other metrics.
    - To identify top performers or laggards.

SQL Functions Used:
    - Window Ranking Functions: RANK(), DENSE_RANK(), ROW_NUMBER(), TOP
    - Clauses: GROUP BY, ORDER BY
===============================================================================
*/

-- Which 5 products Generating the Highest Revenue?
-- Simple Ranking
SELECT TOP 5
    p.product_name,
    SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
    ON p.product_key = s.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC;

-- Complex but Flexible Ranking Using Window Functions
SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(s.sales_amount) AS total_revenue,
        RANK() OVER (ORDER BY SUM(s.sales_amount) DESC) AS rank_products
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p
        ON p.product_key = s.product_key
    GROUP BY p.product_name
) AS ranked_products
WHERE rank_products <= 5;

-- What are the 5 worst-performing products in terms of sales?
SELECT TOP 5
    p.product_name,
    SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
    ON p.product_key = s.product_key
GROUP BY p.product_name
ORDER BY total_revenue;

-- Which 5 subcategory generate the highest revenue?
SELECT *
FROM (
	SELECT
		p.subcategory,
		SUM(s.sales_amount) AS total_revenue,
		ROW_NUMBER() OVER (ORDER BY SUM(s.sales_amount) DESC) AS rank_subcategories
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p
		ON s.product_key = p.product_key
	GROUP BY p.subcategory
	) AS ranked_subcategories
WHERE rank_subcategories <= 5;

-- Find the top 10 customers who have generated the highest revenue
SELECT TOP 10
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
    ON c.customer_key = s.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_revenue DESC;

-- The 3 customers with the fewest orders placed
SELECT TOP 3
    s.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT s.order_number) AS nr_order_placed
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
    ON c.customer_key = s.customer_key
GROUP BY 
    s.customer_key,
    c.first_name,
    c.last_name
ORDER BY nr_order_placed;