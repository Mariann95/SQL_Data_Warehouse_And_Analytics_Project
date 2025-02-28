/*
==============================================================================
Product Report
==============================================================================
Purpose:
	- This report consolidates key product metrics.

Highlights:
	1. Gathers essential fields such as product name, category, subcategory, and cost.
	2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
	3. Aggregates product-level metrics:
		- total orders
		- total sales
		- total quantity sold
		- total customers (unique)
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last sale)
		- average order revenue (AOR)
		- average monthly revenue
==============================================================================
*/

-- =============================================================================
-- Create Report: gold.report_products
-- =============================================================================
IF OBJECT_ID('gold.report_products', 'V') IS NOT NULL
    DROP VIEW gold.report_products;
GO

CREATE VIEW gold.report_products AS
WITH base_query_product AS (
/*
------------------------------------------------------------------------------
1) Base Query: Retrieves core columns from fact_sales and dim_products tables
------------------------------------------------------------------------------
*/
SELECT
	s.order_number,
	s.order_date,
	s.sales_amount,
	s.quantity,
	s.customer_key,
	p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.cost
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
	ON s.product_key = p.product_key
WHERE s.order_date IS NOT NULL -- only consider valid sales dates
)

, product_aggregation AS (
/*
--------------------------------------------------------------------------
2) Product Aggregations: Summarizes key metrics at the product level
--------------------------------------------------------------------------
*/
SELECT
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	COUNT(DISTINCT order_number) AS total_orders,
	SUM(sales_amount) AS total_sales,
	SUM(quantity) AS total_quantity,
	COUNT(DISTINCT customer_key) AS total_unique_customers,
	MAX(order_date) AS last_sale_date,
	DATEDIFF(month, MIN(order_date), MAX(order_date)) AS product_lifespan,
	ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 1) AS avg_selling_price
FROM base_query_product
GROUP BY
	product_key,
	product_name,
	category,
	subcategory,
	cost
)

/*
--------------------------------------------------------------------------
3) Final Query: Combines all product results into one output
--------------------------------------------------------------------------
*/

SELECT
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_sale_date,
	DATEDIFF(month, last_sale_date, GETDATE()) AS recency_in_months,
	CASE
		WHEN total_sales > 50000 THEN 'High-Performer'
		WHEN total_sales >= 10000 THEN 'Mid-Range'
		ELSE 'Low-Performer'
	END product_segment,
	product_lifespan,
	total_orders,
	total_sales,
	total_quantity,
	total_unique_customers,
	avg_selling_price,

	-- Compute average order revenue (AOR)
	CASE
		WHEN total_orders = 0 THEN 0
		ELSE total_sales / total_orders
	END AS avg_order_revenue,

	-- Compute average monthly revenue
	CASE
		WHEN product_lifespan = 0 THEN total_sales
		ELSE total_sales / product_lifespan
	END AS avg_monthly_revenue
FROM product_aggregation;

-- Sample query
SELECT
	product_segment,
	COUNT(product_key) AS total_product,
	SUM(total_sales) AS total_sales
FROM gold.report_products
GROUP BY product_segment
ORDER BY total_sales DESC;