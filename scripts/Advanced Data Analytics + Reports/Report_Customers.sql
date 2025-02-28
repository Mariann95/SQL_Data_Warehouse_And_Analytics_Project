/*
==============================================================================
Customer Report
==============================================================================
Purpose:
	- This report consolidates key customer metrics and behaviors.

Highlights:
	1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
	3. Aggregates customer-level metrics:
		- total orders
		- total sales
		- total quantity purchased
		- total products
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last order)
		- average order value
		- average monthly spend
==============================================================================
*/

-- =============================================================================
-- Create Report: gold.report_customers
-- =============================================================================
IF OBJECT_ID('gold.report_customers', 'V') IS NOT NULL
    DROP VIEW gold.report_customers;
GO

CREATE VIEW gold.report_customers AS
WITH base_query_customers AS (
/*
-------------------------------------------------------------------------------
1) Base Query: Retrieves core columns from fact_sales and dim_customers tables
-------------------------------------------------------------------------------
*/
SELECT
	s.order_number,
	s.product_key,
	s.order_date,
	s.sales_amount,
	s.quantity,
	c.customer_key,
	c.customer_number,
	CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
	DATEDIFF(year, c.birthdate, GETDATE()) AS customer_age
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
	ON s.customer_key = c.customer_key
WHERE s.order_date IS NOT NULL -- only consider valid sales dates
)

, customer_aggregation AS (
/*
--------------------------------------------------------------------------
2) Customer Aggregations: Summarizes key metrics at the customer level
--------------------------------------------------------------------------
*/
SELECT
	customer_key,
	customer_number,
	customer_name,
	customer_age,
	COUNT(DISTINCT order_number) AS total_orders,
	SUM(sales_amount) AS total_sales,
	SUM(quantity) AS total_quantity,
	COUNT(DISTINCT product_key) AS total_products,
	MAX(order_date) AS last_order_date,
	DATEDIFF(month, MIN(order_date), MAX(order_date)) AS customer_lifespan
FROM base_query_customers
GROUP BY
	customer_key,
	customer_number,
	customer_name,
	customer_age
)

/*
--------------------------------------------------------------------------
3) Final Query: Combines all customer results into one output
--------------------------------------------------------------------------
*/

SELECT
	customer_key,
	customer_number,
	customer_name,
	customer_age,
	CASE
		WHEN customer_age < 18 THEN '-18'
		WHEN customer_age BETWEEN 18 AND 25 THEN '18-25'
		WHEN customer_age BETWEEN  25 AND 35 THEN '25-35'
		WHEN customer_age BETWEEN  35 AND 45 THEN '35-45'
		WHEN customer_age BETWEEN  45 AND 55 THEN '45-55'
		WHEN customer_age BETWEEN  55 AND 65 THEN '55-65'
		WHEN customer_age BETWEEN  65 AND 75 THEN '65-75'
		ELSE '75-'
	END customer_age_group,
	CASE
		WHEN customer_lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
		WHEN customer_lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
		ELSE 'New'
	END customer_segment,
	last_order_date,
	DATEDIFF(month, last_order_date, GETDATE()) AS recency_in_months,
	total_orders,
	total_sales,
	total_quantity,
	total_products,
	customer_lifespan,

	-- Compute average order value (AV0)
	CASE
		WHEN total_sales = 0 THEN 0
		ELSE total_sales / total_orders
	END AS avg_order_value,

	-- Compute average monthly spent
	CASE
		WHEN customer_lifespan = 0 THEN total_sales
		ELSE total_sales / customer_lifespan
	END AS avg_monthly_spent
FROM customer_aggregation;

-- Sample query
SELECT
	customer_age_group,
	COUNT(customer_number) AS total_customers,
	SUM(total_sales) AS total_sales
FROM gold.report_customers
GROUP BY customer_age_group
ORDER BY total_customers DESC;