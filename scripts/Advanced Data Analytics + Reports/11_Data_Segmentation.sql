/*
===============================================================================
Data Segmentation Analysis
===============================================================================
Purpose:
    - To group data into meaningful categories for targeted insights.
    - For customer segmentation, product categorization, or regional analysis.

SQL Functions Used:
    - CASE: Defines custom segmentation logic.
    - GROUP BY: Groups data into segments.
===============================================================================
*/

/*Segment products into cost ranges and 
count how many products fall into each segment*/
WITH product_segments AS (
	SELECT
		product_key,
		product_name,
		cost,
		CASE
			WHEN cost < 100 THEN 'Below 100'
			WHEN cost BETWEEN 100 AND 500 THEN '100-500'
			WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
			ELSE 'Above 1000'
		END cost_range
	FROM gold.dim_products
)

SELECT
	cost_range,
	COUNT(product_key) AS number_of_products
FROM product_segments
GROUP BY cost_range
ORDER BY number_of_products DESC;


/*Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than €5,000.
	- Regular: Customers with at least 12 months of history but spending €5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/
WITH customer_segment_spending AS (
	SELECT
		SUM(sales_amount) AS total_sales,
		customer_key,
		CASE
			WHEN DATEDIFF(month, MIN(order_date), MAX(order_date)) >= 12 AND SUM(sales_amount) > 5000 THEN 'VIP'
			WHEN DATEDIFF(month, MIN(order_date), MAX(order_date)) >= 12 AND SUM(sales_amount) <= 5000 THEN 'Regular'
			ELSE 'New'
		END customer_segment
	FROM gold.fact_sales
	GROUP BY customer_key
)

SELECT
	customer_segment,
	COUNT(customer_key) AS number_of_customers
FROM customer_segment_spending
GROUP BY customer_segment
ORDER BY number_of_customers DESC;


-- Segment customers based on their age
WITH customer_segment_age AS (
	SELECT
		customer_key,
		birthdate,
		DATEDIFF(year, birthdate, GETDATE()) AS age,
		CASE
			WHEN DATEDIFF(year, birthdate, GETDATE()) < 18 THEN '-18'
			WHEN DATEDIFF(year, birthdate, GETDATE()) BETWEEN 18 AND 25 THEN '18-25'
			WHEN DATEDIFF(year, birthdate, GETDATE()) BETWEEN  25 AND 35 THEN '25-35'
			WHEN DATEDIFF(year, birthdate, GETDATE()) BETWEEN  35 AND 45 THEN '35-45'
			WHEN DATEDIFF(year, birthdate, GETDATE()) BETWEEN  45 AND 55 THEN '45-55'
			WHEN DATEDIFF(year, birthdate, GETDATE()) BETWEEN  55 AND 65 THEN '55-65'
			WHEN DATEDIFF(year, birthdate, GETDATE()) BETWEEN  65 AND 75 THEN '65-75'
			ELSE '75-'
		END age_segment
	FROM gold.dim_customers
)

SELECT
	COUNT(customer_key) AS number_of_customers,
	age_segment
FROM customer_segment_age
GROUP BY age_segment
ORDER BY age_segment;