/*
===============================================================================
Part-to-Whole Analysis
===============================================================================
Purpose:
    - To compare performance or metrics across dimensions or time periods.
    - To evaluate differences between categories.
    - Useful for A/B testing or regional comparisons.

SQL Functions Used:
    - SUM(), AVG(): Aggregates values for comparison.
    - Window Functions: SUM() OVER() for total calculations.
===============================================================================
*/

-- Which categories contribute the most to overall sales?
WITH category_sales AS (
    SELECT
        p.category,
        SUM(s.sales_amount) AS total_sales_by_category
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p
        ON p.product_key = s.product_key
    GROUP BY p.category
)

SELECT
    category,
    total_sales_by_category,
    SUM(total_sales_by_category) OVER () AS overall_sales,
    CONCAT(ROUND((CAST(total_sales_by_category AS FLOAT) / SUM(total_sales_by_category) OVER ()) * 100, 2), '%') AS percentage_of_total
FROM category_sales
ORDER BY total_sales_by_category DESC;