/* week 3 assignment cei : Analyze sales data using SQL by applying Subqueries, CTEs,and Window Functions to solve business queries.
 Steps: Load Superstore dataset into a table (superstore_raw). Create tables (customers, orders, products) from the dataset. 
 Insert data into these tables using SELECT DISTINCT. Apply subqueries to filter data (above average sales, highest order per customer). 
 Use CTEs to compute aggregations (total sales per customer). Apply window functions (ROW_NUMBER, RANK) for ranking and analysis.
 Combine JOIN + CTE + Window Functions for final result (customer, total sales, rank). 
Solve business queries (top customers, low customers, single-order customers, above-average sales). 
Output: SQL script / Notebook + query results + brief insights.
*/

create database superstore;

use superstore;
# to verify the data
DESCRIBE superstore;

-- Create tables (customers, orders, products) from the dataset. 
-- 1.coustomers tables 
CREATE TABLE customers (

    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code INT,
    region VARCHAR(50)

);

INSERT INTO customers
SELECT
    `Customer ID`,
    MAX(`Customer Name`),
    MAX(Segment),
    MAX(Country),
    MAX(City),
    MAX(State),
    MAX(`Postal Code`),
    MAX(Region)
FROM superstore
GROUP BY `Customer ID`;

SELECT COUNT(*) FROM customers;

-- 2. products table 
CREATE TABLE products (

    product_id VARCHAR(30) PRIMARY KEY,
    product_name TEXT,
    category VARCHAR(50),
    sub_category VARCHAR(50)

);

INSERT INTO products
SELECT
    `Product ID`,
    MAX(`Product Name`),
    MAX(Category),
    MAX(`Sub-Category`)
FROM superstore
GROUP BY `Product ID`;

SELECT COUNT(*) FROM products;

-- 3. orders table

CREATE TABLE orders (

    row_id INT PRIMARY KEY,
    order_id VARCHAR(30),
    order_date TEXT,
    ship_date TEXT,
    ship_mode VARCHAR(50),
    customer_id VARCHAR(30),
    product_id VARCHAR(30),
    sales DOUBLE,
    quantity INT,
    discount DOUBLE,
    profit DOUBLE

);

INSERT INTO orders
SELECT
    `Row ID`,
    `Order ID`,
    `Order Date`,
    `Ship Date`,
    `Ship Mode`,
    `Customer ID`,
    `Product ID`,
    Sales,
    Quantity,
    Discount,
    Profit
FROM superstore;

SELECT COUNT(*) FROM orders;

-- Step 2: Perform Required Queries 
-- Write and execute SQL queries for each of the following: 
-- 1. Find all orders where sales are greater than the average sales. (Subquery)  
 -- this query identifies all orders with sales greater than the average order value 
 SELECT
    order_id,
    customer_id,
    sales
FROM orders
WHERE sales >
(
    SELECT AVG(sales)
    FROM orders
);

-- 2 Find the highest sales order for each customer. (Subquery)  

SELECT
    order_id,
    customer_id,
    sales
FROM orders o
WHERE sales =
(
    SELECT MAX(sales)
    FROM orders
    WHERE customer_id = o.customer_id
);

-- 3. Calculate total sales for each customer. (CTE)  
WITH CustomerSales AS
(
    SELECT
        customer_id,
        SUM(sales) AS total_sales
    FROM orders
    GROUP BY customer_id
)

SELECT *
FROM CustomerSales
ORDER BY total_sales DESC;

-- 4. Find customers whose total sales are above average. (CTE + Subquery)  

WITH CustomerSales AS
(
    SELECT
        customer_id,
        SUM(sales) AS total_sales
    FROM orders
    GROUP BY customer_id
)

SELECT
    customer_id,
    total_sales
FROM CustomerSales
WHERE total_sales >
(
    SELECT AVG(total_sales)
    FROM CustomerSales
)
ORDER BY total_sales DESC;

-- 5. Rank all customers based on total sales. (Window Function)  

WITH CustomerSales AS
(
    SELECT
        customer_id,
        SUM(sales) AS total_sales
    FROM orders
    GROUP BY customer_id
)

SELECT
    customer_id,
    total_sales,
    RANK() OVER (ORDER BY total_sales DESC) AS sales_rank
FROM CustomerSales;

-- 6. Assign row numbers to each order within a customer. (Window Function + PARTITION BY)  

SELECT
    customer_id,
    order_id,
    order_date,
    sales,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    ) AS order_number
FROM orders;

-- 7. Display top 3 customers based on total sales. (Window Function)  
WITH CustomerSales AS
(
    SELECT
        customer_id,
        SUM(sales) AS total_sales
    FROM orders
    GROUP BY customer_id
),
RankedCustomers AS
(
    SELECT
        customer_id,
        total_sales,
        RANK() OVER (ORDER BY total_sales DESC) AS sales_rank
    FROM CustomerSales
)

SELECT *
FROM RankedCustomers
WHERE sales_rank <= 3;

/* Step 3: Final Combined Query 
Write one final query that shows: 
Customer Name  
Total Sales  
Rank  
(Use JOIN + CTE + Window Function together) 
*/ 

WITH CustomerSales AS
(
    SELECT
        customer_id,
        SUM(sales) AS total_sales
    FROM orders
    GROUP BY customer_id
)

SELECT
    c.customer_name,
    cs.total_sales,
    RANK() OVER (ORDER BY cs.total_sales DESC) AS sales_rank
FROM CustomerSales cs
JOIN customers c
ON cs.customer_id = c.customer_id
ORDER BY sales_rank;

-- Mini Project: Customer Sales Insights 
-- 1.  who are the top 5 customers 
WITH CustomerSales AS
(
    SELECT
        customer_id,
        SUM(sales) AS total_sales
    FROM orders
    GROUP BY customer_id
)

SELECT
    c.customer_name,
    cs.total_sales
FROM CustomerSales cs
JOIN customers c
ON cs.customer_id = c.customer_id
ORDER BY cs.total_sales DESC
LIMIT 5;

-- who are the bottom 5 
WITH CustomerSales AS
(
    SELECT
        customer_id,
        SUM(sales) AS total_sales
    FROM orders
    GROUP BY customer_id
)

SELECT
    c.customer_name,
    cs.total_sales
FROM CustomerSales cs
JOIN customers c
ON cs.customer_id = c.customer_id
ORDER BY cs.total_sales ASC
LIMIT 5;

-- Which customers made only one order?  
SELECT
    c.customer_name,
    COUNT(DISTINCT o.order_id) AS total_orders
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY o.customer_id, c.customer_name
HAVING COUNT(DISTINCT o.order_id) = 1;

-- 4. Which Customers Have Above-Average Sales?
WITH CustomerSales AS
(
    SELECT
        customer_id,
        SUM(sales) AS total_sales
    FROM orders
    GROUP BY customer_id
)

SELECT
    c.customer_name,
    cs.total_sales
FROM CustomerSales cs
JOIN customers c
ON cs.customer_id = c.customer_id
WHERE cs.total_sales >
(
    SELECT AVG(total_sales)
    FROM CustomerSales
)
ORDER BY cs.total_sales DESC;

-- 5. What is the Highest Order Value Per Customer?

SELECT
    c.customer_name,
    MAX(o.sales) AS highest_order_value
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY o.customer_id, c.customer_name
ORDER BY highest_order_value DESC;

