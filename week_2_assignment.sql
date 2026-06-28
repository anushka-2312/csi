create database superstore_db;
USE superstore_db;

-- 1. explore dataset
-- Exploring the all recoeds from the table--
SELECT * FROM superstore;

-- the first ten rows --
SELECT *
FROM superstore
LIMIT 10;

-- for the table strature -- 
DESCRIBE superstore;

-- counting the total number of recoerds -- 
SELECT COUNT(*) AS Total_Records
FROM superstore;

-- for the total number of columns in the table -- 
SELECT COUNT(*) AS Total_Columns
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'superstore'
AND TABLE_SCHEMA = 'superstore_db';

-- 2.Explore table (schema, sample data). --
-- Distinct Regions
SELECT DISTINCT Region
FROM superstore;

-- Distinct Categories
SELECT DISTINCT Category
FROM superstore;

-- Distinct Segments
SELECT DISTINCT Segment
FROM superstore;

-- Find date range--
SELECT
MIN(`Order Date`) AS First_Order,
MAX(`Order Date`) AS Last_Order
FROM superstore;

-- 3.Apply WHERE filters (region, category, date, sales). --

SELECT *
FROM superstore
WHERE Region = 'West';

SELECT *
FROM superstore
WHERE Category = 'Technology';

SELECT *
FROM superstore
WHERE Sales > 500;

SELECT *
FROM superstore
WHERE Profit > 300;

SELECT *
FROM superstore
WHERE Category = 'Furniture'
AND Discount > 0;

SELECT *
FROM superstore
WHERE State = 'Texas';

SELECT *
FROM superstore
WHERE `Order Date`
BETWEEN '2016-01-01'
AND '2017-06-01';

SELECT *
FROM superstore
WHERE Region = 'West'
AND Sales > 1500
AND Profit > 250;

SELECT *
FROM superstore
ORDER BY Sales DESC;

-- Highest Profit
SELECT *
FROM superstore
ORDER BY Profit DESC;

-- Lowest Profit
SELECT *
FROM superstore
ORDER BY Profit ASC;

SELECT *
FROM superstore
ORDER BY Sales DESC
LIMIT 10;

-- 4.Use GROUP BY for aggregations (sales, quantity, averages) --
-- Total Sales by Category
SELECT
    Category,
    ROUND(SUM(Sales),2) AS Total_Sales
FROM superstore
GROUP BY Category
ORDER BY Total_Sales DESC;

-- Total Profit by Category
SELECT
    Category,
    ROUND(SUM(Profit),2) AS Total_Profit
FROM superstore
GROUP BY Category
ORDER BY Total_Profit DESC;

-- Total Sales by Region
SELECT
    Region,
    ROUND(SUM(Sales),2) AS Total_Sales
FROM superstore
GROUP BY Region
ORDER BY Total_Sales DESC;

-- average sales by Category
SELECT
    Category,
    ROUND(AVG(Sales),2) AS Average_Sales
FROM superstore
GROUP BY Category;

-- average profit by region
SELECT
    Region,
    ROUND(AVG(Profit),2) AS Average_Profit
FROM superstore
GROUP BY Region;

-- total quantity sold by Category
SELECT
    Category,
    SUM(Quantity) AS Total_Quantity
FROM superstore
GROUP BY Category
ORDER BY Total_Quantity DESC;

-- average discount by Category
SELECT
    Category,
    ROUND(AVG(Discount),2) AS Average_Discount
FROM superstore
GROUP BY Category;

-- Categories with Sales Greater Than ₹6,50,000
SELECT
    Category,
    ROUND(SUM(Sales),2) AS Total_Sales
FROM superstore
GROUP BY Category
HAVING SUM(Sales) > 650000;

-- Regions with More Than 1100 Orders
SELECT
    Region,
    COUNT(*) AS Orders_Count
FROM superstore
GROUP BY Region
HAVING COUNT(*) > 1100;

-- Top 10 Customers
SELECT
    `Customer Name`,
    ROUND(SUM(Sales),2) AS Total_Sales
FROM superstore
GROUP BY `Customer Name`
ORDER BY Total_Sales DESC
LIMIT 10;

-- Top 10 Products
SELECT
    `Product Name`,
    ROUND(SUM(Sales),2) AS Total_Sales
FROM superstore
GROUP BY `Product Name`
ORDER BY Total_Sales DESC
LIMIT 10;

-- Top 10 States
SELECT
    State,
    ROUND(SUM(Sales),2) AS Total_Sales
FROM superstore
GROUP BY State
ORDER BY Total_Sales DESC
LIMIT 10;


-- 5.Sort and limit results (top products, top categories). 
-- Top 10 Products by Total Sales
SELECT
    `Product Name`,
    ROUND(SUM(Sales), 2) AS Total_Sales
FROM superstore
GROUP BY `Product Name`
ORDER BY Total_Sales DESC
LIMIT 10;

-- Top 5 Categories by Total Sales
SELECT
    Category,
    ROUND(SUM(Sales), 2) AS Total_Sales
FROM superstore
GROUP BY Category
ORDER BY Total_Sales DESC
LIMIT 5;

-- Top 10 Customers by Sales
SELECT
    `Customer Name`,
    ROUND(SUM(Sales), 2) AS Total_Sales
FROM superstore
GROUP BY `Customer Name`
ORDER BY Total_Sales DESC
LIMIT 10;

--  6.Solve use cases (monthly trends, top customers, duplicates). --
-- Monthly Sales Trend
SELECT
    YEAR(`Order Date`) AS Order_Year,
    MONTH(`Order Date`) AS Order_Month,
    ROUND(SUM(Sales), 2) AS Monthly_Sales
FROM superstore
GROUP BY YEAR(`Order Date`), MONTH(`Order Date`)
ORDER BY Order_Year, Order_Month;

-- Top 10 Customers
SELECT
    `Customer Name`,
    COUNT(DISTINCT `Order ID`) AS Total_Orders,
    ROUND(SUM(Sales),2) AS Total_Sales,
    ROUND(SUM(Profit),2) AS Total_Profit
FROM superstore
GROUP BY `Customer Name`
ORDER BY Total_Sales DESC
LIMIT 10;

-- Check Duplicate Order IDs
SELECT
    `Order ID`,
    COUNT(*) AS Number_of_Records
FROM superstore
GROUP BY `Order ID`
HAVING COUNT(*) > 1;

-- 7.Validate results (row counts, data quality). --
-- Total Records
SELECT COUNT(*) AS Total_Records
FROM superstore;

-- Missing Customer Names
SELECT COUNT(*) AS Missing_Customer_Name
FROM superstore
WHERE `Customer Name` IS NULL;

-- Missing Product Names
SELECT COUNT(*) AS Missing_Product_Name
FROM superstore
WHERE `Product Name` IS NULL;

-- Orders with Negative Profit
SELECT *
FROM superstore
WHERE Profit < 0;

-- Invalid Sales Values
SELECT *
FROM superstore
WHERE Sales <= 0;

