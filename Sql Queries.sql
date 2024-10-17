use data_spark;

-- 1. Top 10 Most Popular Products by Quantity Sold:
SELECT p.`Product Name`, SUM(s.`Quantity`) AS TotalQuantitySold
FROM sales s
JOIN products p ON s.`ProductKey` = p.`ProductKey`
GROUP BY p.`Product Name`
ORDER BY TotalQuantitySold DESC
LIMIT 10;

-- 2.Total Revenue by Product Category:
SELECT p.`Category`, SUM(s.`Quantity` * p.`Unit Price USD`) AS TotalRevenue
FROM sales s
JOIN products p ON s.`ProductKey` = p.`ProductKey`
GROUP BY p.`Category`
ORDER BY TotalRevenue DESC;

-- 3.Top 10 Cities by Total Sales:
SELECT c.`City`, SUM(s.`Quantity` * p.`Unit Price USD`) AS TotalSales
FROM sales s
JOIN customers c ON s.`CustomerKey` = c.`CustomerKey`
JOIN products p ON s.`ProductKey` = p.`ProductKey`
GROUP BY c.`City`
ORDER BY TotalSales DESC
LIMIT 10;

-- 4.Customer Distribution by Age:
SELECT 
    FLOOR(DATEDIFF(CURRENT_DATE, c.Birthday) / 365) AS Age,
    COUNT(*) AS CustomerCount
FROM Customers c
GROUP BY Age
ORDER BY Age;

-- 4.Top 5 Products Preferred by Female Customers:
SELECT 
    p.`Product Name`,
    gender,
    SUM(s.Quantity) AS TotalQuantity
FROM Sales s
JOIN Customers c ON s.CustomerKey = c.CustomerKey
JOIN Products p ON s.ProductKey = p.ProductKey
WHERE c.Gender = 'Female'
GROUP BY p.`Product Name`
ORDER BY TotalQuantity DESC
LIMIT 5;

-- 5.Top 5 Products Preferred by Male Customers:
SELECT 
    p.`Product Name`,
    gender,
    SUM(s.Quantity) AS TotalQuantity
FROM Sales s
JOIN Customers c ON s.CustomerKey = c.CustomerKey
JOIN Products p ON s.ProductKey = p.ProductKey
WHERE c.Gender = 'Male'
GROUP BY p.`Product Name`
ORDER BY TotalQuantity DESC
LIMIT 5;

-- 6.Total Number of Customers:
SELECT 
    COUNT(DISTINCT CustomerKey) AS TotalCustomers
FROM Customers;

-- 7.Total Revenue by Country:
SELECT 
    c.Country,
    SUM(s.Quantity * p.`Unit Price USD`) AS TotalRevenue
FROM Sales s
JOIN Customers c ON s.CustomerKey = c.CustomerKey
JOIN Products p ON s.ProductKey = p.ProductKey
GROUP BY c.Country
ORDER BY TotalRevenue DESC;

-- 8.Count of Orders by Month:
SELECT 
    DATE_FORMAT(s.`Order Date`, '%Y-%m') AS Month,
    COUNT(*) AS OrderCount
FROM Sales s
GROUP BY Month
ORDER BY Month; 

-- 9.Average Quantity Sold per Product:
SELECT 
    p.`Product Name`,
    AVG(s.Quantity) AS AverageQuantitySold
FROM Sales s
JOIN Products p ON s.ProductKey = p.ProductKey
GROUP BY p.`Product Name`
ORDER BY AverageQuantitySold DESC;

-- 10.Total Revenue and Average Order Value by Gender:
SELECT 
    c.Gender,
    SUM(s.Quantity * p.`Unit Price USD`) AS TotalRevenue,
    AVG(s.Quantity * p.`Unit Price USD`) AS AverageOrderValue
FROM Sales s
JOIN Customers c ON s.CustomerKey = c.CustomerKey
JOIN Products p ON s.ProductKey = p.ProductKey
GROUP BY c.Gender;