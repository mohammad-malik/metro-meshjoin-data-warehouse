USE data_warehouse;

-- Query 1: Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
SELECT
    MONTH(Order_Date) AS Month,
    WEEKDAY(Order_Date) IN (1, 2, 3, 4, 5) AS Is_Weekday,
    DP.Product_Name,
    SUM(FT.Sale) AS Total_Revenue
FROM Fact_Transactions as FT

JOIN Dim_Products AS DP 
ON FT.Product_ID = DP.Product_ID

JOIN Dim_Time AS DT
ON FT.TimeFK = DT.TimePK

WHERE YEAR(Order_Date) = 2019 -- specify the year for analysis here (even though theres only 2019)

GROUP BY 
    MONTH(Order_Date),
    Is_Weekday,
    DP.Product_Name

ORDER BY 
    Month, Is_Weekday,
    Total_Revenue
DESC
LIMIT 5;

-- Query 2: Trend Analysis of Store Revenue Growth Rate Quarterly for 2019* not 2017
-- NOTE: NULLs in output because growth rate needs prior-quarter data, which doesnt exist for a store's first quarter.
WITH QuarterlyRevenue AS (
    SELECT
        Store_ID,
        QUARTER(Order_Date) AS Quarter,
        SUM(Sale) AS Total_Revenue
    FROM Fact_Transactions
    WHERE YEAR(Order_Date) = 2019
    
    GROUP BY
        Store_ID,
        QUARTER(Order_Date)
)
SELECT
    Q1.Store_ID,
    Q1.Quarter,
    (Q1.Total_Revenue - Q2.Total_Revenue) / Q2.Total_Revenue * 100 AS Growth_Rate
FROM QuarterlyRevenue AS Q1

LEFT JOIN QuarterlyRevenue AS Q2
ON
    Q1.Store_ID = Q2.Store_ID AND 
    Q1.Quarter = Q2.Quarter + 1

ORDER BY
    Q1.Store_ID,
    Q1.Quarter;

-- Query 3: Detailed Supplier Sales Contribution by Store and Product Name
SELECT
    DS.Store_Name,
    DP.Supplier_Name,
    DP.Product_Name,
    SUM(FT.Sale) AS Total_Sales
FROM Fact_Transactions AS FT

JOIN Dim_Products AS DP 
ON FT.Product_ID = DP.Product_ID

JOIN Dim_Stores AS DS 
ON FT.Store_ID = DS.Store_ID

GROUP BY
    DS.Store_Name,
    DP.Supplier_Name,
    DP.Product_Name
ORDER BY
    DS.Store_Name,
    DP.Supplier_Name,
    DP.Product_Name;

-- Query 4: Seasonal Analysis of Product Sales Using Dynamic Drill-Down
WITH SeasonalData AS (
    SELECT Product_ID,
        CASE
            WHEN MONTH(Order_Date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(Order_Date) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(Order_Date) IN (9, 10, 11) THEN 'Fall'
            ELSE 'Winter'
        END AS Season,
        SUM(Sale) AS Total_Sales
    FROM Fact_Transactions
    
    GROUP BY
        Product_ID,
        Season
)
SELECT
    DP.Product_Name,
    SD.Season,
    SD.Total_Sales
FROM SeasonalData SD

JOIN Dim_Products AS DP 
ON SD.Product_ID = DP.Product_ID

ORDER BY
    DP.Product_Name,
    SD.Season;

-- Query 5: Store-Wise and Supplier-Wise Monthly Revenue Volatility
WITH MonthlyRevenue AS (
    SELECT
        DS.Store_Name,
        DP.Supplier_Name,
        MONTH(Order_Date) AS Month,
        YEAR(Order_Date) AS Year,
        SUM(Sale) AS Total_Revenue
    FROM Fact_Transactions AS FT
    
    JOIN Dim_Products AS DP
    ON FT.Product_ID = DP.Product_ID
    
    JOIN Dim_Stores AS DS
    ON FT.Store_ID = DS.Store_ID

    GROUP BY
        DS.Store_Name,
        DP.Supplier_Name,
        YEAR(Order_Date),
        MONTH(Order_Date)
)
SELECT
    M1.Store_Name,
    M1.Supplier_Name,
    M1.Month,
    ((M1.Total_Revenue - M2.Total_Revenue) / M2.Total_Revenue) * 100 AS Revenue_Volatility
FROM MonthlyRevenue AS M1

LEFT JOIN MonthlyRevenue AS M2
ON
    M1.Store_Name = M2.Store_Name
    AND M1.Supplier_Name = M2.Supplier_Name
    AND M1.Year = M2.Year
    AND M1.Month = M2.Month + 1

ORDER BY
    M1.Store_Name,
    M1.Supplier_Name,
    M1.Month;

-- Query 6: Top 5 Products Purchased Together Across Multiple Orders
SELECT
    P1.Product_Name AS Product_A,
    P2.Product_Name AS Product_B,
    COUNT(*) AS Frequency
FROM Fact_Transactions AS FT1

JOIN Fact_Transactions AS FT2
ON FT1.Order_ID = FT2.Order_ID
AND FT1.Product_ID < FT2.Product_ID

JOIN Dim_Products AS P1
ON FT1.Product_ID = P1.Product_ID

JOIN Dim_Products AS P2
ON FT2.Product_ID = P2.Product_ID

GROUP BY
    P1.Product_Name,
    P2.Product_Name
ORDER BY
    Frequency DESC
LIMIT 5;

-- Query 7: Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
-- We use the ROLLUP operator to provide hierarchical revenue aggregation from the product level to the store level.
-- NULLs in output as ROLLUP results represent subtotals or grand totals at higher levels of aggregation,
-- where specific details (e.g., product or supplier) are not applicable.
SELECT
    DS.Store_Name,
    DP.Supplier_Name,
    DP.Product_Name,
    YEAR(FT.Order_Date) AS Year,
    SUM(FT.Sale) AS Total_Revenue
FROM Fact_Transactions AS FT

JOIN Dim_Products AS DP
ON FT.Product_ID = DP.Product_ID

JOIN Dim_Stores AS DS
ON FT.Store_ID = DS.Store_ID

GROUP BY
    ROLLUP (DS.Store_Name,
    DP.Supplier_Name,
    DP.Product_Name,
    YEAR(FT.Order_Date))

ORDER BY
    DS.Store_Name,
    DP.Supplier_Name,
    DP.Product_Name,
    Year; -- (extracted earlier from FT.Order_Date)

-- Query 8: Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
WITH ProductSales AS (
    SELECT
        DP.Product_Name,
        CASE WHEN MONTH(FT.Order_Date) BETWEEN 1 AND 6 THEN 'H1' ELSE 'H2' END AS Half_Year,
        SUM(FT.Sale) AS Total_Revenue,
        SUM(FT.Quantity) AS Total_Quantity
    FROM Fact_Transactions AS FT
    
    JOIN Dim_Products AS DP
    ON FT.Product_ID = DP.Product_ID

    GROUP BY
        DP.Product_Name,
        Half_Year
)
SELECT
    Product_Name,
    Half_Year,
    Total_Revenue,
    Total_Quantity
FROM ProductSales

ORDER BY
    Product_Name,
    Half_Year;

-- Query 9: Identify High Revenue Spikes in Product Sales and Highlight Outliers
-- This query flags days where product sales exceed twice the daily average.
WITH DailyProductSales AS (
    SELECT
        DP.Product_Name,
        DATE(FT.Order_Date) AS Sale_Date,
        SUM(FT.Sale) AS Daily_Sales
    FROM Fact_Transactions AS FT
    
    JOIN Dim_Products AS DP
    ON FT.Product_ID = DP.Product_ID
    
    GROUP BY
        DP.Product_Name,
        DATE(FT.Order_Date)
),
ProductDailyAverage AS (
    SELECT
        Product_Name,
        AVG(Daily_Sales) AS Average_Sales
    FROM DailyProductSales

    GROUP BY Product_Name
)
SELECT
    DPS.Product_Name,
    DPS.Sale_Date,
    DPS.Daily_Sales,
    PDA.Average_Sales,
    'Outlier' AS Flag
FROM DailyProductSales AS DPS

JOIN ProductDailyAverage AS PDA
ON DPS.Product_Name = PDA.Product_Name

WHERE DPS.Daily_Sales > PDA.Average_Sales * 2

ORDER BY
    DPS.Product_Name,
    DPS.Sale_Date;

-- Query 10: Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
-- This view aggregates total quarterly sales by store, enabling efficient retrieval of quarterly trends.

CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT
    DS.Store_Name,
    QUARTER(FT.Order_Date) AS Quarter,
    YEAR(FT.Order_Date) AS Year,
    SUM(FT.Sale) AS Total_Sales
FROM Fact_Transactions AS FT

JOIN Dim_Stores AS DS
ON FT.Store_ID = DS.Store_ID

GROUP BY
    DS.Store_Name,
    QUARTER(FT.Order_Date),
    YEAR(FT.Order_Date)

ORDER BY
    DS.Store_Name,
    Year,
    Quarter;

-- Query to Retrieve Data from the STORE_QUARTERLY_SALES View
SELECT * FROM STORE_QUARTERLY_SALES;