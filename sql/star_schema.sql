-- MASTER DATA:
DROP DATABASE IF EXISTS master_db;
DROP DATABASE IF EXISTS data_warehouse;
CREATE DATABASE master_db;
USE master_db;

-- Products Table (Including Store Info)
CREATE TABLE Products (
    Product_ID INT PRIMARY KEY,
    Product_Name VARCHAR(100) NOT NULL,
    Product_Price DECIMAL(10, 2) NOT NULL,
    Supplier_ID INT NOT NULL,
    Supplier_Name VARCHAR(100) NOT NULL,
    Store_ID INT NOT NULL,
    Store_Name VARCHAR(100) NOT NULL
);

-- Customers Table
CREATE TABLE Customers (
    Customer_ID INT PRIMARY KEY,
    Customer_Name VARCHAR(100) NOT NULL,
    Gender VARCHAR(6)
);

-- Loading Products Data
LOAD DATA LOCAL INFILE './data/MasterData_Products.csv'
INTO TABLE Products
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(Product_ID, Product_Name, @Product_Price, Supplier_ID, Supplier_Name, Store_ID, Store_Name)
SET Product_Price = CAST(REPLACE(@Product_Price, '$', '') AS DECIMAL(10, 2));

-- Loading Customers Data
LOAD DATA LOCAL INFILE './data/MasterData_Customers.csv'
INTO TABLE Customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- DATA WAREHOUSE
CREATE DATABASE data_warehouse;
USE data_warehouse;

-- Customers Dimension
CREATE TABLE Dim_Customers (
    Customer_ID INT PRIMARY KEY,
    Customer_Name VARCHAR(100) NOT NULL,
    Gender VARCHAR(6) CHECK (Gender IN ('Male', 'Female'))
);

-- Products Dimension
CREATE TABLE Dim_Products (
    Product_ID INT PRIMARY KEY,
    Product_Name VARCHAR(100) NOT NULL,
    Product_Price DECIMAL(10, 2) NOT NULL,
    Supplier_ID INT NOT NULL,
    Supplier_Name VARCHAR(100) NOT NULL
);

-- Stores Dimension
CREATE TABLE Dim_Stores (
    Store_ID INT PRIMARY KEY,
    Store_Name VARCHAR(100) NOT NULL
);

-- Time Dimension
CREATE TABLE Dim_Time (
    TimePK INT AUTO_INCREMENT PRIMARY KEY,
    Time_ID INT,
    OrderTime TIME,
    Day INT,
    Week INT,
    Month INT,
    Year INT
);	

-- Transactions Fact Table
CREATE TABLE Fact_Transactions (
    Order_ID INT PRIMARY KEY,
    Order_Date DATETIME,
    Product_ID INT,
    Customer_ID INT,
    Store_ID INT,
    Quantity INT,
    Sale DECIMAL(10, 2),
    TimeFK INT,
    FOREIGN KEY (Product_ID) REFERENCES Dim_Products(Product_ID),
    FOREIGN KEY (Customer_ID) REFERENCES Dim_Customers(Customer_ID),
    FOREIGN KEY (Store_ID) REFERENCES Dim_Stores(Store_ID),
    FOREIGN KEY (TimeFK) REFERENCES Dim_Time(TimePK)
);
