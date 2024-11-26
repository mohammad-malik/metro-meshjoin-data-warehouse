# Near-Real-Time Data Warehouse for METRO Shopping Store

## **Project Overview**
This project is a simulation of a near-real-time Data Warehouse (DWH) implementation for the METRO Shopping Store. It uses the MESHJOIN algorithm for real-time ETL processes to integrate streamed transactional data (hence the 'simulated') with master data (MD), enriching and loading it into the DWH. The DWH supports advanced analytical queries to derive business insights.

---

## **Project Components**
1. **Java Implementation (`MeshJoinProcess.java`):**
   - Implements the MESHJOIN algorithm for ETL (Extraction, Transformation, and Loading).
   - Processes transactional data from a CSV file.
   - Enriches data with MD from `master_db`.
   - Loads the transformed data into the `data_warehouse`.

2. **SQL Scripts:**
   - **`star_schema.sql`**: Creates the master database (the source master data) and the star schema for the DWH (fact and dimension tables).
   - **`queries.sql`**: Contains OLAP queries for business analysis, including revenue trends and product affinity.

3. **CSV Files:**
   - Located in the GitHub repository under the `data/` folder.
   - Includes:
     - **`transactions.csv`**: Sample transactional data.
     - **`MasterData_Customers.csv`** and **`MasterData_Products.csv`**: Sample master data used for the project.

---

## **Requirements**
- **Development Tools:**
  - Eclipse IDE for Java development.
  - MySQL Workbench for database management.
- **Prerequisites:**
  - Java JDK 8 or later installed.
  - MySQL Server and MySQL Workbench installed and running.
  - MySQL JDBC driver added to the Java project (e.g., `mysql-connector-java-<version>.jar`) (a version is provided in the repository in `/lib`).

---
## **Setup Instructions**

### **1. Clone Repository**
- Clone this repository to your local machine:
  ```bash
  git clone https://github.com/mohammad-malik/metro-meshjoin-data-warehouse
  cd metro-meshjoin-data-warehouse
  ```
---

### **2. Database Setup in MySQL Workbench**
   **Create Databases:**
   - Open MySQL Workbench.
   - Execute star_schema.sql` in MySQL Workbench. This script creates the master database and the star schema for the DWH.
   - The script creates the following databases:
     - **`master_db`**: Contains master data (e.g., customers, products).
     - **`data_warehouse`**: Contains the star schema for the DWH (fact and dimension tables).
---

### **3. Configure Java Project in Eclipse**
1. **Import the Project:**
   - Open Eclipse IDE.
   - Create a new Java project (e.g., `MeshJoinDW`).
   - Add the `MeshJoinProcess.java` file to the `src` folder.

2. **Add MySQL JDBC Library:**
   - Download the MySQL JDBC driver (`mysql-connector-java-<version>.jar`). A version is provided in the repository in `/lib`.
   - Add it to your project:
     - Right-click on the project > `Build Path` > `Add External JARs` > Select the `.jar` file.

3. **Set Up the Data File Path:**
   - Ensure the path to `data/transactions.csv` is correctly set in the Java code:
     
     ```java
     private static final String TRANSACTIONS_CSV = "./data/transactions.csv";
     ```

4. **Set Up Database Connection:**
   - Update the database connection details in the Java code:
     
     ```java
     // Master Database Connection Details
     private static final String MASTER_DB_URL = "jdbc:mysql://localhost:3306/master_db";
     private static final String MASTER_DB_USER = "root";
     private static final String MASTER_DB_PASSWORD = "password";

     // Data Warehouse Connection Details
     private static final String DWH_URL = "jdbc:mysql://localhost:3306/data_warehouse";
     private static final String DWH_USER = "root";
     private static final String DWH_PASSWORD = "password";
     ```

---

### **4. Running the Project**
1. **Start MySQL Server:** 
   - Ensure the MySQL Server is running on your system.

2. **Run the ETL Process in Eclipse:**
   - Execute the `MeshJoinProcess.java` file in Eclipse.
   - Enter the database credentials as prompted:
     - **Master Database:** URL, username, password.
     - **Warehouse Database:** URL, username, password.

3. **Monitor Progress:**
   - The program will:
     - Extract transactions from `transactions.csv`.
     - Enrich the data using `master_db`.
     - Load the enriched data into the `data_warehouse`.

---

### **5. Perform OLAP Queries**
1. Open `queries.sql` in MySQL Workbench.
2. Execute the queries in the `data_warehouse` database to analyze business performance:
   - Example queries:
     - Top revenue-generating products.
     - Store revenue trends.
     - Product affinity analysis.

---

## **Usage Notes**
1. **Directory Structure:**
   - `src/`: Contains the Java source code.
   - `data/`: Contains sample CSV files.
   - `sql/`: Contains SQL scripts for schema creation and queries.
   - `lib/`: Contains the MySQL JDBC driver.

2. **Error Handling:**
   - Ensure the CSV files are formatted correctly.
   - Verify database credentials if a connection error occurs.

3. **Extensibility:**
   - Add additional master data or transactional records to expand the DWH for broader analyses.

---
## **License**
This project is licensed under the MIT License. Feel free to use and modify it as per your needs.
The MySQL Connector/J is licensed under GPL v2 with FOSS exception.
