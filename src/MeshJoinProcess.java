import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.HashSet;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class MeshJoinProcess {
  // Constants for configuration and database details
  private static final String TRANSACTIONS_CSV = "transactions.csv";
  private static final int QUEUE_SIZE = 10000;
  private static final int BATCH_SIZE = 1000;

  // Database default connection details as instance variables
  private String masterDBURL = "jdbc:mysql://localhost:3306/master_db";
  private String masterDBUser = "root";
  private String masterDBPassword = "password";

  private String warehouseDBURL = "jdbc:mysql://localhost:3306/data_warehouse";
  private String warehouseDBUser = "root";
  private String warehouseDBPassword = "password";

  // Queue to hold processed records
  private final BlockingQueue<JoinedRecord> RECORD_QUEUE = new ArrayBlockingQueue<>(QUEUE_SIZE);

  // !! SIGNAL: Used for synchronization between reader and writer threads
  private volatile boolean isReadingComplete = false;

  private Scanner scanner;

  
  public static void main(String[] args) {
    MeshJoinProcess process = new MeshJoinProcess();
    process.getDatabaseCredentials();
    process.runETL();
  }

  // Add new method to get credentials
  private void getDatabaseCredentials() {
    scanner = new Scanner(System.in);

    System.out.println("\n=== Master Database Configuration ===");
    System.out.printf("Enter Master DB URL [%s]: ", masterDBURL);
    String input = scanner.nextLine().trim(); // trim() removes leading/trailing whitespaces, its like cin.ignore()
    if (!input.isEmpty())
      masterDBURL = input;

    System.out.printf("Enter Master DB Username [%s]: ", masterDBUser);
    input = scanner.nextLine().trim();
    if (!input.isEmpty())
      masterDBUser = input;

    System.out.printf("Enter Master DB Password [%s]: ", masterDBPassword);
    input = scanner.nextLine().trim();
    if (!input.isEmpty())
      masterDBPassword = input;

    System.out.println("\n=== Warehouse Database Configuration ===");
    System.out.printf("Enter Warehouse DB URL [%s]: ", warehouseDBURL);
    input = scanner.nextLine().trim();
    if (!input.isEmpty())
      warehouseDBURL = input;

    System.out.printf("Enter Warehouse DB Username [%s]: ", warehouseDBUser);
    input = scanner.nextLine().trim();
    if (!input.isEmpty())
      warehouseDBUser = input;

    System.out.printf("Enter Warehouse DB Password [%s]: ", warehouseDBPassword);
    input = scanner.nextLine().trim();
    if (!input.isEmpty())
      warehouseDBPassword = input;

    System.out.println("\nConfiguration completed.");
  }

  // Initializes and starts the reader and writer threads.
  public void runETL() {
    Thread databaseWriterThread = new Thread(new DBWriter(), "WriterThread");
    databaseWriterThread.start();

    Thread databaseReaderThread = new Thread(new CSVReader(), "ReaderThread");
    databaseReaderThread.start();

    try {
      databaseReaderThread.join();
      databaseWriterThread.join();
      System.out.println("ETL Process finished successfully.");
    }
    catch (InterruptedException e) {
      System.err.println("ETL Process was interrupted: " + e.getMessage());
      Thread.currentThread().interrupt();
    }
  }

  // Represents a combined record from transactions and enriched data.
  private static class JoinedRecord {
    // Transaction Fields
    int orderID;
    String orderDate; // Format: "yyyy-MM-dd HH:mm:ss"
    int productID;
    int quantityOrdered;
    int customerID;
    int timeID;
    double salesAmount;

    JoinedRecord(
        int orderID,
        String orderDate,
        int productID,
        int quantityOrdered,
        int customerID,
        int timeID,
        double salesAmount) {
      this.orderID = orderID;
      this.orderDate = orderDate;
      this.productID = productID;
      this.quantityOrdered = quantityOrdered;
      this.customerID = customerID;
      this.timeID = timeID;
      this.salesAmount = salesAmount;
    }
  }

  // Reads the CSV file, enriches the data, and adds records to the queue.
  private class CSVReader implements Runnable {

    @Override
    public void run() {
      System.out.println("CSVReader started.");
      try (BufferedReader br = new BufferedReader(new FileReader(TRANSACTIONS_CSV));
          Connection masterDBConnection = DriverManager.getConnection(masterDBURL, masterDBUser, masterDBPassword)) {

        String line = br.readLine();

        if (line == null) {
          System.err.println("The transactions file is empty.");
          return;
        }

        while ((line = br.readLine()) != null) {
          String[] tokens = line.split(",");

          if (tokens.length < 6) {
            System.err.println("Invalid record: " + line);
            continue;
          }

          try {
            int orderID = Integer.parseInt(tokens[0].trim());
            String orderDate = tokens[1].trim();
            int productID = Integer.parseInt(tokens[2].trim());
            int quantityOrdered = Integer.parseInt(tokens[3].trim());
            int customerID = Integer.parseInt(tokens[4].trim());
            int timeID = Integer.parseInt(tokens[5].trim());

            if (!isValidProduct(masterDBConnection, productID)) {
              System.out.println("Product ID " + productID + " is invalid for Order ID " + orderID + ". Skipping.");
              continue;
            }

            double sale = calculateSalesAmount(masterDBConnection, productID, quantityOrdered);

            JoinedRecord record = new JoinedRecord(orderID, orderDate, productID, quantityOrdered, customerID, timeID,
                sale);
            RECORD_QUEUE.put(record);

          }
          catch (NumberFormatException e) {
            System.err.println("Number format error in record: " + line + " - " + e.getMessage());
          }
          catch (SQLException e) {
            System.err.println("SQL error processing line: " + line + " - " + e.getMessage());
          }
        }

        // !! Signal to the writer thread that the reading is complete
        isReadingComplete = true;

        // Add poison pill to signal end of records
        RECORD_QUEUE.put(new JoinedRecord(-1, null, -1, -1, -1, -1, -1.0));
        System.out.println("CSVReader finished reading all records.");

      }
      catch (IOException e) {
        System.err.println("Error reading the transactions file: " + e.getMessage());
        e.printStackTrace();
      }
      catch (SQLException e) {
        System.err.println("Database error in CSVReader: " + e.getMessage());
        e.printStackTrace();
      }
      catch (InterruptedException e) {
        System.err.println("CSVReader was interrupted: " + e.getMessage());
        Thread.currentThread().interrupt();
      }
    }

    // Checks if the product ID exists in the Products table.
    private boolean isValidProduct(Connection conn, int productID) throws SQLException {
      String query = "SELECT COUNT(*) FROM Products WHERE Product_ID = ?";

      try (PreparedStatement stmt = conn.prepareStatement(query)) {
        stmt.setInt(1, productID);

        try (ResultSet rs = stmt.executeQuery()) {
          rs.next();
          return rs.getInt(1) > 0;
        }
      }
      catch (SQLSyntaxErrorException e) {
        throw new SQLException("Database query syntax error: " + e.getMessage());
      }
      catch (SQLTimeoutException e) {
        throw new SQLException("Database query timed out: " + e.getMessage());
      }
      catch (SQLException e) {
        throw new SQLException("Error validating product ID " + productID + ": " + e.getMessage());
      }
    }

    // Calculates the sale amount based on product price and quantityOrdered.
    private double calculateSalesAmount(Connection conn, int productID, int quantity) throws SQLException {
      String query = "SELECT Product_Price FROM Products WHERE Product_ID = ?";

      try (PreparedStatement stmt = conn.prepareStatement(query)) {
        stmt.setInt(1, productID);

        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            double price = rs.getDouble("Product_Price");
            return price * quantity;
          } else {
            throw new SQLException("Product ID " + productID + " not found.");
          }
        }
      }
      catch (SQLSyntaxErrorException e) {
        throw new SQLException("Database query syntax error: " + e.getMessage());
      }
      catch (SQLTimeoutException e) {
        throw new SQLException("Database query timed out: " + e.getMessage());
      }
      catch (SQLException e) {
        throw new SQLException("Error calculating sales amount: " + e.getMessage());
      }
    }
  }
    // Writes records from the queue to the database in batches.
    private class DBWriter implements Runnable {
      // !! Caches to keep track of inserted Customer_IDs and Product to Store mapping
      // for performance
      private Set<Integer> customerCache = new HashSet<>();
      private Map<Integer, Integer> productToStoreMap = new HashMap<>();
      private Map<Integer, Integer> timeIDToTimePKMap = new HashMap<>();

      // PreparedStatements for Warehouse Database
      private PreparedStatement insertIntoCustomerDimStmt;
      private PreparedStatement selectDimCustomersStmt;
      private PreparedStatement insertIntoTransactionFactTableStmt;
      private PreparedStatement insertIntoProductDimStmt;
      private PreparedStatement insertIntoTimeDimStmt;
      private PreparedStatement insertIntoStoreDimStmt;

      private Connection dataWarehouseDBConnection;

      private int batchCount = 0;
      private int totalBatches = 0;
      private int processedRecords = 0;
      private int currentBatchNumber = 0;

      @Override
      public void run() {
        System.out.println("DBWriter started.");

        try {
          dataWarehouseDBConnection = DriverManager.getConnection(warehouseDBURL, warehouseDBUser, warehouseDBPassword);

          dataWarehouseDBConnection.setAutoCommit(false);
          initializeQueryStatements();

          while (true) {
            JoinedRecord record = RECORD_QUEUE.take();

            // Check for Poison Pill
            if (record.orderID == -1 && record.orderDate == null) {
              System.out.println("\nDBWriter received poison pill. Processing final batch...");
              totalBatches = ((processedRecords - 1) / BATCH_SIZE) + 1;

              break;
            }

            processedRecords++;
            processRecord(record);
            batchCount++;

            if (batchCount >= BATCH_SIZE) {
              executeBatch();
              batchCount = 0;
            }
          }

          // Handle remaining records
          if (batchCount > 0) {
            executeBatch();
          }

          dataWarehouseDBConnection.commit();
          System.out.println("\nDBWriter finished writing all batches.");
        }
        catch (InterruptedException e) {
          System.err.println("\nDBWriter was interrupted: " + e.getMessage());
          Thread.currentThread().interrupt();
        }
        catch (SQLException e) {
          System.err.println("\nSQL Exception in DBWriter: " + e.getMessage());
          e.printStackTrace();
          rollback();
        } finally {
          closeResources();
        }
      }

      // Prepares all SQL statements needed for database operations.
      private void initializeQueryStatements() throws SQLException {
        String insertCustomerSQL = "INSERT INTO Dim_Customers (Customer_ID, Customer_Name, Gender) VALUES (?, ?, ?) "
            + "ON DUPLICATE KEY UPDATE Customer_Name = VALUES(Customer_Name), Gender = VALUES(Gender)";
        insertIntoCustomerDimStmt = dataWarehouseDBConnection.prepareStatement(insertCustomerSQL);

        String selectDimCustomersSQL = "SELECT COUNT(*) FROM Dim_Customers WHERE Customer_ID = ?";
        selectDimCustomersStmt = dataWarehouseDBConnection.prepareStatement(selectDimCustomersSQL);

        String insertTransactionSQL = "INSERT INTO Fact_Transactions "
            + "(Order_ID, Order_Date, Product_ID, Customer_ID, Store_ID, Quantity, Sale, TimeFK) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        insertIntoTransactionFactTableStmt = dataWarehouseDBConnection.prepareStatement(insertTransactionSQL);

        String insertProductSQL = "INSERT INTO Dim_Products "
            + "(Product_ID, Product_Name, Product_Price, Supplier_ID, Supplier_Name) "
            + "VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE "
            + "Product_Name = VALUES(Product_Name), Product_Price = VALUES(Product_Price), Supplier_Name = VALUES(Supplier_Name)";
        insertIntoProductDimStmt = dataWarehouseDBConnection.prepareStatement(insertProductSQL);

        String insertTimeSQL = "INSERT INTO Dim_Time (Time_ID, OrderTime, Day, Week, Month, Year) "
            + "VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE "
            + "OrderTime = VALUES(OrderTime), Day = VALUES(Day), Week = VALUES(Week), Month = VALUES(Month), Year = VALUES(Year)";
        insertIntoTimeDimStmt = dataWarehouseDBConnection.prepareStatement(insertTimeSQL);

        String insertStoreSQL = "INSERT INTO Dim_Stores (Store_ID, Store_Name) "
            + "VALUES (?, ?) ON DUPLICATE KEY UPDATE "
            + "Store_Name = VALUES(Store_Name)";
        insertIntoStoreDimStmt = dataWarehouseDBConnection.prepareStatement(insertStoreSQL);
      }

      // Processes a single record by ensuring all dimensions are updated and
      // preparing fact insertion.
      private void processRecord(JoinedRecord record) throws SQLException {
        try {
          int storeID = ensureProductExists(record.productID);
          ensureStoreExists(storeID);
          ensureCustomerExists(record.customerID);
          ensureTimeExists(record.timeID, record.orderDate);

          // Get TimePK from the map
          int timePK = timeIDToTimePKMap.get(record.timeID);

          // Prepare Fact_Transactions insertion with TimeFK
          insertIntoTransactionFactTableStmt.setInt(1, record.orderID);
          insertIntoTransactionFactTableStmt.setString(2, record.orderDate);
          insertIntoTransactionFactTableStmt.setInt(3, record.productID);
          insertIntoTransactionFactTableStmt.setInt(4, record.customerID);
          insertIntoTransactionFactTableStmt.setInt(5, storeID);
          insertIntoTransactionFactTableStmt.setInt(6, record.quantityOrdered);
          insertIntoTransactionFactTableStmt.setDouble(7, record.salesAmount);
          insertIntoTransactionFactTableStmt.setInt(8, timePK); // will be inserted as TimeFK.
          insertIntoTransactionFactTableStmt.addBatch();
        }
        catch (SQLIntegrityConstraintViolationException e) {
          throw new SQLException("Data integrity violation while processing record: " + e.getMessage());
        }
        catch (SQLTimeoutException e) {
          throw new SQLException("Database operation timed out while processing record: " + e.getMessage());
        }
        catch (SQLException e) {
          throw new SQLException("Error processing record (OrderID: " + record.orderID + "): " + e.getMessage());
        }
      }

      // Ensures the product exists in Dim_Products and returns the associated Store_ID. 
      private int ensureProductExists(int productID) throws SQLException {
        String checkSQL = "SELECT COUNT(*) FROM Dim_Products WHERE Product_ID = ?";

        try (PreparedStatement checkStmt = dataWarehouseDBConnection.prepareStatement(checkSQL)) {
          checkStmt.setInt(1, productID);

          try (ResultSet rs = checkStmt.executeQuery()) {
            rs.next();

            if (rs.getInt(1) > 0) {
              // Check cache first
              if (productToStoreMap.containsKey(productID)) {
                return productToStoreMap.get(productID);
              }

              // Fetch from master_db if not in cache
              try (Connection masterDBConnection = DriverManager.getConnection(
                  masterDBURL, masterDBUser, masterDBPassword)) {
                String fetchStoreSQL = "SELECT Store_ID FROM Products WHERE Product_ID = ?";
                try (PreparedStatement fetchStoreStmt = masterDBConnection.prepareStatement(fetchStoreSQL)) {
                  fetchStoreStmt.setInt(1, productID);
                  try (ResultSet storeRs = fetchStoreStmt.executeQuery()) {
                    if (storeRs.next()) {
                      int storeID = storeRs.getInt("Store_ID");
                      productToStoreMap.put(productID, storeID);
                      return storeID;
                    } else {
                      throw new SQLException("Store_ID not found for Product_ID " + productID);
                    }
                  }
                }
              }
              catch (SQLSyntaxErrorException e) {
                throw new SQLException("Invalid SQL syntax in master DB query: " + e.getMessage());
              }
              catch (SQLTimeoutException e) {
                throw new SQLException("Master DB connection timeout: " + e.getMessage());
              }
            }
          }
        }
        catch (SQLException e) {
          throw new SQLException("Error checking product existence: " + e.getMessage());
        }

        // Product doesn't exist, fetch and insert
        try (Connection masterDBConnection = DriverManager.getConnection(
            masterDBURL, masterDBUser, masterDBPassword)) {
          String fetchProductSQL = "SELECT Product_Name, Product_Price, Supplier_ID, Supplier_Name, Store_ID, Store_Name "
              + "FROM Products WHERE Product_ID = ?";

          try (PreparedStatement fetchStmt = masterDBConnection.prepareStatement(fetchProductSQL)) {
            fetchStmt.setInt(1, productID);

            try (ResultSet rs = fetchStmt.executeQuery()) {
              if (rs.next()) {
                String productName = rs.getString("Product_Name");
                double productPrice = rs.getDouble("Product_Price");
                int supplierID = rs.getInt("Supplier_ID");
                String supplierName = rs.getString("Supplier_Name");
                int storeID = rs.getInt("Store_ID");
                String storeName = rs.getString("Store_Name");

                try {
                  // Insert dimensions and commit
                  insertIntoProductDimStmt.setInt(1, productID);
                  insertIntoProductDimStmt.setString(2, productName);
                  insertIntoProductDimStmt.setDouble(3, productPrice);
                  insertIntoProductDimStmt.setInt(4, supplierID);
                  insertIntoProductDimStmt.setString(5, supplierName);
                  insertIntoProductDimStmt.addBatch();

                  insertIntoStoreDimStmt.setInt(1, storeID);
                  insertIntoStoreDimStmt.setString(2, storeName);
                  insertIntoStoreDimStmt.addBatch();

                  insertIntoProductDimStmt.executeBatch();
                  insertIntoStoreDimStmt.executeBatch();
                  dataWarehouseDBConnection.commit();

                  productToStoreMap.put(productID, storeID);
                  return storeID;
                }
                catch (BatchUpdateException e) {
                  dataWarehouseDBConnection.rollback();
                  throw new SQLException("Batch update failed: " + e.getMessage());
                }
              } else {
                throw new SQLException("Product_ID " + productID + " not found in master database.");
              }
            }
          }
        }
        catch (SQLTimeoutException e) {
          throw new SQLException("Database operation timed out: " + e.getMessage());
        }
        catch (SQLIntegrityConstraintViolationException e) {
          throw new SQLException("Data integrity violation: " + e.getMessage());
        }
        catch (SQLException e) {
          throw new SQLException("Error processing product ID " + productID + ": " + e.getMessage());
        }
      }

      // Ensures the store exists in Dim_Stores.
      private void ensureStoreExists(int storeID) throws SQLException {
        String checkSQL = "SELECT COUNT(*) FROM Dim_Stores WHERE Store_ID = ?";

        try (PreparedStatement checkStmt = dataWarehouseDBConnection.prepareStatement(checkSQL)) {
          checkStmt.setInt(1, storeID);

          try (ResultSet rs = checkStmt.executeQuery()) {
            rs.next();
            if (rs.getInt(1) > 0) { // If store already exists
              return;
            }
          }
        }
        catch (SQLException e) {
          throw new SQLException("Error checking store existence: " + e.getMessage());
        }

        // Get Store_Name from master_db
        try (Connection masterDBConnection = DriverManager.getConnection(
            masterDBURL,
            masterDBUser,
            masterDBPassword)) {
          String fetchStoreSQL = "SELECT Store_Name FROM Products WHERE Store_ID = ? LIMIT 1";

          try (PreparedStatement fetchStoreStmt = masterDBConnection.prepareStatement(fetchStoreSQL)) {
            fetchStoreStmt.setInt(1, storeID);

            try (ResultSet rs = fetchStoreStmt.executeQuery()) {
              if (rs.next()) {
                String storeName = rs.getString("Store_Name");
                try {
                  insertIntoStoreDimStmt.setInt(1, storeID);
                  insertIntoStoreDimStmt.setString(2, storeName);
                  insertIntoStoreDimStmt.addBatch();
                  insertIntoStoreDimStmt.executeBatch();
                  dataWarehouseDBConnection.commit();
                }
                catch (BatchUpdateException e) {
                  dataWarehouseDBConnection.rollback();
                  throw new SQLException("Failed to insert store data: " + e.getMessage());
                }
              } else {
                throw new SQLException("Store_ID " + storeID + " not found in master database.");
              }
            }
          }
        }
        catch (SQLTimeoutException e) {
          throw new SQLException("Database operation timed out: " + e.getMessage());
        }
        catch (SQLIntegrityConstraintViolationException e) {
          throw new SQLException("Data integrity violation for Store_ID " + storeID + ": " + e.getMessage());
        }
        catch (SQLException e) {
          throw new SQLException("Error processing Store_ID " + storeID + ": " + e.getMessage());
        }
      }

      // Ensures the customer exists in Dim_Customers.
      private void ensureCustomerExists(int customerID) throws SQLException {
        if (customerCache.contains(customerID)) {
          return;
        }

        // Check if Customer_ID exists
        String checkSQL = "SELECT COUNT(*) FROM Dim_Customers WHERE Customer_ID = ?";

        try (PreparedStatement selectDimCustomersStmt = dataWarehouseDBConnection.prepareStatement(checkSQL)) {
          selectDimCustomersStmt.setInt(1, customerID);

          try (ResultSet rs = selectDimCustomersStmt.executeQuery()) {
            rs.next();
            if (rs.getInt(1) > 0) {
              customerCache.add(customerID);
              return;
            }
          }
          catch (SQLException e) {
            throw new SQLException("Error checking customer existence: " + e.getMessage());
          }
        }

        // Get customer details from master_db
        try (Connection masterDBConnection = DriverManager.getConnection(
            masterDBURL,
            masterDBUser,
            masterDBPassword)) {
          String fetchCustomerSQL = "SELECT Customer_Name, Gender FROM Customers WHERE Customer_ID = ?";

          try (PreparedStatement fetchStmt = masterDBConnection.prepareStatement(fetchCustomerSQL)) {
            fetchStmt.setInt(1, customerID);

            try (ResultSet rs = fetchStmt.executeQuery()) {
              if (rs.next()) {
                String customerName = rs.getString("Customer_Name");
                String gender = normalizeGender(rs.getString("Gender"));

                insertIntoCustomerDimStmt.setInt(1, customerID);
                insertIntoCustomerDimStmt.setString(2, customerName);
                insertIntoCustomerDimStmt.setString(3, gender);
                insertIntoCustomerDimStmt.addBatch();
                customerCache.add(customerID);
              }
            }
            catch (SQLException e) {
              throw new SQLException("Error fetching customer data: " + e.getMessage());
            }
          }
          catch (SQLException e) {
            throw new SQLException("Error preparing customer fetch statement: " + e.getMessage());
          }
        }
      }

      // Normalizes gender values to "Male", "Female", or null
      private String normalizeGender(String gender) {
        if (gender == null)
          return null;
        gender = gender.trim().toLowerCase();

        switch (gender) {
          case "m":
          case "male":
            return "Male";
          case "f":
          case "female":
            return "Female";
          default:
            return null;
        }
      }

      // Ensures the time entry exists in Dim_Time.
      private void ensureTimeExists(int timeID, String orderDate) throws SQLException {
        // Insert new Dim_Time record
        try {
            // Parse orderDate to extract time details
            Timestamp timestamp = Timestamp.valueOf(orderDate);
            Time orderTime = new Time(timestamp.getTime());
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(timestamp.getTime());

            int day = calendar.get(Calendar.DAY_OF_MONTH);
            int week = calendar.get(Calendar.WEEK_OF_YEAR);
            int month = calendar.get(Calendar.MONTH) + 1;
            int year = calendar.get(Calendar.YEAR);

            String insertTimeSQL = "INSERT INTO Dim_Time "
                + "(Time_ID, OrderTime, Day, Week, Month, Year) "
                + "VALUES (?, ?, ?, ?, ?, ?)";

            try (PreparedStatement insertStmt = dataWarehouseDBConnection.prepareStatement(insertTimeSQL, Statement.RETURN_GENERATED_KEYS)) {
                insertStmt.setInt(1, timeID);
                insertStmt.setTime(2, orderTime);
                insertStmt.setInt(3, day);
                insertStmt.setInt(4, week);
                insertStmt.setInt(5, month);
                insertStmt.setInt(6, year);
                insertStmt.executeUpdate();

                try (ResultSet generatedKeys = insertStmt.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        int timePK = generatedKeys.getInt(1);
                        timeIDToTimePKMap.put(timeID, timePK);
                    } else {
                        throw new SQLException("Creating Dim_Time failed, no ID obtained.");
                    }
                }
            }
        }
        catch (IllegalArgumentException e) {
            throw new SQLException("Invalid date format: " + orderDate);
        }
        catch (SQLException e) {
            throw new SQLException("Error inserting time dimension for timeID " + timeID + ": " + e.getMessage());
        }
    }

      private void executeBatch() throws SQLException {
        currentBatchNumber++;
        int start = (currentBatchNumber - 1) * BATCH_SIZE + 1;
        int end = Math.min(currentBatchNumber * BATCH_SIZE, processedRecords);

        System.out.println("\nProcessing batch " + currentBatchNumber +
            " (records " + start + "-" + end + ")...");

        try {
          // Execute all prepared statements
          synchronized (insertIntoCustomerDimStmt) {
            try {
              insertIntoCustomerDimStmt.executeBatch();
              System.out.println("- Dim_Customers updated");
            }
            catch (BatchUpdateException e) {
              dataWarehouseDBConnection.rollback();
              throw new SQLException("Failed to update customer dimension: " + e.getMessage());
            }
            catch (SQLTimeoutException e) {
              dataWarehouseDBConnection.rollback();
              throw new SQLException("Timeout while updating customer dimension: " + e.getMessage());
            }
          }

          synchronized (insertIntoStoreDimStmt) {
            try {
              insertIntoStoreDimStmt.executeBatch();
              System.out.println("- Dim_Stores updated");
            }
            catch (BatchUpdateException e) {
              dataWarehouseDBConnection.rollback();
              throw new SQLException("Failed to update store dimension: " + e.getMessage());
            }
            catch (SQLTimeoutException e) {
              dataWarehouseDBConnection.rollback();
              throw new SQLException("Timeout while updating store dimension: " + e.getMessage());
            }
          }

          synchronized (insertIntoTimeDimStmt) {
            try {
              insertIntoTimeDimStmt.executeBatch();
              System.out.println("- Dim_Time updated");
            }
            catch (BatchUpdateException e) {
              dataWarehouseDBConnection.rollback();
              throw new SQLException("Failed to update time dimension: " + e.getMessage());
            }
            catch (SQLTimeoutException e) {
              dataWarehouseDBConnection.rollback();
              throw new SQLException("Timeout while updating time dimension: " + e.getMessage());
            }
          }

          synchronized (insertIntoProductDimStmt) {
            try {
              insertIntoProductDimStmt.executeBatch();
              System.out.println("- Dim_Products updated");
            }
            catch (BatchUpdateException e) {
              dataWarehouseDBConnection.rollback();
              throw new SQLException("Failed to update product dimension: " + e.getMessage());
            }
            catch (SQLTimeoutException e) {
              dataWarehouseDBConnection.rollback();
              throw new SQLException("Timeout while updating product dimension: " + e.getMessage());
            }
          }

          synchronized (insertIntoTransactionFactTableStmt) {
            try {
              insertIntoTransactionFactTableStmt.executeBatch();
              System.out.println("- Fact_Transactions updated");
            }
            catch (BatchUpdateException e) {
              dataWarehouseDBConnection.rollback();
              throw new SQLException("Failed to update transaction facts: " + e.getMessage());
            }
            catch (SQLTimeoutException e) {
              dataWarehouseDBConnection.rollback();
              throw new SQLException("Timeout while updating transaction facts: " + e.getMessage());
            }
          }

          // Commit transactions
          try {
            dataWarehouseDBConnection.commit();
            System.out.println("\n✓ Batch " + currentBatchNumber + " committed successfully");
          }
          catch (SQLException e) {
            dataWarehouseDBConnection.rollback();
            throw new SQLException("Failed to commit batch " + currentBatchNumber + ": " + e.getMessage());
          }
        }
        catch (SQLException e) {
          try {
            dataWarehouseDBConnection.rollback();
          }
          catch (SQLException rollbackEx) {
            throw new SQLException("Batch " + currentBatchNumber + " failed and rollback failed: "
                + rollbackEx.getMessage() + ". Original error: " + e.getMessage());
          }
          throw new SQLException("Batch " + currentBatchNumber + " failed: " + e.getMessage());
        }
      }

      // Rolls back the transaction in case of an error.
      private void rollback() {
        try {
          if (dataWarehouseDBConnection != null) {
            dataWarehouseDBConnection.rollback();
            System.out.println("Transaction rolled back.");
          }
        }
        catch (SQLException e) {
          System.err.println("Error during rollback: " + e.getMessage());
          e.printStackTrace();
        }
      }

      // Closes all database resources.
      private void closeResources() {
        try {
          if (insertIntoCustomerDimStmt != null && !insertIntoCustomerDimStmt.isClosed()) {
            insertIntoCustomerDimStmt.close();
          }
          if (selectDimCustomersStmt != null && !selectDimCustomersStmt.isClosed()) {
            selectDimCustomersStmt.close();
          }
          if (insertIntoTransactionFactTableStmt != null && !insertIntoTransactionFactTableStmt.isClosed()) {
            insertIntoTransactionFactTableStmt.close();
          }
          if (insertIntoProductDimStmt != null && !insertIntoProductDimStmt.isClosed()) {
            insertIntoProductDimStmt.close();
          }
          if (insertIntoTimeDimStmt != null && !insertIntoTimeDimStmt.isClosed()) {
            insertIntoTimeDimStmt.close();
          }
          if (insertIntoStoreDimStmt != null && !insertIntoStoreDimStmt.isClosed()) {
            insertIntoStoreDimStmt.close();
          }
          if (dataWarehouseDBConnection != null && !dataWarehouseDBConnection.isClosed()) {
            dataWarehouseDBConnection.close();
          }
          if (scanner != null) {
            scanner.close();
            System.out.println("\nScanner closed.");
          }
          System.out.println("\nAll resources closed.");
        }
        catch (SQLException e) {
          System.err.println("\nError closing resources: " + e.getMessage());
          e.printStackTrace();
        }
      }
    }
  }
