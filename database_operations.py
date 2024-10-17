# Connect to MySQL
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            database='data_spark',
            user='root',
            password='aashi'
        )
        if conn.is_connected():
            print("Connected to MySQL database")
            return conn, conn.cursor()
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None, None


# Close MySQL connection    
def close_mysql_connection(conn, cursor):
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("MySQL connection is closed")



# Create MySQL table
def create_table(table_name, cursor):
    table_definitions = {
        "Sales": """
            CREATE TABLE IF NOT EXISTS Sales (
                `Order Number` INT,
                `Line Item` INT,
                `Order Date` DATE,
                `Delivery Date` DATE,
                CustomerKey INT,
                StoreKey INT,
                ProductKey INT,
                Quantity INT,
                `Currency Code` VARCHAR(3),
                PRIMARY KEY (`Order Number`, `Line Item`),  -- Composite primary key
                FOREIGN KEY (CustomerKey) REFERENCES Customers(CustomerKey),
                FOREIGN KEY (ProductKey) REFERENCES Products(ProductKey)
            );
        """,
        "Customers": """
            CREATE TABLE IF NOT EXISTS Customers (
                CustomerKey INT PRIMARY KEY,
                Gender VARCHAR(10),
                `Name` VARCHAR(255),
                City VARCHAR(255),
                State VARCHAR(255),
                `State Code` VARCHAR(5),
                `Zip Code` VARCHAR(10),
                Country VARCHAR(255),
                Continent VARCHAR(255),
                Birthday DATE
            );
        """,
        "Products": """
            CREATE TABLE IF NOT EXISTS Products (
                ProductKey INT PRIMARY KEY,
                `Product Name` VARCHAR(255),
                Brand VARCHAR(255),
                Color VARCHAR(50),
                `Unit Cost USD` DECIMAL(10,2),
                `Unit Price USD` DECIMAL(10,2),
                SubcategoryKey INT,
                Subcategory VARCHAR(255),
                CategoryKey INT,
                Category VARCHAR(255)
            );
        """,
        "Exchange_Rates": """
            CREATE TABLE IF NOT EXISTS Exchange_Rates (
                `Date` DATE PRIMARY KEY,
                `Currency` VARCHAR(3),
                `Exchange` DECIMAL(10,4)
            );
        """,
        "Data_Dictionary": """
            CREATE TABLE IF NOT EXISTS Data_Dictionary (
                `Table` VARCHAR(255),
                `Field` VARCHAR(255),
                `Description` TEXT
            );
        """,
        "Stores": """
            CREATE TABLE IF NOT EXISTS Stores(
                StoreKey INT PRIMARY KEY,
                Country VARCHAR(100),
                State VARCHAR(255), 
               `Square Meters` INT, 
               `Open Date` DATE 
            );
        """
          
    }

    if table_name in table_definitions:
        try:
            cursor.execute(table_definitions[table_name])
            print(f"Table {table_name} created or already exists.")
        except mysql.connector.Error as err:
            print(f"Error creating table {table_name}: {err}")


# Insert data into MySQL table with handling for NaN and duplicates
def insert_data(table_name, df, cursor, conn):
    try:
        for index, row in df.iterrows():
            # Replace NaN values with None (for SQL compatibility)
            row = row.where(pd.notnull(row), None)

            if table_name == "Sales":
                insert_stmt = """
                    INSERT INTO Sales (`Order Number`, `Line Item`, `Order Date`, `Delivery Date`, CustomerKey, StoreKey, ProductKey, Quantity, `Currency Code`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    `Order Date` = VALUES(`Order Date`),
                    `Delivery Date` = VALUES(`Delivery Date`),
                    CustomerKey = VALUES(CustomerKey),
                    StoreKey = VALUES(StoreKey),
                    ProductKey = VALUES(ProductKey),
                    Quantity = VALUES(Quantity),
                    `Currency Code` = VALUES(`Currency Code`);
                """
                cursor.execute(insert_stmt, (
                    row['Order Number'], row['Line Item'], row['Order Date'],
                    row['Delivery Date'], row['CustomerKey'], row['StoreKey'],
                    row['ProductKey'], row['Quantity'], row['Currency Code']
                ))

            elif table_name == "Customers":
                insert_stmt = """
                    INSERT INTO Customers (CustomerKey, Gender, Name, City, State, `State Code`, `Zip Code`, Country, Continent, Birthday)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    Gender = VALUES(Gender),
                    Name = VALUES(Name),
                    City = VALUES(City),
                    State = VALUES(State), 
                    `State Code` = VALUES(`State Code`),
                    `Zip Code` = VALUES(`Zip Code`),
                    Country = VALUES(Country),
                    Continent = VALUES(Continent),
                    Birthday = VALUES(Birthday);
                """
                cursor.execute(insert_stmt, (
                    row['CustomerKey'], row['Gender'], row['Name'], row['City'], row['State'],
                    row['State Code'], row['Zip Code'], row['Country'], row['Continent'], row['Birthday']
                ))

            elif table_name == "Products":
                insert_stmt = """
                    INSERT INTO Products (ProductKey, `Product Name`, Brand, Color, `Unit Cost USD`, `Unit Price USD`, SubcategoryKey, Subcategory, CategoryKey, Category)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    `Product Name` = VALUES(`Product Name`),
                    Brand = VALUES(Brand),
                    Color = VALUES(Color),
                    `Unit Cost USD` = VALUES(`Unit Cost USD`),
                    `Unit Price USD` = VALUES(`Unit Price USD`),
                    SubcategoryKey = VALUES(SubcategoryKey),
                    Subcategory = VALUES(Subcategory),
                    CategoryKey = VALUES(CategoryKey),
                    Category = VALUES(Category);
                """
                cursor.execute(insert_stmt, (
                    row['ProductKey'], row['Product Name'], row['Brand'], row['Color'],
                    row['Unit Cost USD'], row['Unit Price USD'], row['SubcategoryKey'],
                    row['Subcategory'], row['CategoryKey'], row['Category']
                ))

            elif table_name == 'Exchange_Rates':
                insert_stmt = '''
                   INSERT INTO Exchange_Rates (Date, Currency, Exchange)
                   VALUES (%s,%s,%s)
                   ON DUPLICATE KEY UPDATE
                   Exchange = VALUES(Exchange);
                '''
                cursor.execute(insert_stmt, (
                    row['Date'], row['Currency'], row['Exchange']
                ))

            elif table_name == 'Data_Dictionary':
                insert_stmt = '''
                   INSERT INTO Data_Dictionary(`Table`, `Field`, `Description`)
                   VALUES (%s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                   `Description` = VALUES(`Description`);
                '''
                cursor.execute(insert_stmt, (
                    row['Table'], row['Field'], row['Description']
                ))

            elif table_name == "Stores":  # Insert logic for Stores
                insert_stmt = """
                    INSERT INTO Stores (StoreKey, Country, State, `Square Meters`, `Open Date`)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    Country = VALUES(Country),
                    State = VALUES(State),
                    `Square Meters` = VALUES(`Square Meters`),
                    `Open Date` = VALUES(`Open Date`);
                """
                cursor.execute(insert_stmt, (
                    row['StoreKey'], row['Country'], row['State'], row['Square Meters'], row['Open Date']
                ))    

        # Commit the transaction
        conn.commit()
        print(f"Inserted data successfully into {table_name}.")

    except mysql.connector.Error as err:
        print(f"Error inserting data into {table_name}: {err}")