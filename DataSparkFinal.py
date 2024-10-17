# Required Libraries
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector
from mysql.connector import Error
import chardet
from datetime import datetime



def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        rawdata = f.read()
    result = chardet.detect(rawdata)
    return result['encoding']

#Perform summary statistics and basic EDA
def eda_summary(df, file_name):
    print(f"\nEDA Summary for {file_name}")

    # Print summary statistics
    print(f'\nSummary Statistics:')
    print(df.describe(include='all'))

    # Correlation matrix
    if df.select_dtypes(include=['number']).shape[1] > 1:
        plt.figure(figsize=(10, 8))
        numeric_df = df.select_dtypes(include=['number'])
        sns.heatmap(numeric_df.corr().round(2), annot=True, cmap='viridis')
        plt.title(f'Correlation Matrix for {file_name}')
        plt.show(block=False)

    # Check for unique values
    print('\nUnique values per column:')
    for col in df.columns:
        print(f'{col}: {df[col].nunique()} unique values')


# Customer demographics analysis
def analyze_customer_demographics(mydf):
    print('\nCustomer Demographics Analysis')
    mydf['Birthday'] = pd.to_datetime(mydf['Birthday'], errors='coerce')
    today = pd.Timestamp('today')
    mydf['Age'] = (today - mydf['Birthday']).dt.days // 365

    plt.figure(figsize=(8, 6))
    sns.countplot(data=mydf, x='Gender')
    plt.title('Gender Distribution')
    plt.show(block=False)

    plt.figure(figsize=(8, 6))
    sns.histplot(mydf['Age'].dropna(), bins=20, kde=True)
    plt.title('Age Distribution')
    plt.show(block=False)

    plt.figure(figsize=(8, 6))
    mydf['City'].value_counts().head(10).plot(kind='bar')
    plt.title('Top 10 Cities by Number of Customers')
    plt.xlabel('City')
    plt.ylabel('Number of Customers')
    plt.show(block=False)

# Customer purchase analysis
def analyze_customer_purchases(df):
    print('\nCustomer Purchase Analysis')
    if 'Quantity' in df.columns and 'Unit Price USD' in df.columns:
        # Clean 'Unit Price USD' by removing extra dollar signs and any non-numeric characters
        df['Unit Price USD'] = df['Unit Price USD'].replace({r'\$': '', r',': '', r' ': '', r'\s+\$.*': ''}, regex=True)

        # Convert cleaned column to float
        df['Unit Price USD'] = pd.to_numeric(df['Unit Price USD'], errors='coerce')

        # Ensure 'Quantity' is numeric
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')

        # Calculate Order Value
        df['Order Value'] = df['Quantity'] * df['Unit Price USD']

        # Check if 'Order Value' contains non-numeric data
        df['Order Value'] = pd.to_numeric(df['Order Value'], errors='coerce')
        
        # Plot Order Value Distribution
        plt.figure(figsize=(10, 6))
        sns.histplot(df['Order Value'], bins=30, kde=True)
        plt.title('Order Value Distribution')
        plt.xlabel('Order Value')
        plt.ylabel('Frequency')
        plt.show(block=False)

        # Calculate Average Order Value
        avg_order_value = round(df['Order Value'].mean(), 2)
        print(f"Average Order Value: ${avg_order_value:.2f}")
    
        # Total Orders by Product        
        plt.figure(figsize=(10, 6))
        df['Product Name'].value_counts().head(10).plot(kind='bar', color='skyblue')
        plt.title('Top 10 Products by Total Orders')
        plt.xlabel('Product')
        plt.ylabel('Total Orders')
        plt.xticks(rotation=45, ha='right')
        plt.show(block=False)


# Sales trend analysis
def analyze_sales_trends(mydf):
    print('\nSales Trends Analysis')
    if 'Order Date' in mydf.columns:
        mydf['Order Date'] = pd.to_datetime(mydf['Order Date'], errors='coerce')
        mydf.set_index('Order Date', inplace=True)
        monthly_sales = mydf.resample('ME').size()

        plt.figure(figsize=(12, 6))
        monthly_sales.plot()
        plt.title('Monthly Sales Trends')
        plt.xlabel('Date')
        plt.ylabel('Number of Sales')
        plt.show(block=False)


# Product analysis
def analyze_products(mydf):
    print('\nProduct Analysis')

    if 'Unit Price USD' in mydf.columns:
        mydf['Unit Price USD'] = mydf['Unit Price USD'].replace({r'\$': '', ' ': ''}, regex=True)
        mydf['Unit Price USD'] = pd.to_numeric(mydf['Unit Price USD'], errors='coerce')

        plt.figure(figsize=(8, 6))
        sns.histplot(mydf['Unit Price USD'].dropna(), bins=20, kde=True)
        plt.title('Product Price Distribution')
        plt.xlabel('Product Price (USD)')
        plt.ylabel('Frequency')
        plt.show(block=False)

    if 'Category' in mydf.columns:
        plt.figure(figsize=(10, 8))
        mydf['Category'].value_counts().plot(kind='bar')
        plt.title('Product Category Distribution')
        plt.xlabel('Category')
        plt.ylabel('Number of Products')
        plt.show()


# Load csv file
def detect_and_load_csv(file_path):
    try:
        encoding = detect_encoding(file_path)
        print(f"Detected encoding for {file_path}: {encoding}")
        df = pd.read_csv(file_path, encoding=encoding)
        return df
    except FileNotFoundError:
        print(f"File {file_path} not found. Please check the file path.")
    except Exception as e:
        print(f"An error occurred while loading {file_path}: {e}")
    return None

# Convert date format 
def convert_date_format(date_str):
    # Check if the date_str is NaN or not a string
    if pd.isna(date_str) or not isinstance(date_str, str):
        return None  # Return None for NaN or non-string values
    try:
        # Convert the date format
        return datetime.strptime(date_str, "%m/%d/%Y").strftime('%Y-%m-%d')
    except ValueError:
        return None  # Return None if date cannot be parsed

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


def main():
    data_paths = {
        "Sales": "C:/Users/aashi/Downloads/Sales.csv",
        "Customers": "C:/Users/aashi/Downloads/Customers.csv",
        "Products": "C:/Users/aashi/Downloads/Products.csv",
        "Exchange_Rates": "C:/Users/aashi/Downloads/Exchange_Rates.csv",
        "Data_Dictionary": "C:/Users/aashi/Downloads/Data_Dictionary.csv",
        "Stores": "C:/Users/aashi/Downloads/Stores.csv"
    }

    data_frames = {}
    
    # Load data
    for key, path in data_paths.items():
        print(f"Loading {key} data from {path}")
        df = detect_and_load_csv(path)  # Assuming this function exists
        if df is not None:
            data_frames[key] = df
            print(f"Loaded {key} data successfully.")
        else:
            print(f"Failed to load {key} data.")
    
    # Clean Sales Data
    if "Sales" in data_frames:
        df_sales = data_frames["Sales"]
        df_sales['Order Date'] = df_sales['Order Date'].apply(convert_date_format)  # Assuming this function exists
        df_sales['Delivery Date'] = df_sales['Delivery Date'].apply(convert_date_format)

    # Clean Products Data
    if "Products" in data_frames:
        df_products = data_frames["Products"]
        df_products['Unit Cost USD'] = df_products['Unit Cost USD'].replace({r'\$': '', ',': ''}, regex=True).astype(float)
        df_products['Unit Price USD'] = df_products['Unit Price USD'].replace({r'\$': '', ',': ''}, regex=True).astype(float)

    # Clean Customers Data
    if "Customers" in data_frames:
        df_customers = data_frames["Customers"]
        df_customers['Birthday'] = pd.to_datetime(df_customers['Birthday'], format='%m/%d/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        # Replace NaN with None before inserting into database
        df_customers = df_customers.where(pd.notnull(df_customers), None)

    # Clean and process Stores Data
    if "Stores" in data_frames:
        df_stores = data_frames["Stores"]
        # Convert 'Open Date' to YYYY-MM-DD format
        df_stores['Open Date'] = pd.to_datetime(df_stores['Open Date'], format='%m/%d/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        df_stores = df_stores.where(pd.notnull(df_stores), None)  

    conn, cursor = connect_to_mysql()
    
    if conn and cursor:  # Check if the connection and cursor are valid
        for table_name in ["Sales","Customers", "Products", "Exchange_Rates", "Data_Dictionary","Stores"]:
            create_table(table_name, cursor)  # Adjust this to pass table_name if necessary

            # Clean and prepare data for insertion
            if table_name == "Exchange_Rates":
                df_exchange_rates = data_frames["Exchange_Rates"]
                df_exchange_rates['Date'] = pd.to_datetime(df_exchange_rates['Date']).dt.strftime('%Y-%m-%d')

            # Insert data into the database
            insert_data(table_name, data_frames[table_name], cursor, conn)  # Insert data

    close_mysql_connection(conn, cursor)

    merged_df = pd.merge(df_sales, df_products, on='ProductKey', how='left')

    # Call EDA summary and plotting functions for each DataFrame
    for key, df in data_frames.items():
        eda_summary(df, key)
        if key == 'Customers':
            analyze_customer_demographics(df_customers)
            analyze_customer_purchases(merged_df)
        elif key == 'Sales':
            analyze_sales_trends(df_sales)
        elif key == 'Products':
            analyze_products(df_products)

if __name__ == "__main__":
    main()