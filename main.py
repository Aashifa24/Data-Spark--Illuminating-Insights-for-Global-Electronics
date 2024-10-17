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