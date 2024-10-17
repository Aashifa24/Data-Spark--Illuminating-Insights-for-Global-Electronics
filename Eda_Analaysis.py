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