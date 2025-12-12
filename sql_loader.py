"""
SQL SERVER DATA LOADER
Purpose: Load cleaned data from CSV files into SQL Server
Reads from: cleaned_data/ directory
"""

import pandas as pd
from sqlalchemy import create_engine, text
import os
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================

CLEANED_DATA_PATH = "cleaned_data/"

# SQL Server connection settings
SQL_SERVER = 'DESKTOP-LF8V7TT'
SQL_DATABASE = 'RetailDB'
SQL_DRIVER = 'ODBC Driver 17 for SQL Server'

# Set to True for Windows Authentication, False for SQL Server Authentication
USE_WINDOWS_AUTH = True

# SQL Server Authentication credentials (if USE_WINDOWS_AUTH = False)
SQL_USERNAME = 'your_username'
SQL_PASSWORD = 'your_password'

print("="*80)
print("SQL SERVER DATA LOADER")
print("="*80)


# ============================================================================
# LOAD CLEANED DATA
# ============================================================================

def load_cleaned_data():
    """Load cleaned CSV files from output directory"""
    print("\n" + "="*80)
    print("LOADING CLEANED DATA")
    print("="*80)
    
    if not os.path.exists(CLEANED_DATA_PATH):
        print(f"\n✗ ERROR: Directory '{CLEANED_DATA_PATH}' not found!")
        print("   Please run transform_pipeline.py first")
        return None
    
    files = [
        'cleaned_brands.csv',
        'cleaned_categories.csv',
        'cleaned_products.csv',
        'cleaned_customers.csv',
        'cleaned_orders.csv',
        'cleaned_order_items.csv',
        'cleaned_staffs.csv',
        'cleaned_stores.csv',
        'cleaned_stocks.csv'
    ]
    
    dfs = {}
    missing_files = []
    
    for file in files:
        filepath = f"{CLEANED_DATA_PATH}{file}"
        table_name = file.replace('cleaned_', '').replace('.csv', '')
        
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            dfs[table_name] = df
            print(f"✓ {table_name:15} : {len(df):,} rows loaded")
        else:
            missing_files.append(file)
            print(f"✗ {table_name:15} : FILE NOT FOUND")
    
    if missing_files:
        print(f"\n⚠️  Missing {len(missing_files)} file(s):")
        for file in missing_files:
            print(f"   - {file}")
        return None
    
    print(f"\n✓ All {len(dfs)} datasets loaded successfully")
    return dfs


# ============================================================================
# SQL SERVER CONNECTION
# ============================================================================

def create_sql_connection():
    """Create SQL Server connection engine"""
    print("\n" + "="*80)
    print("CONNECTING TO SQL SERVER")
    print("="*80)
    
    try:
        if USE_WINDOWS_AUTH:
            # Windows Authentication
            connection_string = (
                f'mssql+pyodbc://{SQL_SERVER}/{SQL_DATABASE}?'
                f'driver={SQL_DRIVER}&trusted_connection=yes'
            )
            print(f"\nConnection mode: Windows Authentication")
        else:
            # SQL Server Authentication
            connection_string = (
                f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@'
                f'{SQL_SERVER}/{SQL_DATABASE}?driver={SQL_DRIVER}'
            )
            print(f"\nConnection mode: SQL Server Authentication")
        
        print(f"Server: {SQL_SERVER}")
        print(f"Database: {SQL_DATABASE}")
        
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT @@VERSION"))
            version = result.fetchone()[0]
            print(f"\n✓ Connected successfully!")
            print(f"  SQL Server version: {version.split('-')[0].strip()}")
        
        return engine
    
    except Exception as e:
        print(f"\n✗ Connection failed!")
        print(f"   Error: {e}")
        print("\nTroubleshooting:")
        print("  1. Verify SQL Server is running")
        print("  2. Check server name and database name")
        print("  3. Verify ODBC driver is installed")
        print("  4. Check authentication credentials")
        return None


# ============================================================================
# CREATE DATABASE SCHEMA
# ============================================================================

def create_database_schema(engine):
    """Create all database tables with proper relationships"""
    print("\n" + "="*80)
    print("CREATING DATABASE SCHEMA")
    print("="*80)
    
    sql_script = """
    -- Drop tables in reverse order of dependencies
    IF OBJECT_ID('Stocks', 'U') IS NOT NULL DROP TABLE Stocks;
    IF OBJECT_ID('OrderItems', 'U') IS NOT NULL DROP TABLE OrderItems;
    IF OBJECT_ID('Orders', 'U') IS NOT NULL DROP TABLE Orders;
    IF OBJECT_ID('Customers', 'U') IS NOT NULL DROP TABLE Customers;
    IF OBJECT_ID('Products', 'U') IS NOT NULL DROP TABLE Products;
    IF OBJECT_ID('Staffs', 'U') IS NOT NULL DROP TABLE Staffs;
    IF OBJECT_ID('Stores', 'U') IS NOT NULL DROP TABLE Stores;
    IF OBJECT_ID('Categories', 'U') IS NOT NULL DROP TABLE Categories;
    IF OBJECT_ID('Brands', 'U') IS NOT NULL DROP TABLE Brands;
    
    -- Create Brands table
    CREATE TABLE Brands (
        brand_id INT PRIMARY KEY,
        brand_name NVARCHAR(255) NOT NULL
    );
    
    -- Create Categories table
    CREATE TABLE Categories (
        category_id INT PRIMARY KEY,
        category_name NVARCHAR(255) NOT NULL
    );
    
    -- Create Stores table
    CREATE TABLE Stores (
        store_id INT PRIMARY KEY,
        store_name NVARCHAR(255),
        phone NVARCHAR(25),
        email NVARCHAR(255),
        street NVARCHAR(255),
        city NVARCHAR(255),
        state NVARCHAR(10),
        zip_code NVARCHAR(10)
    );
    
    -- Create Staffs table
    CREATE TABLE Staffs (
        staff_id INT PRIMARY KEY,
        first_name NVARCHAR(50),
        last_name NVARCHAR(50),
        email NVARCHAR(255),
        phone NVARCHAR(25),
        active INT,
        store_id INT,
        manager_id INT,
        FOREIGN KEY (store_id) REFERENCES Stores(store_id)
    );
    
    -- Create Products table
    CREATE TABLE Products (
        product_id INT PRIMARY KEY,
        product_name NVARCHAR(255) NOT NULL,
        brand_id INT,
        category_id INT,
        model_year INT,
        list_price FLOAT,
        brand_name NVARCHAR(255),
        category_name NVARCHAR(255),
        FOREIGN KEY (brand_id) REFERENCES Brands(brand_id),
        FOREIGN KEY (category_id) REFERENCES Categories(category_id)
    );
    
    -- Create Customers table
    CREATE TABLE Customers (
        customer_id INT PRIMARY KEY,
        first_name NVARCHAR(255),
        last_name NVARCHAR(255),
        phone NVARCHAR(25),
        email NVARCHAR(255),
        street NVARCHAR(255),
        city NVARCHAR(255),
        state NVARCHAR(10),
        zip_code NVARCHAR(10),
        full_name NVARCHAR(510)
    );
    
    -- Create Orders table
    CREATE TABLE Orders (
        order_id INT PRIMARY KEY,
        customer_id INT,
        order_status INT,
        order_date DATETIME,
        required_date DATETIME,
        shipped_date DATETIME,
        store_id INT,
        staff_id INT,
        order_total FLOAT,
        FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
        FOREIGN KEY (store_id) REFERENCES Stores(store_id),
        FOREIGN KEY (staff_id) REFERENCES Staffs(staff_id)
    );
    
    -- Create OrderItems table
    CREATE TABLE OrderItems (
        order_id INT,
        item_id INT,
        product_id INT,
        quantity INT,
        list_price FLOAT,
        discount FLOAT,
        total_price FLOAT,
        PRIMARY KEY (order_id, item_id),
        FOREIGN KEY (order_id) REFERENCES Orders(order_id),
        FOREIGN KEY (product_id) REFERENCES Products(product_id)
    );
    
    -- Create Stocks table
    CREATE TABLE Stocks (
        store_id INT,
        product_id INT,
        quantity INT,
        PRIMARY KEY (store_id, product_id),
        FOREIGN KEY (store_id) REFERENCES Stores(store_id),
        FOREIGN KEY (product_id) REFERENCES Products(product_id)
    );
    """
    
    try:
        print("\nDropping existing tables (if any)...")
        print("Creating new tables...")
        
        with engine.begin() as conn:
            # Execute each statement separately
            statements = [s.strip() for s in sql_script.split(';') if s.strip()]
            for statement in statements:
                conn.execute(text(statement))
        
        print("\n✓ Database schema created successfully")
        print("\nTables created:")
        tables = [
            'Brands', 'Categories', 'Stores', 'Staffs', 
            'Products', 'Customers', 'Orders', 'OrderItems', 'Stocks'
        ]
        for table in tables:
            print(f"  ✓ {table}")
        
        return True
    
    except Exception as e:
        print(f"\n✗ Schema creation failed!")
        print(f"   Error: {e}")
        return False


# ============================================================================
# LOAD DATA TO SQL SERVER
# ============================================================================

def load_data_to_sql(dfs, engine):
    """Load data into SQL Server tables"""
    print("\n" + "="*80)
    print("LOADING DATA TO SQL SERVER")
    print("="*80)
    
    # Load order: respect foreign key constraints
    load_order = [
        ('brands', 'Brands'),
        ('categories', 'Categories'),
        ('stores', 'Stores'),
        ('staffs', 'Staffs'),
        ('products', 'Products'),
        ('customers', 'Customers'),
        ('orders', 'Orders'),
        ('order_items', 'OrderItems'),
        ('stocks', 'Stocks')
    ]
    
    print("\nLoading tables:")
    loaded_count = 0
    total_rows = 0
    
    try:
        for df_name, table_name in load_order:
            if df_name not in dfs:
                print(f"  ⚠️  {table_name:15} : Dataset not found, skipping")
                continue
            
            df = dfs[df_name]
            
            # Load data
            df.to_sql(table_name, engine, if_exists='append', index=False)
            
            loaded_count += 1
            total_rows += len(df)
            print(f"  ✓ {table_name:15} : {len(df):,} rows loaded")
        
        print(f"\n✓ Successfully loaded {loaded_count} tables")
        print(f"  Total rows inserted: {total_rows:,}")
        return True
    
    except Exception as e:
        print(f"\n✗ Data loading failed!")
        print(f"   Error: {e}")
        return False


# ============================================================================
# VERIFY DATA LOAD
# ============================================================================

def verify_data_load(engine):
    """Verify data was loaded correctly"""
    print("\n" + "="*80)
    print("VERIFYING DATA LOAD")
    print("="*80)
    
    tables = [
        'Brands', 'Categories', 'Stores', 'Staffs',
        'Products', 'Customers', 'Orders', 'OrderItems', 'Stocks'
    ]
    
    print("\nTable row counts:")
    try:
        with engine.connect() as conn:
            for table in tables:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.fetchone()[0]
                print(f"  {table:15} : {count:,} rows")
        
        print("\n✓ Data verification complete")
        return True
    
    except Exception as e:
        print(f"\n✗ Verification failed!")
        print(f"   Error: {e}")
        return False


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Execute SQL Server loading process"""
    
    print("\n📤 Starting SQL Server Loading Process...\n")
    
    # Step 1: Load cleaned data from CSV
    dfs = load_cleaned_data()
    if dfs is None:
        print("\n" + "="*80)
        print("❌ LOADING ABORTED")
        print("="*80)
        return False
    
    # Step 2: Connect to SQL Server
    engine = create_sql_connection()
    if engine is None:
        print("\n" + "="*80)
        print("❌ LOADING ABORTED")
        print("="*80)
        return False
    
    # Step 3: Create database schema
    schema_created = create_database_schema(engine)
    if not schema_created:
        print("\n" + "="*80)
        print("❌ LOADING ABORTED")
        print("="*80)
        return False
    
    # Step 4: Load data to SQL Server
    data_loaded = load_data_to_sql(dfs, engine)
    if not data_loaded:
        print("\n" + "="*80)
        print("❌ LOADING FAILED")
        print("="*80)
        return False
    
    # Step 5: Verify data load
    verified = verify_data_load(engine)
    
    # Summary
    print("\n" + "="*80)
    print("LOADING SUMMARY")
    print("="*80)
    
    print("\n✓ Process Complete:")
    print(f"  ✓ Cleaned data loaded from: {CLEANED_DATA_PATH}")
    print(f"  ✓ Database schema created")
    print(f"  ✓ Data loaded to: {SQL_SERVER}/{SQL_DATABASE}")
    print(f"  ✓ Data verification: {'Passed' if verified else 'Skipped'}")
    
    print("\n" + "="*80)
    print("✅ SQL SERVER LOADING COMPLETE!")
    print("="*80)
    print(f"\nData is now available in SQL Server database: {SQL_DATABASE}")
    print("="*80)
    
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)