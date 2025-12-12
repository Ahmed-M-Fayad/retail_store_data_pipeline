"""
RETAIL DATABASE ANALYSIS & REPORTING SCRIPT
Purpose: Execute SQL queries, generate visualizations, and create analysis reports
Output: CSV reports + visualizations + HTML summary report
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from datetime import datetime
import warnings
import os

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================

# SQL Server Connection Settings
SQL_SERVER = 'DESKTOP-LF8V7TT'
SQL_DATABASE = 'RetailDB'
SQL_DRIVER = 'ODBC Driver 17 for SQL Server'
USE_WINDOWS_AUTH = True

# SQL Server Authentication credentials (if USE_WINDOWS_AUTH = False)
SQL_USERNAME = 'your_username'
SQL_PASSWORD = 'your_password'

# Output Configuration
OUTPUT_DIR = 'reports'
REPORT_DATE = datetime.now().strftime('%Y%m%d_%H%M%S')

# Visualization Settings
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

print("="*80)
print("RETAIL DATABASE ANALYSIS & REPORTING")
print("="*80)
print(f"Report Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def create_connection():
    """Create SQL Server connection"""
    print("\n🔗 Connecting to SQL Server...")
    
    try:
        if USE_WINDOWS_AUTH:
            connection_string = (
                f'mssql+pyodbc://{SQL_SERVER}/{SQL_DATABASE}?'
                f'driver={SQL_DRIVER}&trusted_connection=yes'
            )
        else:
            connection_string = (
                f'mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@'
                f'{SQL_SERVER}/{SQL_DATABASE}?driver={SQL_DRIVER}'
            )
        
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        print(f"✅ Connected to {SQL_SERVER}/{SQL_DATABASE}")
        return engine
    
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

# ============================================================================
# ANALYSIS QUERIES
# ============================================================================

QUERIES = {
    # Sales Analysis
    'top_products': """
        SELECT TOP 10
            p.product_id,
            p.product_name,
            p.brand_name,
            p.category_name,
            SUM(oi.quantity) AS total_quantity_sold,
            SUM(oi.total_price) AS total_revenue,
            AVG(oi.list_price) AS avg_selling_price
        FROM OrderItems oi
        INNER JOIN Products p ON oi.product_id = p.product_id
        GROUP BY p.product_id, p.product_name, p.brand_name, p.category_name
        ORDER BY total_quantity_sold DESC
    """,
    
    'top_customers': """
        SELECT TOP 5
            c.customer_id,
            c.full_name,
            c.city,
            c.state,
            COUNT(DISTINCT o.order_id) AS total_orders,
            SUM(o.order_total) AS total_spent,
            AVG(o.order_total) AS avg_order_value
        FROM Customers c
        INNER JOIN Orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.full_name, c.city, c.state
        ORDER BY total_spent DESC
    """,
    
    'revenue_by_store': """
        SELECT 
            s.store_id,
            s.store_name,
            s.city,
            COUNT(DISTINCT o.order_id) AS total_orders,
            SUM(o.order_total) AS total_revenue,
            AVG(o.order_total) AS avg_order_value
        FROM Stores s
        INNER JOIN Orders o ON s.store_id = o.store_id
        GROUP BY s.store_id, s.store_name, s.city
        ORDER BY total_revenue DESC
    """,
    
    'revenue_by_category': """
        SELECT 
            c.category_name,
            COUNT(DISTINCT p.product_id) AS number_of_products,
            SUM(oi.quantity) AS total_quantity_sold,
            SUM(oi.total_price) AS total_revenue
        FROM Categories c
        INNER JOIN Products p ON c.category_id = p.category_id
        INNER JOIN OrderItems oi ON p.product_id = oi.product_id
        GROUP BY c.category_name
        ORDER BY total_revenue DESC
    """,
    
    'monthly_sales': """
        SELECT 
            YEAR(o.order_date) AS order_year,
            MONTH(o.order_date) AS order_month,
            DATENAME(MONTH, o.order_date) AS month_name,
            COUNT(DISTINCT o.order_id) AS total_orders,
            SUM(o.order_total) AS total_revenue,
            SUM(oi.quantity) AS total_items_sold
        FROM Orders o
        INNER JOIN OrderItems oi ON o.order_id = oi.order_id
        WHERE o.order_date IS NOT NULL
        GROUP BY YEAR(o.order_date), MONTH(o.order_date), DATENAME(MONTH, o.order_date)
        ORDER BY order_year, order_month
    """,
    
    # Inventory Analysis
    'low_stock_products': """
        SELECT 
            p.product_id,
            p.product_name,
            p.brand_name,
            p.category_name,
            p.list_price,
            SUM(st.quantity) AS total_stock
        FROM Products p
        LEFT JOIN Stocks st ON p.product_id = st.product_id
        GROUP BY p.product_id, p.product_name, p.brand_name, p.category_name, p.list_price
        HAVING SUM(st.quantity) < 10 OR SUM(st.quantity) IS NULL
        ORDER BY total_stock ASC
    """,
    
    'store_inventory': """
        SELECT 
            s.store_name,
            s.city,
            COUNT(DISTINCT st.product_id) AS unique_products,
            SUM(st.quantity) AS total_inventory_units,
            SUM(st.quantity * p.list_price) AS total_inventory_value
        FROM Stores s
        INNER JOIN Stocks st ON s.store_id = st.store_id
        INNER JOIN Products p ON st.product_id = p.product_id
        GROUP BY s.store_name, s.city
        ORDER BY total_inventory_units DESC
    """,
    
    # Staff Performance
    'staff_orders': """
        SELECT 
            st.staff_id,
            st.first_name + ' ' + st.last_name AS staff_name,
            s.store_name,
            COUNT(DISTINCT o.order_id) AS total_orders_handled,
            SUM(o.order_total) AS total_sales_revenue,
            AVG(o.order_total) AS avg_order_value
        FROM Staffs st
        LEFT JOIN Orders o ON st.staff_id = o.staff_id
        LEFT JOIN Stores s ON st.store_id = s.store_id
        WHERE st.active = 1
        GROUP BY st.staff_id, st.first_name, st.last_name, s.store_name
        ORDER BY total_orders_handled DESC
    """,
    
    'best_staff': """
        SELECT TOP 1
            st.staff_id,
            st.first_name + ' ' + st.last_name AS staff_name,
            s.store_name,
            COUNT(DISTINCT o.order_id) AS total_orders_handled,
            SUM(o.order_total) AS total_sales_revenue,
            AVG(o.order_total) AS avg_order_value
        FROM Staffs st
        INNER JOIN Orders o ON st.staff_id = o.staff_id
        LEFT JOIN Stores s ON st.store_id = s.store_id
        WHERE st.active = 1
        GROUP BY st.staff_id, st.first_name, st.last_name, s.store_name
        ORDER BY total_sales_revenue DESC
    """,
    
    # Customer Insights
    'customers_no_orders': """
        SELECT 
            c.customer_id,
            c.full_name,
            c.email,
            c.city,
            c.state
        FROM Customers c
        LEFT JOIN Orders o ON c.customer_id = o.customer_id
        WHERE o.order_id IS NULL
        ORDER BY c.customer_id
    """,
    
    'customer_spending': """
        SELECT 
            c.customer_id,
            c.full_name,
            c.city,
            c.state,
            COUNT(o.order_id) AS total_orders,
            ISNULL(SUM(o.order_total), 0) AS total_spent,
            ISNULL(AVG(o.order_total), 0) AS avg_order_value,
            CASE 
                WHEN COUNT(o.order_id) = 0 THEN 'No Orders'
                WHEN SUM(o.order_total) < 500 THEN 'Low Spender'
                WHEN SUM(o.order_total) BETWEEN 500 AND 2000 THEN 'Medium Spender'
                ELSE 'High Spender'
            END AS customer_segment
        FROM Customers c
        LEFT JOIN Orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.full_name, c.city, c.state
        ORDER BY total_spent DESC
    """,
    
    # Additional Insights
    'revenue_by_brand': """
        SELECT 
            b.brand_name,
            COUNT(DISTINCT p.product_id) AS number_of_products,
            SUM(oi.quantity) AS total_quantity_sold,
            SUM(oi.total_price) AS total_revenue
        FROM Brands b
        INNER JOIN Products p ON b.brand_id = p.brand_id
        INNER JOIN OrderItems oi ON p.product_id = oi.product_id
        GROUP BY b.brand_name
        ORDER BY total_revenue DESC
    """,
    
    'order_status_dist': """
        SELECT 
            order_status,
            CASE 
                WHEN order_status = 1 THEN 'Pending'
                WHEN order_status = 2 THEN 'Processing'
                WHEN order_status = 3 THEN 'Rejected'
                WHEN order_status = 4 THEN 'Completed'
            END AS status_name,
            COUNT(*) AS order_count,
            SUM(order_total) AS total_revenue,
            CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS percentage
        FROM Orders
        GROUP BY order_status
        ORDER BY order_status
    """
}

# ============================================================================
# DATA RETRIEVAL
# ============================================================================

def execute_queries(engine):
    """Execute all analysis queries and return results"""
    print("\n📊 Executing Analysis Queries...")
    
    results = {}
    
    for query_name, query_sql in QUERIES.items():
        try:
            df = pd.read_sql(query_sql, engine)
            results[query_name] = df
            print(f"  ✅ {query_name}: {len(df)} rows")
        except Exception as e:
            print(f"  ❌ {query_name}: {e}")
            results[query_name] = None
    
    return results

# ============================================================================
# SAVE CSV REPORTS
# ============================================================================

def save_csv_reports(results):
    """Save all analysis results to CSV files"""
    print("\n💾 Saving CSV Reports...")
    
    # Create output directory
    csv_dir = os.path.join(OUTPUT_DIR, 'csv', REPORT_DATE)
    os.makedirs(csv_dir, exist_ok=True)
    
    for query_name, df in results.items():
        if df is not None and not df.empty:
            filename = os.path.join(csv_dir, f"{query_name}.csv")
            df.to_csv(filename, index=False)
            print(f"  ✅ {query_name}.csv")
    
    print(f"\n📁 CSV reports saved to: {csv_dir}")
    return csv_dir

# ============================================================================
# VISUALIZATIONS
# ============================================================================

def create_visualizations(results):
    """Create visualizations for analysis results"""
    print("\n📈 Generating Visualizations...")
    
    viz_dir = os.path.join(OUTPUT_DIR, 'visualizations', REPORT_DATE)
    os.makedirs(viz_dir, exist_ok=True)
    
    # 1. Top 10 Products Bar Chart
    if results['top_products'] is not None:
        plt.figure(figsize=(12, 6))
        df = results['top_products'].head(10)
        plt.barh(df['product_name'], df['total_quantity_sold'], color='steelblue')
        plt.xlabel('Total Quantity Sold')
        plt.title('Top 10 Best-Selling Products', fontsize=14, fontweight='bold')
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, '1_top_products.png'), dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✅ Top Products Chart")
    
    # 2. Revenue by Store
    if results['revenue_by_store'] is not None:
        plt.figure(figsize=(10, 6))
        df = results['revenue_by_store']
        plt.bar(df['store_name'], df['total_revenue'], color='coral')
        plt.xlabel('Store')
        plt.ylabel('Total Revenue ($)')
        plt.title('Revenue by Store', fontsize=14, fontweight='bold')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, '2_revenue_by_store.png'), dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✅ Revenue by Store Chart")
    
    # 3. Revenue by Category Pie Chart
    if results['revenue_by_category'] is not None:
        plt.figure(figsize=(10, 8))
        df = results['revenue_by_category']
        plt.pie(df['total_revenue'], labels=df['category_name'], autopct='%1.1f%%', startangle=90)
        plt.title('Revenue Distribution by Category', fontsize=14, fontweight='bold')
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, '3_revenue_by_category.png'), dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✅ Revenue by Category Chart")
    
    # 4. Monthly Sales Trend
    if results['monthly_sales'] is not None:
        df = results['monthly_sales'].copy()
        df['year_month'] = df['order_year'].astype(str) + '-' + df['order_month'].astype(str).str.zfill(2)
        
        fig, ax1 = plt.subplots(figsize=(14, 6))
        
        ax1.plot(df['year_month'], df['total_revenue'], marker='o', color='green', linewidth=2, label='Revenue')
        ax1.set_xlabel('Month')
        ax1.set_ylabel('Total Revenue ($)', color='green')
        ax1.tick_params(axis='y', labelcolor='green')
        ax1.tick_params(axis='x', rotation=45)
        
        ax2 = ax1.twinx()
        ax2.plot(df['year_month'], df['total_orders'], marker='s', color='blue', linewidth=2, label='Orders')
        ax2.set_ylabel('Total Orders', color='blue')
        ax2.tick_params(axis='y', labelcolor='blue')
        
        plt.title('Monthly Sales Trend', fontsize=14, fontweight='bold')
        fig.tight_layout()
        plt.savefig(os.path.join(viz_dir, '4_monthly_sales_trend.png'), dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✅ Monthly Sales Trend Chart")
    
    # 5. Staff Performance
    if results['staff_orders'] is not None:
        plt.figure(figsize=(12, 6))
        df = results['staff_orders'].sort_values('total_sales_revenue', ascending=True)
        plt.barh(df['staff_name'], df['total_sales_revenue'], color='purple')
        plt.xlabel('Total Sales Revenue ($)')
        plt.title('Staff Performance by Sales Revenue', fontsize=14, fontweight='bold')
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, '5_staff_performance.png'), dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✅ Staff Performance Chart")
    
    # 6. Customer Segmentation
    if results['customer_spending'] is not None:
        plt.figure(figsize=(10, 6))
        df = results['customer_spending']
        segment_counts = df['customer_segment'].value_counts()
        colors = ['#ff9999','#66b3ff','#99ff99','#ffcc99']
        plt.pie(segment_counts, labels=segment_counts.index, autopct='%1.1f%%', colors=colors, startangle=90)
        plt.title('Customer Segmentation', fontsize=14, fontweight='bold')
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, '6_customer_segmentation.png'), dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✅ Customer Segmentation Chart")
    
    # 7. Revenue by Brand
    if results['revenue_by_brand'] is not None:
        plt.figure(figsize=(12, 6))
        df = results['revenue_by_brand'].sort_values('total_revenue', ascending=True)
        plt.barh(df['brand_name'], df['total_revenue'], color='teal')
        plt.xlabel('Total Revenue ($)')
        plt.title('Revenue by Brand', fontsize=14, fontweight='bold')
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, '7_revenue_by_brand.png'), dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✅ Revenue by Brand Chart")
    
    # 8. Order Status Distribution
    if results['order_status_dist'] is not None:
        plt.figure(figsize=(10, 6))
        df = results['order_status_dist']
        plt.bar(df['status_name'], df['order_count'], color='orange')
        plt.xlabel('Order Status')
        plt.ylabel('Number of Orders')
        plt.title('Order Status Distribution', fontsize=14, fontweight='bold')
        for i, (count, pct) in enumerate(zip(df['order_count'], df['percentage'])):
            plt.text(i, count, f"{count}\n({pct}%)", ha='center', va='bottom')
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, '8_order_status_dist.png'), dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✅ Order Status Distribution Chart")
    
    # 9. Store Inventory Comparison
    if results['store_inventory'] is not None:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        df = results['store_inventory']
        
        ax1.bar(df['store_name'], df['total_inventory_units'], color='skyblue')
        ax1.set_xlabel('Store')
        ax1.set_ylabel('Total Inventory Units')
        ax1.set_title('Inventory Units by Store')
        ax1.tick_params(axis='x', rotation=45)
        
        ax2.bar(df['store_name'], df['total_inventory_value'], color='lightcoral')
        ax2.set_xlabel('Store')
        ax2.set_ylabel('Total Inventory Value ($)')
        ax2.set_title('Inventory Value by Store')
        ax2.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, '9_store_inventory.png'), dpi=300, bbox_inches='tight')
        plt.close()
        print("  ✅ Store Inventory Charts")
    
    print(f"\n📁 Visualizations saved to: {viz_dir}")
    return viz_dir

# ============================================================================
# HTML REPORT GENERATION
# ============================================================================

def generate_html_report(results, viz_dir, csv_dir):
    """Generate comprehensive HTML report"""
    print("\n📄 Generating HTML Report...")
    
    html_dir = os.path.join(OUTPUT_DIR, 'html')
    os.makedirs(html_dir, exist_ok=True)
    
    html_file = os.path.join(html_dir, f'retail_analysis_report_{REPORT_DATE}.html')
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Retail Analysis Report - {REPORT_DATE}</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: #333;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                padding: 40px;
                border-radius: 10px;
                box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            }}
            h1 {{
                color: #667eea;
                text-align: center;
                font-size: 2.5em;
                margin-bottom: 10px;
            }}
            .subtitle {{
                text-align: center;
                color: #666;
                margin-bottom: 30px;
                font-size: 1.1em;
            }}
            h2 {{
                color: #764ba2;
                border-bottom: 3px solid #667eea;
                padding-bottom: 10px;
                margin-top: 40px;
            }}
            .metric-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin: 20px 0;
            }}
            .metric-card {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            }}
            .metric-value {{
                font-size: 2em;
                font-weight: bold;
                margin: 10px 0;
            }}
            .metric-label {{
                font-size: 0.9em;
                opacity: 0.9;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            th {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 12px;
                text-align: left;
            }}
            td {{
                padding: 10px;
                border-bottom: 1px solid #ddd;
            }}
            tr:hover {{
                background-color: #f5f5f5;
            }}
            .chart-container {{
                margin: 30px 0;
                text-align: center;
            }}
            .chart-container img {{
                max-width: 100%;
                height: auto;
                border-radius: 8px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            }}
            .footer {{
                text-align: center;
                margin-top: 50px;
                padding-top: 20px;
                border-top: 2px solid #eee;
                color: #666;
            }}
            .status-badge {{
                padding: 5px 10px;
                border-radius: 20px;
                font-size: 0.85em;
                font-weight: bold;
            }}
            .high-spender {{ background: #4caf50; color: white; }}
            .medium-spender {{ background: #ff9800; color: white; }}
            .low-spender {{ background: #f44336; color: white; }}
            .no-orders {{ background: #9e9e9e; color: white; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🏪 Retail Store Analysis Report</h1>
            <div class="subtitle">
                Generated on {datetime.now().strftime('%B %d, %Y at %H:%M:%S')}<br>
                Database: {SQL_DATABASE} | Server: {SQL_SERVER}
            </div>
    """
    
    # Key Metrics Section
    if results['revenue_by_store'] is not None and results['customer_spending'] is not None:
        total_revenue = results['revenue_by_store']['total_revenue'].sum()
        total_orders = results['revenue_by_store']['total_orders'].sum()
        avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
        total_customers = len(results['customer_spending'])
        
        html_content += f"""
            <h2>📊 Key Business Metrics</h2>
            <div class="metric-grid">
                <div class="metric-card">
                    <div class="metric-label">Total Revenue</div>
                    <div class="metric-value">${total_revenue:,.2f}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Total Orders</div>
                    <div class="metric-value">{total_orders:,}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Average Order Value</div>
                    <div class="metric-value">${avg_order_value:,.2f}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Total Customers</div>
                    <div class="metric-value">{total_customers:,}</div>
                </div>
            </div>
        """
    
    # Sales Analysis
    html_content += "<h2>💰 Sales Analysis</h2>"
    
    # Top Products
    if results['top_products'] is not None:
        html_content += """
            <h3>Top 10 Best-Selling Products</h3>
            <div class="chart-container">
                <img src="../visualizations/{}/1_top_products.png" alt="Top Products">
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Product Name</th>
                        <th>Brand</th>
                        <th>Category</th>
                        <th>Quantity Sold</th>
                        <th>Revenue</th>
                    </tr>
                </thead>
                <tbody>
        """.format(REPORT_DATE)
        
        for _, row in results['top_products'].head(10).iterrows():
            html_content += f"""
                <tr>
                    <td>{row['product_name']}</td>
                    <td>{row['brand_name']}</td>
                    <td>{row['category_name']}</td>
                    <td>{row['total_quantity_sold']:,}</td>
                    <td>${row['total_revenue']:,.2f}</td>
                </tr>
            """
        html_content += "</tbody></table>"
    
    # Revenue Charts
    html_content += f"""
        <div class="chart-container">
            <img src="../visualizations/{REPORT_DATE}/2_revenue_by_store.png" alt="Revenue by Store">
        </div>
        <div class="chart-container">
            <img src="../visualizations/{REPORT_DATE}/3_revenue_by_category.png" alt="Revenue by Category">
        </div>
        <div class="chart-container">
            <img src="../visualizations/{REPORT_DATE}/4_monthly_sales_trend.png" alt="Monthly Sales Trend">
        </div>
        <div class="chart-container">
            <img src="../visualizations/{REPORT_DATE}/7_revenue_by_brand.png" alt="Revenue by Brand">
        </div>
    """
    
    # Staff Performance
    html_content += f"""
        <h2>👥 Staff Performance</h2>
        <div class="chart-container">
            <img src="../visualizations/{REPORT_DATE}/5_staff_performance.png" alt="Staff Performance">
        </div>
    """
    
    if results['best_staff'] is not None and not results['best_staff'].empty:
        best = results['best_staff'].iloc[0]
        html_content += f"""
            <div class="metric-grid">
                <div class="metric-card">
                    <div class="metric-label">Best Performing Staff</div>
                    <div class="metric-value" style="font-size: 1.5em;">{best['staff_name']}</div>
                    <div class="metric-label">{best['store_name']}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Total Sales</div>
                    <div class="metric-value">${best['total_sales_revenue']:,.2f}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Orders Handled</div>
                    <div class="metric-value">{best['total_orders_handled']:,}</div>
                </div>
            </div>
        """
    
    # Customer Insights
    html_content += f"""
        <h2>👤 Customer Insights</h2>
        <div class="chart-container">
            <img src="../visualizations/{REPORT_DATE}/6_customer_segmentation.png" alt="Customer Segmentation">
        </div>
    """
    
    if results['customers_no_orders'] is not None:
        inactive_count = len(results['customers_no_orders'])
        html_content += f"""
            <div class="metric-card" style="margin: 20px 0;">
                <div class="metric-label">⚠️ Customers with No Orders</div>
                <div class="metric-value">{inactive_count:,}</div>
                <div class="metric-label">Potential for re-engagement campaigns</div>
            </div>
        """
    
    # Inventory Analysis
    html_content += f"""
        <h2>📦 Inventory Analysis</h2>
        <div class="chart-container">
            <img src="../visualizations/{REPORT_DATE}/9_store_inventory.png" alt="Store Inventory">
        </div>
    """
    
    if results['low_stock_products'] is not None and not results['low_stock_products'].empty:
        html_content += f"""
            <h3>⚠️ Low Stock Alert ({len(results['low_stock_products'])} products)</h3>
            <table>
                <thead>
                    <tr>
                        <th>Product Name</th>
                        <th>Brand</th>
                        <th>Category</th>
                        <th>Stock Level</th>
                        <th>List Price</th>
                    </tr>
                </thead>
                <tbody>
        """
        for _, row in results['low_stock_products'].head(10).iterrows():
            stock = row['total_stock'] if pd.notna(row['total_stock']) else 0
            html_content += f"""
                <tr style="background-color: #fff3cd;">
                    <td>{row['product_name']}</td>
                    <td>{row['brand_name']}</td>
                    <td>{row['category_name']}</td>
                    <td>{int(stock)}</td>
                    <td>${row['list_price']:,.2f}</td>
                </tr>
            """
        html_content += "</tbody></table>"
    
    # Order Status
    html_content += f"""
        <h2>📋 Order Status</h2>
        <div class="chart-container">
            <img src="../visualizations/{REPORT_DATE}/8_order_status_dist.png" alt="Order Status Distribution">
        </div>
    """
    
    # Footer
    html_content += f"""
            <div class="footer">
                <p><strong>Report Information</strong></p>
                <p>CSV Files: {csv_dir}</p>
                <p>Visualizations: {viz_dir}</p>
                <p>© 2024 Retail Database Analysis System</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"  ✅ HTML report saved to: {html_file}")
    return html_file

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Execute complete analysis and reporting pipeline"""
    
    print("\n🚀 Starting Analysis & Reporting Process...\n")
    
    # Step 1: Connect to database
    engine = create_connection()
    if engine is None:
        print("\n❌ Cannot proceed without database connection")
        return False
    
    # Step 2: Execute queries
    results = execute_queries(engine)
    
    # Step 3: Save CSV reports
    csv_dir = save_csv_reports(results)
    
    # Step 4: Create visualizations
    viz_dir = create_visualizations(results)
    
    # Step 5: Generate HTML report
    html_file = generate_html_report(results, viz_dir, csv_dir)
    
    # Summary
    print("\n" + "="*80)
    print("✅ ANALYSIS COMPLETE!")
    print("="*80)
    print(f"\n📊 Reports Generated:")
    print(f"  • CSV Files: {csv_dir}")
    print(f"  • Visualizations: {viz_dir}")
    print(f"  • HTML Report: {html_file}")
    print("\n" + "="*80)
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)