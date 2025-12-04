#!/usr/bin/env python
# coding: utf-8

# # 📊 E-Commerce Sales Analytics
# ## Complete Analysis using Pandas, NumPy, and Matplotlib

# ### 1. Setup and Imports

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from datetime import datetime, timedelta
import json
import os

# Configure display
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.float_format', '{:,.2f}'.format)
warnings.filterwarnings('ignore')

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

# ### 2. Load Data

print("📥 Loading data...")

# Load sales data
sales_data = {
    'sale_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'customer_email': ['john@example.com', 'jane@example.com', 'bob@example.com', 
                      'alice@example.com', 'john@example.com', 'jane@example.com',
                      'bob@example.com', 'alice@example.com', 'john@example.com', 'jane@example.com'],
    'total_amount': [1299.99, 289.98, 89.99, 249.99, 199.99, 79.99, 129.99, 29.99, 39.99, 199.99],
    'payment_method': ['Credit Card', 'PayPal', 'Credit Card', 'Credit Card', 'PayPal',
                      'Credit Card', 'Credit Card', 'PayPal', 'Credit Card', 'Credit Card'],
    'sale_date': pd.date_range('2024-01-01', periods=10, freq='D')
}

# Load product data
products_data = {
    'id': [1, 2, 3, 4, 5, 6, 7, 8],
    'name': ['Laptop Pro', 'Wireless Headphones', 'Running Shoes', 'Coffee Maker',
             'Python Programming Book', 'Smart Watch', 'Yoga Mat', 'Blender'],
    'category': ['Electronics', 'Electronics', 'Sports', 'Home & Kitchen',
                'Books', 'Electronics', 'Sports', 'Home & Kitchen'],
    'price': [1299.99, 199.99, 89.99, 79.99, 39.99, 249.99, 29.99, 129.99],
    'stock_quantity': [50, 100, 200, 75, 150, 60, 300, 40],
    'rating': [4.5, 4.2, 4.7, 4.3, 4.8, 4.4, 4.6, 4.1]
}

# Load sale items data
sale_items_data = {
    'sale_id': [1, 2, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'product_id': [1, 2, 4, 3, 6, 2, 4, 7, 8, 5, 2],
    'quantity': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    'unit_price': [1299.99, 199.99, 79.99, 89.99, 249.99, 199.99, 79.99, 129.99, 29.99, 39.99, 199.99],
    'total_price': [1299.99, 199.99, 79.99, 89.99, 249.99, 199.99, 79.99, 129.99, 29.99, 39.99, 199.99]
}

# Create DataFrames
sales_df = pd.DataFrame(sales_data)
products_df = pd.DataFrame(products_data)
sale_items_df = pd.DataFrame(sale_items_data)

print(f"✅ Data loaded successfully!")
print(f"   Sales records: {len(sales_df)}")
print(f"   Products: {len(products_df)}")
print(f"   Sale items: {len(sale_items_df)}")

# ### 3. Data Cleaning with Pandas

print("\n🧹 Cleaning data...")

# Convert dates
sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
sales_df['sale_day'] = sales_df['sale_date'].dt.day_name()
sales_df['sale_month'] = sales_df['sale_date'].dt.month_name()
sales_df['sale_week'] = sales_df['sale_date'].dt.isocalendar().week

# Add derived columns
sales_df['sale_year'] = sales_df['sale_date'].dt.year
sales_df['sale_quarter'] = sales_df['sale_date'].dt.quarter

# Clean product data
products_df['category'] = products_df['category'].str.strip()
products_df['name'] = products_df['name'].str.strip()
products_df['rating'] = pd.to_numeric(products_df['rating'], errors='coerce')
products_df['price'] = pd.to_numeric(products_df['price'], errors='coerce')

# Merge data for analysis
sales_with_items = pd.merge(sale_items_df, sales_df, on='sale_id')
full_data = pd.merge(sales_with_items, products_df, left_on='product_id', right_on='id')

print("✅ Data cleaning completed!")

# ### 4. Exploratory Data Analysis with NumPy

print("\n🔍 Exploratory Data Analysis...")

# Basic statistics
print("\n📊 Basic Statistics:")
print(f"Total Sales Amount: ${sales_df['total_amount'].sum():,.2f}")
print(f"Average Order Value: ${sales_df['total_amount'].mean():,.2f}")
print(f"Number of Unique Customers: {sales_df['customer_email'].nunique()}")
print(f"Number of Unique Products Sold: {sale_items_df['product_id'].nunique()}")

# Calculate using NumPy
sales_amounts = sales_df['total_amount'].values
print(f"\n🧮 NumPy Statistics:")
print(f"Median Sales: ${np.median(sales_amounts):,.2f}")
print(f"Standard Deviation: ${np.std(sales_amounts):,.2f}")
print(f"Variance: ${np.var(sales_amounts):,.2f}")
print(f"Skewness: {pd.Series(sales_amounts).skew():.3f}")
print(f"Kurtosis: {pd.Series(sales_amounts).kurtosis():.3f}")

# ### 5. Sales Trends Analysis

print("\n📈 Sales Trends Analysis...")

# Daily sales
daily_sales = sales_df.groupby(sales_df['sale_date'].dt.date)['total_amount'].sum()

# Weekly sales
weekly_sales = sales_df.groupby('sale_week')['total_amount'].sum()

# Payment method analysis
payment_analysis = sales_df.groupby('payment_method').agg({
    'total_amount': ['sum', 'mean', 'count'],
    'sale_id': 'nunique'
}).round(2)

print("\n💳 Payment Method Analysis:")
print(payment_analysis)

# ### 6. Product Performance Analysis

print("\n📦 Product Performance Analysis...")

# Product sales performance
product_performance = full_data.groupby(['product_id', 'name', 'category']).agg({
    'quantity': 'sum',
    'total_price': 'sum',
    'sale_id': 'nunique'
}).rename(columns={
    'quantity': 'total_quantity_sold',
    'total_price': 'total_revenue',
    'sale_id': 'order_count'
}).reset_index()

# Calculate additional metrics using NumPy
product_performance['avg_order_quantity'] = product_performance['total_quantity_sold'] / product_performance['order_count']
product_performance['avg_revenue_per_order'] = product_performance['total_revenue'] / product_performance['order_count']

# Add product details
product_performance = pd.merge(product_performance, products_df[['id', 'price', 'rating', 'stock_quantity']], 
                              left_on='product_id', right_on='id')

# Calculate inventory metrics using NumPy
prices = product_performance['price'].values
stocks = product_performance['stock_quantity'].values
revenues = product_performance['total_revenue'].values

product_performance['inventory_value'] = prices * stocks
product_performance['revenue_per_stock'] = revenues / np.where(stocks > 0, stocks, 1)
product_performance['turnover_ratio'] = product_performance['total_quantity_sold'] / np.where(stocks > 0, stocks, 1)

# Rank products
product_performance['revenue_rank'] = product_performance['total_revenue'].rank(ascending=False, method='dense')
product_performance['quantity_rank'] = product_performance['total_quantity_sold'].rank(ascending=False, method='dense')

print("\n🏆 Top Performing Products:")
print(product_performance.sort_values('total_revenue', ascending=False).head())

# ### 7. Customer Analysis

print("\n👥 Customer Analysis...")

# Customer segmentation
customer_analysis = sales_df.groupby('customer_email').agg({
    'total_amount': ['sum', 'mean', 'count'],
    'sale_date': ['min', 'max']
}).round(2)

customer_analysis.columns = ['total_spent', 'avg_order_value', 'order_count', 'first_purchase', 'last_purchase']

# Calculate customer metrics
customer_analysis['days_since_last_purchase'] = (datetime.now() - customer_analysis['last_purchase']).dt.days
customer_analysis['customer_lifetime_days'] = (customer_analysis['last_purchase'] - customer_analysis['first_purchase']).dt.days

# RFM Analysis using NumPy
recency = customer_analysis['days_since_last_purchase'].values
frequency = customer_analysis['order_count'].values
monetary = customer_analysis['total_spent'].values

# Calculate RFM scores
customer_analysis['recency_score'] = pd.qcut(recency, q=5, labels=[5, 4, 3, 2, 1], duplicates='drop')
customer_analysis['frequency_score'] = pd.qcut(frequency, q=5, labels=[1, 2, 3, 4, 5], duplicates='drop')
customer_analysis['monetary_score'] = pd.qcut(monetary, q=5, labels=[1, 2, 3, 4, 5], duplicates='drop')

# Convert scores to numeric
customer_analysis['recency_score'] = pd.to_numeric(customer_analysis['recency_score'])
customer_analysis['frequency_score'] = pd.to_numeric(customer_analysis['frequency_score'])
customer_analysis['monetary_score'] = pd.to_numeric(customer_analysis['monetary_score'])

# Calculate RFM segment
def get_rfm_segment(row):
    if row['recency_score'] >= 4 and row['frequency_score'] >= 4 and row['monetary_score'] >= 4:
        return 'Champions'
    elif row['recency_score'] >= 3 and row['frequency_score'] >= 3:
        return 'Loyal Customers'
    elif row['recency_score'] >= 3:
        return 'Potential Loyalists'
    elif row['recency_score'] <= 2 and row['frequency_score'] >= 3:
        return 'At Risk'
    else:
        return 'Need Attention'

customer_analysis['rfm_segment'] = customer_analysis.apply(get_rfm_segment, axis=1)

print("\n🎯 Customer Segmentation:")
print(customer_analysis[['total_spent', 'order_count', 'days_since_last_purchase', 'rfm_segment']].head())

# ### 8. Time Series Analysis

print("\n⏰ Time Series Analysis...")

# Resample daily sales
sales_df.set_index('sale_date', inplace=True)
daily_sales_resampled = sales_df['total_amount'].resample('D').sum().fillna(0)

# Calculate moving averages using NumPy
window = 3
daily_sales_resampled['MA_3'] = daily_sales_resampled['total_amount'].rolling(window=window).mean()
daily_sales_resampled['MA_7'] = daily_sales_resampled['total_amount'].rolling(window=7).mean()

# Calculate growth rates
daily_sales_resampled['daily_growth'] = daily_sales_resampled['total_amount'].pct_change() * 100
daily_sales_resampled['weekly_growth'] = daily_sales_resampled['total_amount'].pct_change(periods=7) * 100

print("\n📅 Daily Sales Summary:")
print(daily_sales_resampled.tail())

# ### 9. Advanced Analytics with NumPy

print("\n🧮 Advanced Analytics with NumPy...")

# Correlation analysis
numeric_cols = ['total_amount', 'quantity', 'unit_price', 'price', 'rating']
correlation_data = full_data[numeric_cols].dropna()

if len(correlation_data) > 1:
    correlation_matrix = np.corrcoef(correlation_data.values.T)
    print("\n🔗 Correlation Matrix:")
    correlation_df = pd.DataFrame(correlation_matrix, 
                                 index=numeric_cols, 
                                 columns=numeric_cols)
    print(correlation_df.round(3))

# Outlier detection using Z-score
sales_amounts = sales_df['total_amount'].values
z_scores = np.abs((sales_amounts - np.mean(sales_amounts)) / np.std(sales_amounts))
outliers = sales_amounts[z_scores > 2]

print(f"\n⚠️ Outlier Detection:")
print(f"Total sales: {len(sales_amounts)}")
print(f"Outliers (Z-score > 2): {len(outliers)}")
if len(outliers) > 0:
    print(f"Outlier range: ${outliers.min():,.2f} - ${outliers.max():,.2f}")

# Predictive analytics (simple linear regression example)
print("\n📈 Simple Linear Regression Example:")
# Predicting revenue based on product rating (simplified)
if len(product_performance) > 1:
    X = product_performance['rating'].values.reshape(-1, 1)
    y = product_performance['total_revenue'].values
    
    # Simple linear regression using NumPy
    X_mean = np.mean(X)
    y_mean = np.mean(y)
    
    numerator = np.sum((X - X_mean) * (y - y_mean))
    denominator = np.sum((X - X_mean) ** 2)
    
    if denominator != 0:
        beta = numerator / denominator
        alpha = y_mean - beta * X_mean
        
        print(f"Regression Equation: Revenue = {alpha:.2f} + {beta:.2f} * Rating")
        print(f"Interpretation: For each 1-point increase in rating, revenue increases by ${beta:.2f}")

# ### 10. Data Visualization with Matplotlib

print("\n🎨 Creating visualizations...")

# Create charts directory
os.makedirs('./analytics/charts', exist_ok=True)

# 1. Sales Trend Chart
plt.figure(figsize=(14, 7))
plt.plot(daily_sales_resampled.index, daily_sales_resampled['total_amount'], 
         marker='o', linewidth=2, label='Daily Sales', color='blue')
plt.fill_between(daily_sales_resampled.index, daily_sales_resampled['total_amount'], 
                 alpha=0.2, color='blue')
plt.title('Daily Sales Trend', fontsize=16, fontweight='bold')
plt.xlabel('Date', fontsize=12)
plt.ylabel('Sales Amount ($)', fontsize=12)
plt.grid(True, alpha=0.3)
plt.legend()
plt.tight_layout()
plt.savefig('./analytics/charts/sales_trends.png', dpi=300, bbox_inches='tight')
plt.close()

# 2. Product Revenue Distribution
plt.figure(figsize=(12, 8))
top_products = product_performance.nlargest(10, 'total_revenue')
bars = plt.barh(top_products['name'], top_products['total_revenue'], color='green')
plt.xlabel('Total Revenue ($)', fontsize=12)
plt.title('Top 10 Products by Revenue', fontsize=16, fontweight='bold')
plt.gca().invert_yaxis()

# Add value labels
for bar in bars:
    width = bar.get_width()
    plt.text(width + width*0.01, bar.get_y() + bar.get_height()/2,
             f'${width:,.0f}', ha='left', va='center')

plt.tight_layout()
plt.savefig('./analytics/charts/top_products_revenue.png', dpi=300, bbox_inches='tight')
plt.close()

# 3. Category Distribution
plt.figure(figsize=(10, 10))
category_sales = product_performance.groupby('category')['total_revenue'].sum()
colors = plt.cm.Set3(np.arange(len(category_sales)))
wedges, texts, autotexts = plt.pie(category_sales, labels=category_sales.index,
                                   autopct='%1.1f%%', colors=colors,
                                   startangle=90, textprops={'fontsize': 11})
plt.title('Revenue Distribution by Category', fontsize=16, fontweight='bold', pad=20)
plt.savefig('./analytics/charts/category_distribution.png', dpi=300, bbox_inches='tight')
plt.close()

# 4. Customer Segmentation
plt.figure(figsize=(12, 6))
segment_counts = customer_analysis['rfm_segment'].value_counts()
bars = plt.bar(segment_counts.index, segment_counts.values, 
               color=plt.cm.viridis(np.linspace(0, 1, len(segment_counts))))
plt.xlabel('Customer Segment', fontsize=12)
plt.ylabel('Number of Customers', fontsize=12)
plt.title('Customer Segmentation Distribution', fontsize=16, fontweight='bold')
plt.xticks(rotation=45, ha='right')

# Add value labels
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, height + 0.5,
             f'{int(height)}', ha='center', va='bottom')

plt.tight_layout()
plt.savefig('./analytics/charts/customer_segmentation.png', dpi=300, bbox_inches='tight')
plt.close()

# 5. Payment Method Analysis
plt.figure(figsize=(10, 6))
payment_summary = sales_df.groupby('payment_method').agg({
    'total_amount': 'sum',
    'sale_id': 'count'
}).rename(columns={'total_amount': 'total_revenue', 'sale_id': 'order_count'})

x = np.arange(len(payment_summary))
width = 0.35

fig, ax1 = plt.subplots(figsize=(12, 6))
ax2 = ax1.twinx()

bars1 = ax1.bar(x - width/2, payment_summary['order_count'], width,
                label='Order Count', color='skyblue', alpha=0.8)
bars2 = ax2.bar(x + width/2, payment_summary['total_revenue'], width,
                label='Total Revenue', color='lightcoral', alpha=0.8)

ax1.set_xlabel('Payment Method', fontsize=12)
ax1.set_ylabel('Order Count', fontsize=12, color='skyblue')
ax2.set_ylabel('Total Revenue ($)', fontsize=12, color='lightcoral')

ax1.set_xticks(x)
ax1.set_xticklabels(payment_summary.index, rotation=45, ha='right')
ax1.set_title('Payment Method Performance', fontsize=16, fontweight='bold')

ax1.legend(loc='upper left')
ax2.legend(loc='upper right')

plt.tight_layout()
plt.savefig('./analytics/charts/payment_analysis.png', dpi=300, bbox_inches='tight')
plt.close()

# 6. Stock vs Revenue Scatter Plot
plt.figure(figsize=(12, 8))
scatter = plt.scatter(product_performance['stock_quantity'],
                     product_performance['total_revenue'],
                     c=product_performance['rating'],
                     s=product_performance['price'] * 2,
                     alpha=0.6, cmap='viridis')

plt.xlabel('Stock Quantity', fontsize=12)
plt.ylabel('Total Revenue ($)', fontsize=12)
plt.title('Stock vs Revenue Analysis', fontsize=16, fontweight='bold')
plt.colorbar(scatter, label='Product Rating')

# Add labels for top performers
top_performers = product_performance.nlargest(3, 'total_revenue')
for idx, row in top_performers.iterrows():
    plt.annotate(row['name'],
                 xy=(row['stock_quantity'], row['total_revenue']),
                 xytext=(5, 5), textcoords='offset points',
                 fontsize=9, fontweight='bold')

plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('./analytics/charts/stock_vs_revenue.png', dpi=300, bbox_inches='tight')
plt.close()

print("✅ Created 6 charts in ./analytics/charts/")

# ### 11. Generate Analytics Report

print("\n📋 Generating Analytics Report...")

# Calculate key metrics
total_revenue = sales_df['total_amount'].sum()
avg_order_value = sales_df['total_amount'].mean()
total_customers = sales_df['customer_email'].nunique()
repeat_customers = customer_analysis[customer_analysis['order_count'] > 1].shape[0]
repeat_rate = (repeat_customers / total_customers * 100) if total_customers > 0 else 0

# Product metrics
top_product = product_performance.loc[product_performance['total_revenue'].idxmax()]
best_category = product_performance.groupby('category')['total_revenue'].sum().idxmax()

# Time metrics
sales_growth = daily_sales_resampled['daily_growth'].mean() if 'daily_growth' in daily_sales_resampled.columns else 0

# Create report
report = f"""
============================================
📊 E-COMMERCE SALES ANALYTICS REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
============================================

📈 EXECUTIVE SUMMARY:
• Total Revenue: ${total_revenue:,.2f}
• Average Order Value: ${avg_order_value:,.2f}
• Total Customers: {total_customers}
• Repeat Customer Rate: {repeat_rate:.1f}%
• Sales Growth Rate: {sales_growth:.1f}% (daily avg)

🏆 TOP PERFORMERS:
• Best Selling Product: {top_product['name']} (${top_product['total_revenue']:,.2f})
• Top Category: {best_category}
• Highest Rated Product: {product_performance.loc[product_performance['rating'].idxmax()]['name']}
• Most Stocked Product: {product_performance.loc[product_performance['stock_quantity'].idxmax()]['name']}

🎯 CUSTOMER INSIGHTS:
• Customer Segments: {customer_analysis['rfm_segment'].value_counts().to_dict()}
• Average Customer Lifetime: {customer_analysis['customer_lifetime_days'].mean():.0f} days
• Days Since Last Purchase (avg): {customer_analysis['days_since_last_purchase'].mean():.0f} days

💰 FINANCIAL METRICS:
• Total Inventory Value: ${product_performance['inventory_value'].sum():,.2f}
• Average Profit Margin: {product_performance['total_revenue'].sum() / product_performance['inventory_value'].sum() * 100:.1f}%
• Payment Method Distribution: {dict(sales_df['payment_method'].value_counts())}

📊 DATA QUALITY:
• Total Records Analyzed: {len(sales_df):,}
• Missing Values: {sales_df.isnull().sum().sum()}
• Data Range: {sales_df['sale_date'].min().date()} to {sales_df['sale_date'].max().date()}

📈 RECOMMENDATIONS:
1. Focus on {best_category} category for maximum revenue
2. Target {customer_analysis['rfm_segment'].value_counts().idxmax()} segment for retention
3. Optimize stock for top {len(top_products)} products
4. Improve {payment_summary['total_revenue'].idxmin()} payment method adoption

📁 CHARTS GENERATED:
• sales_trends.png - Daily sales trends
• top_products_revenue.png - Top 10 products by revenue
• category_distribution.png - Revenue by category
• customer_segmentation.png - Customer segment distribution
• payment_analysis.png - Payment method performance
• stock_vs_revenue.png - Stock quantity vs revenue analysis

============================================
"""

# Save report
with open('./analytics/analytics_report.txt', 'w') as f:
    f.write(report)

print(report)
print("✅ Report saved to ./analytics/analytics_report.txt")

# ### 12. Export Results

print("\n💾 Exporting results...")

# Export DataFrames to CSV
sales_df.to_csv('./analytics/sales_data.csv')
products_df.to_csv('./analytics/products_data.csv')
product_performance.to_csv('./analytics/product_performance.csv')
customer_analysis.to_csv('./analytics/customer_analysis.csv')

# Export summary statistics
summary_stats = {
    'total_revenue': total_revenue,
    'avg_order_value': avg_order_value,
    'total_customers': total_customers,
    'repeat_customers': repeat_customers,
    'repeat_rate': repeat_rate,
    'total_products': len(products_df),
    'total_sales': len(sales_df),
    'data_start_date': sales_df['sale_date'].min().strftime('%Y-%m-%d'),
    'data_end_date': sales_df['sale_date'].max().strftime('%Y-%m-%d')
}

with open('./analytics/summary_stats.json', 'w') as f:
    json.dump(summary_stats, f, indent=4)

print("✅ Results exported to CSV and JSON files")
print("\n" + "="*60)
print("🎉 ANALYSIS COMPLETED SUCCESSFULLY!")
print("="*60)
print(f"📁 Output directory: ./analytics/")
print(f"📊 Charts generated: 6")
print(f"📄 Reports generated: 2")
print(f"💾 Data files exported: 4")
print("="*60)