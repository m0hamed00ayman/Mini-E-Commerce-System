"""
PySpark Job for Sales Analytics
Calculates best-selling products and average purchase value
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os
import sys

class SparkSalesAnalytics:
    def __init__(self):
        """Initialize Spark session"""
        self.spark = SparkSession.builder \
            .appName("ECommerceSalesAnalytics") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "50") \
            .getOrCreate()
        
        self.output_dir = "./analytics/output"
        self.charts_dir = "./analytics/charts"
        
        # Create directories if they don't exist
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.charts_dir, exist_ok=True)
    
    def load_data(self):
        """Load sales data from SQL Server"""
        try:
            # JDBC connection properties
            jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=ECommerceDB"
            properties = {
                "user": "sa",
                "password": "YourPassword123",
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            }
            
            print("📥 Loading data from SQL Server...")
            
            # Load sales data
            sales_df = self.spark.read.jdbc(
                url=jdbc_url,
                table="sales",
                properties=properties
            )
            
            # Load sale items
            sale_items_df = self.spark.read.jdbc(
                url=jdbc_url,
                table="sale_items",
                properties=properties
            )
            
            # Load products
            products_df = self.spark.read.jdbc(
                url=jdbc_url,
                table="products",
                properties=properties
            )
            
            print(f"✅ Data loaded successfully:")
            print(f"   - Sales records: {sales_df.count():,}")
            print(f"   - Sale items: {sale_items_df.count():,}")
            print(f"   - Products: {products_df.count():,}")
            
            return sales_df, sale_items_df, products_df
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            sys.exit(1)
    
    def calculate_best_selling_products(self, sale_items_df, products_df):
        """Calculate best-selling products"""
        print("\n📊 Calculating best-selling products...")
        
        # Join sale items with products
        sales_with_products = sale_items_df.join(
            products_df,
            sale_items_df.product_id == products_df.id,
            "inner"
        )
        
        # Aggregate sales by product
        product_sales = sales_with_products.groupBy(
            "product_id",
            "name",
            "category",
            "price"
        ).agg(
            sum("quantity").alias("total_quantity_sold"),
            sum("total_price").alias("total_revenue"),
            countDistinct("sale_id").alias("order_count"),
            avg("unit_price").alias("avg_selling_price"),
            sum("discount_amount").alias("total_discount")
        )
        
        # Calculate additional metrics
        product_sales = product_sales.withColumn(
            "avg_order_quantity",
            col("total_quantity_sold") / col("order_count")
        ).withColumn(
            "discount_percentage",
            (col("total_discount") / (col("total_revenue") + col("total_discount"))) * 100
        )
        
        # Rank products by revenue
        window_spec = Window.orderBy(col("total_revenue").desc())
        product_sales = product_sales.withColumn(
            "revenue_rank",
            rank().over(window_spec)
        )
        
        # Rank products by quantity
        window_spec_qty = Window.orderBy(col("total_quantity_sold").desc())
        product_sales = product_sales.withColumn(
            "quantity_rank",
            rank().over(window_spec_qty)
        )
        
        # Calculate performance score (weighted ranking)
        product_sales = product_sales.withColumn(
            "performance_score",
            (100 / col("revenue_rank")) * 0.6 + (100 / col("quantity_rank")) * 0.4
        )
        
        # Order by performance score
        best_selling_products = product_sales.orderBy(col("performance_score").desc())
        
        # Show top 20 products
        print("\n🏆 Top 20 Best-Selling Products:")
        best_selling_products.select(
            "name",
            "category",
            "total_quantity_sold",
            "total_revenue",
            "order_count",
            "revenue_rank",
            "quantity_rank",
            "performance_score"
        ).show(20, truncate=False)
        
        return best_selling_products
    
    def calculate_sales_metrics(self, sales_df, sale_items_df):
        """Calculate average purchase value and other sales metrics"""
        print("\n💰 Calculating sales metrics...")
        
        # Join sales with sale items to get complete order details
        sales_with_items = sales_df.join(
            sale_items_df,
            sales_df.id == sale_items_df.sale_id,
            "inner"
        )
        
        # Calculate metrics at order level
        order_metrics = sales_with_items.groupBy("sale_id").agg(
            sum("total_price").alias("order_total"),
            sum("quantity").alias("total_items"),
            countDistinct("product_id").alias("unique_products"),
            avg("unit_price").alias("avg_item_price"),
            sum("discount_amount").alias("total_discount")
        )
        
        # Calculate overall metrics
        overall_metrics = order_metrics.agg(
            avg("order_total").alias("avg_order_value"),
            stddev("order_total").alias("std_order_value"),
            min("order_total").alias("min_order_value"),
            max("order_total").alias("max_order_value"),
            sum("order_total").alias("total_revenue"),
            count("sale_id").alias("total_orders"),
            avg("total_items").alias("avg_items_per_order"),
            avg("unique_products").alias("avg_products_per_order")
        )
        
        # Show overall metrics
        print("\n📈 Overall Sales Metrics:")
        overall_metrics.show(truncate=False)
        
        # Calculate metrics by payment method
        payment_metrics = sales_df.groupBy("payment_method").agg(
            count("*").alias("order_count"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value"),
            stddev("total_amount").alias("std_order_value")
        ).orderBy(col("total_revenue").desc())
        
        print("\n💳 Metrics by Payment Method:")
        payment_metrics.show(truncate=False)
        
        # Calculate daily trends
        sales_df_with_date = sales_df.withColumn(
            "sale_date_formatted",
            date_format(col("sale_date"), "yyyy-MM-dd")
        )
        
        daily_sales = sales_df_with_date.groupBy("sale_date_formatted").agg(
            count("*").alias("daily_orders"),
            sum("total_amount").alias("daily_revenue"),
            avg("total_amount").alias("avg_daily_order_value")
        ).orderBy("sale_date_formatted")
        
        print("\n📅 Daily Sales Trends (Sample):")
        daily_sales.show(10, truncate=False)
        
        return {
            'overall_metrics': overall_metrics,
            'payment_metrics': payment_metrics,
            'daily_sales': daily_sales
        }
    
    def customer_segmentation(self, sales_df):
        """Perform customer segmentation analysis"""
        print("\n👥 Performing customer segmentation...")
        
        # Aggregate customer data
        customer_metrics = sales_df.groupBy("customer_email").agg(
            count("*").alias("order_count"),
            sum("total_amount").alias("total_spent"),
            avg("total_amount").alias("avg_order_value"),
            min("sale_date").alias("first_purchase"),
            max("sale_date").alias("last_purchase"),
            datediff(current_date(), max("sale_date")).alias("days_since_last_purchase")
        )
        
        # Calculate RFM scores
        customer_metrics = customer_metrics.withColumn(
            "recency_score",
            when(col("days_since_last_purchase") <= 7, 5)
            .when(col("days_since_last_purchase") <= 30, 4)
            .when(col("days_since_last_purchase") <= 90, 3)
            .when(col("days_since_last_purchase") <= 180, 2)
            .otherwise(1)
        ).withColumn(
            "frequency_score",
            when(col("order_count") >= 10, 5)
            .when(col("order_count") >= 5, 4)
            .when(col("order_count") >= 3, 3)
            .when(col("order_count") >= 2, 2)
            .otherwise(1)
        ).withColumn(
            "monetary_score",
            when(col("total_spent") >= 1000, 5)
            .when(col("total_spent") >= 500, 4)
            .when(col("total_spent") >= 200, 3)
            .when(col("total_spent") >= 100, 2)
            .otherwise(1)
        )
        
        # Create RFM segment
        customer_metrics = customer_metrics.withColumn(
            "rfm_segment",
            when((col("recency_score") >= 4) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4), "Champions")
            .when((col("recency_score") >= 3) & (col("frequency_score") >= 3) & (col("monetary_score") >= 3), "Loyal Customers")
            .when((col("recency_score") >= 3) & (col("frequency_score") >= 2), "Potential Loyalists")
            .when((col("recency_score") >= 3) & (col("frequency_score") <= 2), "Recent Customers")
            .when((col("recency_score") <= 2) & (col("frequency_score") >= 3), "At Risk")
            .when((col("recency_score") <= 2) & (col("frequency_score") <= 2) & (col("monetary_score") >= 3), "Cant Lose Them")
            .when((col("recency_score") <= 2) & (col("frequency_score") <= 2) & (col("monetary_score") <= 2), "Lost")
            .otherwise("Need Attention")
        )
        
        # Segment distribution
        segment_distribution = customer_metrics.groupBy("rfm_segment").agg(
            count("*").alias("customer_count"),
            avg("total_spent").alias("avg_spent"),
            avg("order_count").alias("avg_orders")
        ).orderBy(col("customer_count").desc())
        
        print("\n🎯 Customer Segmentation Distribution:")
        segment_distribution.show(truncate=False)
        
        return customer_metrics, segment_distribution
    
    def time_series_analysis(self, sales_df):
        """Perform time series analysis of sales"""
        print("\n⏰ Performing time series analysis...")
        
        # Extract time components
        sales_with_time = sales_df.withColumn(
            "sale_hour", hour(col("sale_date"))
        ).withColumn(
            "sale_dayofweek", dayofweek(col("sale_date"))
        ).withColumn(
            "sale_month", month(col("sale_date"))
        ).withColumn(
            "sale_quarter", quarter(col("sale_date"))
        )
        
        # Hourly analysis
        hourly_sales = sales_with_time.groupBy("sale_hour").agg(
            count("*").alias("order_count"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value")
        ).orderBy("sale_hour")
        
        print("\n🕒 Hourly Sales Analysis:")
        hourly_sales.show(24, truncate=False)
        
        # Day of week analysis
        weekday_sales = sales_with_time.groupBy("sale_dayofweek").agg(
            count("*").alias("order_count"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value")
        ).orderBy("sale_dayofweek")
        
        print("\n📅 Day of Week Analysis:")
        weekday_sales.show(7, truncate=False)
        
        # Monthly analysis
        monthly_sales = sales_with_time.groupBy("sale_month").agg(
            count("*").alias("order_count"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order_value")
        ).orderBy("sale_month")
        
        print("\n📊 Monthly Sales Analysis:")
        monthly_sales.show(12, truncate=False)
        
        return {
            'hourly_sales': hourly_sales,
            'weekday_sales': weekday_sales,
            'monthly_sales': monthly_sales
        }
    
    def generate_pandas_analytics(self, spark_dataframes):
        """Generate analytics using Pandas and create visualizations"""
        print("\n📈 Generating advanced analytics with Pandas...")
        
        try:
            # Convert Spark DataFrames to Pandas
            best_selling_pd = spark_dataframes['best_selling'].toPandas()
            overall_metrics_pd = spark_dataframes['overall_metrics'].toPandas()
            payment_metrics_pd = spark_dataframes['payment_metrics'].toPandas()
            segment_dist_pd = spark_dataframes['segment_distribution'].toPandas()
            hourly_sales_pd = spark_dataframes['hourly_sales'].toPandas()
            
            # Set style for charts
            plt.style.use('seaborn-v0_8-darkgrid')
            sns.set_palette("husl")
            
            # 1. Top Products Bar Chart
            plt.figure(figsize=(14, 8))
            top_20 = best_selling_pd.head(20)
            bars = plt.barh(top_20['name'], top_20['total_revenue'], color='skyblue')
            plt.xlabel('Total Revenue ($)')
            plt.title('Top 20 Products by Revenue', fontsize=16, fontweight='bold')
            plt.gca().invert_yaxis()
            
            # Add value labels
            for bar in bars:
                width = bar.get_width()
                plt.text(width + width*0.01, bar.get_y() + bar.get_height()/2,
                        f'${width:,.0f}', ha='left', va='center')
            
            plt.tight_layout()
            plt.savefig(f'{self.charts_dir}/top_products_revenue.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            # 2. Sales by Category Pie Chart
            category_revenue = best_selling_pd.groupby('category')['total_revenue'].sum().sort_values(ascending=False)
            
            plt.figure(figsize=(10, 10))
            colors = plt.cm.Set3(np.arange(len(category_revenue)))
            wedges, texts, autotexts = plt.pie(category_revenue, labels=category_revenue.index,
                                              autopct='%1.1f%%', colors=colors,
                                              startangle=90, textprops={'fontsize': 12})
            
            plt.title('Revenue Distribution by Category', fontsize=16, fontweight='bold', pad=20)
            plt.savefig(f'{self.charts_dir}/revenue_by_category.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            # 3. Hourly Sales Trend
            plt.figure(figsize=(14, 7))
            plt.plot(hourly_sales_pd['sale_hour'], hourly_sales_pd['total_revenue'],
                    marker='o', linewidth=2, markersize=8, color='green')
            plt.fill_between(hourly_sales_pd['sale_hour'], hourly_sales_pd['total_revenue'],
                            alpha=0.2, color='green')
            plt.xlabel('Hour of Day', fontsize=12)
            plt.ylabel('Total Revenue ($)', fontsize=12)
            plt.title('Hourly Sales Revenue Trend', fontsize=16, fontweight='bold')
            plt.grid(True, alpha=0.3)
            plt.xticks(range(0, 24))
            plt.tight_layout()
            plt.savefig(f'{self.charts_dir}/hourly_sales_trend.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            # 4. Customer Segmentation Bar Chart
            plt.figure(figsize=(12, 6))
            segment_dist_pd = segment_dist_pd.sort_values('customer_count', ascending=False)
            bars = plt.bar(segment_dist_pd['rfm_segment'], segment_dist_pd['customer_count'],
                          color=plt.cm.viridis(np.linspace(0, 1, len(segment_dist_pd))))
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
            plt.savefig(f'{self.charts_dir}/customer_segmentation.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            # 5. Payment Method Analysis
            plt.figure(figsize=(10, 6))
            payment_data = payment_metrics_pd.set_index('payment_method')
            x = np.arange(len(payment_data))
            width = 0.35
            
            fig, ax1 = plt.subplots(figsize=(12, 6))
            ax2 = ax1.twinx()
            
            bars1 = ax1.bar(x - width/2, payment_data['order_count'], width,
                           label='Order Count', color='skyblue', alpha=0.8)
            bars2 = ax2.bar(x + width/2, payment_data['total_revenue'], width,
                           label='Total Revenue', color='lightcoral', alpha=0.8)
            
            ax1.set_xlabel('Payment Method', fontsize=12)
            ax1.set_ylabel('Order Count', fontsize=12, color='skyblue')
            ax2.set_ylabel('Total Revenue ($)', fontsize=12, color='lightcoral')
            
            ax1.set_xticks(x)
            ax1.set_xticklabels(payment_data.index, rotation=45, ha='right')
            ax1.set_title('Payment Method Performance', fontsize=16, fontweight='bold')
            
            # Add legends
            ax1.legend(loc='upper left')
            ax2.legend(loc='upper right')
            
            plt.tight_layout()
            plt.savefig(f'{self.charts_dir}/payment_method_analysis.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            # 6. Product Performance Scatter Plot
            plt.figure(figsize=(12, 8))
            scatter = plt.scatter(best_selling_pd['total_quantity_sold'],
                                 best_selling_pd['total_revenue'],
                                 c=best_selling_pd['order_count'],
                                 s=best_selling_pd['avg_selling_price'] * 10,
                                 alpha=0.6, cmap='viridis')
            
            plt.xlabel('Total Quantity Sold', fontsize=12)
            plt.ylabel('Total Revenue ($)', fontsize=12)
            plt.title('Product Performance Analysis', fontsize=16, fontweight='bold')
            plt.colorbar(scatter, label='Order Count')
            
            # Add annotations for top performers
            top_performers = best_selling_pd.nlargest(5, 'total_revenue')
            for idx, row in top_performers.iterrows():
                plt.annotate(row['name'],
                            xy=(row['total_quantity_sold'], row['total_revenue']),
                            xytext=(5, 5), textcoords='offset points',
                            fontsize=9, fontweight='bold')
            
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(f'{self.charts_dir}/product_performance_scatter.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"✅ Generated 6 charts in {self.charts_dir}/")
            
            # Generate analytics report using NumPy
            self.generate_numpy_report(best_selling_pd, overall_metrics_pd)
            
        except Exception as e:
            print(f"❌ Error in Pandas analytics: {e}")
            import traceback
            traceback.print_exc()
    
    def generate_numpy_report(self, products_df, metrics_df):
        """Generate advanced analytics report using NumPy"""
        print("\n🧮 Generating NumPy analytics report...")
        
        try:
            # Calculate statistics using NumPy
            revenue = products_df['total_revenue'].values
            quantity = products_df['total_quantity_sold'].values
            prices = products_df['avg_selling_price'].values
            
            # Basic statistics
            revenue_stats = {
                'mean': np.mean(revenue),
                'median': np.median(revenue),
                'std': np.std(revenue),
                'min': np.min(revenue),
                'max': np.max(revenue),
                'total': np.sum(revenue),
                'q1': np.percentile(revenue, 25),
                'q3': np.percentile(revenue, 75),
                'skewness': float(pd.Series(revenue).skew()),
                'kurtosis': float(pd.Series(revenue).kurtosis())
            }
            
            # Correlation analysis
            correlation_matrix = np.corrcoef([revenue, quantity, prices])
            
            # Outlier detection using Z-score
            z_scores = np.abs((revenue - np.mean(revenue)) / np.std(revenue))
            outliers = revenue[z_scores > 3]
            
            # Performance segments
            revenue_segments = {
                'high_performers': revenue[revenue > np.percentile(revenue, 75)],
                'medium_performers': revenue[(revenue >= np.percentile(revenue, 25)) & 
                                            (revenue <= np.percentile(revenue, 75))],
                'low_performers': revenue[revenue < np.percentile(revenue, 25)]
            }
            
            # Generate report
            report = f"""
            ============================================
            NUMPY ANALYTICS REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            ============================================
            
            📊 REVENUE STATISTICS:
            • Mean Revenue: ${revenue_stats['mean']:,.2f}
            • Median Revenue: ${revenue_stats['median']:,.2f}
            • Std Deviation: ${revenue_stats['std']:,.2f}
            • Total Revenue: ${revenue_stats['total']:,.2f}
            • Revenue Range: ${revenue_stats['min']:,.2f} - ${revenue_stats['max']:,.2f}
            • Interquartile Range: ${revenue_stats['q1']:,.2f} - ${revenue_stats['q3']:,.2f}
            • Skewness: {revenue_stats['skewness']:.3f}
            • Kurtosis: {revenue_stats['kurtosis']:.3f}
            
            📈 CORRELATION ANALYSIS:
            Revenue vs Quantity: {correlation_matrix[0,1]:.3f}
            Revenue vs Price: {correlation_matrix[0,2]:.3f}
            Quantity vs Price: {correlation_matrix[1,2]:.3f}
            
            ⚠️ OUTLIER DETECTION:
            • Outliers detected (Z-score > 3): {len(outliers)} products
            • Outlier revenue range: ${np.min(outliers):,.2f} - ${np.max(outliers):,.2f}
            
            🎯 PERFORMANCE SEGMENTS:
            • High Performers (>75th percentile): {len(revenue_segments['high_performers'])} products
            • Medium Performers (25th-75th): {len(revenue_segments['medium_performers'])} products
            • Low Performers (<25th percentile): {len(revenue_segments['low_performers'])} products
            
            🔢 PRODUCT COUNT STATISTICS:
            • Total Products Analyzed: {len(products_df)}
            • Products with Revenue > $1,000: {np.sum(revenue > 1000)}
            • Products with Quantity > 100 units: {np.sum(quantity > 100)}
            • Average Price per Product: ${np.mean(prices):.2f}
            
            ============================================
            """
            
            # Save report
            report_path = f"{self.output_dir}/numpy_analytics_report.txt"
            with open(report_path, 'w') as f:
                f.write(report)
            
            print(f"✅ NumPy analytics report saved to: {report_path}")
            print(report)
            
        except Exception as e:
            print(f"❌ Error in NumPy report generation: {e}")
    
    def save_results_to_csv(self, dataframes):
        """Save analysis results to CSV files"""
        print("\n💾 Saving results to CSV files...")
        
        try:
            # Save best-selling products
            best_selling_pd = dataframes['best_selling'].toPandas()
            best_selling_pd.to_csv(f'{self.output_dir}/best_selling_products.csv', index=False)
            
            # Save overall metrics
            overall_metrics_pd = dataframes['overall_metrics'].toPandas()
            overall_metrics_pd.to_csv(f'{self.output_dir}/overall_sales_metrics.csv', index=False)
            
            # Save customer segments
            segment_dist_pd = dataframes['segment_distribution'].toPandas()
            segment_dist_pd.to_csv(f'{self.output_dir}/customer_segments.csv', index=False)
            
            # Save time series data
            hourly_sales_pd = dataframes['hourly_sales'].toPandas()
            hourly_sales_pd.to_csv(f'{self.output_dir}/hourly_sales.csv', index=False)
            
            print(f"✅ Results saved to {self.output_dir}/")
            
        except Exception as e:
            print(f"❌ Error saving CSV files: {e}")
    
    def run_full_analysis(self):
        """Run complete sales analysis pipeline"""
        print("=" * 60)
        print("🚀 STARTING E-COMMERCE SALES ANALYSIS")
        print("=" * 60)
        
        start_time = datetime.now()
        
        try:
            # 1. Load data
            sales_df, sale_items_df, products_df = self.load_data()
            
            # 2. Calculate best-selling products
            best_selling = self.calculate_best_selling_products(sale_items_df, products_df)
            
            # 3. Calculate sales metrics
            sales_metrics = self.calculate_sales_metrics(sales_df, sale_items_df)
            
            # 4. Customer segmentation
            customer_metrics, segment_distribution = self.customer_segmentation(sales_df)
            
            # 5. Time series analysis
            time_series = self.time_series_analysis(sales_df)
            
            # Prepare data for Pandas analytics
            spark_dataframes = {
                'best_selling': best_selling,
                'overall_metrics': sales_metrics['overall_metrics'],
                'payment_metrics': sales_metrics['payment_metrics'],
                'segment_distribution': segment_distribution,
                'hourly_sales': time_series['hourly_sales']
            }
            
            # 6. Generate Pandas analytics and charts
            self.generate_pandas_analytics(spark_dataframes)
            
            # 7. Save results to CSV
            self.save_results_to_csv(spark_dataframes)
            
            # Calculate execution time
            end_time = datetime.now()
            execution_time = end_time - start_time
            
            print("\n" + "=" * 60)
            print("✅ ANALYSIS COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            print(f"⏱️  Total Execution Time: {execution_time}")
            print(f"📁 Output Directory: {self.output_dir}/")
            print(f"📊 Charts Directory: {self.charts_dir}/")
            print("=" * 60)
            
        except Exception as e:
            print(f"\n❌ Analysis failed: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        finally:
            # Stop Spark session
            self.spark.stop()
            print("🔚 Spark session stopped.")

# Main execution
if __name__ == "__main__":
    # Initialize and run analytics
    analyzer = SparkSalesAnalytics()
    analyzer.run_full_analysis()