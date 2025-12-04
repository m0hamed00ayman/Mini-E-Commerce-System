import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os

# Create sample sales data
dates = pd.date_range('2024-01-01', periods=30, freq='D')
sales = np.random.randn(30).cumsum() + 100
sales = np.abs(sales)  # Ensure positive values

# Create chart
plt.figure(figsize=(14, 7))
plt.plot(dates, sales, marker='o', linewidth=2, color='blue', label='Daily Sales')
plt.fill_between(dates, sales, alpha=0.2, color='blue')

# Customize chart
plt.title('Daily Sales Trend - Last 30 Days', fontsize=16, fontweight='bold')
plt.xlabel('Date', fontsize=12)
plt.ylabel('Sales Amount ($)', fontsize=12)
plt.grid(True, alpha=0.3)
plt.legend()
plt.xticks(rotation=45)

# Add annotations
max_sale = sales.max()
max_date = dates[sales.argmax()]
plt.annotate(f'Peak: ${max_sale:,.0f}', 
             xy=(max_date, max_sale),
             xytext=(max_date + timedelta(days=2), max_sale),
             arrowprops=dict(arrowstyle='->', color='red'),
             fontsize=10, fontweight='bold', color='red')

min_sale = sales.min()
min_date = dates[sales.argmin()]
plt.annotate(f'Low: ${min_sale:,.0f}', 
             xy=(min_date, min_sale),
             xytext=(min_date + timedelta(days=2), min_sale + 10),
             arrowprops=dict(arrowstyle='->', color='green'),
             fontsize=10, fontweight='bold', color='green')

# Calculate and show trend line
z = np.polyfit(range(len(sales)), sales, 1)
p = np.poly1d(z)
trend_line = p(range(len(sales)))
plt.plot(dates, trend_line, 'r--', alpha=0.5, label='Trend Line')

# Add statistics
avg_sales = sales.mean()
plt.axhline(y=avg_sales, color='orange', linestyle='--', alpha=0.5, label=f'Average: ${avg_sales:,.0f}')

plt.tight_layout()

# Create directory if it doesn't exist
os.makedirs('./analytics/charts', exist_ok=True)

# Save chart
plt.savefig('sales_trends.png', dpi=300, bbox_inches='tight')
plt.close()

print("✅ Chart generated: ./analytics/charts/sales_trends.png")