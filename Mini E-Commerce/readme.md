# 🛒 Mini E-Commerce System

A complete, production-ready e-commerce system with full-stack development, data engineering pipelines, and analytics capabilities.

## ✨ Features

### Backend
- **Flask REST API** with 6+ endpoints
- **Dual Database Integration**: SQL Server (OLTP) + MongoDB (NoSQL)
- **Pandas & NumPy** for data processing and calculations
- **JWT Authentication** ready

### Frontend
- **Responsive HTML/CSS** interface
- **Jinja2 templating** for dynamic content
- **5+ Interactive Pages**: Home, Products, Cart, Checkout, Admin

### Data Engineering
- **Apache Airflow DAG** for ETL pipelines
- **DBT** for data transformation
- **PySpark** for big data processing
- **Data Warehouse** with star schema

### Analytics
- **Jupyter Notebook** for data analysis
- **Matplotlib & Seaborn** for visualization
- **Sales trend analysis** and forecasting

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- SQL Server 2019+
- MongoDB 6.0+
- JDK 11+ (for PySpark)

### Installation
```bash
# 1. Clone and setup
git clone <repository>
cd mini_ecommerce_project

# 2. Install dependencies
pip install -r requirements.txt

# 3. Setup environment
cp .env.example .env
# Edit .env with your database credentials

# 4. Initialize databases
# Run backend/database/schema.sql in SQL Server
# Setup MongoDB collections as per mongo_notes.txt

# 5. Run the application
python app.py