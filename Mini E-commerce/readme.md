# Mini E-Commerce Project

## Project Description
This is a mini E-Commerce system that allows displaying products by category, adding new products (Admin only), and viewing a sales dashboard with charts.  
The application is built with Flask and uses MySQL as the database.

---

## Project Structure

```
Mini-Ecommerce/
├─ app.py
├─ config.py
├─ requirements.txt
├─ README.md
├─ templates/
│   ├─ dashboard.html
│   ├─ products.html
│   └─ add_product.html
├─ static/
│   ├─ css/
│   │   └─ style.css
│   ├─ js/
│   │   └─ app.js
│   └─ images/
├─ data/
│   ├─ ecommerce.csv
│   ├─ customers_cleaned.csv
│   ├─ orders_cleaned.csv
│   └─ order_items_cleaned.csv
├─ scripts/
│   └─ Clean_data.ipynb
└─ database_backup/
    └─ mini_ecommerce.sql
```

- `app.py` : Main Flask application file.  
- `config.py` : Database connection settings.  
- `requirements.txt` : Required Python libraries.  
- `README.md` : Instructions to run the project.  
- `templates/` : HTML templates (products, add_product, dashboard).  
- `static/` : CSS, JS, and images.  
- `data/` : Original and cleaned CSV files.  
- `scripts/` : Data processing scripts (Clean_data.ipynb).  
- `database_backup/` : Database backup ready for import.

---

## 1️⃣ Setting Up the Database

1. Open MySQL on your machine.  
2. Create a new database:

```sql
CREATE DATABASE mini_ecommerce;
```

---

## 2️⃣ Restoring the Database

- Restore the database from the backup file:

```bash
mysql -u root -p mini_ecommerce < database_backup/mini_ecommerce.sql
```

> Enter your MySQL password. All tables and data will be restored.

---

## 3️⃣ Installing Requirements

- Make sure Python 3.8+ is installed.  
- Install the required libraries:

```bash
pip install -r requirements.txt
```

Required libraries include:
- Flask
- mysql-connector-python
- pandas
- matplotlib
- Werkzeug

---

## 4️⃣ Running the Application

- To run the local server:

```bash
python app.py
```

- Open your browser and go to:

```
http://127.0.0.1:5000/products
```

---

## 5️⃣ Admin Login

- A default Admin user is included:

```
Username: admin
Password: password
```

- After logging in as Admin:
  - **Add Product** button appears.  
  - **Dashboard** link appears to view charts.  

- If not logged in:
  - Add Product and Dashboard options are hidden.  
  - Users can only view products by category.

---

## 6️⃣ Notes

- Default product images are stored in `static/images`.  
- Products are grouped by categories; clicking a category shows/hides its products.  
- Scripts in `scripts/` are used to convert CSV data to SQL tables and generate visualizations.  
- Anyone receiving the project can restore the database and run the application locally without issues.

---

## 7️⃣ Running the Complete Project

1. Restore the database (`mini_ecommerce.sql`).  
2. Install the requirements (`pip install -r requirements.txt`).  
3. Run `app.py`.  
4. Open the browser at `http://127.0.0.1:5000/`.  
