from flask import Flask, render_template, request, redirect, session, url_for
from config import get_db_connection
import pandas as pd
import matplotlib.pyplot as plt
import os

app = Flask(__name__)
app.secret_key = "secret_key_123"

# ------------------- الصفحة الرئيسية -------------------
@app.route("/")
def home():
    # كل زائر جديد يبدأ مسجل خروج
    session.pop("username", None)
    return redirect("/products")

# ------------------- تسجيل الدخول -------------------
@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE username=%s AND password=%s", (username, password))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if user:
            session["username"] = user["username"]
            return redirect("/products")
        else:
            return render_template("login.html", error="Invalid credentials")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.pop("username", None)
    return redirect("/products")

# ------------------- عرض المنتجات -------------------
@app.route("/products")
def products():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT product_id, name, category, price FROM products")
    products = cursor.fetchall()
    conn.close()

    # تقسيم المنتجات حسب الكاتيجوري
    categorized = {}
    for item in products:
        cat = item['category'] if item['category'] else 'Uncategorized'
        if cat not in categorized:
            categorized[cat] = []
        categorized[cat].append(item)

    is_logged_in = "username" in session
    return render_template("products.html", categorized=categorized, is_logged_in=is_logged_in)

# ------------------- إضافة منتج -------------------
@app.route("/add-product", methods=["GET","POST"])
def add_product():
    if "username" not in session:
        return redirect("/login")  # فقط admin يمكنه الوصول

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # جلب كل الكاتيجوري الموجودة مسبقًا في قاعدة البيانات
    cursor.execute("SELECT DISTINCT category FROM products")
    categories_list = [row['category'] for row in cursor.fetchall() if row['category']]

    if request.method == "POST":
        name = request.form["name"]
        price = request.form["price"]
        category = request.form["category"]

        # جلب أعلى product_id موجود
        cursor.execute("SELECT MAX(product_id) AS max_id FROM products")
        max_id = cursor.fetchone()["max_id"] or 0
        new_id = max_id + 1

        # إضافة المنتج الجديد
        cursor.execute(
            "INSERT INTO products (product_id, name, category, price) VALUES (%s,%s,%s,%s)",
            (new_id, name, category, price)
        )
        conn.commit()
        cursor.close()
        conn.close()

        return redirect("/products")

    cursor.close()
    conn.close()
    return render_template("add_product.html", categories=categories_list)


# ------------------- داشبورد -------------------
@app.route("/dashboard")
def dashboard():
    if "username" not in session:
        return redirect("/login")

    conn = get_db_connection()
    os.makedirs("static/images", exist_ok=True)

    # كل الرسومات البيانية كما في النسخة السابقة
    # Orders per month
    df_orders = pd.read_sql("SELECT DATE_FORMAT(order_date, '%Y-%m') AS month, COUNT(*) AS orders FROM orders GROUP BY month", conn)
    plt.figure(figsize=(10,4))
    plt.plot(df_orders['month'], df_orders['orders'], marker='o')
    plt.title("Orders per Month")
    plt.xlabel("Month")
    plt.ylabel("Number of Orders")
    plt.tight_layout()
    plt.savefig("static/images/monthly_orders.png")
    plt.close()

    # Top customers
    df_customers = pd.read_sql("""
        SELECT CONCAT(first_name,' ',last_name) AS customer_name, SUM(total_price) AS revenue
        FROM orders o JOIN customers c ON o.customer_id=c.customer_id
        GROUP BY c.customer_id
        ORDER BY revenue DESC
        LIMIT 5
    """, conn)
    plt.figure(figsize=(8,4))
    plt.bar(df_customers["customer_name"], df_customers["revenue"])
    plt.title("Top 5 Customers by Revenue")
    plt.xlabel("Customer")
    plt.ylabel("Revenue")
    plt.tight_layout()
    plt.savefig("static/images/top_customers.png")
    plt.close()

    # Sales by category
    df_category = pd.read_sql("""
        SELECT p.category, SUM(oi.quantity*oi.price) AS revenue
        FROM order_items oi JOIN products p ON oi.product_id=p.product_id
        GROUP BY p.category
    """, conn)
    plt.figure(figsize=(6,6))
    plt.pie(df_category["revenue"], labels=df_category["category"], autopct='%1.1f%%')
    plt.title("Sales by Category")
    plt.savefig("static/images/sales_by_category.png")
    plt.close()

    # Top products
    df_products = pd.read_sql("""
        SELECT p.name AS product_name, SUM(oi.quantity) AS total_quantity
        FROM order_items oi JOIN products p ON oi.product_id=p.product_id
        GROUP BY p.name
        ORDER BY total_quantity DESC
        LIMIT 5
    """, conn)
    plt.figure(figsize=(8,4))
    plt.bar(df_products["product_name"], df_products["total_quantity"])
    plt.title("Top 5 Products")
    plt.xlabel("Product")
    plt.ylabel("Quantity Sold")
    plt.tight_layout()
    plt.savefig("static/images/top_products.png")
    plt.close()

    # Sales trend
    df_sales = pd.read_sql("SELECT DATE(order_date) AS order_date, SUM(total_price) AS total_sales FROM orders GROUP BY DATE(order_date)", conn)
    plt.figure(figsize=(10,4))
    plt.plot(df_sales["order_date"], df_sales["total_sales"], marker='o')
    plt.title("Daily Sales Trend")
    plt.xlabel("Date")
    plt.ylabel("Sales")
    plt.tight_layout()
    plt.savefig("static/images/sales_trend.png")
    plt.close()

    conn.close()
    return render_template("dashboard.html")


if __name__ == "__main__":
    app.run(debug=True)
