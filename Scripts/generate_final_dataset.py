"""
Final, consolidated synthetic data generator for the Retail Sales Lakehouse project.
Single source of truth -- produces all 6 files with guaranteed CustomerID consistency.
"""
import csv
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

categories_products = {
    "Electronics": ["Wireless Mouse", "Bluetooth Speaker", "Laptop Stand", "USB Hub", "Keyboard", "Webcam", "Power Bank", "Smartwatch"],
    "Furniture": ["Office Chair", "Desk Lamp", "Monitor Stand", "Bookshelf", "Study Table", "Filing Cabinet"],
    "Stationery": ["Notebook Set", "Desk Organizer", "Pen Set", "Sticky Notes", "Whiteboard"],
    "Accessories": ["Water Bottle", "Backpack", "Laptop Sleeve", "Travel Mug", "Umbrella"],
    "Home & Kitchen": ["Coffee Maker", "Blender", "Toaster", "Air Purifier", "Table Lamp"]
}
countries_cities = {
    "India": ["Bangalore", "Mumbai", "Delhi", "Chennai", "Pune", "Hyderabad", "Kolkata", "Ahmedabad", "Jaipur", "Kochi"],
    "USA": ["New York", "Chicago", "Boston", "Dallas", "Seattle", "Austin"],
    "UK": ["London", "Manchester", "Leeds", "Birmingham"],
    "Germany": ["Berlin", "Munich", "Hamburg"],
    "Australia": ["Sydney", "Melbourne", "Perth"],
    "Canada": ["Toronto", "Vancouver", "Montreal"],
}
order_statuses = ["Delivered", "Shipped", "Processing", "Cancelled", "Returned"]
payment_modes = ["Credit Card", "Debit Card", "UPI", "Net Banking", "Cash on Delivery"]

NUM_CUSTOMERS = 5000
NUM_HISTORICAL_ORDERS = 100000
OUT = "/mnt/user-data/outputs/"

# ============================================================
# STEP 1: Canonical customer pool (single source of truth)
# ============================================================
six_digit_ids = random.sample(range(100000, 999999), NUM_CUSTOMERS)
customers = []
for cust_id in six_digit_ids:
    country = random.choice(list(countries_cities.keys()))
    city = random.choice(countries_cities[country])
    customers.append({
        "customer_id": cust_id,
        "customer_name": fake.name(),
        "email": fake.unique.email(),
        "country": country,
        "city": city,
        "customer_since": fake.date_between(start_date=datetime(2023, 1, 1), end_date=datetime(2025, 8, 31))
    })
existing_ids = {c["customer_id"] for c in customers}

# ============================================================
# STEP 2: customer_master.csv (Day 0 baseline for dim_customer)
# ============================================================
MASTER_HEADER = ["CustomerID", "CustomerName", "Email", "City", "Country", "CustomerSince"]
with open(OUT + "customer_master.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(MASTER_HEADER)
    for c in customers:
        writer.writerow([c["customer_id"], c["customer_name"], c["email"], c["city"], c["country"],
                          c["customer_since"].strftime("%Y-%m-%d")])

# ============================================================
# STEP 3: customer_profile_changes.csv (full-row snapshot: updates + new customers)
# ============================================================
CHANGE_HEADER = MASTER_HEADER  # identical schema by design
updated_customers = random.sample(customers, 60)
change_rows = []
for c in updated_customers:
    change_type = random.choice(["CityChange", "EmailChange", "Both"])
    new_city, new_country, new_email = c["city"], c["country"], c["email"]
    if change_type in ("CityChange", "Both"):
        new_country = random.choice(list(countries_cities.keys()))
        new_city = random.choice(countries_cities[new_country])
    if change_type in ("EmailChange", "Both"):
        new_email = fake.email()
    change_rows.append([c["customer_id"], c["customer_name"], new_email, new_city, new_country,
                         c["customer_since"].strftime("%Y-%m-%d")])

new_ids = random.sample([i for i in range(100000, 999999) if i not in existing_ids], 25)
for nid in new_ids:
    country = random.choice(list(countries_cities.keys()))
    city = random.choice(countries_cities[country])
    signup_date = fake.date_between(start_date=datetime(2026, 8, 25), end_date=datetime(2026, 8, 30))
    change_rows.append([nid, fake.name(), fake.email(), city, country, signup_date.strftime("%Y-%m-%d")])

random.shuffle(change_rows)
with open(OUT + "customer_profile_changes.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(CHANGE_HEADER)
    writer.writerows(change_rows)

# ============================================================
# STEP 4: Orders -- line-item grain, order-only columns (no customer attributes)
# ============================================================
def random_price(category):
    ranges = {"Electronics": (299, 4999), "Furniture": (799, 5999), "Stationery": (49, 999),
              "Accessories": (149, 2999), "Home & Kitchen": (999, 7999)}
    lo, hi = ranges[category]
    return round(random.uniform(lo, hi), 2)

LINE_HEADER = ["OrderLineID", "OrderID", "LineNumber", "CustomerID", "OrderDate", "LastModifiedTS",
               "Product", "Category", "Quantity", "UnitPrice", "PaymentMode", "OrderStatus"]

def make_order_lines(order_id, order_date, last_modified, status, line_id_start):
    num_lines = random.choices([1, 2, 3, 4], weights=[40, 35, 15, 10])[0]
    cust = random.choice(customers)
    payment = random.choice(payment_modes)
    lines = []
    for line_num in range(1, num_lines + 1):
        category = random.choice(list(categories_products.keys()))
        product = random.choice(categories_products[category])
        qty = random.randint(1, 5)
        price = random_price(category)
        lines.append([line_id_start + line_num - 1, order_id, line_num, cust["customer_id"],
                      order_date.strftime("%Y-%m-%d"), last_modified.strftime("%Y-%m-%d %H:%M:%S"),
                      product, category, qty, price, payment, status])
    return lines

start_date, end_date = datetime(2025, 9, 1), datetime(2026, 8, 27)
date_range_days = (end_date - start_date).days

order_id_counter, line_id_counter = 500001, 800001
historical_rows = []
order_id_to_lines = {}
for _ in range(NUM_HISTORICAL_ORDERS):
    order_date = start_date + timedelta(days=random.randint(0, date_range_days))
    last_modified = order_date + timedelta(hours=random.randint(0, 48))
    status = random.choices(order_statuses, weights=[55, 20, 10, 10, 5])[0]
    lines = make_order_lines(order_id_counter, order_date, last_modified, status, line_id_counter)
    historical_rows.extend(lines)
    order_id_to_lines[order_id_counter] = lines
    line_id_counter += len(lines)
    order_id_counter += 1

with open(OUT + "orders_historical.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(LINE_HEADER)
    writer.writerows(historical_rows)

# Incremental daily batches
incremental_days = [datetime(2026, 8, 28), datetime(2026, 8, 29), datetime(2026, 8, 30)]
sample_existing_order_ids = random.sample(list(order_id_to_lines.keys()), 300)

for i, day in enumerate(incremental_days, start=1):
    batch_rows = []
    new_orders_count = random.randint(300, 450)
    for _ in range(new_orders_count):
        last_modified = day + timedelta(hours=random.randint(0, 23))
        status = random.choices(order_statuses, weights=[55, 20, 10, 10, 5])[0]
        lines = make_order_lines(order_id_counter, day, last_modified, status, line_id_counter)
        batch_rows.extend(lines)
        line_id_counter += len(lines)
        order_id_counter += 1

    updated_order_ids = sample_existing_order_ids[(i-1)*100: i*100]
    for oid in updated_order_ids:
        original_lines = order_id_to_lines.get(oid)
        if original_lines:
            new_status = random.choice(["Delivered", "Shipped", "Returned", "Cancelled"])
            for line in original_lines:
                updated_line = line.copy()
                updated_line[11] = new_status
                updated_line[5] = day.strftime("%Y-%m-%d %H:%M:%S")
                batch_rows.append(updated_line)

    random.shuffle(batch_rows)
    with open(OUT + f"orders_incremental_day{i}.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(LINE_HEADER)
        writer.writerows(batch_rows)

# ============================================================
# STEP 5: Sanity checks
# ============================================================
master_ids = {c["customer_id"] for c in customers}
orders_ids = {row[3] for row in historical_rows}
change_existing_ids = {r[0] for r in change_rows if r[0] in master_ids}
change_new_ids = {r[0] for r in change_rows if r[0] not in master_ids}

print("=== FINAL DATASET SUMMARY ===")
print(f"customer_master.csv:            {len(customers):>7} customers")
print(f"customer_profile_changes.csv:   {len(change_rows):>7} rows  ({len(change_existing_ids)} updates, {len(change_new_ids)} new customers)")
print(f"orders_historical.csv:          {len(historical_rows):>7} lines across {len(order_id_to_lines)} orders")
for i in range(1, 4):
    with open(OUT + f"orders_incremental_day{i}.csv") as f:
        n = sum(1 for _ in f) - 1
    print(f"orders_incremental_day{i}.csv:   {n:>7} rows")
print(f"\nSanity -- orders CustomerIDs not in master: {len(orders_ids - master_ids)}")
print(f"Sanity -- change-feed 'update' rows not matching an existing master ID: {60 - len(change_existing_ids)}")
print(f"Sanity -- change-feed 'new' rows colliding with an existing master ID: {25 - len(change_new_ids)}")
