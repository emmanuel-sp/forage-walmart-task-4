import pandas as pd
import sqlite3

#Data 0
data_0 = pd.read_csv('data/shipping_data_0.csv')
products_0 = data_0['product']
quantity_0 = data_0['product_quantity']
origin_0 = data_0['origin_warehouse']
destination_0 = data_0['destination_store']

#Data 1
data_1 = pd.read_csv('data/shipping_data_1.csv')
data_2 = pd.read_csv('data/shipping_data_2.csv')
quantity_set = {}
for item in data_1['shipment_identifier']:
    if item not in quantity_set:
        quantity_set[item] = 1
    else:
        quantity_set[item] += 1
data_1 = data_1.drop_duplicates()
products_1 = data_1['product']
quantity_1 = []
for value in quantity_set.values():
    quantity_1.append(value)
origin_1 = data_2['origin_warehouse']
destination_1 = data_2['destination_store']

con = sqlite3.connect('shipment_database.db')
cur = con.cursor()
# Add product names to db (avoiding duplicates)
existing_products = set()
for row in cur.execute("SELECT name FROM product"):
    existing_products.add(row[0])

for name in products_0:
    if name not in existing_products:
        cur.execute("INSERT INTO product (name) VALUES (?)", (name,))
        existing_products.add(name)

for name in products_1:
    if name not in existing_products:
        cur.execute("INSERT INTO product (name) VALUES (?)", (name,))
        existing_products.add(name)
        
#Get product ids
ids = cur.execute("SELECT id FROM product")
ids = ids.fetchall()

#Add shipment product_id,quantity, origin, and destination
sqlite_insert = '''INSERT INTO shipment (product_id, quantity, origin, destination) 
# VALUES (?,?,?,?)'''
for i in range(0,20):
    cur.execute(sqlite_insert, ids[i][0], quantity_0[i][0], origin_0[i][0], destination_0[i][0])
    cur.execute(sqlite_insert, ids[i + 20][0], quantity_1[i][0], origin_1[i][0], destination_1[i][0])

con.commit()
con.close()


