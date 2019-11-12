import psycopg2
from urllib.parse import urlparse, uses_netloc


def initialize():
    # this function will get called once, when the application starts.
    


    uses_netloc.append("postgres")
    url = urlparse("postgres://mcpxvutz:dFoRNsFXOVp4GveucIsC598ocShMxGRg@isilo.db.elephantsql.com:5432/mcpxvutz")
    

    conn = psycopg2.connect(database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port)

    with conn.cursor() as cursor:
        #cursor.execute('drop table if exists customers; drop table if exists orders; drop table if exists products;')
        cursor.execute("CREATE TABLE if NOT EXISTS customers (id  serial PRIMARY KEY, firstName Varchar(15), lastName Varchar(15), street Varchar(35), city Varchar(15), state VarChar(15), zip Text)")
        cursor.execute("CREATE TABLE if not exists products (id serial PRIMARY KEY , name Varchar(15), price INTEGER)")
        cursor.execute("CREATE TABLE if not exists orders (id  serial  PRIMARY KEY, customerId INTEGER, productId INTEGER, date Varchar (10), Foreign key(customerId) References customers (id) ON Update Cascade ON Delete Cascade, Foreign key(productID) References Products(id) ON Update Cascade ON Delete Cascade)")
        

    conn.commit()

    return conn
  
def get_customers():
    with conn.cursor() as cursor:
        cursor.execute("Select * From customers")
        for each in cursor:
            customerDetails = {'id':each[0], 'firstName':each[1], 'lastName': each[2], 'street': each[3], 'city': each[4], 'state': each[5], 'zip': each[6]}
            yield customerDetails
    
    
    
def get_customer(id):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * from customers where id = %s",(id,))
        results=cursor.fetchone()
        customer = {'id':results[0], 'firstName': results[1], 'lastName': results[2], 'street':results[3], 'city':results[4], 'state':results[5], 'zip':results[6]}
        return customer
          

def upsert_customer(customer):
    with conn.cursor() as cursor:
        if 'id' in customer:
            cursor.execute("UPDATE customers SET firstName = %s,lastName= %s,street=%s,city=%s,state=%s,zip=%s where id = %s",(customer['firstName'],customer['lastName'],customer['street'],customer['city'],customer['state'],customer['zip'],customer['id']))
        else:
            cursor.execute("INSERT INTO customers(firstName,lastName,street,city,state,zip) VALUES(%s,%s,%s,%s,%s,%s)",(customer['firstName'],customer['lastName'],customer['street'],customer['city'],customer['state'],customer['zip']))


def delete_customer(id):
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM Customers where id = %s",(id,))
    
    
def get_products():
    with conn.cursor() as cursor:
        cursor.execute("Select * From Products")
        for each in cursor:
            productDetails = {'id':each[0], 'name':each[1], 'price': each[2]}
            yield productDetails
    

def get_product(id):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * from Products where id = %s",(id,))
        results=cursor.fetchone()
        product={'id':results[0], 'name':results[1], 'price':results[2]}
        return product

def upsert_product(product): 
    with conn.cursor() as cursor:
        if 'id' in product:
            cursor.execute("UPDATE Products SET name=%s, price=%s WHERE id=%s", (product['name'], product['price'], product['id']))
        else:
            cursor.execute("INSERT INTO Products(name, price) Values(%s,%s)", (product['name'], product['price']))
    

def delete_product(id):
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM Products where id = %s",(id,))
    

def get_orders():
    with conn.cursor() as cursor:
        cursor.execute("Select * From orders")
        for each in cursor:
            orderInfo = {'id':each[0], 'customerId':each[1], 'productId': each[2], 'date': each[3],'customer':get_customer(int(each[1])), 'product':get_product(int(each[2]))}
            yield orderInfo
    

def get_order(id): 
    with conn.cursor() as cursor:
        cursor.execute("SELECT * from orders where id = %s",(id,))
        results=cursor.fetchone()
        order={'id':results[0], 'customerId':results[1], 'productId':results[2],'date':results[3]}
        return order


def upsert_order(order): 
    with conn.cursor() as cursor:
        if 'id' in order:
            cursor.execute("UPDATE Orders SET customerId=%s, productId=%s, date=%s WHERE id=%s", (order['customerId'], order['productId'],order['date'], order['id']))
        else:
            cursor.execute("INSERT INTO Orders(customerId, productId, date) Values(%s,%s, %s)", (order['customerId'], order['productId'], order['date']))
    

def delete_order(id): 
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM orders where id = %s",(id,))
    

def customer_report(id):
    with conn.cursor() as cursor:
        customer = get_customer(id)
        cursor.execute("SELECT * FROM Orders WHERE customerId=%s", (id,))
        cust_order = []
        report=[]
        for each in cursor:
            cust_order = {'id':each[0], 'customerId':each[1], 'productId':each[2], 'date':each[3]}
            cust_order['product']= get_product(cust_order['productId'])
            report.append(cust_order)
        customer['orders']=report
    return customer
    
    
def sales_report():
    with conn.cursor() as cursor:
        cursor.execute("SELECT name, count(Products.id),price,max(date) FROM Products JOIN Orders ON Products.id=Orders.productId GROUP BY Products.id")
        for each in cursor:
            sales_report= {'name':each[0], 'total_sales':each[1], 'gross_revenue':each[2]*each[1], 'last_order_date':each[3]}
            yield sales_report
    
conn=initialize()

