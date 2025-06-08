# Databricks notebook source
# MAGIC %md
# MAGIC **JOINS --LEFT,RIGHT,INNER,SELF| UNION**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE customers (
# MAGIC     customer_id INT ,
# MAGIC     first_name VARCHAR(50),
# MAGIC     last_name VARCHAR(50),
# MAGIC     city VARCHAR(50),
# MAGIC     email VARCHAR(100)
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE orders (
# MAGIC     order_id INT ,
# MAGIC     customer_id INT,
# MAGIC     order_date DATE,
# MAGIC     total_amount DECIMAL(10, 2)
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE order_details (
# MAGIC     order_detail_id INT ,
# MAGIC     order_id INT,
# MAGIC     customer_id INT,
# MAGIC     product_id INT,
# MAGIC     product_name VARCHAR(100),
# MAGIC     category VARCHAR(50),
# MAGIC     quantity INT,
# MAGIC     price_each DECIMAL(10,2),
# MAGIC     order_date DATE
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO customers VALUES
# MAGIC (1, 'John', 'Doe', 'Delhi', 'john.doe@example.com'),
# MAGIC (2, 'Jane', 'Smith', 'Mumbai', 'jane.smith@example.com'),
# MAGIC (3, 'Raj', 'Kumar', 'Bangalore', 'raj.kumar@example.com'),
# MAGIC (4, 'Priya', 'Sharma', 'Hyderabad', 'priya.sharma@example.com'),
# MAGIC (5, 'Tom', 'Brown', 'Pune', 'tom.brown@example.com'),
# MAGIC (6, 'Sara', 'Ali', 'Chennai', 'sara.ali@example.com'),
# MAGIC (7, 'Vikram', 'Singh', 'Delhi', 'vikram.singh@example.com'),
# MAGIC (8, 'Neha', 'Patel', 'Mumbai', 'neha.patel@example.com'),
# MAGIC (9, 'David', 'Lee', 'Kolkata', 'david.lee@example.com'),
# MAGIC (10, 'Amit', 'Verma', 'Jaipur', 'amit.verma@example.com'),
# MAGIC (11, 'Emily', 'Clark', 'Delhi', 'emily.clark@example.com'),
# MAGIC (12, 'Ananya', 'Roy', 'Mumbai', 'ananya.roy@example.com'),
# MAGIC (13, 'Sam', 'Wilson', 'Pune', 'sam.wilson@example.com'),
# MAGIC (14, 'Pooja', 'Das', 'Hyderabad', 'pooja.das@example.com'),
# MAGIC (15, 'Arjun', 'Nair', 'Chennai', 'arjun.nair@example.com'),
# MAGIC (16, 'Kavita', 'Gupta', 'Bangalore', 'kavita.gupta@example.com'),
# MAGIC (17, 'Robert', 'King', 'Delhi', 'robert.king@example.com'),
# MAGIC (18, 'Meera', 'Joshi', 'Mumbai', 'meera.joshi@example.com'),
# MAGIC (19, 'Michael', 'Scott', 'Kolkata', 'michael.scott@example.com'),
# MAGIC (20, 'Ravi', 'Menon', 'Pune', 'ravi.menon@example.com'),
# MAGIC (21, 'Tina', 'Fernandez', 'Delhi', 'tina.fernandez@example.com'),
# MAGIC (22, 'Arvind', 'Singh', 'Hyderabad', 'arvind.singh@example.com'),
# MAGIC (23, 'Lucy', 'Adams', 'Chennai', 'lucy.adams@example.com'),
# MAGIC (24, 'Karan', 'Malhotra', 'Mumbai', 'karan.malhotra@example.com'),
# MAGIC (25, 'Sneha', 'Reddy', 'Pune', 'sneha.reddy@example.com'),
# MAGIC (26, 'George', 'Mason', 'Bangalore', 'george.mason@example.com'),
# MAGIC (27, 'Vishal', 'Kapoor', 'Kolkata', 'vishal.kapoor@example.com'),
# MAGIC (28, 'Emma', 'Watson', 'Delhi', 'emma.watson@example.com'),
# MAGIC (29, 'Siddharth', 'Jain', 'Hyderabad', 'siddharth.jain@example.com'),
# MAGIC (30, 'Olivia', 'Brown', 'Chennai', 'olivia.brown@example.com'),
# MAGIC (31, 'Nikhil', 'Arora', 'Mumbai', 'nikhil.arora@example.com'),
# MAGIC (32, 'Ayesha', 'Khan', 'Delhi', 'ayesha.khan@example.com'),
# MAGIC (33, 'Chris', 'Evans', 'Bangalore', 'chris.evans@example.com'),
# MAGIC (34, 'Ishita', 'Bhatt', 'Kolkata', 'ishita.bhatt@example.com'),
# MAGIC (35, 'Shyam', 'Patil', 'Pune', 'shyam.patil@example.com'),
# MAGIC (36, 'Sophia', 'Green', 'Chennai', 'sophia.green@example.com'),
# MAGIC (37, 'Harsh', 'Aggarwal', 'Hyderabad', 'harsh.aggarwal@example.com'),
# MAGIC (38, 'Liam', 'Martin', 'Delhi', 'liam.martin@example.com'),
# MAGIC (39, 'Diya', 'Saxena', 'Mumbai', 'diya.saxena@example.com'),
# MAGIC (40, 'Jason', 'Taylor', 'Kolkata', 'jason.taylor@example.com'),
# MAGIC (41, 'Ritika', 'Mehra', 'Pune', 'ritika.mehra@example.com'),
# MAGIC (42, 'Steve', 'Jobs', 'Bangalore', 'steve.jobs@example.com'),
# MAGIC (43, 'Alisha', 'Paul', 'Hyderabad', 'alisha.paul@example.com'),
# MAGIC (44, 'Dhruv', 'Singhania', 'Chennai', 'dhruv.singhania@example.com'),
# MAGIC (45, 'Rebecca', 'Lewis', 'Delhi', 'rebecca.lewis@example.com'),
# MAGIC (46, 'Naveen', 'Shetty', 'Mumbai', 'naveen.shetty@example.com'),
# MAGIC (47, 'Julia', 'Roberts', 'Pune', 'julia.roberts@example.com'),
# MAGIC (48, 'Parth', 'Desai', 'Hyderabad', 'parth.desai@example.com'),
# MAGIC (49, 'Nina', 'Shah', 'Chennai', 'nina.shah@example.com'),
# MAGIC (50, 'Aria', 'Gill', 'Bangalore', 'aria.gill@example.com');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders VALUES
# MAGIC (1001, 1, '2023-01-10', 1500.00),
# MAGIC (1002, 2, '2023-01-11', 2500.00),
# MAGIC (1003, 3, '2023-01-15', 500.00),
# MAGIC (1004, 1, '2023-02-01', 750.00),
# MAGIC (1005, 4, '2023-02-05', 2000.00),
# MAGIC (1006, 5, '2023-02-08', 1800.00),
# MAGIC (1007, 6, '2023-03-01', 2200.00),
# MAGIC (1008, 7, '2023-03-05', 3000.00),
# MAGIC (1009, 8, '2023-03-09', 1200.00),
# MAGIC (1010, 9, '2023-03-10', 900.00),
# MAGIC (1011, 10, '2023-03-12', 1900.00),
# MAGIC (1012, 11, '2023-03-15', 1500.00),
# MAGIC (1013, 12, '2023-03-18', 2400.00),
# MAGIC (1014, 13, '2023-03-20', 600.00),
# MAGIC (1015, 14, '2023-03-22', 3000.00),
# MAGIC (1016, 15, '2023-03-25', 2800.00),
# MAGIC (1017, 16, '2023-03-28', 3200.00),
# MAGIC (1018, 17, '2023-03-30', 500.00),
# MAGIC (1019, 18, '2023-04-01', 950.00),
# MAGIC (1020, 19, '2023-04-03', 1750.00),
# MAGIC (1021, 20, '2023-04-05', 2100.00),
# MAGIC (1022, 21, '2023-04-08', 1200.00),
# MAGIC (1023, 22, '2023-04-10', 1400.00),
# MAGIC (1024, 23, '2023-04-12', 800.00),
# MAGIC (1025, 24, '2023-04-14', 3300.00),
# MAGIC (1026, 25, '2023-04-16', 2400.00),
# MAGIC (1027, 26, '2023-04-18', 1800.00),
# MAGIC (1028, 27, '2023-04-20', 2000.00),
# MAGIC (1029, 28, '2023-04-22', 2200.00),
# MAGIC (1030, 29, '2023-04-24', 1950.00),
# MAGIC (1031, 30, '2023-04-26', 2600.00),
# MAGIC (1032, 31, '2023-04-28', 1550.00),
# MAGIC (1033, 32, '2023-05-01', 2700.00),
# MAGIC (1034, 33, '2023-05-03', 1000.00),
# MAGIC (1035, 34, '2023-05-05', 1900.00),
# MAGIC (1036, 35, '2023-05-07', 2100.00),
# MAGIC (1037, 36, '2023-05-09', 1100.00),
# MAGIC (1038, 37, '2023-05-11', 1250.00),
# MAGIC (1039, 38, '2023-05-13', 1850.00),
# MAGIC (1040, 39, '2023-05-15', 1400.00),
# MAGIC (1041, 40, '2023-05-17', 1750.00),
# MAGIC (1042, 41, '2023-05-19', 900.00),
# MAGIC (1043, 42, '2023-05-21', 2500.00),
# MAGIC (1044, 43, '2023-05-23', 3000.00),
# MAGIC (1045, 44, '2023-05-25', 2100.00),
# MAGIC (1046, 45, '2023-05-27', 2600.00),
# MAGIC (1047, 46, '2023-05-29', 2700.00),
# MAGIC (1048, 47, '2023-05-31', 2800.00),
# MAGIC (1049, 48, '2023-06-02', 1500.00),
# MAGIC (1050, 49, '2023-06-04', 1200.00),
# MAGIC (1051, 50, '2023-06-06', 800.00),
# MAGIC (1052, 1, '2023-06-08', 2000.00),
# MAGIC (1053, 2, '2023-06-10', 2300.00),
# MAGIC (1054, 3, '2023-06-12', 1400.00),
# MAGIC (1055, 4, '2023-06-14', 1750.00),
# MAGIC (1056, 5, '2023-06-16', 2600.00),
# MAGIC (1057, 6, '2023-06-18', 2100.00),
# MAGIC (1058, 7, '2023-06-20', 900.00),
# MAGIC (1059, 8, '2023-06-22', 3000.00),
# MAGIC (1060, 9, '2023-06-24', 1200.00),
# MAGIC (1061, 10, '2023-06-26', 1850.00),
# MAGIC (1062, 11, '2023-06-28', 1750.00),
# MAGIC (1063, 12, '2023-06-30', 1600.00),
# MAGIC (1064, 13, '2023-07-02', 950.00),
# MAGIC (1065, 14, '2023-07-04', 2100.00),
# MAGIC (1066, 15, '2023-07-06', 2500.00),
# MAGIC (1067, 16, '2023-07-08', 3000.00),
# MAGIC (1068, 17, '2023-07-10', 2700.00),
# MAGIC (1069, 18, '2023-07-12', 1500.00),
# MAGIC (1070, 19, '2023-07-14', 1400.00);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO order_details VALUES
# MAGIC (1, 1001, 1, 201, 'Laptop', 'Electronics', 1, 50000.00, '2023-01-10'),
# MAGIC (2, 1001, 1, 203, 'Headphones', 'Accessories', 2, 2000.00, '2023-01-10'),
# MAGIC (3, 1002, 2, 202, 'Smartphone', 'Electronics', 1, 30000.00, '2023-01-11'),
# MAGIC (4, 1003, 3, 205, 'Water Bottle', 'Kitchen', 3, 500.00, '2023-01-15'),
# MAGIC (5, 1004, 1, 208, 'Bluetooth Speaker', 'Electronics', 1, 3500.00, '2023-02-01'),
# MAGIC (6, 1005, 4, 204, 'Desk Chair', 'Furniture', 1, 4000.00, '2023-02-05'),
# MAGIC (7, 1006, 5, 207, 'Office Desk', 'Furniture', 1, 7000.00, '2023-02-08'),
# MAGIC (8, 1007, 6, 211, 'Tablet', 'Electronics', 2, 25000.00, '2023-03-01'),
# MAGIC (9, 1008, 7, 215, 'Gaming Chair', 'Furniture', 1, 15000.00, '2023-03-05'),
# MAGIC (10, 1009, 8, 209, 'Notebook', 'Stationery', 5, 200.00, '2023-03-09');

# COMMAND ----------

# MAGIC %sql
# MAGIC --List all the cusotmer orders with product details 
# MAGIC --INNER JOIN
# MAGIC select c.customer_id,
# MAGIC c.first_name, c.last_name,o.order_id, o.product_name, o.quantity
# MAGIC
# MAGIC from CUSTOMERS as c INNER JOIN ORDER_DETAILS as o
# MAGIC ON c.customer_id=o.customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC --Full Breakdown : custoemrs,order, product_info
# MAGIC
# MAGIC SELECT 
# MAGIC od.order_id, c.first_name, od.product_name, od.quantity, od.price_each
# MAGIC
# MAGIC  FROM ORDER_DETAILS as od 
# MAGIC  INNER JOIN  ORDERS as o ON od.order_id=o.order_id
# MAGIC  INNER JOIN CUSTOMERS as c ON o.customer_id=c.customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC --show inactive and active customers 
# MAGIC
# MAGIC SELECT 
# MAGIC c.first_name,c.last_name,o.order_id,o.order_date
# MAGIC  FROM CUSTOMERS as c 
# MAGIC  LEFT JOIN ORDERS o ON c.customer_id=o.customer_id