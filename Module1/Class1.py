# Databricks notebook source
# MAGIC %md
# MAGIC **CREATE,DROP,SELECT,INSERT,
# MAGIC MULTIPLE QUERIES IN CELL,SECOND HIGHEST SALARY 
# MAGIC ,Describe,
# MAGIC Distinct,where,Group by,having,count**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employees (
# MAGIC     emp_id INT,
# MAGIC     first_name VARCHAR(50),
# MAGIC     last_name VARCHAR(50),
# MAGIC     department VARCHAR(50),
# MAGIC     job_title VARCHAR(50),
# MAGIC     salary DECIMAL(10, 2),
# MAGIC     hire_date DATE,
# MAGIC     status VARCHAR(20),
# MAGIC     city VARCHAR(50),
# MAGIC     email VARCHAR(100)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees VALUES
# MAGIC (1, 'John', 'Doe', 'Engineering', 'Software Engineer', 75000.00, '2020-05-15', 'Active', 'Bangalore', 'john.doe@example.com'),
# MAGIC (2, 'Jane', 'Smith', 'HR', 'HR Manager', 65000.00, '2019-03-10', 'Active', 'Mumbai', 'jane.smith@example.com'),
# MAGIC (3, 'Raj', 'Kumar', 'Engineering', 'Data Engineer', 85000.00, '2021-07-01', 'Active', 'Hyderabad', 'raj.kumar@example.com'),
# MAGIC (4, 'Priya', 'Sharma', 'Marketing', 'Marketing Analyst', 60000.00, '2018-11-22', 'Active', 'Delhi', 'priya.sharma@example.com'),
# MAGIC (5, 'Tom', 'Brown', 'Sales', 'Sales Executive', 55000.00, '2022-01-05', 'Active', 'Chennai', 'tom.brown@example.com'),
# MAGIC (6, 'Sara', 'Ali', 'Engineering', 'Backend Developer', 78000.00, '2020-09-17', 'Active', 'Pune', 'sara.ali@example.com'),
# MAGIC (7, 'Amit', 'Verma', 'Finance', 'Accountant', 62000.00, '2017-06-30', 'Inactive', 'Bangalore', 'amit.verma@example.com'),
# MAGIC (8, 'Emily', 'Clark', 'HR', 'Recruiter', 50000.00, '2021-02-14', 'Active', 'Mumbai', 'emily.clark@example.com'),
# MAGIC (9, 'Vikram', 'Singh', 'Engineering', 'DevOps Engineer', 90000.00, '2019-08-19', 'Active', 'Hyderabad', 'vikram.singh@example.com'),
# MAGIC (10, 'Neha', 'Patel', 'Sales', 'Regional Manager', 95000.00, '2015-04-01', 'Active', 'Delhi', 'neha.patel@example.com');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees VALUES
# MAGIC (11, 'David', 'Lee', 'Engineering', 'Frontend Developer', 72000.00, '2020-03-11', 'Active', 'Chennai', 'david.lee@example.com'),
# MAGIC (12, 'Ananya', 'Roy', 'Finance', 'Financial Analyst', 67000.00, '2019-07-25', 'Active', 'Pune', 'ananya.roy@example.com'),
# MAGIC (13, 'Sam', 'Wilson', 'Marketing', 'Content Strategist', 58000.00, '2021-11-30', 'Active', 'Delhi', 'sam.wilson@example.com'),
# MAGIC (14, 'Pooja', 'Das', 'Sales', 'Sales Associate', 52000.00, '2018-10-02', 'Inactive', 'Bangalore', 'pooja.das@example.com'),
# MAGIC (15, 'Arjun', 'Nair', 'Engineering', 'Data Scientist', 98000.00, '2021-05-20', 'Active', 'Hyderabad', 'arjun.nair@example.com'),
# MAGIC (16, 'Kavita', 'Gupta', 'HR', 'Compensation Analyst', 61000.00, '2017-01-16', 'Active', 'Mumbai', 'kavita.gupta@example.com'),
# MAGIC (17, 'Robert', 'King', 'Engineering', 'Software Architect', 120000.00, '2016-09-05', 'Active', 'Delhi', 'robert.king@example.com'),
# MAGIC (18, 'Meera', 'Joshi', 'Finance', 'Auditor', 69000.00, '2020-12-11', 'Active', 'Pune', 'meera.joshi@example.com'),
# MAGIC (19, 'Michael', 'Scott', 'Sales', 'Regional Sales Head', 115000.00, '2015-06-20', 'Active', 'Chennai', 'michael.scott@example.com'),
# MAGIC (20, 'Ravi', 'Menon', 'Engineering', 'Machine Learning Engineer', 93000.00, '2019-04-15', 'Active', 'Hyderabad', 'ravi.menon@example.com'),
# MAGIC (21, 'Tina', 'Fernandez', 'Marketing', 'SEO Specialist', 57000.00, '2018-05-22', 'Active', 'Delhi', 'tina.fernandez@example.com'),
# MAGIC (22, 'Arvind', 'Singh', 'Finance', 'Tax Consultant', 65000.00, '2020-01-25', 'Active', 'Mumbai', 'arvind.singh@example.com'),
# MAGIC (23, 'Lucy', 'Adams', 'HR', 'HR Coordinator', 48000.00, '2021-09-10', 'Active', 'Bangalore', 'lucy.adams@example.com'),
# MAGIC (24, 'Karan', 'Malhotra', 'Engineering', 'Tech Lead', 105000.00, '2017-11-18', 'Active', 'Chennai', 'karan.malhotra@example.com'),
# MAGIC (25, 'Sneha', 'Reddy', 'Sales', 'Sales Manager', 88000.00, '2016-02-09', 'Active', 'Hyderabad', 'sneha.reddy@example.com'),
# MAGIC (26, 'George', 'Mason', 'Finance', 'Payroll Specialist', 60000.00, '2022-04-08', 'Active', 'Pune', 'george.mason@example.com'),
# MAGIC (27, 'Vishal', 'Kapoor', 'Marketing', 'Brand Manager', 75000.00, '2019-03-19', 'Active', 'Delhi', 'vishal.kapoor@example.com'),
# MAGIC (28, 'Emma', 'Watson', 'Engineering', 'QA Engineer', 70000.00, '2020-07-15', 'Active', 'Bangalore', 'emma.watson@example.com'),
# MAGIC (29, 'Siddharth', 'Jain', 'Engineering', 'Cloud Engineer', 89000.00, '2018-06-25', 'Active', 'Hyderabad', 'siddharth.jain@example.com'),
# MAGIC (30, 'Olivia', 'Brown', 'Sales', 'Territory Manager', 81000.00, '2017-04-05', 'Active', 'Mumbai', 'olivia.brown@example.com'),
# MAGIC (31, 'Nikhil', 'Arora', 'Finance', 'Business Analyst', 68000.00, '2021-03-27', 'Active', 'Chennai', 'nikhil.arora@example.com'),
# MAGIC (32, 'Ayesha', 'Khan', 'HR', 'Training Specialist', 59000.00, '2019-08-02', 'Active', 'Pune', 'ayesha.khan@example.com'),
# MAGIC (33, 'Chris', 'Evans', 'Engineering', 'Security Analyst', 94000.00, '2020-12-18', 'Active', 'Delhi', 'chris.evans@example.com'),
# MAGIC (34, 'Ishita', 'Bhatt', 'Marketing', 'Social Media Manager', 63000.00, '2018-07-21', 'Active', 'Bangalore', 'ishita.bhatt@example.com'),
# MAGIC (35, 'Shyam', 'Patil', 'Engineering', 'Data Analyst', 76000.00, '2017-02-15', 'Active', 'Hyderabad', 'shyam.patil@example.com'),
# MAGIC (36, 'Sophia', 'Green', 'Sales', 'Sales Coordinator', 54000.00, '2022-05-01', 'Active', 'Mumbai', 'sophia.green@example.com'),
# MAGIC (37, 'Harsh', 'Aggarwal', 'Finance', 'Investment Analyst', 88000.00, '2016-11-28', 'Active', 'Delhi', 'harsh.aggarwal@example.com'),
# MAGIC (38, 'Liam', 'Martin', 'Engineering', 'AI Engineer', 115000.00, '2019-09-30', 'Active', 'Chennai', 'liam.martin@example.com'),
# MAGIC (39, 'Diya', 'Saxena', 'HR', 'HRBP', 74000.00, '2020-02-07', 'Active', 'Pune', 'diya.saxena@example.com'),
# MAGIC (40, 'Jason', 'Taylor', 'Marketing', 'Market Research Analyst', 58000.00, '2021-06-11', 'Active', 'Hyderabad', 'jason.taylor@example.com'),
# MAGIC (41, 'Ritika', 'Mehra', 'Engineering', 'Mobile App Developer', 77000.00, '2018-01-13', 'Active', 'Delhi', 'ritika.mehra@example.com'),
# MAGIC (42, 'Steve', 'Jobs', 'Engineering', 'Product Manager', 125000.00, '2015-12-20', 'Active', 'Bangalore', 'steve.jobs@example.com'),
# MAGIC (43, 'Alisha', 'Paul', 'Sales', 'Inside Sales Rep', 56000.00, '2020-04-23', 'Active', 'Chennai', 'alisha.paul@example.com'),
# MAGIC (44, 'Dhruv', 'Singhania', 'Finance', 'Risk Manager', 97000.00, '2017-10-14', 'Active', 'Mumbai', 'dhruv.singhania@example.com'),
# MAGIC (45, 'Rebecca', 'Lewis', 'Marketing', 'PR Specialist', 64000.00, '2019-05-18', 'Active', 'Pune', 'rebecca.lewis@example.com'),
# MAGIC (46, 'Naveen', 'Shetty', 'Engineering', 'Technical Support Engineer', 66000.00, '2021-01-12', 'Active', 'Hyderabad', 'naveen.shetty@example.com'),
# MAGIC (47, 'Julia', 'Roberts', 'HR', 'HR Generalist', 61000.00, '2018-09-03', 'Active', 'Delhi', 'julia.roberts@example.com'),
# MAGIC (48, 'Parth', 'Desai', 'Finance', 'Treasury Analyst', 70000.00, '2019-12-29', 'Active', 'Chennai', 'parth.desai@example.com'),
# MAGIC (49, 'Nina', 'Shah', 'Engineering', 'Systems Engineer', 73000.00, '2020-06-17', 'Active', 'Bangalore', 'nina.shah@example.com'),
# MAGIC (50, 'Aria', 'Gill', 'Marketing', 'Advertising Manager', 82000.00, '2017-08-09', 'Active', 'Mumbai', 'aria.gill@example.com');
# MAGIC

# COMMAND ----------

queries = [
    "select emp_id,first_name,salary from employees where salary > 100000",
    "SELECT * from employees where   department in ('Engineering','HR') and salary>70000",
    "select * from employees order by salary desc",
    "select * from employees order by department desc, first_name asc , salary desc",
]

for query in queries:
    display(spark.sql(query))


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employees order by salary desc limit 1 offset 1 ;--second highest salary

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE employees;