"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv
import os
from cleo import cursor

DB_PASS = os.getenv('DB_PASS')

conn = psycopg2.connect(host="localhost", database="north", user="postgres", password=DB_PASS)
try:
    with conn:
        with conn.cursor() as cursor:
            with open('north_data/customers_data.csv', 'r') as file:
                read_csv = csv.reader(file)
                next(read_csv)
                for row in read_csv:
                    cursor.execute(
                        "insert into customers (customer_id, company_name, contact_name) values (%s, %s, %s)",
                        row
                    )
finally:
    conn.close()

conn = psycopg2.connect(host="localhost", database="north", user="postgres", password=DB_PASS)
try:
    with conn:
        with conn.cursor() as cursor:
            with open('north_data/employees_data.csv', 'r', encoding='utf-8') as file:
                read_csv = csv.reader(file)
                next(read_csv)
                for row in read_csv:
                    cursor.execute(
                        "insert into employees (employee_id, first_name, last_name, title, birth_day, notes) values (%s, %s, %s, %s, %s, %s)",
                        row
                    )
finally:
    conn.close()

conn = psycopg2.connect(host="localhost", database="north", user="postgres", password=DB_PASS)

try:
    with conn:
        with conn.cursor() as cursor:
            with open('north_data/orders_data.csv', 'r') as file:
                read_csv = csv.reader(file)
                next(read_csv)
                for row in read_csv:
                    cursor.execute(
                        "insert into orders (order_id, customer_id, employee_id, order_date, ship_city) values (%s, %s, %s, %s, %s)",
                        row
                    )
finally:
    conn.close()


