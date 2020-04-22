country = 'rwanda'

country_code = 'rw'
if country == 'kenya':
    country_code = 'co.ke'
#------------------------------------------------------------------------------
# Importing relevant packages and credentials
import pandas as pd
import os
from datetime import datetime, timedelta
from woocommerce import API
import mysql.connector
import smtplib, ssl
from tabulate import tabulate

import sys
sys.path.append('/Users/jjgutzmann/Kasha/Notebooks/Python_scripts')

from settings import DATABASE_HOST
from settings import DATABASE_USERNAME
from settings import DATABASE_PASSWORD
from settings import LAMBERT_WC_RW_CONSUMER_KEY
from settings import LAMBERT_WC_RW_CONSUMER_SECRET
from settings import SENDER_EMAIL
from settings import EMAIL_PASSWORD

start_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d') + 'T00:00:00'
end_date   = datetime.strftime(datetime.now(), '%Y-%m-%d') + 'T00:00:00'
day        = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

#------------------------------------------------------------------------------
# MySQL-DWH
my_database = mysql.connector.connect(host=DATABASE_HOST,
                        database='KASHA_DWH',
                        user=DATABASE_USERNAME,
                        password=DATABASE_PASSWORD)
cursor = my_database.cursor()

SELECT = """SELECT order_id, date_created, total, status"""
WHERE = f"""
WHERE country="{country}"
AND DATE_FORMAT(date_created, "%Y-%m-%dT%hh:%mm:%ss") > "{start_date}"
AND DATE_FORMAT(date_created, "%Y-%m-%dT%hh:%mm:%ss") < "{end_date}"
"""
cursor.execute(f'{SELECT} FROM KASHA_DWH.woocommerce_orders {WHERE};')

columns = cursor.fetchall()
order_id     = [int(x[0]) for x in columns]
date_created = [pd.to_datetime(x[1]) for x in columns]
total        = [int(x[2]) for x in columns]
status       = [str(x[3]) for x in columns]

all_orders_DWH = pd.DataFrame(list(zip(order_id, date_created, total, status)),
                               columns=['order_id', 'date_created', 'total', 'status'])

#------------------------------------------------------------------------------
# Woocommerce
url = f'https://www.kasha.{country_code}/'
consumer_key = LAMBERT_WC_RW_CONSUMER_KEY
consumer_secret = LAMBERT_WC_RW_CONSUMER_SECRET

api_connection = API(
    url=url,
    consumer_key=consumer_key,
    consumer_secret=consumer_secret,
    wp_api=True,
    version="wc/v3",
    timeout=100,
    query_string_auth=True)

request = api_connection.get('orders', params={'before':end_date, 'after':start_date, 'per_page':100})
WC_number_of_pages = int(request.headers['X-WP-TotalPages'])
WC_number_of_orders = int(request.headers['X-WP-Total'])

i = 1
all_orders_WC = []
for i in range(1,WC_number_of_pages+1):
    orders = api_connection.get("orders", params={'before':end_date, 'after':start_date, 'per_page':100, 'page':i}).json()
    all_orders_WC.extend(orders)
    i += 1
all_orders_WC = pd.DataFrame.from_dict(all_orders_WC)
all_orders_WC.rename(columns={'id':'order_id'}, inplace=True)

#------------------------------------------------------------------------------
# Comparison
comparison = all_orders_WC.merge(all_orders_DWH, on='order_id', how='left', suffixes=('_WC', '_DWH'))[['order_id', 'status_WC', 'status_DWH', 'total_WC', 'total_DWH']]
comparison.total_DWH = comparison.total_DWH.astype(float)
comparison.total_WC = comparison.total_WC.astype(float)
comparison['difference'] = comparison['total_WC'] - comparison['total_DWH']

send_order_total_email = False
if any(comparison['difference']!=0):
    problems = comparison[comparison.difference!=0]
    problems.reset_index(drop=True, inplace=True)
    send_order_total_email = True

#------------------------------------------------------------------------------
# Set up email
port = 465  # For SSL
smtp_server = "smtp.gmail.com"
receiver_email = "jakob.gutzmann@gmail.com"
sender_email = SENDER_EMAIL
password = EMAIL_PASSWORD

message = f"""\
Subject: Data monitor ALERT! - Order total discrepancy

The order_total of at least one order between Woocommerce {country} and the Data Warehouse for {day} does not match.
The offending order/s are/is:
{tabulate(problems, headers=['index','order_id','status_WC','status_DWH','total_WC','total_WC','difference'], tablefmt="github", numalign="right")}
"""

# Send email
if send_order_total_email:
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)
