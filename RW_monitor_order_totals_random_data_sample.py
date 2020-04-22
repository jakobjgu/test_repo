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
sys.path.append(f'{path_to_settings_script}')

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

# Fetch 10 random order IDs from MySQL-DWH and then fetch the order_totals for those IDs
cursor.execute(f'SELECT order_id FROM KASHA_DWH.woocommerce_orders WHERE status="completed" AND country="{country}" ORDER BY RAND() LIMIT 10')
random_order_ids = [int(x[0]) for (x) in cursor.fetchall()]
combo = random_order_ids.copy()
combo.insert(0,'order_id')
combo = ','.join([str(elem) for elem in combo])
cursor.execute(f'SELECT order_id, total FROM KASHA_DWH.woocommerce_orders WHERE country="{country}" AND order_id IN {tuple(random_order_ids)} ORDER BY field({combo})')
random_order_totals = [int(x[1]) for (x) in cursor.fetchall()]
totals = pd.DataFrame(zip(random_order_ids,random_order_totals), columns=['order_id', 'DWH_total'])

#------------------------------------------------------------------------------
# Woocommerce
url=f'https://www.kasha.{country_code}/'
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

# Get order totals for the random order IDs from Woocommerce REST-API
temp = pd.DataFrame()
for order_id in random_order_ids:
    request = api_connection.get(f"orders/{order_id}?").json()
    temp2 = pd.json_normalize(request)
    temp = pd.concat([temp, temp2], axis=0)

temp.rename(columns={'id':'order_id', 'total':'WC_total'}, inplace=True)
totals = pd.merge(totals, temp[['order_id', 'WC_total']], on='order_id', how='left')
totals.DWH_total = totals.DWH_total.astype(float)
totals.WC_total = totals.WC_total.astype(float)
totals['difference'] = totals.DWH_total - totals.WC_total

#------------------------------------------------------------------------------
# Comparison
send_order_total_email = True
if any(totals.difference != 0):
    send_order_total_email = True

#------------------------------------------------------------------------------
# Set up email
port = 465  # For SSL
smtp_server = "smtp.gmail.com"
receiver_email = "jakob.gutzmann@gmail.com"
sender_email = SENDER_EMAIL
password = EMAIL_PASSWORD

message = f"""\
Subject: Data monitor ALERT! - Random order pull from historic data found a discrepancy

The data monitor has encountered an error. 10 random {country} order_ids were drawn and the order_total for
each was compared between Wocommerce and the Data Warehouse. There was a mismatch.

{tabulate(totals, headers=['index','order_id','DWH_total','WC_total','difference'], tablefmt="github", numalign="right")}
"""

# Send email
if send_order_total_email:
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)
