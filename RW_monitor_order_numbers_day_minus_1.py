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

SELECT = """SELECT COUNT(order_id)"""
WHERE = f"""
WHERE country="{country}"
AND DATE_FORMAT(date_created, "%Y-%m-%dT%hh:%mm:%ss") > "{start_date}"
AND DATE_FORMAT(date_created, "%Y-%m-%dT%hh:%mm:%ss") < "{end_date}"
"""

cursor.execute(f'{SELECT} FROM KASHA_DWH.woocommerce_orders {WHERE}')
(DWH_number_of_orders,)=cursor.fetchone()

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
WC_number_of_orders = int(request.headers['X-WP-Total'])

#------------------------------------------------------------------------------
# Comparison
send_order_number_email = False
if DWH_number_of_orders != WC_number_of_orders:
    send_order_number_email = True

#------------------------------------------------------------------------------
# Set up email
port = 465  # For SSL
smtp_server = "smtp.gmail.com"
receiver_email = "jakob.gutzmann@gmail.com"
sender_email = SENDER_EMAIL
password = EMAIL_PASSWORD

message = f"""\
Subject: Data monitor ALERT! - Order number discrepancy

The number of orders between Woocommerce {country} and the Data Warehouse for {day} does not match.
Woocommerce reports {WC_number_of_orders} orders, and
DWH reports {DWH_number_of_orders} orders.
"""

# Send email
if send_order_number_email:
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)
