"""
This data monitor loads the order details of 10 randomly selected
order_ids from the MySQL data warehouse (DWH), and checks
the order details for those same order_ids from Woocommerce (WC).
In the #Comparison section the monitor calculates the difference
between the order totals for each order_id between WC and DWH.
If there is a difference detected in any of the order totals
between WC and DHW, an email is sent, detailing the order_ids
for which a difference was detected.
"""

country = 'rwanda'
country_code = 'rw'
if country == 'kenya':
    country_code = 'co.ke'

#------------------------------------------------------------------------------
# Importing relevant packages and credentials
import pandas as pd
from datetime import datetime, timedelta
from monitor_helper_functions import connect_to_WC, get_db_connector, send_email
from tabulate import tabulate
import sys
from settings import MAIN_DIRECTORY
sys.path.append('MAIN_DIRECTORY') #find the os.path for your project and replace here
from settings import WC_RW_CONSUMER_KEY
woo_key    = WC_RW_CONSUMER_KEY
from settings import WC_RW_CONSUMER_SECRET
woo_secret = WC_RW_CONSUMER_SECRET

start_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d') + 'T00:00:00'
end_date   = datetime.strftime(datetime.now(), '%Y-%m-%d') + 'T00:00:00'
day        = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

#------------------------------------------------------------------------------
# MySQL-DWH
con = get_db_connector('KASHA_DWH')
cursor = con.cursor()

# Fetch 10 random order IDs from MySQL-DWH and then fetch the order_totals for those IDs
cursor.execute(f'SELECT order_id FROM KASHA_DWH.woocommerce_orders WHERE status="completed" AND country="{country}" ORDER BY RAND() LIMIT 100')
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
api_connection = connect_to_WC(url, woo_key, woo_secret)

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
problems = pd.DataFrame()
send_order_total_email = False
if any(totals.difference != 0):
    problems = totals[totals.difference != 0]
    problems.reset_index(drop=True, inplace=True)
    send_order_total_email = True

#------------------------------------------------------------------------------
# Send email
message = f"""\
Subject: Data monitor #02 ALERT! - Random order pull from historic data found a discrepancy

The data monitor has encountered an error. 10 random {country} order_ids were drawn and the order_total for
each was compared between Wocommerce and the Data Warehouse. There was a mismatch.
{tabulate(problems, headers=['index','order_id','DWH_total','WC_total','difference'], tablefmt="github", numalign="right")}
"""

# Send email
if send_order_total_email:
    send_email(message)
