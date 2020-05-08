"""
This data monitor loads order details from Woocommerce (WC)
and the MySQL data warehouse (DWH) for orders placed on the
previous day (between midnight and midnight).
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
api_connection = connect_to_WC(url, woo_key, woo_secret)
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

problems = pd.DataFrame()
send_order_total_email = False
if any(comparison['difference']!=0):
    problems = comparison[comparison.difference!=0]
    problems.reset_index(drop=True, inplace=True)
    send_order_total_email = True

#------------------------------------------------------------------------------
# Send email
message = f"""\
Subject: Data monitor #03 ALERT! - Order total discrepancy
The order_total of at least one order between Woocommerce {country} and the Data Warehouse for {day} does not match.
The offending order/s are/is:
{tabulate(problems, headers=['index','order_id','status_WC','status_DWH','total_WC','total_WC','difference'], tablefmt="github", numalign="right")}
"""
if send_order_total_email:
    send_email(message)
