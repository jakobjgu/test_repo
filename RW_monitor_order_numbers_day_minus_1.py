"""
This data monitor counts the number of orders (but no details)
from Woocommerce (WC) and the MySQL data warehouse (DWH) for
orders placed on the previous day (between midnight and midnight).
In the #Comparison section the monitor calculates the difference
in order numbers and sends out an email, in case a difference
is detected.
"""

country = 'rwanda'
country_code = 'rw'
if country == 'kenya':
    country_code = 'co.ke'

#------------------------------------------------------------------------------
# Importing relevant packages and credentials
from datetime import datetime, timedelta
from monitor_helper_functions import connect_to_WC, get_db_connector, send_email
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
api_connection = connect_to_WC(url, woo_key, woo_secret)

request = api_connection.get('orders', params={'before':end_date, 'after':start_date, 'per_page':100})
WC_number_of_orders = int(request.headers['X-WP-Total'])

#------------------------------------------------------------------------------
# Comparison
send_order_number_email = False
if DWH_number_of_orders != WC_number_of_orders:
    send_order_number_email = True

#------------------------------------------------------------------------------
# Snd email
message = f"""\
Subject: Data monitor #04 ALERT! - Order number discrepancy

The number of orders between Woocommerce {country} and the Data Warehouse for {day} does not match.
Woocommerce reports {WC_number_of_orders} orders, and
DWH reports {DWH_number_of_orders} orders.
"""

# Send email
if send_order_number_email:
    send_email(message)
