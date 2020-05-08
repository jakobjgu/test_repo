# Function to connect to WooCommerce API
def connect_to_WC(url, consumer_key, consumer_secret):
    """
    This function takes country_specific credentials and url as arguments,
    specifying whether the api connection is being made to the Rwanda or the Kenya
    instance of the WooCommerce website.
    The function then connects to the WC API and returns an api_connection object,
    on which for example a .get operation can be executed.
    """
    from woocommerce import API
    return API(
        url=url,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        wp_api=True,
        version="wc/v3",
        query_string_auth=True,
        timeout=100
    )


# Function to send emails
def send_email(message: str) -> None:
    """
    This function takes a string as an argument. If the message is supposed
    to contain text in the subject field of the email, the message must be structured as follows:
    Start with tripple-double-quotes followd by a back slash.
    Next line write 'Subject:' and the subject text.
    All following lines can contain the email body.
    End with tripple-double-quotes
    """
    import smtplib
    import ssl
    from settings import SENDER_EMAIL
    from settings import RECEIVER_EMAIL
    from settings import EMAIL_PASSWORD
    port = 465
    smtp_server = "smtp.gmail.com"
    sender_email = SENDER_EMAIL
    receiver_email = RECEIVER_EMAIL
    password = EMAIL_PASSWORD
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)


def get_db_connector(database):
    import mysql.connector as connector
    from settings import DATABASE_HOST
    from settings import DATABASE_USERNAME
    from settings import DATABASE_PASSWORD
    config = {
        "user": DATABASE_USERNAME,
        "password": DATABASE_PASSWORD,
        "host": DATABASE_HOST,
        "database": database
    }
    # important! do not create an intermediate variable here
    return connector.connect(**config)
