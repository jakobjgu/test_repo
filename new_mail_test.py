def send_mailgun_message(subject=str, message=str):
    import requests
    from settings import MAILGUN_API_KEY
    from settings import MAILGUN_DOMAIN
    from settings import RECEIVER_EMAIL
    return requests.post(
        f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages",
        auth=("api", MAILGUN_API_KEY),
        data={"from": f"Kasha Alerts <mailgun@{MAILGUN_DOMAIN}>",
              "to": RECEIVER_EMAIL,
              "subject": subject,
              "text": message})


subject = """
The subject goes here
"""
message = """
This is a Mailgun email service test!
I bet it totally worked!
"""
send_mailgun_message(subject, message)
