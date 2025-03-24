import os
import pprint
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List

import pydantic
import requests
import sentry_sdk
import supabase
from dotenv import load_dotenv
from retry import retry

from ai_ta_backend.types.types import ClerkUser

load_dotenv(override=True)


def get_all_users_from_clerk() -> List[ClerkUser]:
  """
  Use the Clerk API to get all users. Returns typed variable.
  """
  done = False
  all_users = []
  offset = 0
  limit = 100

  # Get all users from Clerk
  headers = {'Authorization': f'Bearer {os.environ["CLERK_BEARER_TOKEN"]}'}
  while not done:
    users = requests.get(f"https://api.clerk.com/v1/users?limit={limit}&offset={offset}&order_by=-created_at",
                         headers=headers,
                         timeout=12)
    all_users.extend(users.json())
    if len(users.json()) == 0:
      done = True
    offset = offset + limit

  # Parse users into typed objects
  clerkUsers = []
  for u in all_users:
    # pprint.pprint(u)
    try:
      clerkUsers.append(ClerkUser(**u))
    except pydantic.error_wrappers.ValidationError as e:
      pprint.pprint(u)
      print("Error parsing above user into Pydantic types:", e)

  return clerkUsers


def send_html_email(subject: str, html_text: str, sender: str, receipients: list | None = None):
  """
  If receipients is empty, send to all users (unless they've unsubscribed from newsletter). 
  If recipients is supplied, send to ONLY the receipients.

  bcc_receipients will be added to ALL emails, in all cases. 

  Note account limits:
    * Maximum send rate: 14 emails per second
    * Daily sending quota: 50,000 emails per 24-hour period

  Send an email using the AWS SES service
  :param subject: The subject of the email
  :param body_text: The body of the email
  :param sender: The email address of the sender
  :param receipients: A list of email addresses to send the email to
  :param bcc_receipients: A list of email addresses to send the email to as BCC
  :return: A string indicating the result of the email send operation
  """

  supabase_client = supabase.create_client(supabase_url=os.environ['SUPABASE_URL'],
                                           supabase_key=os.environ['SUPABASE_API_KEY'])

  emails = []
  if not receipients:
    users = get_all_users_from_clerk()
    # emails = [user['email_addresses'][0]['email_address'] for user in users]
    for user in users:
      # print("user.email_addresses", user.email_addresses)
      if user.email_addresses:
        try:
          emails.append(user.email_addresses[0]['email_address'])
        except:
          print("Error getting email address for user", user)
    # emails = [user.email_addresses[0]['email_address'] for user in users]
  else:
    emails = receipients

  print(len(emails), "BEFORE emails to send to")
  # Read all the emails from successful_sends.txt and exclude those from the emails list
  try:
    with open("successful_sends.txt", "r") as file:
      successful_emails = set(file.read().splitlines())
    emails = [email for email in emails if email not in successful_emails]
  except FileNotFoundError:
    print("No successful_sends.txt file found. Proceeding with all gathered emails.")

  print(len(emails), "emails to send to")

  print("Emails to send to: ", emails)
  with open("emails_to_be_sent.txt", "a") as file:
    for r in emails:
      file.write(r + "\n")

  # Get the list of unsubscribed emails
  unsubscribed = supabase_client.table(table_name='email-newsletter').select("email").eq(
      "unsubscribed-from-newsletter", "TRUE").execute()
  unsubscribe_list = [row['email'] for row in unsubscribed.data]
  print("Unsubscribed emails: ", unsubscribe_list)

  # Remove any receipients that are in the unsubscribe list
  new_receipients = [r for r in emails if r not in unsubscribe_list]

  start_time = time.time()
  emails_sent = 0

  for user_email in new_receipients:
    # Limit to 14 emails per second
    # print("Sending email to", user_email)
    if emails_sent >= 14:
      elapsed = time.time() - start_time
      if elapsed < 1:
        time.sleep(1 - elapsed)
      start_time = time.time()
      emails_sent = 0

    # Add custom unsubscribe links
    customized_html_content = html_text.replace('https://uiuc.chat/newsletter-unsubscribe',
                                                f'https://uiuc.chat/newsletter-unsubscribe?email={user_email}')

    # CRITICAL -- Must re-create the message every time to avoid multiple recipients.
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = user_email
    # Add plain text part
    part1 = MIMEText(customized_html_content, "html")
    message.attach(part1)
    try:
      send_email_safely(sender, user_email, message)
      emails_sent += 1
    except Exception as e:
      print("Error sending email to", user_email, e)
      sentry_sdk.capture_exception(e)

  return "All email sent!"


# start with 1 second delay, increment by 1 at a time. Max tries of 65 (> 1 minute)
@retry(exceptions=Exception, tries=65, delay=1, max_delay=None, backoff=1, jitter=0)
def send_email_safely(sender, recipients: str, message):
  """
  Send an email using the AWS SES service. Retry if there is an exception.
  Note account limits:
    * Maximum send rate: 14 emails per second
    * Daily sending quota: 50,000 emails per 24-hour period
  """
  print("receipient in safe-send", recipients)

  with smtplib.SMTP_SSL(os.getenv('SES_HOST'), os.getenv('SES_PORT')) as server:  # type: ignore
    server.login(os.getenv('USERNAME_SMTP'), os.getenv('PASSWORD_SMTP'))  # type: ignore
    server.sendmail(sender, recipients, message.as_string())

    # log successful sends
    with open("successful_sends.txt", "a") as file:
      file.write(recipients + "\n")


if __name__ == "__main__":

  # Test with: python -m ai_ta_backend.utils.send_newsletter_email

  with open("ai_ta_backend/utils/email/product-update-1-minified.html", "r", encoding="utf-8") as file:
    html_content = file.read()

    success_or_fail = send_html_email(
        subject="UIUC.chat Product Update 1",
        html_text=html_content,
        sender="kvday2@illinois.edu",
    )
    # receipients=["kvday2@illinois.edu"])
    print("success_or_fail:", success_or_fail)
