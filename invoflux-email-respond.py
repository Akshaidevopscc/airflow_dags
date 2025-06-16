from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import base64
import json
import logging
import re
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from ollama import Client
from ollama._types import ResponseError
from email import message_from_bytes
from email.header import decode_header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup
import os

# Configure detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "lowtouch.ai_developers",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 18),
    "retries": 0,
    "retry_delay": timedelta(seconds=15),
}



ODOO_FROM_ADDRESS = Variable.get("INVOFLUX_FROM_ADDRESS")  
GMAIL_CREDENTIALS = Variable.get("INVOFLUX_GMAIL_CREDENTIALS")

OLLAMA_HOST = "http://agentomatic:8000/"

def authenticate_gmail():
    try:
        creds = Credentials.from_authorized_user_info(json.loads(GMAIL_CREDENTIALS))
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        if logged_in_email.lower() != ODOO_FROM_ADDRESS.lower():
            raise ValueError(f"Wrong Gmail account! Expected {ODOO_FROM_ADDRESS}, but got {logged_in_email}")
        logging.info(f"Authenticated Gmail account: {logged_in_email}")
        return service
    except Exception as e:
        logging.error(f"Failed to authenticate Gmail: {str(e)}")
        return None

def decode_email_payload(msg):
    try:
        # Handle multipart emails
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type in ["text/plain", "text/html"]:
                    try:
                        body = part.get_payload(decode=True).decode()
                        return body
                    except UnicodeDecodeError:
                        body = part.get_payload(decode=True).decode('latin-1')
                        return body
        # Handle single-part emails
        else:
            try:
                body = msg.get_payload(decode=True).decode()
                return body
            except UnicodeDecodeError:
                body = msg.get_payload(decode=True).decode('latin-1')
                return body
        return ""
    except Exception as e:
        logging.error(f"Error decoding email payload: {str(e)}")
        return ""

def get_email_thread(service, email_data):
    try:
        thread_id = email_data.get("threadId")
        message_id = email_data["headers"].get("Message-ID", "")
        
        if not thread_id:
            query_result = service.users().messages().list(userId="me", q=f"rfc822msgid:{message_id}").execute()
            messages = query_result.get("messages", [])
            if messages:
                message = service.users().messages().get(userId="me", id=messages[0]["id"]).execute()
                thread_id = message.get("threadId")
        
        if not thread_id:
            logging.warning(f"No thread ID found for message ID {message_id}. Treating as a single email.")
            raw_message = service.users().messages().get(userId="me", id=email_data["id"], format="raw").execute()
            msg = message_from_bytes(base64.urlsafe_b64decode(raw_message["raw"]))
            return [{
                "headers": {header["name"]: header["value"] for header in email_data.get("payload", {}).get("headers", [])},
                "content": decode_email_payload(msg)
            }]

        thread = service.users().threads().get(userId="me", id=thread_id).execute()
        email_thread = []
        for msg in thread.get("messages", []):
            raw_msg = base64.urlsafe_b64decode(msg["raw"]) if "raw" in msg else None
            if not raw_msg:
                raw_message = service.users().messages().get(userId="me", id=msg["id"], format="raw").execute()
                raw_msg = base64.urlsafe_b64decode(raw_message["raw"])
            email_msg = message_from_bytes(raw_msg)
            headers = {header["name"]: header["value"] for header in msg.get("payload", {}).get("headers", [])}
            content = decode_email_payload(email_msg)
            email_thread.append({
                "headers": headers,
                "content": content.strip()
            })
        # Sort by date to ensure chronological order
        email_thread.sort(key=lambda x: x["headers"].get("Date", ""), reverse=False)
        logging.debug(f"Retrieved thread with {len(email_thread)} messages")
        return email_thread
    except Exception as e:
        logging.error(f"Error retrieving email thread: {str(e)}")
        return []

def get_ai_response(user_query):
    try:
        logging.debug(f"Query received: {user_query}")
        
        # Validate input
        if not user_query or not isinstance(user_query, str):
            return "<html><body>Invalid input provided. Please enter a valid query.</body></html>"

        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'InvoFlux'})
        logging.debug(f"Connecting to Ollama at {OLLAMA_HOST} with model 'InvoFlux:0.3'")

        response = client.chat(
            model='invoflux-email:0.3',
            messages=[{"role": "user", "content": user_query}],
            stream=False
        )
        logging.info(f"Raw response from agent: {str(response)[:500]}...")

        # Extract content
        if not (hasattr(response, 'message') and hasattr(response.message, 'content')):
            logging.error("Response lacks expected 'message.content' structure")
            return "<html><body>Invalid response format from AI. Please try again later.</body></html>"
        
        ai_content = response.message.content
        logging.info(f"Full message content from agent: {ai_content[:500]}...")

        # Check for error messages
        if "technical difficulties" in ai_content.lower() or "error" in ai_content.lower():
            logging.warning("AI response contains potential error message")
            return "<html><body>Unexpected response received. Please contact support.</body></html>"

        # Clean up markdown markers
        ai_content = re.sub(r'```html\n|```', '', ai_content).strip()
        logging.info(f"Cleaned content extracted from agent response: {ai_content[:500]}...")

        # Validate content
        if not ai_content.strip():
            logging.warning("AI returned empty content")
            return "<html><body>No response generated. Please try again later.</body></html>"

        # Ensure proper HTML structure
        if not ai_content.strip().startswith('<!DOCTYPE') and not ai_content.strip().startswith('<html'):
            logging.warning("Response doesn't appear to be proper HTML, wrapping it")
            ai_content = f"<html><body>{ai_content}</body></html>"

        return ai_content

    except Exception as e:
        logging.error(f"Error in get_ai_response: {str(e)}")
        return "<html><body>An error occurred while processing your request. Please try again later or contact support.</body></html>"

def send_email(service, recipient, subject, body, in_reply_to, references):
    try:
        logging.debug(f"Preparing email to {recipient} with subject: {subject}")
        msg = MIMEMultipart()
        msg["From"] = f"InvoFlux via lowtouch.ai <{ODOO_FROM_ADDRESS}>"
        msg["To"] = recipient
        msg["Subject"] = subject
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = references
        msg.attach(MIMEText(body, "html"))
        raw_msg = base64.urlsafe_b64encode(msg.as_string().encode("utf-8")).decode("utf-8")
        result = service.users().messages().send(userId="me", body={"raw": raw_msg}).execute()
        logging.info(f"Email sent successfully: {result}")
        return result
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        return None
def send_response(**kwargs):
    try:
        email_data = kwargs['dag_run'].conf.get("email_data", {})
        logging.info(f"Received email data: {email_data}")
        if not email_data:
            logging.warning("No email data received! This DAG was likely triggered manually.")
            return
        
        service = authenticate_gmail()
        if not service:
            logging.error("Gmail authentication failed, aborting email response.")
            return
        
        sender_email = email_data["headers"].get("From", "")
        subject = f"Re: {email_data['headers'].get('Subject', 'No Subject')}"
        in_reply_to = email_data["headers"].get("Message-ID", "")
        references = email_data["headers"].get("References", "")
        
        # Fetch the full email thread
        email_thread = get_email_thread(service, email_data)
        if not email_thread:
            logging.warning("No thread history retrieved, using only current email content.")
            user_query = email_data.get("content", "").strip()
            # Include attachment data for the current email if no thread history
            if email_data.get("attachments"):
                attachment_content = ""
                for attachment in email_data["attachments"]:
                    if "extracted_content" in attachment and "content" in attachment["extracted_content"]:
                        attachment_content += f"\nAttachment ({attachment['filename']}):\n{attachment['extracted_content']['content']}\n"
                user_query += f"\n\n{attachment_content}" if attachment_content else ""
        else:
            # Format thread history into a single query
            thread_history = ""
            for idx, email in enumerate(email_thread, 1):
                email_content = email.get("content", "").strip()
                email_from = email["headers"].get("From", "Unknown")
                email_date = email["headers"].get("Date", "Unknown date")
                # Clean HTML content if present
                if email_content:
                    soup = BeautifulSoup(email_content, "html.parser")
                    email_content = soup.get_text(separator=" ", strip=True)
                thread_history += f"Email {idx} (From: {email_from}, Date: {email_date}):\n{email_content}\n\n"
            
            # Append the current email as the latest query
            current_content = email_data.get("content", "").strip()
            if current_content:
                soup = BeautifulSoup(current_content, "html.parser")
                current_content = soup.get_text(separator=" ", strip=True)
            thread_history += f"Current Email (From: {sender_email}):\n{current_content}\n"
            
            # Append attachment data for the current email
            if email_data.get("attachments"):
                attachment_content = ""
                for attachment in email_data["attachments"]:
                    if "extracted_content" in attachment and "content" in attachment["extracted_content"]:
                        attachment_content += f"\nAttachment ({attachment['filename']}):\n{attachment['extracted_content']['content']}\n"
                thread_history += f"\n{attachment_content}" if attachment_content else ""
            
            logging.info(f"Thread History:: {thread_history}")
            user_query = f"Here is the email thread history:\n\n{thread_history}\n"
        
        logging.debug(f"Sending query to AI: {user_query}")
        
        ai_response_html = get_ai_response(user_query) if user_query else "<html><body>No content provided in the email.</body></html>"
        logging.debug(f"AI response received (first 200 chars): {ai_response_html[:200]}...")

        send_email(
            service, sender_email, subject, ai_response_html,
            in_reply_to, references
        )
    except Exception as e:
        logging.error(f"Unexpected error in send_response: {str(e)}")

readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'email_responder.md')
with open(readme_path, 'r') as file:
    readme_content = file.read()

with DAG("invoflux_send_message_email", default_args=default_args, schedule_interval=None, catchup=False, doc_md=readme_content, tags=["email", "shared", "send", "message"]) as dag:
    send_response_task = PythonOperator(
        task_id="send-response",
        python_callable=send_response,
        provide_context=True
    )
