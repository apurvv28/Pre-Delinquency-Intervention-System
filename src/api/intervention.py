import os
import json
import uuid
import smtplib
from email.message import EmailMessage
from sqlalchemy.orm import Session
from sqlalchemy import Column, String, DateTime, func, Text
from dotenv import load_dotenv

load_dotenv()

from src.api.database import Base, SessionLocal, engine

# Mock Ticket Table
class SupportTicket(Base):
    __tablename__ = "support_tickets"

    id          = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    customer_id = Column(String, nullable=False, index=True)
    issue       = Column(Text, nullable=False)
    status      = Column(String, default="OPEN")
    created_at  = Column(DateTime, default=func.now())

# Make sure ticket table exists
Base.metadata.create_all(bind=engine)

def generate_ai_email_content(risk_bucket: str, transaction_data: dict) -> tuple:
    """Mock AI-generated email content for High and Critical risk customers."""
    amt = transaction_data.get('amount', 'Unknown')
    merch = transaction_data.get('merchant_category', 'Unknown')
    
    if risk_bucket == "HIGH_RISK":
        subject = "PIE System, ABC Bank Alert: High Default Risk Detected"
        text_body = f"""Hello,\n\nWe noticed a recent high-risk transaction of ${amt} at {merch}. Based on your recent payment patterns, our AI systems detect an elevated risk of default.\n\nPlease monitor your account actively and ensure sufficient funds to cover upcoming obligations.\n\nBest,\nPIE System, ABC Bank"""
        
        html_body = f"""
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden;">
            <div style="background-color: #f59e0b; padding: 20px; text-align: center;">
                <h2 style="color: white; margin: 0;">Account Alert</h2>
            </div>
            <div style="padding: 30px; color: #334155; line-height: 1.6;">
                <p>Hello,</p>
                <p>We noticed a recent high-risk transaction of <strong>${amt}</strong> at <strong>{merch}</strong>. Based on your recent payment patterns, our AI systems detect an elevated risk of default.</p>
                <div style="background-color: #fef3c7; border-left: 4px solid #f59e0b; padding: 15px; margin: 20px 0;">
                    <p style="margin: 0; color: #92400e;"><strong>Recommendation:</strong> Please monitor your account actively and ensure sufficient funds to cover upcoming obligations.</p>
                </div>
                <p style="margin-bottom: 0;">Best,</p>
                <p style="margin-top: 5px; font-weight: bold;">PIE System, ABC Bank</p>
            </div>
        </div>
        """
        return subject, text_body, html_body
        
    elif risk_bucket == "CRITICAL":
        subject = "PIE System, ABC Bank ACTION REQUIRED: Critical Default Risk - Escalation Plan"
        text_body = f"""Hello,\n\nYour recent transaction of ${amt} at {merch}, combined with your payment history, has flagged your account with CRITICAL default risk.\n
We have generated an AI-powered escalation plan to help you get back on track:
1. Immediate minimum payment of 10% of total balance.
2. Restricting new merchant transactions temporarily.

Please respond with your chosen action:
OPTION 1: "Follow the plan" (Reply to this email to acknowledge)
OPTION 2: "Create a ticket" with our PIE department to discuss other options.

Best,\nPIE System, ABC Bank"""

        html_body = f"""
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden;">
            <div style="background-color: #dc2626; padding: 20px; text-align: center;">
                <h2 style="color: white; margin: 0;">Action Required: Escalation Plan</h2>
            </div>
            <div style="padding: 30px; color: #334155; line-height: 1.6;">
                <p>Hello,</p>
                <p>Your recent transaction of <strong>${amt}</strong> at <strong>{merch}</strong>, combined with your payment history, has flagged your account with <strong>CRITICAL</strong> default risk.</p>
                <p>We have generated an AI-powered escalation plan to help you get back on track:</p>
                <ol style="background-color: #fee2e2; border: 1px solid #fca5a5; padding: 20px 20px 20px 40px; border-radius: 6px; color: #991b1b; font-weight: bold;">
                    <li style="margin-bottom: 10px;">Immediate minimum payment of 10% of total balance.</li>
                    <li>Restricting new merchant transactions temporarily.</li>
                </ol>
                <p style="margin-top: 30px;">Please select your chosen action below:</p>
                <div style="display: flex; gap: 15px; margin: 25px 0;">
                    <a href="mailto:pie-dept@abcbank.com?subject=Acknowledge%20Escalation%20Plan&body=I%20acknowledge%20and%20will%20follow%20the%20plan." style="flex: 1; background-color: #2563eb; color: white; text-align: center; padding: 14px 20px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;">✅ Follow the Plan</a>
                    <a href="mailto:pie-dept@abcbank.com?subject=Create%20Service%20Ticket&body=I%20would%20like%20to%20create%20a%20ticket%20to%20discuss%20my%20options." style="flex: 1; background-color: #475569; color: white; text-align: center; padding: 14px 20px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;">🎫 Create a Ticket</a>
                </div>
                <p style="margin-bottom: 0;">Best,</p>
                <p style="margin-top: 5px; font-weight: bold;">PIE System, ABC Bank</p>
            </div>
        </div>
        """
        return subject, text_body, html_body
        
    return "", "", ""

def send_real_email(to_email: str, subject: str, text_body: str, html_body: str):
    """Sends a real email using SMTP."""
    print(f"📧 Preparing to email {to_email}...")
    try:
        # Pull your SMTP configuration from the .env file!
        smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USERNAME", "your-email@gmail.com")
        smtp_pass = os.getenv("SMTP_PASSWORD", "")
        
        if not smtp_pass or smtp_user == "your-email@gmail.com":
            print("⚠️ WARNING: SMTP credentials not configured in .env. Falling back to mock print.")
            print(f"Subject: {subject}\n{text_body}")
            return

        msg = EmailMessage()
        msg.set_content(text_body)
        msg.add_alternative(html_body, subtype='html')
        msg['Subject'] = subject
        msg['From'] = smtp_user
        msg['To'] = to_email

        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
        server.quit()
        print(f"✅ Successfully sent REAL email to {to_email}")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
    
def create_service_ticket(customer_id: str, issue: str):
    """Mocks ticketing system (like ServiceNow) by saving to SQLite."""
    with SessionLocal() as db:
        ticket = SupportTicket(
            customer_id=customer_id,
            issue=issue
        )
        db.add(ticket)
        db.commit()
    print(f"🎫 TICKET CREATED: {ticket.id} for Customer {customer_id}")

def execute_intervention(customer_id: str, risk_bucket: str, transaction_data: dict):
    """Main intervention engine logic."""
    target_email = "apurvsaktepar@gmail.com"
    
    if risk_bucket == "LOW_RISK":
        # No intervention
        pass
    
    elif risk_bucket == "HIGH_RISK":
        subject, text_body, html_body = generate_ai_email_content(risk_bucket, transaction_data)
        send_real_email(target_email, subject, text_body, html_body)
        
    elif risk_bucket == "CRITICAL":
        subject, text_body, html_body = generate_ai_email_content(risk_bucket, transaction_data)
        send_real_email(target_email, subject, text_body, html_body)
        
        # For demonstration purposes, mock creating a ticket automatically
        # if the user selects "Option 2" (assuming async webhooks later).
        # We will just print the capability here.
        # create_service_ticket(customer_id, f"Escalation discussion requested for transaction at {transaction_data.get('merchant_category')}")
        
    elif risk_bucket == "VERY_CRITICAL":
        # Don't implement anything per instructions
        pass
