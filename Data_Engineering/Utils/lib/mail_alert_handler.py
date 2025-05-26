import smtplib, ssl
import os 
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import traceback
import yaml 

os.environ['SENDER_EMAIL'] = 'pierre@datagem-consulting.com'
os.environ['RECEIVER_EMAIL'] = 'datagemconsulting@gmail.com'
os.environ['SENDER_PASSWORD'] = yaml.load(open('./src/var/mailCredentials.yml'),Loader=yaml.CLoader)['password']

class MailAlertHandler:
    def __init__(self):
        self.sender_email = os.environ.get('SENDER_EMAIL')
        self.receiver_email = os.environ.get('RECEIVER_EMAIL')
        self.password = os.environ.get('SENDER_PASSWORD')

    def createMessage(self,plateform, project_id, subjet, content):
        message = MIMEMultipart("alternative")
        message["Subject"] = subjet
        message["From"] = "DataGem Consulting Alert"
        message["To"] = self.receiver_email

        part1 = MIMEText(content, "html")
        message.attach(part1)
        return message

    def sendEmail(self, plateform, project_id, subject, message):
        # Create a secure SSL context
        
        context = ssl.create_default_context()
        self.message = self.createMessage(plateform, project_id,subjet=subject, content=message)
        # Try to log in to server and send email
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                print("Logging in")
                server.login(self.sender_email, self.password)
                print("Sending email")
                server.sendmail(self.sender_email, self.receiver_email, self.message.as_string())
                print("Email sent")
                server.quit() 
            
        except Exception as e:
            # Print any error messages to stdout
            traceback.print_exc()
            print(e)
            