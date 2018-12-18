import json
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import boto3

"""
This module allows for the separation of data by organizations, then subdivided by admin/clinical/qi reports
for individuals.

This module is predicated on the definition of canned reports, as well as creation of custom reports.

"""


class EmailDriver:
    """
    Utilizes the boto3 library in order to send an email.
    """

    def __init__(self, organization_dict: object):
        self.client = boto3.client('ses',
                                   aws_access_key_id="blank",
                                   aws_secret_access_key="blank",
                                   )
        self.organization_dict = organization_dict

    def send_raw_email(self, data, recipient_list):
        """
        Takes a MIME formatted content, and recipient list to construct an email
        :param data: This is the return value of the create_email() method of the MIMEEmail object
        :param recipient_list: a list of emails of people who should receive this information
        e.g. [test@abc.com, jim@ccorp.com]
        :return: returns the boto response object
        """
        response = self.client.send_raw_email(
            Source="beta@algernon.solutions",
            Destinations=recipient_list,
            RawMessage={
                'Data': data.as_string()
            }
        )
        return response

    def send_bulk_templated_email(self, email_list: list, email_data, template, template_arn):
        """
        Calls to the email utility object in order to reduce the size of the email list to the prescribed maximum of 50
        recipients. Then utilized the boto client to send a pre templated email to users in bulk.
        """
        chunked_email_list = EmailUtility.email_chunker(email_list)
        for smaller_email_list in chunked_email_list:
            response = self.client.send_bulk_templated_email(
                Source='beta@algernon.solutions',
                ReplyToAddresses=[
                    'beta@algernon.solutions',
                ],
                ReturnPath='beta@algernon.solutions',
                Template='example template',
                TemplateArn='arn of the template used to send this bulk email',
                DefaultTemplateData='string',
                Destinations=[
                    {
                        'Destination': {
                            'ToAddresses': smaller_email_list,
                            'CcAddresses': [
                                'string',
                            ],
                            'BccAddresses': [
                                'string',
                            ]
                        },
                        'ReplacementTags': [
                            {
                                'Name': 'string',
                                'Value': 'string'
                            },
                        ],
                        'ReplacementTemplateData': 'string'
                    },
                ]
            )
            return response


class MIMEEmail:
    """
    Handles the creation of the email body, and attachments that will be sent in the email.
    """
    def __init__(self, recipient: str, subject: str, html_body: str, plain_body: str, attachments: list):
        """
        :param recipient: Name of the recipient who will be receiving the email
        :param subject: The subject line of the email
        :param html_body: html version of the email
        :param plain_body: plain text of the email
        :param attachments: list of pre formatted csv strings
        """
        self.recipient = recipient
        self.subject = subject
        self.html_body = html_body
        self.plain_body = plain_body
        self.attachments = attachments
        self.msg = MIMEMultipart("mixed")
        self.msg_body = MIMEMultipart("alternative")

    def create_email(self):
        """
        Completes the assembly of all the various parts of the MIMEEmail object then returns the final contents
        to be used in the EmailDriver
        :return: returns the constructed MIMEEmail
        """
        self.create_email_header()
        self.create_email_body()
        self.create_email_attachments()
        if sys.getsizeof(self.msg) > 1000000:
            raise ValueError
        return self.msg

    def create_email_header(self):
        """
        Adds the subject, recipient, and sender to the MIME email
        """
        self.msg["Subject"] = self.subject
        self.msg["From"] = "beta@algernon.solutions"
        self.msg["To"] = self.recipient

    def create_email_body(self):
        """
        Creates the content of the email, providing for an html, and plain text version
        """
        text_part = MIMEText(self.plain_body.encode("utf-8"), "plain", "utf-8")
        html_part = MIMEText(self.html_body.encode("utf-8"), "html", "utf-8")
        self.msg_body.attach(text_part)
        self.msg_body.attach(html_part)
        self.msg.attach(self.msg_body)

    def create_email_attachments(self):
        """
        iterate through and create email attachments for the MIME message
        """
        for attachment in self.attachments:
            att = MIMEApplication(attachment)
            att.add_header("Content-Disposition", "attachment", filename="data.csv")
            self.msg.attach(att)


class EmailForm:
    """
    Formats the data and injects it into an email format. This is then submitted to the EmailDriver to be sent on to the
    recipient
    """

    def __init__(self, email_subscriber: object):
        self.email_subscriber = email_subscriber
        self.email_data = {
            "csv": [],
            "email": []
        }

    def create_csv(self):
        pass

    def create_email(self):
        pass


class OrganizationDict:
    """
    Collection of EmailSubscribers organized into a dictionary based on company.
    example:

    {
    "mbi": [EmailSubscriber],
    "psi": [EmailSubscriber]
    }

    """
    def __init__(self):
        self.organization_dict = {}

    def add_subscriber(self, email_subscriber: object):
        if email_subscriber.organization in self.organization_dict:
            self.organization_dict[email_subscriber.organization].append(email_subscriber)
        else:
            self.organization_dict[email_subscriber.organization] = []
            self.organization_dict[email_subscriber.organization].append(email_subscriber)


class EmailSubscriber:
    """
    An individual, who is setup to receive an email. Formatted to his or her liking.
    json representation:

    *view is what privilege level the individual has to view data in the system.
        admin = all teams
        team = only the assigned team

    {
    "first_name": "Joe",
    "last_name": "Schmo",
    "email": "jschmo@gmail.com",
    "team": "CSP 1",
    "organization": "MBI",
    "view": "admin",
    "reports": {
        "admin": {"payroll": "csv", "bonus": "email"},
        "clinical": {"tx": "csv", "da": "email"}
    }
    """
    def __init__(self, **kwargs):
        self.first_name = kwargs.get("first_name")
        self.last_name = kwargs.get("last_name")
        self.email = kwargs.get("email")
        self.team = kwargs.get("team")
        self.organization = kwargs.get("organization")
        self.view = kwargs.get("view", "null")
        self.reports = kwargs.get("reports", "null")

    def get_data(self):
        """
        used to retrieve data from an s3 bucket containing reports specific to the individual user
        """
        pass


class EmailUtility:

    @staticmethod
    def email_chunker(email_list, max_chunk_size=50):
        """
        Splits email into chunks set by the max_chunk_size variable, then returns a list of smaller email lists.
        :param email_list: a list of emails
        :param max_chunk_size: maximum amount of emails allowed in a specific list
        :return: list of smaller email lists
        """
        chunked_email_list = []
        if len(email_list) > max_chunk_size:
            for i in range(0, len(email_list), max_chunk_size):
                chunked_email_list.append(email_list[i: i + max_chunk_size])
        else:
            chunked_email_list.append(email_list)
        return chunked_email_list




