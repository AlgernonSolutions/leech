import json
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import boto3

"""
This module allows for the separation of data by organizations, then subdivided by admin/clinical/qi reports
for individuals.

Data is then formatted in an appropriate way and distributed to subscribers

This module accepts JSON data objects. Defined within the EmailDataExtractor object.

This drives the identification of subscribers, as well as the data that will be submitted to them.

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

    def send_bulk_templated_email(self, email_list: list, ):
        chunked_email_list = []
        if len(email_list) > 50:
            for i in range(0, len(email_list), 50):
                chunked_email_list.append(email_list[i: i + 50])
        else:
            chunked_email_list.append(email_list)
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


class EmailDataExtractor:
    """
    Converts data into various metrics, and reports. Then returns the reports back to the
        data = {
        "organization": {
            "mbi": {
                "clients": [
                    {
                        "first_name": "Joe",
                        "last_name": "Schmo",
                        "team": "CSP 1",
                        "id": "1111",
                        "csw_first_name": "John",
                        "csw_last_name": "Test",
                        "tx_plan_start": "1/1/1111 1:11:11",
                        "tx_plan_end": "1/7/1111 1:11:11",
                        "da_plan_start": "1/1/1111 1:11:11",
                        "da_plan_end": "1/1/1112 1:11:11",
                        "services_past_week": 11,
                        "services_past_month": 39,
                        "auths": {
                            "diagnostic": "1/1",
                            "community_supt": "245/300",
                            "psychiatry": "22/30"
                        }
                    },
                    {
                        "first_name": "Mr",
                        "last_name": "Smith",
                        "team": "CSP 1",
                        "id": "1112",
                        "csw_first_name": "John",
                        "csw_last_name": "Test",
                        "tx_plan_start": "1/1/1111 1:11:11",
                        "tx_plan_end": "1/7/1111 1:11:11",
                        "da_plan_start": "1/1/1111 1:11:11",
                        "da_plan_end": "1/1/1112 1:11:11",
                        "services_past_week": 11,
                        "services_past_month": 39,
                        "auths": {
                            "diagnostic": "1/1",
                            "community_supt": "245/300",
                            "psychiatry": "22/30"
                            }
                    }
                ],
                "services": [
                    {
                        "service_id": "102546",

                    }
                ],
                "users": [
                         {
                            "first_name": "Joe",
                            "last_name": "Schmo",
                            "email": "jschmo@gmail.com",
                            "team": "CSP 1",
                            "view": "admin",
                            "reports": {
                                "admin": {"payroll": "csv", "bonus": "email"},
                                "clinical": {"tx": "csv", "da": "email"}
                                }
                        },
                        {
                            "first_name": "John",
                            "last_name": "Test",
                            "email": "jschmo@gmail.com",
                            "team": "CSP 1",
                            "view": "admin",
                            "reports": {
                                "admin": {"payroll": "csv", "bonus": "email"},
                                "clinical": {"tx": "csv", "da": "email"}
                        }
                        }
                ]
            },
            "psi": {

            },
            "icfs": {

            }
        }
    }
    """
    def __init__(self, data: json):
        self.data = data

    def create_user_list(self):
        """
         Parses through the JSON object in order to generate a list of users, then creates an OrganizationDict from the
         users contained in the data.
        """
        org_dict = OrganizationDict()
        for organization in self.data["organization"]:
            if "users" in self.data["organization"][organization]:
                for user in self.data["organization"][organization]["users"]:
                    new_user = EmailSubscriber(
                        first_name=user["first_name"],
                        last_name=user["last_name"],
                        email=user["email"],
                        team=user["team"],
                        organization=organization,
                        view=user["view"],
                        reports=user["reports"],
                    )
                    self.create_report_views(new_user.reports)
                    org_dict.add_subscriber(new_user)
            else:
                continue
        return org_dict

    def create_report_views(self, report_data):
        pass


class UserReports:
    """
    defines and creates the canned and custom reports necessary fo each individual. Returns JSON object for further
    processing
    """
    def __init__(self, data: object):
        self.data = data
        self.user_report = {}

    def tx_report(self, subscriber: object):
        """Defines report that returns TX plan information regarding start dates and end dates"""
        tx_report = []
        if subscriber.view is "admin":
            for client in self.data["organization"][subscriber.organization]["clients"]:
                data = {
                    "first_name": client["first_name"],
                    "last_name": client["last_name"],
                    "id": client["id"],
                    "team": client["team"],
                    "tx_plan_start": client["tx_plan_start"],
                    "tx_plan_end": client["tx_plan_end"]
                }
                tx_report.append(data)
        if subscriber.view is "team":
            for client in self.data["organization"][subscriber.organization]["clients"]:
                if client["team"] is subscriber.team:
                    data = {
                        "first_name": client["first_name"],
                        "last_name": client["last_name"],
                        "id": client["id"],
                        "team": client["team"],
                        "tx_plan_start": client["tx_plan_start"],
                        "tx_plan_end": client["tx_plan_end"]
                    }
                    tx_report.append(data)
                else:
                    continue
        self.user_report["tx_report"] = tx_report




