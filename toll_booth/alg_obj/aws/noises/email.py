import json
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

    def send_raw_email(self, data):
        response = self.client.send_raw_email(
            Source="beta@algernon.solutions",
            Destinations=[
                "mschappacher@algernon.solutions",
            ],
            RawMessage={
                'Data': data.as_string()
            }
        )
        return response

    @staticmethod
    def create_raw_message(recipient, subject, html_body, plain_body, attachments):
        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"] = "beta@algernon.solutions"
        msg["To"] = recipient
        msg_body = MIMEMultipart("alternative")
        textpart = MIMEText(plain_body.encode("utf-8"), "plain", "utf-8")
        htmlpart = MIMEText(html_body.encode("utf-8"), "html", "utf-8")
        msg_body.attach(textpart)
        msg_body.attach(htmlpart)
        att = MIMEApplication(attachments)
        att.add_header("Content-Disposition", "attachment", filename="data.csv")
        msg.attach(msg_body)
        msg.attach(att)
        return msg


    def send_bulk_email(self):
        pass


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




