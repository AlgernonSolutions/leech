import sys
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3

"""
This module allows for the separation of data by organizations, then subdivided by admin/clinical/qi reports
for individuals.

This module is predicated on the definition of canned reports, as well as creation of custom reports.

"""


class EmailMessageText:
    """
        the text provided for an email, the actual words that go into the email itself
    """
    def __init__(self, message_text, html_tag):
        """

        :param message_text: the text which show up in the email itself
        :param html_tag: the wrapping html tag to be used in formatting the text
        """
        self._message_text = message_text
        self._html_tag = html_tag

    @property
    def message_text(self):
        return self._message_text

    @property
    def html_text(self):
        return f'<{self._html_tag}>{self._message_text}</{self._html_tag}>'


class EmailBody:
    """
        the composed email text object, contains all the text blocks and their formatting
    """

    def __init__(self, message_text_sections: [EmailMessageText], **kwargs):
        """
        :param message_text_sections: ordered iterable containing EmailMessageText objects
        """
        self._message_text_sections = message_text_sections
        self._separators = kwargs.get("separators", '\n')

    @property
    def plain_text(self):
        return self._separators.join([x.message_text for x in self._message_text_sections])

    @property
    def plain_text_mime(self):
        return MIMEText(self.plain_text.encode("utf-8"), "plain", "utf-8")

    @property
    def html_text(self):
        return ''.join([x.html_text for x in self._message_text_sections])

    @property
    def html_text_mime(self):
        return MIMEText(self.html_text.encode("utf-8"), "html", "utf-8")


class MIMEEmail:
    """
    Handles the creation of the email body, and attachments that will be sent in the email.
    """
    def __init__(self, recipient: str, subject: str, message: MIMEMultipart):
        """
        :param recipient: Name of the recipient who will be receiving the email
        :param subject: The subject line of the email
        :param message: a MIMEMultipart object containing the final email object
        """
        self.recipient = recipient
        self.subject = subject
        self.message = message

    def __str__(self):
        return self.message.as_string()

    @classmethod
    def build_email(cls, recipient_email_address: str, subject: str, email_message_text: EmailBody, **kwargs):
        """
        :param recipient_email_address: the email address of the intended recipient
        :param subject: the subject line of the sent email
        :param email_message_text: EmailMessageText object
        :param kwargs:
        :return: MIMEEmail object
        """
        from_sender = kwargs.get('from', 'beta@algernon.solutions')
        attachments = kwargs.get('attachments', [])
        message = MIMEMultipart("mixed")
        message = cls._set_message_attributes(message, recipient_email_address, from_sender, subject)
        message_body = MIMEMultipart("alternative")
        message_body.attach(email_message_text.plain_text_mime)
        message_body.attach(email_message_text.html_text_mime)
        message.attach(message_body)
        for attachment in attachments:
            message.attach(cls._create_attachments(attachment, **kwargs))
        return cls(recipient_email_address, subject, message)

    @classmethod
    def _set_message_attributes(cls, message: MIMEMultipart, recipient_email: str, from_sender: str, subject: str):
        message['Subject'] = subject
        message['From'] = from_sender
        message['To'] = recipient_email
        return message

    @classmethod
    def _create_attachments(cls, attachment: object, **kwargs):
        attachment_type = kwargs.get('attachment_type', 'data.csv')
        att = MIMEApplication(attachment)
        att.add_header("Content-Disposition", "attachment", filename=attachment_type)
        return att


class EmailDriver:
    """
    Utilizes the boto3 library in order to send an email.
    """

    def __init__(self, organization_dict: object):
        self.client = boto3.client('ses')
        self.organization_dict = organization_dict

    def send_raw_email(self, mime_email: MIMEEmail):
        """
        Takes a MIME formatted content, and recipient list to construct an email
        :param mime_email: MIMEEmail object
        :return: returns the boto response object
        """
        command_kwargs = {
            'Source': 'beta@algernon.solutions',
            'Destination': mime_email.recipient,
            'RawMessage': {'Data': str(mime_email)}
        }
        response = self.client.send_raw_email(**command_kwargs)
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

    def add_subscriber(self, email_subscriber):
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
