import os

import boto3


class Doug:
    @classmethod
    def send_email(cls, recipient, subject_line, body_text, body_html, **kwargs):
        client = boto3.client('pinpoint-email')
        sending_address = os.getenv('SENDING_EMAIL_ADDRESS', 'algernon@algernon.solutions')
        email_config_set = os.getenv('EMAIL_CONFIG_SET', 'leech')
        email_args = {
            'FromEmailAddress': sending_address,
            'Destination': {
                'ToAddresses': [recipient]
            },
            'ReplyToAddresses': [sending_address],
            'ConfigurationSetName ': email_config_set,
            'Content': {
                'Simple': {
                    'Subject': {
                        'Data': subject_line
                    },
                    'Body': {
                        'Text': {
                            'Data': body_text
                        },
                        'Html': {
                            'Data': body_html
                        }
                    }
                }
            },
        }
        if 'cc_recipients' in kwargs:
            email_args['Destination']['CcAddresses'] = kwargs['cc_recipients']
        response = client.send_templated_email(**email_args)
        message_id = response['MessageId']
