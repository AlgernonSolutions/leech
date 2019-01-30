import logging
import os

import boto3
from botocore.exceptions import ClientError

from admin.set_logging import set_logging
from toll_booth.alg_obj.aws.gyrocopter.template import EmailTemplate


def refresh_templates():
    logging.info('going to refresh the remote ses templates')
    admin_file_name = os.path.dirname(__file__)
    admin_directory = os.path.dirname(admin_file_name)
    schema_file_path = os.path.join(admin_directory, 'starters', 'ses_templates')
    templates = {}
    for subdir, dirs, files in os.walk(schema_file_path):
        for file in files:
            container = os.path.basename(subdir)
            if container not in templates:
                templates[container] = {}
            with open(os.path.join(subdir, file)) as reader:
                templates[container][file] = reader.read()
    for template_name, template_body in templates.items():
        template = EmailTemplate(
            template_name,
            template_body['subject_line.txt'],
            template_body['html_body.html'],
            template_body['text_body.txt']
        )
        try:
            put_template(template)
        except ClientError as e:
            if e.response['Error']['Code'] != 'AlreadyExists':
                raise e
            logging.warning(f'template already exists for {template_name}, can not overwrite')


def put_template(template):
    client = boto3.client('ses')
    client.create_template(**template.for_ses)


if __name__ == '__main__':
    set_logging()
    refresh_templates()
    logging.info('completed refreshing the remote ses templates')
