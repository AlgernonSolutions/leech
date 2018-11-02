import json
import logging

import boto3
from botocore.exceptions import ClientError

from toll_booth.admin.set_logging import set_logging

logs_client = boto3.client('logs')
iam_client = boto3.client('iam')

log_trust_policy = {
  "Statement": {
    "Effect": "Allow",
    "Principal": { "Service": "logs.us-east-1.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }
}

fire_hose_trust_policy = {
  "Statement": {
    "Effect": "Allow",
    "Principal": { "Service": "firehose.amazonaws.com" },
    "Action": "sts:AssumeRole",
    "Condition": { "StringEquals": { "sts:ExternalId":"803040539655" } }
  }
}

fire_hose_permissions_policy = {
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutObject" ],
      "Resource": [
          "arn:aws:s3:::alg_leech",
          "arn:aws:s3:::alg_leech/*" ]
    }
  ]
}

log_permission_policy = {
    "Statement":[
      {
        "Effect":"Allow",
        "Action":["firehose:*"],
        "Resource":["arn:aws:firehose:us-east-1:803040539655:*"]
      },
      {
        "Effect":"Allow",
        "Action":["iam:PassRole"],
        "Resource":["arn:aws:iam::803040539655:role/CWLtoKinesisFirehoseRole"]
      }
    ]
}


def put_subscription_filter():
    return logs_client.put_subscription_filter(
        logGroupName='/aws/lambda/leech-monitor',
        filterName='fire',
        filterPattern='',
        destinationArn='arn:aws:firehose:us-east-1:803040539655:deliverystream/lambda',
        roleArn='arn:aws:iam::803040539655:role/CWLtoKinesisFirehoseRole'
    )


def create_log_role():
    return iam_client.create_role(
        Path='/',
        RoleName='CWLtoKinesisFirehoseRole',
        AssumeRolePolicyDocument=json.dumps(log_trust_policy),
        Description='allows cloudwatch logs to put logs into a kinesis firehose'
    )


def create_fire_hose_role():
    return iam_client.create_role(
        Path='/',
        RoleName='FirehoseToS3Role',
        AssumeRolePolicyDocument=json.dumps(fire_hose_trust_policy),
        Description='allows kinesis fire hose to put objects in an S3 bucket'
    )


def put_log_policy():
    return iam_client.put_role_policy(
        RoleName='CWLtoKinesisFirehoseRole',
        PolicyName='Permissions-Policy-For-CWL',
        PolicyDocument=json.dumps(log_permission_policy)
    )


def put_fire_hose_policy():
    return iam_client.put_role_policy(
        RoleName='FirehoseToS3Role',
        PolicyName='Permissions-Policy-For-Fire-Hose',
        PolicyDocument=json.dumps(fire_hose_permissions_policy)
    )


if __name__ == '__main__':
    set_logging()
    logging.info('configuring the subscriptions for the lambda functions')

    try:
        logging.info('going to create the cloud watch logging role')
        create_log_role()
        logging.info('created the cloud watch logging role')
    except ClientError as e:
        if e.response['Error']['Code'] != 'EntityAlreadyExists':
            raise e
        logging.info('role already existed, no problem')

    try:
        logging.info('going to add the cloud watch logging role to the cloud watch service')
        put_log_policy()
        logging.info('added the cloud watch logging role')
    except ClientError as e:
        if e.response['Error']['Code'] != 'EntityAlreadyExists':
            raise e
        logging.info('role already existed, no problem')

    try:
        logging.info('going to create the fire hose to s3 role')
        create_fire_hose_role()
        logging.info('created the the fire hose to s3 role')
    except ClientError as e:
        if e.response['Error']['Code'] != 'EntityAlreadyExists':
            raise e
        logging.info('role already existed, no problem')

    try:
        logging.info('going to add the fire hose to s3 role to the fire hose service')
        put_fire_hose_policy()
        logging.info('added the fire hose to s3 role')
    except ClientError:
        print()
    try:
        put_subscription_filter()
    except ClientError:
        print()
