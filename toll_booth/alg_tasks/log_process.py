import base64
import json
import gzip
import re

import boto3

patterns = {
    'report': '(((Duration:\s)(?P<duration>\w+\.\w+))+)|((Billed Duration:\s)(?P<billed_duration>\w+))|((Memory\sSize:\s)(?P<memory_size>\w+))|((Max\sMemory\sUsed:\s)(?P<memory_used>\w+))',
    'log_timestamp': '(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3})',
    'log_level': '(\[\w+\])',
    'lambda': '(?P<status>(START)|(END)|(REPORT))\s(RequestId):\s(?P<request_id>\S+)',
    'python_lambda': '((\|)(\|function_name:\s(?P<function_name>\w+))|(\|function_arn:\s(?P<function_arn>[\w:,-]+))|(\|request_id:\s(?P<request_id>[\w\d-]+))(\|\|))'

}


def transform_python_log(timestamp, message, log_level_match, log_timestamp_match):
    lambda_pattern = re.compile(patterns['python_lambda'])
    lambda_match = lambda_pattern.search(message)
    lambda_section_pattern = re.compile('(\|\|.+\|\|)')
    lambda_section_match = lambda_section_pattern.search(message)
    log_level_identifier = log_level_match.group()
    log_level = log_level_identifier[1:len(log_level_identifier) - 1]
    log_timestamp = log_timestamp_match.group()
    log_message = message.replace(log_level_identifier, '')
    log_message = log_message.replace(log_timestamp, '')
    if lambda_section_match:
        lambda_section = lambda_section_match.group()
        log_message = log_message.replace(lambda_section, '')
    log_message = log_message.strip()
    python_log = {
        'timestamp': timestamp,
        'log_level': log_level,
        'log_timestamp': log_timestamp,
        'log_message': log_message
    }
    if lambda_match:
        python_log['lambda_function_name'] = lambda_match.group('function_name')
        field_names = ['function_arn', 'request_id']
        for field_name in field_names:
            lambda_match = lambda_pattern.search(message, lambda_match.start() + 1)
            field_value = lambda_match.group(field_name)
            python_log[field_name] = field_value
    return json.dumps(python_log)


def transform_lambda_record(lambda_timestamp, message, lambda_match):
    report_pattern = re.compile(patterns['report'], re.MULTILINE)
    report_match = report_pattern.search(message)
    lambda_log = {
        'timestamp': lambda_timestamp,
        'request_id': lambda_match.group('request_id'),
        'lambda_status': lambda_match.group('status')
    }
    if report_match:
        lambda_log['duration'] = report_match.group('duration')
        field_names = ['billed_duration', 'memory_size', 'memory_used']
        for field_name in field_names:
            report_match = report_pattern.search(message, report_match.start() + 1)
            field_value = report_match.group(field_name)
            lambda_log[field_name] = field_value
    return json.dumps(lambda_log)


def transform_log_event(log_event):
    message = log_event['message']
    log_timestamp_pattern = re.compile(patterns['log_timestamp'])
    log_timestamp_match = log_timestamp_pattern.search(message)
    log_level_pattern = re.compile(patterns['log_level'])
    log_level_match = log_level_pattern.search(message)
    lambda_invocation_pattern = re.compile(patterns['lambda'])
    lambda_match = lambda_invocation_pattern.search(message)
    log_timestamp = log_event['timestamp']
    transformed_log = json.dumps({'message': message, 'timestamp': log_timestamp})
    if log_level_match and log_timestamp_match:
        transformed_log = transform_python_log(log_timestamp, message, log_level_match, log_timestamp_match)
    if lambda_match:
        transformed_log = transform_lambda_record(log_timestamp, message, lambda_match)
    return transformed_log + '\n'


def process_records(records):
    for r in records:
        raw_data = base64.b64decode(r['data'])
        decompressed_data = gzip.decompress(raw_data)
        data = json.loads(decompressed_data)

        record_id = r['recordId']
        if data['messageType'] == 'CONTROL_MESSAGE':
            yield {
                'result': 'Dropped',
                'recordId': record_id
            }
        elif data['messageType'] == 'DATA_MESSAGE':
            data = ''.join([transform_log_event(e) for e in data['logEvents']])
            encoded_data = data.encode()
            data = base64.b64encode(encoded_data).decode()

            yield {
                'data': data,
                'result': 'Ok',
                'recordId': record_id
            }
        else:
            yield {
                'result': 'ProcessingFailed',
                'recordId': record_id
            }


def put_records_to_fire_hose_stream(stream_name, records, client, attempts_made, max_attempts):
    failed_records = []
    codes = []
    err_msg = ''
    # if put_record_batch throws for whatever reason, response['xx'] will error out, adding a check for a valid
    # response will prevent this
    response = None
    try:
        response = client.put_record_batch(DeliveryStreamName=stream_name, Records=records)
    except Exception as e:
        failed_records = records
        err_msg = str(e)

    # if there are no failedRecords (put_record_batch succeeded), iterate over the response to gather results
    if not failed_records and response and response['FailedPutCount'] > 0:
        for idx, res in enumerate(response['RequestResponses']):
            # (if the result does not have a key 'ErrorCode' OR if it does and is empty) => we do not need to re-ingest
            if 'ErrorCode' not in res or not res['ErrorCode']:
                continue

            codes.append(res['ErrorCode'])
            failed_records.append(records[idx])

        err_msg = 'Individual error codes: ' + ','.join(codes)

    if len(failed_records) > 0:
        if attempts_made + 1 < max_attempts:
            print('Some records failed while calling PutRecordBatch to Firehose stream, retrying. %s' % err_msg)
            put_records_to_fire_hose_stream(stream_name, failed_records, client, attempts_made + 1, max_attempts)
        else:
            raise RuntimeError('Could not put records after %s attempts. %s' % (str(max_attempts), err_msg))


def put_records_to_kinesis_stream(stream_name, records, client, attempts_made, max_attempts):
    failed_records = []
    codes = []
    err_msg = ''
    # if put_records throws for whatever reason, response['xx'] will error out, adding a check for a valid
    # response will prevent this
    response = None
    try:
        response = client.put_records(StreamName=stream_name, Records=records)
    except Exception as e:
        failed_records = records
        err_msg = str(e)

    # if there are no failedRecords (put_record_batch succeeded), iterate over the response to gather results
    if not failed_records and response and response['FailedRecordCount'] > 0:
        for idx, res in enumerate(response['Records']):
            # (if the result does not have a key 'ErrorCode' OR if it does and is empty) => we do not need to re-ingest
            if 'ErrorCode' not in res or not res['ErrorCode']:
                continue

            codes.append(res['ErrorCode'])
            failed_records.append(records[idx])

        err_msg = 'Individual error codes: ' + ','.join(codes)

    if len(failed_records) > 0:
        if attempts_made + 1 < max_attempts:
            print('Some records failed while calling PutRecords to Kinesis stream, retrying. %s' % err_msg)
            put_records_to_kinesis_stream(stream_name, failed_records, client, attempts_made + 1, max_attempts)
        else:
            raise RuntimeError('Could not put records after %s attempts. %s' % (str(max_attempts), err_msg))


def create_reingestion_record(is_sas, original_record):
    if is_sas:
        return {'data': base64.b64decode(original_record['data']), 'partitionKey': original_record['kinesisRecordMetadata']['partitionKey']}
    else:
        return {'data': base64.b64decode(original_record['data'])}


def get_reingestion_record(is_sas, reingestion_record):
    if is_sas:
        return {'Data': reingestion_record['data'], 'PartitionKey': reingestion_record['partitionKey']}
    else:
        return {'Data': reingestion_record['data']}


def handler(event, context):
    is_sas = 'sourceKinesisStreamArn' in event
    stream_arn = event['sourceKinesisStreamArn'] if is_sas else event['deliveryStreamArn']
    region = stream_arn.split(':')[3]
    stream_name = stream_arn.split('/')[1]
    records = list(process_records(event['records']))
    projected_size = 0
    data_by_record_id = {rec['recordId']: create_reingestion_record(is_sas, rec) for rec in event['records']}
    put_record_batches = []
    records_to_reingest = []
    total_records_to_be_reingested = 0

    for idx, rec in enumerate(records):
        if rec['result'] != 'Ok':
            continue
        projected_size += len(rec['data']) + len(rec['recordId'])
        # 6000000 instead of 6291456 to leave ample headroom for the stuff we didn't account for
        if projected_size > 6000000:
            total_records_to_be_reingested += 1
            records_to_reingest.append(
                get_reingestion_record(is_sas, data_by_record_id[rec['recordId']])
            )
            records[idx]['result'] = 'Dropped'
            del(records[idx]['data'])

        # split out the record batches into multiple groups, 500 records at max per group
        if len(records_to_reingest) == 500:
            put_record_batches.append(records_to_reingest)
            records_to_reingest = []

    if len(records_to_reingest) > 0:
        # add the last batch
        put_record_batches.append(records_to_reingest)

    # iterate and call putRecordBatch for each group
    records_reingested_so_far = 0
    if len(put_record_batches) > 0:
        client = boto3.client('kinesis', region_name=region) if is_sas else boto3.client('firehose', region_name=region)
        for recordBatch in put_record_batches:
            if is_sas:
                put_records_to_kinesis_stream(stream_name, recordBatch, client, attempts_made=0, max_attempts=20)
            else:
                put_records_to_fire_hose_stream(stream_name, recordBatch, client, attempts_made=0, max_attempts=20)
            records_reingested_so_far += len(recordBatch)
            print('Reingested %d/%d records out of %d' % (records_reingested_so_far, total_records_to_be_reingested, len(event['records'])))
    else:
        print('No records to be reingested')

    return {"records": records}
