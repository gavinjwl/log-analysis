import gzip
import json
import os
import re
from pathlib import Path

import boto3

# https://aws.amazon.com/tw/blogs/compute/choosing-between-aws-lambda-data-storage-options-in-web-apps/
TEMP_DIR = '/tmp'

# https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime
FUNCTION_NAME = os.environ['AWS_LAMBDA_FUNCTION_NAME']

REDSHIFT_CLUSTER_IDENTIFIER = os.environ['REDSHIFT_CLUSTER_IDENTIFIER']
REDSHIFT_DATABASE = os.environ['REDSHIFT_DATABASE']
REDSHIFT_SCHEMA = os.environ['REDSHIFT_SCHEMA']
REDSHIFT_TABLE = os.environ['REDSHIFT_TABLE']
REDSHIFT_USER = os.environ['REDSHIFT_USER']


def lambda_handler(event, context):
    # print(json.dumps(event))

    for record in event['Records']:
        if record['eventSource'] == 'aws:sqs':
            s3_event = json.loads(record['body'])
            if 'Records' in s3_event.keys():
                handle_s3_events(s3_event)
            else:
                print('Passing S3 test event')
        else:
            print('Unsupported event source')


def handle_s3_events(event):
    processed = list()
    for _record in event['Records']:
        bucket_name = _record['s3']['bucket']['name']
        object_key = _record['s3']['object']['key']
        object_size = _record['s3']['object']['size']
        if object_size > 0:
            input_name = extract(bucket_name, object_key)
            output_name = transform(input_name)
            output_key_prefix = 'curated/'
            load(bucket_name, output_key_prefix, output_name)
            data_api_id = redshift_copy(bucket_name, output_key_prefix, output_name)
            processed.append({
                'data-api-id': data_api_id,
                'input_object': f's3://{bucket_name}/{object_key}',
                'output_object': f's3://{bucket_name}/{output_key_prefix}{output_name}',
            })
        else:
            print(f's3://{bucket_name}/{object_key} less than zero.')
    print(json.dumps({'Records': processed}))


def extract(bucket_name, object_key) -> str:
    '''
    Download from S3
    '''
    file = Path(object_key)
    file_path = os.path.join(TEMP_DIR, file.name)
    s3 = boto3.client('s3')
    with open(file_path, 'wb') as f:
        s3.download_fileobj(bucket_name, object_key, f)
    return file.name


def transform(input_name) -> str:
    '''
    Extract column from data
    '''
    input_path = os.path.join(TEMP_DIR, input_name)
    output_name = f'curated-{input_name}.gz'
    output_path = os.path.join(TEMP_DIR, output_name)

    date_pattern = r"\s?date=(\S+)\s?"
    time_pattern = r"\s?time=(\S+)\s?"
    patterns = [
        ('device_name', r"\s?devname=(\S+)\s?"),
        ('source_ip', r"\s?srcip=(\S+)\s?"),
        ('source_port', r"\s?srcport=(\S+)\s?"),
        ('destination_ip', r"\s?dstip=(\S+)\s?"),
        ('destination_port', r"\s?dstport=(\S+)\s?"),
        ('action', r"\s?action=(\S+)\s?"),
        ('duration', r"\s?duration=(\S+)\s?"),
        ('sentbyte', r"\s?sentbyte=(\S+)\s?"),
        ('rcvdbyte', r"\s?rcvdbyte=(\S+)\s?"),
        # ('device_name', r'\s?devname=(\S+)\s?'),
        ('source_ip', r'\s?src=(\S+)\s?'),
        ('source_port', r'\s?src_port=(\S+)\s?'),
        ('destination_ip', r'\s?dst=(\S+)\s?'),
        ('destination_port', r'\s?dst_port=(\S+)\s?'),
        ('action', r'\s?status=(\S+)\s?'),
        # ('duration', r'\s?duration=(\S+)\s?'),
        ('sentbyte', r'\s?sent=(\S+)\s?'),
        ('rcvdbyte', r'\s?rcvd=(\S+)\s?'),
    ]

    with open(input_path, 'r') as input, gzip.open(output_path, 'w') as output:
        for input_line in input:
            json_data = dict()

            date_match = re.search(date_pattern, input_line)
            time_match = re.search(time_pattern, input_line)
            if date_match and time_match:
                (_date) = date_match.group(1)
                (_time) = time_match.group(1)
                json_data['date_time_raw'] = f'{_date} {_time}'

            for (key, pattern) in patterns:
                match = re.search(pattern, input_line)
                if match:
                    value = match.group(1)
                    json_data[key] = value.strip('"')
            output_line = json.dumps(json_data)
            output.write(f'{output_line}\n'.encode('utf-8'))

    return output_name


def load(bucket_name, key_prefix, output_name):
    '''
    Upload curated data onto S3
    '''
    output_path = os.path.join(TEMP_DIR, output_name)

    s3 = boto3.client('s3')
    with open(output_path, 'rb') as f:
        s3.upload_fileobj(f, bucket_name, f'{key_prefix}{output_name}')
    os.remove(output_path)


def redshift_copy(bucket_name, key_prefix, output_name):
    '''
    Execute COPY command via Redshift Data API
    '''
    client = boto3.client('redshift-data')
    response = client.batch_execute_statement(
        ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
        # WorkgroupName='string',
        Database=REDSHIFT_DATABASE,
        DbUser=REDSHIFT_USER,
        # SecretArn='string',
        Sqls=[
            f'''COPY {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} FROM 's3://{bucket_name}/{key_prefix}{output_name}' IAM_ROLE default JSON AS 'auto ignorecase' GZIP''',
        ],
        StatementName=FUNCTION_NAME,
        WithEvent=True,
    )
    return response['Id']
