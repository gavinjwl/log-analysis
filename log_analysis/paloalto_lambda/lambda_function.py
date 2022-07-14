import json
import boto3
import os


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
            data_api_id = redshift_copy(bucket_name, object_key)
            processed.append({
                'input_object': f's3://{bucket_name}/{object_key}',
                'output_object': f's3://{bucket_name}/{object_key}',
                'data-api-id': data_api_id,
            })
        else:
            print(f's3://{bucket_name}/{object_key} less than zero.')
    print(json.dumps({'Records': processed}))


def redshift_copy(bucket_name, object_key):
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
            f'''COPY {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} FROM 's3://{bucket_name}/{object_key}' IAM_ROLE default CSV DELIMITER ',' QUOTE '"' IGNOREBLANKLINES''',
        ],
        StatementName=FUNCTION_NAME,
        WithEvent=True,
    )
    return response['Id']
