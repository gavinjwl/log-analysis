import json
import os

import boto3
import redshift_connector

REDSHIFT_CLUSTER_IDENTIFIER = os.environ['REDSHIFT_CLUSTER_IDENTIFIER']
REDSHIFT_DATABASE = os.environ['REDSHIFT_DATABASE']
REDSHIFT_SCHEMA = os.environ['REDSHIFT_SCHEMA']
REDSHIFT_TABLE = os.environ['REDSHIFT_TABLE']
REDSHIFT_USER = os.environ['REDSHIFT_USER']

# https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime
AWS_REGION = os.environ['AWS_REGION']


def lambda_handler(event, context):
    print(json.dumps(event))
    event_time = event['time']

    detail = event['detail']
    principal = detail['principal']
    statement_name = detail['statementName']
    statement_id = detail['statementId']
    state = detail['state']

    statement = f'''
INSERT INTO network_log.event_monitor VALUES 
('{event_time}', '{principal}', '{statement_name}', '{statement_id}', '{state}')
;
    '''
    print(statement)
    data_api(statement)


def data_api(statement):
    client = boto3.client('redshift-data')
    client.execute_statement(
        ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
        # WorkgroupName='string',
        Database=REDSHIFT_DATABASE,
        DbUser=REDSHIFT_USER,
        # SecretArn='string',
        Sql=statement,
        # StatementName=FUNCTION_NAME,
        WithEvent=False,
    )


def direct_commit(statement):
    conn = redshift_connector.connect(
        iam=True,
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        port=5439,
        database=REDSHIFT_DATABASE,
        db_user=REDSHIFT_USER,
        user='',
        password='',
        region=AWS_REGION,
    )
    conn.rollback()
    conn.autocommit = True

    with conn.cursor() as cursor:
        cursor.execute(statement)
