import json
import boto3
import os

from config import DEPENDENCIES

# https://aws.amazon.com/tw/blogs/compute/choosing-between-aws-lambda-data-storage-options-in-web-apps/
TEMP_DIR = '/tmp'

# https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime
FUNCTION_NAME = os.environ['AWS_LAMBDA_FUNCTION_NAME']

REDSHIFT_CLUSTER_IDENTIFIER = 'redshift-cluster-1'
REDSHIFT_DATABASE = 'dev'
# REDSHIFT_TABLE = 'traffic'
REDSHIFT_USER = os.environ['REDSHIFT_USER']

'''
Sample event
{
    "version": "0",
    "id": "4b9379d1-ad5a-6c24-e5a6-904c8bc04d9d",
    "detail-type": "Redshift Data Statement Status Change",
    "source": "aws.redshift-data",
    "account": "<account-id>",
    "time": "2022-07-15T02:41:06Z",
    "region": "ap-northeast-1",
    "resources": [
        "arn:aws:redshift:ap-northeast-1:<account-id>:cluster:my-redshift-cluster"
    ],
    "detail": {
        "principal": "arn:aws:sts::<account-id>:assumed-role/FortigateOdsStack-functionServiceRoleEF216095-1MUS87LVSN35I/FortigateOdsStack-functionF19B1A04-wy4pNPleJwDl",
        "statementName": "FortigateOdsStack-functionF19B1A04-wy4pNPleJwDl",
        "statementId": "97b3c928-1e4c-4c7a-adac-51a509923491",
        "state": "FINISHED",
        "rows": -1,
        "expireAt": 1658112063
    }
}
'''


def lambda_handler(event, context):
    print(json.dumps(event))
    depended = event['detail']['statementName']
    depended_statement = DEPENDENCIES.get(depended, None)
    if depended_statement:
        sqls = load_sqls('traffic', depended_statement)
        print(sqls)
        execute_id = data_api(sqls)
        print(f'execute_id: {execute_id}')
    else:
        # TODO exception handling
        pass


def load_sqls(*statements) -> dict:
    sqls = list()
    for statement in statements:
        with open(f'{statement}.sql', 'r') as f:
            sql_lines = [line.strip() for line in f.readlines()]
            sqls.append(' '.join(sql_lines))
    return sqls


def data_api(sqls):
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
        Sqls=sqls,
        StatementName=FUNCTION_NAME,
        WithEvent=True,
    )
    return response['Id']
