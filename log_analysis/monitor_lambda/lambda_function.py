import json
import os

# import redshift_connector

REDSHIFT_CLUSTER_IDENTIFIER = os.environ['REDSHIFT_CLUSTER_IDENTIFIER']
REDSHIFT_DATABASE = os.environ['REDSHIFT_DATABASE']
REDSHIFT_SCHEMA = os.environ['REDSHIFT_SCHEMA']
REDSHIFT_TABLE = os.environ['REDSHIFT_TABLE']
REDSHIFT_USER = os.environ['REDSHIFT_USER']

# https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime
AWS_REGION = os.environ['AWS_REGION']


def lambda_handler(event, context):
    print(json.dumps(event))
    # conn = get_redshift_connection()
    # with conn.cursor() as cursor:
    #     cursor.execute('SELECT * FROM "dev"."tpcds_100gb"."item" LIMIT 10;')


# def get_redshift_connection():
#     conn = redshift_connector.connect(
#         iam=True,
#         cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
#         port=5439,
#         database=REDSHIFT_DATABASE,
#         db_user=REDSHIFT_USER,
#         user='',
#         password='',
#         region=AWS_REGION,
#     )
#     conn.rollback()
#     conn.autocommit = True

#     return conn
