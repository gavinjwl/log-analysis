from aws_cdk.aws_s3 import NotificationKeyFilter
from aws_cdk import (CfnParameter, CfnOutput, Duration, Stack, aws_lambda,
                     aws_lambda_event_sources, aws_s3, aws_s3_notifications,
                     aws_sqs, aws_iam, )
from constructs import Construct


class OdsStack(Stack):
    def __init__(self, scope: Construct, id: str, name: str, lambda_code_path: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        redshift_cluster_identifier = CfnParameter(
            self, id='redshift_cluster_identifier', default='redshift-cluster-1',
        )
        redshift_database = CfnParameter(
            self, id='redshift_database', default='dev',
        )
        redshift_schema = CfnParameter(
            self, id='redshift_schema', default='network_log',
        )
        # redshift_table = CfnParameter(self, id='redshift_table')
        # redshift_user = CfnParameter(self, id='redshift_user')

        bucket = aws_s3.Bucket(
            self, id='bucket',
            # bucket_name=f'{cfn_ref.ACCOUNT_ID}_{cfn_ref.REGION}_{cfn_ref.STACK_NAME}',
            event_bridge_enabled=True,
        )
        queue = aws_sqs.Queue(self, id="queue")
        bucket.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED,
            aws_s3_notifications.SqsDestination(queue),
            NotificationKeyFilter(prefix='raw/'),
        )

        function = aws_lambda.Function(
            self, id='function',
            code=aws_lambda.Code.from_asset(lambda_code_path),
            handler='lambda_function.lambda_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            memory_size=128,
            timeout=Duration.seconds(30),
            reserved_concurrent_executions=1,
            dead_letter_queue_enabled=True,
            events=[
                aws_lambda_event_sources.SqsEventSource(queue),
            ],
            environment={
                'BUCKET_NAME': bucket.bucket_name,
                'REDSHIFT_CLUSTER_IDENTIFIER': redshift_cluster_identifier.value_as_string,
                'REDSHIFT_DATABASE': redshift_database.value_as_string,
                'REDSHIFT_SCHEMA': redshift_schema.value_as_string,
                'REDSHIFT_TABLE': name,
            }
        )
        function.add_environment('REDSHIFT_USER', function.role.role_name)

        function.role.attach_inline_policy(aws_iam.Policy(
            self, id='function_inline_policy',
            statements=[
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "redshift-data:*",
                    ],
                    resources=["*"],
                ),
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        "redshift:GetClusterCredentials",
                    ],
                    resources=[
                        f"arn:aws:redshift:*:*:dbname:*/*",
                        f"arn:aws:redshift:*:*:dbuser:*/{function.role.role_name}"
                    ],
                )
            ]
        ))

        bucket.grant_read_write(function)

        CfnOutput(self, id='bucket_name', value=bucket.bucket_name)
        CfnOutput(self, id='function_name', value=function.function_name)
        CfnOutput(self, id='function_role', value=function.role.role_name)
