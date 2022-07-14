from aws_cdk import (CfnOutput, CfnParameter, Duration, Stack, aws_events,
                     aws_events_targets, aws_iam, aws_lambda)
from constructs import Construct


class MonitorStack(Stack):
    def __init__(self, scope: Construct, id: str, lambda_code_path: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        redshift_cluster_identifier = CfnParameter(
            self, id='redshift_cluster_identifier', default='my-redshift-cluster',
        )
        redshift_database = CfnParameter(
            self, id='redshift_database', default='dev',
        )
        redshift_schema = CfnParameter(
            self, id='redshift_schema', default='network_log',
        )
        redshift_table = CfnParameter(
            self, id='redshift_table', default='event_history',
        )

        rule = aws_events.Rule(
            self, id='event_rule',
            event_pattern=aws_events.EventPattern(
                source=['aws.redshift-data'],
                detail_type=["Redshift Data Statement Status Change"],
            ),
        )

        function = aws_lambda.Function(
            self, id='function',
            code=aws_lambda.EcrImageCode.from_asset_image(lambda_code_path),
            handler=aws_lambda.Handler.FROM_IMAGE,
            runtime=aws_lambda.Runtime.FROM_IMAGE,
            memory_size=128,
            timeout=Duration.seconds(30),
            dead_letter_queue_enabled=True,
            environment={
                'REDSHIFT_CLUSTER_IDENTIFIER': redshift_cluster_identifier.value_as_string,
                'REDSHIFT_DATABASE': redshift_database.value_as_string,
                'REDSHIFT_SCHEMA': redshift_schema.value_as_string,
                'REDSHIFT_TABLE': redshift_table.value_as_string,
            }
        )
        function.add_environment('REDSHIFT_USER', function.role.role_name)

        function.role.attach_inline_policy(aws_iam.Policy(
            self, id='function_inline_policy',
            statements=[
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        'redshift-data:*',
                        'redshift:DescribeClusters', # For JDBC/ODBC with IAM auth
                    ],
                    resources=['*'],
                ),
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        'redshift:GetClusterCredentials',
                    ],
                    resources=[
                        f'arn:aws:redshift:*:*:dbname:*/*',
                        f'arn:aws:redshift:*:*:dbuser:*/{function.role.role_name}'
                    ],
                )
            ]
        ))

        rule.add_target(aws_events_targets.LambdaFunction(function))

        CfnOutput(self, id='function_name', value=function.function_name)
        CfnOutput(self, id='function_role', value=function.role.role_name)
