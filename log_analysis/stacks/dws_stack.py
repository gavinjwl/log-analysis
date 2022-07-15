from typing import List
from aws_cdk import (CfnOutput, Duration, Stack, aws_events,
                     aws_events_targets, aws_iam, aws_lambda)
from constructs import Construct


class DwsStack(Stack):
    def __init__(self, scope: Construct, id: str, depends_on: List[str], lambda_code_path: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        rule = aws_events.Rule(
            self, id='event_rule',
            event_pattern=aws_events.EventPattern(
                source=['aws.redshift-data'],
                detail_type=["Redshift Data Statement Status Change"],
                detail={
                    "statementName": depends_on,
                    "state": ["FINISHED"],
                }
            ),
        )

        function = aws_lambda.Function(
            self, id='function',
            code=aws_lambda.Code.from_asset(lambda_code_path),
            handler='lambda_function.lambda_handler',
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            memory_size=128,
            timeout=Duration.seconds(30),
            dead_letter_queue_enabled=True,
        )
        function.add_environment('REDSHIFT_USER', function.role.role_name)

        function.role.attach_inline_policy(aws_iam.Policy(
            self, id='function_inline_policy',
            statements=[
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    actions=[
                        'redshift-data:*',
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

        CfnOutput(self, id='output_depends_on', value=', '.join(depends_on))
        CfnOutput(self, id='function_name', value=function.function_name)
        CfnOutput(self, id='function_role', value=function.role.role_name)
