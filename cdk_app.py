import os

from aws_cdk import App

from log_analysis.stacks import OdsStack, MonitorStack, DwsStack

app = App()

#
# fortigate_ods_stack
fortigate_ods_stack = OdsStack(
    app, 'FortigateOdsStack', 'fortigate',
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'log_analysis', 'fortigate_lambda',)
)

#
# paloalto_ods_stack
paloalto_ods_stack = OdsStack(
    app, 'PaloaltoOdsStack', 'paloalto',
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'log_analysis', 'paloalto_lambda',)
)

#
# monitor_stack
monitor_stack = MonitorStack(
    app, 'MonitorStack',
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'log_analysis', 'monitor_lambda',)
)

#
# traffic_dws_stack
from log_analysis.traffic_lambda.config import DEPENDENCIES as traffic_dependencies
traffic_dws_stack = DwsStack(
    app, 'TrafficDwsStack',
    list(traffic_dependencies.keys()),
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'log_analysis', 'traffic_lambda',
    )
)

app.synth()
