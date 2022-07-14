import os

from aws_cdk import App

from log_analysis.stacks import OdsStack, MonitorStack

app = App()

fortigate_dir = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'log_analysis', 'fortigate_lambda',
)
FortigateOdsStack = OdsStack(app, 'FortigateOdsStack', 'fortigate', fortigate_dir)

paloalto_dir = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'log_analysis', 'paloalto_lambda',
)
FortigateOdsStack = OdsStack(app, 'PaloaltoOdsStack', 'paloalto', paloalto_dir)

monitor_dir = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'log_analysis', 'monitor_lambda',
)
MonitorStack = MonitorStack(app, 'MonitorStack', monitor_dir)

app.synth()
