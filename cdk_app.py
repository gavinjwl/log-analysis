import os

from aws_cdk import App

from log_analysis.stacks import OdsStack

app = App()

fortigate_dir = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'log_analysis', 'fortigate_lambda',
)
print(fortigate_dir)
FortigateOdsStack = OdsStack(app, 'FortigateOdsStack', 'fortigate', fortigate_dir)


paloalto_dir = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'log_analysis', 'paloalto_lambda',
)
print(paloalto_dir)
FortigateOdsStack = OdsStack(app, 'PaloaltoOdsStack', 'paloalto', paloalto_dir)

app.synth()
