FROM public.ecr.aws/lambda/python:3.9

# Install the function's dependencies using pip3 command
RUN  pip3 install --target "${LAMBDA_TASK_ROOT}" \
    boto3==1.24.4 \
    redshift-connector==2.0.908

# Copy function code
COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]
