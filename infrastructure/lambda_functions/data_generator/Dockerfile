FROM public.ecr.aws/lambda/python:3.12


RUN pip install --no-cache-dir pandas numpy boto3 pyarrow


COPY app.py ${LAMBDA_TASK_ROOT}/app.py
COPY generator.py ${LAMBDA_TASK_ROOT}/generator.py

# Set handler
CMD ["app.lambda_handler"]
