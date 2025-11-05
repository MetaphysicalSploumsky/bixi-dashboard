FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY lambda.py ${LAMBDA_TASK_ROOT}

CMD [ "lambda.lambda_handler"]