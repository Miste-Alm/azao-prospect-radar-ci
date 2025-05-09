FROM public.ecr.aws/lambda/python:3.12
COPY requirements.txt .
RUN pip install -r requirements.txt -t .
COPY . .
CMD ["pipeline_lambda.lambda_handler"]
