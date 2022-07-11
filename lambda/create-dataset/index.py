import os
import boto3

forecast = boto3.client('forecast')
RESOURCE_BUCKET = os.environ['RESOURCE_BUCKET']

def handler(event, context):
    account_id = context.invoked_function_arn.split(":")[4]
    region = os.environ['AWS_REGION']

    dataset_dict = event['config']['Dataset']
    status = None

    for kind, dataset in dataset_dict.items():
        csv_s3_uri = f's3://{RESOURCE_BUCKET}/input/{kind}.csv'
        dataset_arn = f'arn:aws:forecast:{region}:{account_id}:dataset/{dataset["DatasetName"]}'

        try:
            status = forecast.describe_dataset(
                DatasetArn=dataset_arn
            )['Status']
        except forecast.exceptions.ResourceNotFoundException:
            print(f'Dataset not found! Creating {kind}: {dataset_arn}')

            forecast.create_dataset(**dataset)

            status = forecast.describe_dataset(
                DatasetArn=dataset_arn
            )['Status']

        dataset['Arn'] = dataset_arn
        dataset['CsvS3Uri'] = csv_s3_uri

        if status in ['CREATE_PENDING', 'CREATE_IN_PROGRESS']:
            raise ResourcePending

    return event


class ResourcePending(Exception):
    pass
