import os
import boto3

forecast = boto3.client('forecast')

def handler(event, context):
    account_id = context.invoked_function_arn.split(":")[4]
    region = os.environ['AWS_REGION']

    dataset_group = event['config']['DatasetGroup']
    dataset_group_arn = f'arn:aws:forecast:{region}:{account_id}:dataset-group/{dataset_group["DatasetGroupName"]}'

    dataset_dict = event['config']['Dataset']

    dataset_group['DatasetArns'] = []
    for kind, dataset in dataset_dict.items():
        dataset_group['DatasetArns'].append(dataset['Arn'])

    status = None

    try:
        status = forecast.describe_dataset_group(
            DatasetGroupArn=dataset_group_arn
        )['Status']
    except forecast.exceptions.ResourceNotFoundException:
        print(f'Dataset Group not found! Creating: {dataset_group_arn}')

        forecast.create_dataset_group(
            **dataset_group
        )

        status = forecast.describe_dataset_group(
            DatasetGroupArn=dataset_group_arn
        )['Status']
    
    dataset_group['Arn'] = dataset_group_arn

    if status in ['CREATE_PENDING', 'CREATE_IN_PROGRESS']:
        raise ResourcePending

    return event


class ResourcePending(Exception):
    pass
