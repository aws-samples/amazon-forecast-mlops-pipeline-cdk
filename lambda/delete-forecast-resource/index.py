import os
import boto3

forecast = boto3.client('forecast')

def handler(event, context):
    dataset_group_arn = event['config']['DatasetGroup']['Arn']
    status = None

    try:
        status = forecast.describe_dataset_group(
            DatasetGroupArn=dataset_group_arn
        )['Status']

        if status in ['DELETE_PENDING', 'DELETE_IN_PROGRESS']:
            raise ResourcePending

        elif status in ['ACTIVE']:
            forecast.delete_resource_tree(
                ResourceArn=dataset_group_arn
            )
        
    except forecast.exceptions.ResourceNotFoundException:
        pass

    return event


class ResourcePending(Exception):
    pass
