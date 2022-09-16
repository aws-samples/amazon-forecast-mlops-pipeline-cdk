import os
import boto3
import time


forecast = boto3.client('forecast')


def delete_dataset_import_jobs(dataset_dict):
    for kind, dataset in dataset_dict.items():
        try:
            status = forecast.describe_dataset_import_job(
                DatasetImportJobArn=dataset['ImportJobArn']
            )['Status']

            if status in ['ACTIVE']:
                forecast.delete_dataset_import_job(
                    DatasetImportJobArn=dataset['ImportJobArn']
                )

                raise ResourcePending

            elif status in ['DELETE_PENDING', 'DELETE_IN_PROGRESS']:
                raise ResourcePending

        except forecast.exceptions.ResourceNotFoundException:
            pass


def delete_datasets(dataset_dict):
    for kind, dataset in dataset_dict.items():
        try:
            status = forecast.describe_dataset(
                DatasetArn=dataset['Arn']
            )['Status']

            if status in ['ACTIVE']:
                forecast.delete_dataset(
                    DatasetArn=dataset['Arn']
                )
                raise ResourcePending

            elif status in ['DELETE_PENDING', 'DELETE_IN_PROGRESS']:
                raise ResourcePending

        except forecast.exceptions.ResourceNotFoundException:
            pass


def delete_resource_tree(dataset_group_arn):
    try:
        status = forecast.describe_dataset_group(
            DatasetGroupArn=dataset_group_arn
        )['Status']

        if status in ['ACTIVE']:
            forecast.delete_resource_tree(
                ResourceArn=dataset_group_arn
            )
            raise ResourcePending

        elif status in ['DELETE_PENDING', 'DELETE_IN_PROGRESS']:
            raise ResourcePending
        
    except forecast.exceptions.ResourceNotFoundException:
        pass


def handler(event, context):
    dataset_dict = event['config']['Dataset']
    dataset_group_arn = event['config']['DatasetGroup']['Arn']

    delete_resource_tree(dataset_group_arn)

    delete_dataset_import_jobs(dataset_dict)
    delete_datasets(dataset_dict)

    return event


class ResourcePending(Exception):
    pass
