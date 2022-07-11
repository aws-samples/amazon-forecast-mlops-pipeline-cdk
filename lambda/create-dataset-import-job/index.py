import os
import boto3

forecast = boto3.client('forecast')

def handler(event, context):
    uid = event['uid']
    account_id = context.invoked_function_arn.split(":")[4]
    region = os.environ['AWS_REGION']

    config = event['config']
    dataset_dict = config['Dataset']
    status = None

    for kind, dataset in dataset_dict.items():
        import_job_arn = f'arn:aws:forecast:{region}:{account_id}:dataset-import-job/{dataset["DatasetName"]}/{dataset["DatasetName"]}_{uid}'

        try:
            status = forecast.describe_dataset_import_job(
                DatasetImportJobArn=import_job_arn
            )['Status']
        except forecast.exceptions.ResourceNotFoundException:
            print(
                f'Dataset Import Job not found! Creating {kind} Import Job: {import_job_arn}'
            )

            forecast.create_dataset_import_job(
                DatasetImportJobName=f'{dataset["DatasetName"]}_{uid}',
                DatasetArn=dataset['Arn'],
                DataSource={
                    'S3Config': {
                        'Path': dataset['CsvS3Uri'],
                        'RoleArn': os.environ['FORECAST_ROLE'],
                    }
                },
                TimestampFormat=config['TimestampFormat']
            )

            status = forecast.describe_dataset_import_job(
                DatasetImportJobArn=import_job_arn
            )['Status']

        dataset['ImportJobArn'] = import_job_arn

        if status in ['CREATE_PENDING', 'CREATE_IN_PROGRESS']:
            raise ResourcePending

    return event


class ResourcePending(Exception):
    pass
