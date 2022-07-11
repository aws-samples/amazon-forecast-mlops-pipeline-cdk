import os
import boto3

forecast = boto3.client('forecast')

def handler(event, context):
    account_id = context.invoked_function_arn.split(":")[4]
    region = os.environ['AWS_REGION']

    uid = event['uid']
    predictor = event['config']['Predictor']
    predictor['PredictorName'] += '_' + uid
    predictor_arn = f'arn:aws:forecast:{region}:{account_id}:predictor/{predictor["PredictorName"]}'

    status = None

    try:
        status = forecast.describe_predictor(
            PredictorArn=predictor_arn
        )['Status']
    except forecast.exceptions.ResourceNotFoundException:
        print(f'Predictor not found! Creating: {predictor_arn}')

        if 'InputDataConfig' in predictor.keys():
            predictor['InputDataConfig']['DatasetGroupArn'] = event['config']['DatasetGroup']['Arn']
        else:
            predictor['InputDataConfig'] = {
                'DatasetGroupArn': event['config']['DatasetGroup']['Arn']
            }

        forecast.create_predictor(
            **predictor
        )

        status = forecast.describe_predictor(
            PredictorArn=predictor_arn
        )['Status']

    predictor['Arn'] = predictor_arn

    if status in ['CREATE_PENDING', 'CREATE_IN_PROGRESS']:
        raise ResourcePending

    return event


class ResourcePending(Exception):
    pass
