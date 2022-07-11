import os
import boto3

forecast = boto3.client('forecast')
cw = boto3.client('cloudwatch')

def post_metric(metrics):
    for metric in metrics['PredictorEvaluationResults']:
        cw.put_metric_data(
            Namespace='FORECAST',
            MetricData=[
                {
                    'Dimensions':
                        [
                            {
                                'Name': 'Algorithm',
                                'Value': metric['AlgorithmArn']
                            }, {
                                'Name': 'Quantile',
                                'Value': str(quantile['Quantile'])
                            }
                        ],
                    'MetricName': 'WQL',
                    'Unit': 'None',
                    'Value': quantile['LossValue']
                } for quantile in metric['TestWindows'][0]['Metrics']
                ['WeightedQuantileLosses']
            ] + [
                {
                    'Dimensions':
                        [
                            {
                                'Name': 'Algorithm',
                                'Value': metric['AlgorithmArn']
                            }
                        ],
                    'MetricName': 'RMSE',
                    'Unit': 'None',
                    'Value': metric['TestWindows'][0]['Metrics']['RMSE']
                }
            ]
        )


def handler(event, context):
    account_id = context.invoked_function_arn.split(":")[4]
    region = os.environ['AWS_REGION']

    uid = event['uid']
    predictor_arn = event['config']['Predictor']['Arn']
    forecast_config = event['config']['Forecast']
    forecast_config['ForecastName'] += '_' + uid
    forecast_arn = f'arn:aws:forecast:{region}:{account_id}:forecast/{forecast_config["ForecastName"]}'

    status = None

    # create forecast
    try:
        status = forecast.describe_forecast(
            ForecastArn=forecast_arn
        )['Status']
    except forecast.exceptions.ResourceNotFoundException:
        post_metric(
            forecast.get_accuracy_metrics(
                PredictorArn=predictor_arn
            )
        )

        print(f'Forecast not found! Creating: {forecast_arn}')

        forecast.create_forecast(
            **forecast_config, PredictorArn=predictor_arn
        )

        status = forecast.describe_forecast(
            ForecastArn=forecast_arn
        )['Status']

    forecast_config['Arn'] = forecast_arn

    if status in ['CREATE_PENDING', 'CREATE_IN_PROGRESS']:
        raise ResourcePending

    # put metrics int context
    metrics = forecast.get_accuracy_metrics(
        PredictorArn=predictor_arn
    )['PredictorEvaluationResults'][0]

    for row in metrics['TestWindows']:
        if row['EvaluationType'] == 'COMPUTED':
            row['TestWindowStart'] = row['TestWindowStart'].strftime("%Y%m%dT%H%M%S")
            row['TestWindowEnd'] = row['TestWindowEnd'].strftime("%Y%m%dT%H%M%S")

    event['metrics'] = metrics

    # create forecast export job
    export_job_arn = f'arn:aws:forecast:{region}:{account_id}:forecast-export-job/{forecast_config["ForecastName"]}/{forecast_config["ForecastName"]}'

    try:
        status = forecast.describe_forecast_export_job(
            ForecastExportJobArn=export_job_arn
        )['Status']
    except forecast.exceptions.ResourceNotFoundException:
        print(f'Forecast export job not found! Creating: {export_job_arn}')

        forecast.create_forecast_export_job(
            ForecastExportJobName=forecast_config['ForecastName'],
            ForecastArn=forecast_arn,
            Destination={
                'S3Config':
                    {
                        'Path': f's3://{os.environ["RESOURCE_BUCKET"]}/export/{uid}/',
                        'RoleArn': os.environ['FORECAST_ROLE']
                    }
            }
        )
        status = forecast.describe_forecast_export_job(
            ForecastExportJobArn=export_job_arn
        )['Status']

    forecast_config['ExportJobArn'] = export_job_arn

    if status in ['CREATE_PENDING', 'CREATE_IN_PROGRESS']:
        raise ResourcePending

    return event


class ResourcePending(Exception):
    pass
