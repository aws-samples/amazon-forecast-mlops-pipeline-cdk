import { Construct } from 'constructs';
import { Duration } from 'aws-cdk-lib';
import {
    aws_ec2 as ec2, 
    aws_iam as iam, 
    aws_logs as logs, 
    aws_s3 as s3,
    aws_stepfunctions as sfn, 
    aws_stepfunctions_tasks as tasks 
} from 'aws-cdk-lib';

import { GlueConstruct } from './glue';
import { LambdaConstruct } from './lambda';
import { TriggerConstruct } from './trigger';

export interface StateMachineProps {
    resourceBucket: s3.Bucket;
}

export class StateMachine extends Construct {
    constructor(scope: Construct, id: string, props: StateMachineProps) {
        super(scope, id);

        const resourceBucket = props.resourceBucket;

        // IAM Role to pass to Forecast
        const forecastRole = new iam.Role(this, 'ForecastRole', {
			assumedBy: new iam.ServicePrincipal('forecast.amazonaws.com'),
			roleName: 'ForecastMLOpsPipeline-ForecastRole',
			managedPolicies: [
				{managedPolicyArn: 'arn:aws:iam::aws:policy/CloudWatchFullAccess'},
				{managedPolicyArn: 'arn:aws:iam::aws:policy/AmazonS3FullAccess'},
			]
		});

        // Preprocessing
        const preprocess = new GlueConstruct(this, 'ForecastMLOpsPreprocess', {
            taskName: 'Forecast-MLOps-Preprocess',
            pythonFilePath: 'glue/preprocess.py',
            defaultArguments: {
                '--bucket': props.resourceBucket.bucketName,
                '--fileuri': 'raw/data.zip',
            },
        });

        // Create Dataset
        const createDataset = new LambdaConstruct(
            this, 'ForecastMLOpsCreateDataset', 
            {
                taskName: 'Forecast-MLOps-Create-Dataset',
                lambdaCodePath: 'lambda/create-dataset',
                timeout: Duration.seconds(30),
                environment: {
                    RESOURCE_BUCKET: props.resourceBucket.bucketName,
                }
            }
        );

        // Create DatasetGroup
        const createDatasetGroup = new LambdaConstruct(
            this, 'ForecastMLOpsCreateDatasetGroup', 
            {
                taskName: 'Forecast-MLOps-Create-Dataset-Group',
                lambdaCodePath: 'lambda/create-dataset-group',
                timeout: Duration.seconds(30),
            }
        );

        // Create DatasetImportJob
        const createDatasetImportJob = new LambdaConstruct(
            this, 'ForecastMLOpsCreateDatasetImportJob', 
            {
                taskName: 'Forecast-MLOps-Create-Dataset-Import-Job',
                lambdaCodePath: 'lambda/create-dataset-import-job',
                timeout: Duration.seconds(30),
                environment: {
                    FORECAST_ROLE: forecastRole.roleArn,
                }
            }
        );

        // Create Predictor
        const createPredictor = new LambdaConstruct(
            this, 'ForecastMLOpsCreatePredictor', 
            {
                taskName: 'Forecast-MLOps-Create-Predictor',
                lambdaCodePath: 'lambda/create-predictor',
                timeout: Duration.seconds(30),
            }
        );

        // Create Forecast
        const createForecast = new LambdaConstruct(
            this, 'ForecastMLOpsCreateForecast', 
            {
                taskName: 'Forecast-MLOps-Create-Forecast',
                lambdaCodePath: 'lambda/create-forecast',
                timeout: Duration.seconds(30),
                environment: {
                    FORECAST_ROLE: forecastRole.roleArn,
                    RESOURCE_BUCKET: props.resourceBucket.bucketName,
                }
            }
        );

        // Postprocessing
        const postprocess = new GlueConstruct(this, 'ForecastMLOpsPostprocess', {
            taskName: 'Forecast-MLOps-Postprocess',
            pythonFilePath: 'glue/postprocess.py',
            arguments: {
                '--uid': sfn.JsonPath.stringAt("$.uid"),
                '--bucket': props.resourceBucket.bucketName,
            }
        });

        // Delete Forecast Resources
        const deleteForecastResource = new LambdaConstruct(
            this, 'ForecastMLOpsDeleteForecastResource', 
            {
                taskName: 'Forecast-MLOps-Delete-Forecast-Resource',
                lambdaCodePath: 'lambda/delete-forecast-resource',
                timeout: Duration.seconds(30),
                environment: {
                    FORECAST_ROLE: forecastRole.roleArn,
                }
            }
        );

        // common
        const skipDeletion = new sfn.Pass(this, 'ForecastMLOpsPipelineSkipDeletion');
		const deleteOrNot = new sfn.Choice(this, 'ForecastMLOpsPipelineStrategyChoice', {
		}).when(
			sfn.Condition.booleanEquals('$.config.PerformDelete', true),
            deleteForecastResource.task
		).otherwise(
            skipDeletion   
        );

        /****** StateMachine - Begin ******/
        // IAM Role for StateMachine
        const statesMachineExecutionRole = new iam.Role(
            this, 'ForecastMLOpsPipelineStateMachineExecutionRole', {
                assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
                roleName: 'ForecastMLOpsPipelineStateMachineExecutionRole',
                managedPolicies: [
                    {managedPolicyArn: 'arn:aws:iam::aws:policy/service-role/AWSLambdaRole'},
                    {managedPolicyArn: 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'},
                ],
            }
        );

        // StateMachine Definition
        const definition = preprocess.task
                        .next(createDataset.task)
                        .next(createDatasetGroup.task)
                        .next(createDatasetImportJob.task)
                        .next(createPredictor.task)
                        .next(createForecast.task)
                        .next(postprocess.task)
                        .next(deleteOrNot);
                
        const stateMachine = new sfn.StateMachine(
            this, 'ForecastMLOpsPipelineStateMachine', {
                role: statesMachineExecutionRole,
                definition: definition,
                stateMachineName: 'Forecast-MLOps-Pipeline',
            }
        );
        /****** StateMachine - End ******/

        // Configure S3 Upload Trigger for StateMachine
        const trainTrigger = new TriggerConstruct(
            this, 
            'TrainTrigger', {
                resourceBucket: resourceBucket,
                stateMachine: stateMachine,
                s3Prefix: 'raw/',
                s3Suffix: '.zip',
            }
        );

    }

}
