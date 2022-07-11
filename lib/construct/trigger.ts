import { Construct } from 'constructs';
import {
    aws_glue as glue, 
    aws_iam as iam, 
    aws_lambda as lambda, 
    aws_s3 as s3, 
    aws_s3_deployment as s3Deployment, 
    aws_stepfunctions as sfn, 
    aws_stepfunctions_tasks as tasks 
} from 'aws-cdk-lib';
import { S3EventSource } from 'aws-cdk-lib/aws-lambda-event-sources';

export interface TriggerConstructProps {
    stateMachine: sfn.StateMachine;
    resourceBucket: s3.Bucket;
	s3Prefix: string,
	s3Suffix: string,
}

export class TriggerConstruct extends Construct {
    public readonly role: iam.Role;
    public readonly lambda: lambda.Function;
    public readonly task: sfn.TaskStateBase;

    constructor(scope: Construct, id: string, props: TriggerConstructProps) {
        super(scope, id);

        // IAM Role
        this.role = new iam.Role(this, 'ForecastMLOpsPipelineTriggerRole', {
			assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
			roleName: `ForecastMLOpsPipelineTriggerRole`,
			managedPolicies: [
				{managedPolicyArn: 'arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess'},
				{managedPolicyArn: 'arn:aws:iam::aws:policy/CloudWatchFullAccess'},
				{managedPolicyArn: 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'},
                {managedPolicyArn: 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'},
                {managedPolicyArn: 'arn:aws:iam::aws:policy/service-role/AWSLambdaRole'},
			],
		});

        // Lambda Function
        this.lambda = new lambda.Function(
			this, 'UploadTriggerLambda', 
			{
				code: lambda.Code.fromAsset('lambda/trigger'),
				handler: 'index.handler',
                functionName: `Forecast-MLOps-Pipeline-Upload-Trigger`,
				runtime: lambda.Runtime.PYTHON_3_7,
				role: this.role,
				environment: {
					STEP_FUNCTIONS_ARN: props.stateMachine.stateMachineArn,
				},
		});

        // add S3Bucket as a Lambda's Event Source
        this.lambda.addEventSource(new S3EventSource(
			props.resourceBucket, {
				events: [
                    s3.EventType.OBJECT_CREATED,
                ],
				filters: [
					{
						prefix: props.s3Prefix,
						suffix: props.s3Suffix
					},
				]
			}
		));
    }


}