import { Construct } from 'constructs';
import {
    aws_glue as glue, 
    aws_iam as iam, 
    aws_lambda as lambda, 
    aws_s3 as s3, 
    aws_s3_deployment as s3Deployment, 
    aws_stepfunctions as sfn, 
    aws_stepfunctions_tasks as tasks, 
    Duration
} from 'aws-cdk-lib';

export interface LambdaConstructProps {
    taskName: string;
    lambdaCodePath: string;
    timeout: Duration;
    environment?: {
        [key:string]: string;
    };
}

export class LambdaConstruct extends Construct {

    public readonly role: iam.Role;
    public readonly lambda: lambda.Function;
    public readonly task: sfn.TaskStateBase;

    constructor(scope: Construct, id: string, props: LambdaConstructProps) {
        super(scope, id);

        // IAM Role
        this.role = new iam.Role(this, `${props.taskName}LambdaRole`, {
			assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
			roleName: `${props.taskName}-Lambda-Role`,
			managedPolicies: [
				{managedPolicyArn: 'arn:aws:iam::aws:policy/AmazonS3FullAccess'},
                {managedPolicyArn: 'arn:aws:iam::aws:policy/CloudWatchFullAccess'},
                {managedPolicyArn: 'arn:aws:iam::aws:policy/AmazonForecastFullAccess'}
			],
		});

        // Lambda Function
        this.lambda = new lambda.Function(
			this, `${props.taskName}Lambda`,
			{
				code: lambda.Code.fromAsset(props.lambdaCodePath),
				handler: 'index.handler',
                functionName: props.taskName,
				runtime: lambda.Runtime.PYTHON_3_7,
                timeout: props.timeout,
				role: this.role,
                environment: props.environment,
		});

        // StepFunctions Task
        this.task = new tasks.LambdaInvoke(
            this, 
            `${props.taskName}`, 
            {
                lambdaFunction: this.lambda,
                integrationPattern: sfn.IntegrationPattern.REQUEST_RESPONSE,
                resultPath: sfn.JsonPath.stringAt('$'),
                outputPath: sfn.JsonPath.stringAt('$.Payload'),
            }
        );

        this.task.addRetry({
			backoffRate: 1.0,
			errors: ["ResourcePending"],
			interval: Duration.seconds(30),
            maxAttempts: 600
		});

    }

}