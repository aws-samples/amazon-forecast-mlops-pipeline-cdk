import { Construct } from 'constructs';
import {
    aws_iam as iam, 
    aws_s3 as s3, 
    aws_s3_deployment as s3Deployment, 
    aws_stepfunctions as sfn, 
    aws_stepfunctions_tasks as tasks 
} from 'aws-cdk-lib';

import * as glue from "@aws-cdk/aws-glue-alpha";

export interface GlueConstructProps {
    taskName: string,
    pythonFilePath: string,
    defaultArguments?: {
        [key:string]: string;
    }, 
    arguments?: {
        [key:string]: any;
    }
}

export class GlueConstruct extends Construct {
    public readonly role: iam.Role;
    public readonly task: tasks.GlueStartJobRun;

    constructor(scope: Construct, id: string, props: GlueConstructProps) {
        super(scope, id);

        // IAM Role
        this.role = new iam.Role(this, '${props.taskName}GlueRole', {
			assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
			roleName: `${props.taskName}-Glue-Role`,
			managedPolicies: [
				{managedPolicyArn: 'arn:aws:iam::aws:policy/AmazonS3FullAccess'},
                {managedPolicyArn: 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'},
			],
		});

        // Glue Python Job
        const pythonJob = new glue.Job(this, `${props.taskName}Job`, {
            executable: glue.JobExecutable.pythonShell({
                glueVersion: glue.GlueVersion.V1_0,
                pythonVersion: glue.PythonVersion.THREE,
                script: glue.Code.fromAsset(props.pythonFilePath),
            }),
            role: this.role,
            jobName: props.taskName,
            defaultArguments: props.defaultArguments,
        });

        // StepFunction Task
        this.task = new tasks.GlueStartJobRun(
            this, 
            props.taskName, 
            {
                glueJobName: pythonJob.jobName,
                integrationPattern: sfn.IntegrationPattern.RUN_JOB,
                resultPath: sfn.JsonPath.stringAt('$.result'),
                arguments: sfn.TaskInput.fromObject(props.arguments!),
            }
        );

    }
}