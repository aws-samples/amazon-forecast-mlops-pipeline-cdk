import { Stack, StackProps, RemovalPolicy } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import {
    aws_s3 as s3, 
} from 'aws-cdk-lib';

import { StateMachine } from './construct/state-machine';

export class ForecastMlopsPipelineStack extends Stack {
    constructor(scope: Construct, id: string, props?: StackProps) {
        super(scope, id, props);

        // S3 Bucket for pipeline resources
        const resourceBucket = new s3.Bucket(this, `ForecastMLOpsPipelineResourceBucket`, {
            bucketName: `forecast-mlops-pipeline-resource-bucket-${Stack.of(this).account}`,
            versioned: false,
            autoDeleteObjects: true,
            removalPolicy: RemovalPolicy.DESTROY
        });

        const stateMachine = new StateMachine(
            this, 'ForecastMLOpsPiplineStateMachine', 
            {
                resourceBucket: resourceBucket,
            }
        );

    }
}
