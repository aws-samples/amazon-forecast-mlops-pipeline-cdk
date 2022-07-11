#!/usr/bin/env node

import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { ForecastMlopsPipelineStack } from '../lib/forecast-mlops-pipeline-stack';

const app = new cdk.App();
const fcstMlopsPplStack =  new ForecastMlopsPipelineStack(
    app, 'ForecastMlopsPipelineStack', {}
);