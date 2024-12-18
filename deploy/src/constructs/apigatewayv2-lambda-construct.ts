/**
 * Copyright 2023 Amazon.com, Inc. and its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *   http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import * as apigw from '@aws-cdk/aws-apigatewayv2-alpha';
import * as apigwIntegrations from '@aws-cdk/aws-apigatewayv2-integrations-alpha';
import * as apigwAuthorizers from '@aws-cdk/aws-apigatewayv2-authorizers-alpha';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';

export interface ApiGatewayV2LambdaConstructProps extends cdk.StackProps {
	/**
	 * The lambda function
	 */
	readonly lambdaFn: cdk.aws_lambda.Function;
	/**
	 * The apigatewayv2 route path
	 */
	readonly routePath: string;
	/**
	 * Api methods supported by this API
	 */
	readonly methods: Array<apigw.HttpMethod>;
	/**
	 * The ApiGatewayV2 HttpApi to attach the lambda
	 */
	readonly api: apigw.HttpApi;
	/**
	 * The custom lambda authorizer function
	 */
	readonly lambdaAuthorizerFn?: cdk.aws_lambda.Function;
}

const defaultProps: Partial<ApiGatewayV2LambdaConstructProps> = {};

/**
 * Deploys a lambda and attaches it to a route on the apigatewayv2
 */
export class ApiGatewayV2LambdaConstruct extends Construct {
	constructor(
		parent: Construct,
		name: string,
		props: ApiGatewayV2LambdaConstructProps
	) {
		super(parent, name);

		props = { ...defaultProps, ...props };

		// add lambda policies
		props.lambdaFn.grantInvoke(
			new cdk.aws_iam.ServicePrincipal('apigateway.amazonaws.com')
		);

		// add lambda integration
		const lambdaFnIntegration = new apigwIntegrations.HttpLambdaIntegration(
			'apiInt',
			props.lambdaFn,
			{}
		);

		const route_config: any = {
			path: props.routePath,
			methods: props.methods,
			integration: lambdaFnIntegration,
		};

		if (props.lambdaAuthorizerFn !== undefined) {
			props.lambdaAuthorizerFn.grantInvoke(
				new cdk.aws_iam.ServicePrincipal('apigateway.amazonaws.com')
			);

			const lamndaAuthorizer = new apigwAuthorizers.HttpLambdaAuthorizer(
				'lamndaAuthorizer',
				props.lambdaAuthorizerFn,
				{
					authorizerName: 'LamndaAuthorizer',
					resultsCacheTtl: cdk.Duration.minutes(5),
					responseTypes: [
						apigwAuthorizers.HttpLambdaResponseType.SIMPLE,
					],
				}
			);
			route_config.authorizer = lamndaAuthorizer;
		}

		// add route to the api gateway
		props.api.addRoutes(route_config);
	}
}
