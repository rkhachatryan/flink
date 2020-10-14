/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.s3.common;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.retry.RetryMode;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;

import java.util.Optional;

/**
 * Flink {@link org.apache.hadoop.fs.s3a.S3ClientFactory}. Allows to set AWS SDK specific parameters such as request timeout.
 */
public class FlinkS3ClientFactory extends DefaultS3ClientFactory {

	@Override
	protected AmazonS3 newAmazonS3Client(AWSCredentialsProvider credentials, ClientConfiguration awsConf) {
		getProperty("retryMode").map(RetryMode::fromName).ifPresent(awsConf::setRetryMode);
		getProperty("requestTimeoutMs").map(Integer::parseInt).ifPresent(awsConf::setRequestTimeout);
		getProperty("maxErrorRetry").map(Integer::parseInt).ifPresent(awsConf::setMaxErrorRetry); // todo: review fs.s3a.attempts.maximum
		return super.newAmazonS3Client(credentials, awsConf);
	}

	private Optional<String> getProperty(String key) {
		// todo: read from config
		return Optional.ofNullable(System.getProperty(key));
	}

}
