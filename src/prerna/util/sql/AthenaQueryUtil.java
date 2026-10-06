/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util.sql;

import java.util.Map;

import prerna.engine.impl.CaseInsensitiveProperties;

public class AthenaQueryUtil extends AnsiSqlQueryUtil {

	private String region;
	private String accessKey;
	private String secretKey;
	private String output;

	AthenaQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.ATHENA);
	}

	AthenaQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.ATHENA);
	}

	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if (configMap == null || configMap.isEmpty()) {
			throw new RuntimeException("Configuration map is null or empty");
		}

		this.connectionUrl = getOptionalStringValue(configMap, AbstractSqlQueryUtil.CONNECTION_URL, null);
		this.region = getOptionalStringValue(configMap, AbstractSqlQueryUtil.REGION, null);
		this.accessKey = getOptionalStringValue(configMap, AbstractSqlQueryUtil.ACCESS_KEY, null);
		this.secretKey = getOptionalStringValue(configMap, AbstractSqlQueryUtil.SECRET_KEY, null);
		this.output = getOptionalStringValue(configMap, AbstractSqlQueryUtil.OUTPUT, null);
		this.schema = getOptionalStringValue(configMap, AbstractSqlQueryUtil.SCHEMA, null);
		if (this.schema == null || this.schema.isEmpty()) {
			this.schema = "default";
		}
		this.additionalProps = getOptionalStringValue(configMap, AbstractSqlQueryUtil.ADDITIONAL, null);
		return buildConnectionString();
	}

	@Override
	public String setConnectionDetailsFromSMSS(CaseInsensitiveProperties prop) throws RuntimeException {
		if (prop == null || prop.isEmpty()) {
			throw new RuntimeException("Properties object is null or empty");
		}

		this.connectionUrl = getOptionalStringValue(prop, AbstractSqlQueryUtil.CONNECTION_URL, null);
		this.region = getOptionalStringValue(prop, AbstractSqlQueryUtil.REGION, null);
		this.accessKey = getOptionalStringValue(prop, AbstractSqlQueryUtil.ACCESS_KEY, null);
		this.secretKey = getOptionalStringValue(prop, AbstractSqlQueryUtil.SECRET_KEY, null);
		this.output = getOptionalStringValue(prop, AbstractSqlQueryUtil.OUTPUT, null);
		this.schema = getOptionalStringValue(prop, AbstractSqlQueryUtil.SCHEMA, null);
		if (this.schema == null || this.schema.isEmpty()) {
			this.schema = "default";
		}
		this.additionalProps = getOptionalStringValue(prop, AbstractSqlQueryUtil.ADDITIONAL, null);
		return buildConnectionString();
	}

	@Override
	public String buildConnectionString() {
		if (this.connectionUrl != null && !this.connectionUrl.isEmpty()) {
			return this.connectionUrl;
		}

		if (this.region == null || this.region.isEmpty()) {
			throw new RuntimeException("Must pass in a region");
		}

		if (this.accessKey == null || this.accessKey.isEmpty()) {
			throw new RuntimeException("Must pass in an access key");
		}

		if (this.secretKey == null || this.secretKey.isEmpty()) {
			throw new RuntimeException("Must pass in a secret key");
		}

		if (this.output == null || this.output.isEmpty()) {
			throw new RuntimeException("Must pass in an S3 bucket location for query outputs to be stored");
		}

		if (this.schema == null || this.schema.isEmpty()) {
			this.schema = "default";
		}

		this.connectionUrl = this.dbType.getUrlPrefix() + "://AwsRegion=" + region + ";User=" + accessKey + ";Password="
				+ secretKey + ";S3OutputLocation=" + output + ";Schema=" + schema;

		this.connectionUrl = appendAdditionalProps(this.connectionUrl);

		return this.connectionUrl;
	}

	@Override
	public String getConnectionUserKey() {
		return AbstractSqlQueryUtil.ACCESS_KEY;
	}

	@Override
	public String getConnectionPasswordKey() {
		return AbstractSqlQueryUtil.SECRET_KEY;
	}

	@Override
	public String getUsername() {
		return this.accessKey;
	}

	@Override
	public String getPassword() {
		return this.secretKey;
	}
}
