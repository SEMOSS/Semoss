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

public class SEMOSSQueryUtil extends AnsiSqlQueryUtil {

	private String protocol = null;
	private String endpoint = null;
	private String subURL = null;

	private String projectId = null;
	private String insightId = null;

	SEMOSSQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.SEMOSS);
	}

	SEMOSSQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.SEMOSS);
	}

	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if (configMap == null || configMap.isEmpty()) {
			throw new RuntimeException("Configuration map is null or empty");
		}

		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);

		this.hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty()) && (hostname == null || hostname.isEmpty())) {
			throw new RuntimeException("Must pass in a hostname");
		}

		this.port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "443";
		}

		this.projectId = (String) configMap.get(AbstractSqlQueryUtil.PROJECT);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty())
				&& (this.projectId == null || this.projectId.isEmpty())) {
			throw new RuntimeException("Must pass in project id");
		}

		this.insightId = (String) configMap.get(AbstractSqlQueryUtil.INSIGHT);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty())
				&& (this.insightId == null || this.insightId.isEmpty())) {
			throw new RuntimeException("Must pass in insight id");
		}

		this.protocol = (String) configMap.get(AbstractSqlQueryUtil.PROTOCOL);
		if (this.protocol == null || this.protocol.isEmpty()) {
			this.protocol = "https";
		}

		this.endpoint = (String) configMap.get(AbstractSqlQueryUtil.ENDPOINT);
		if (this.endpoint == null || this.endpoint.isEmpty()) {
			this.endpoint = "Monolith";
		}

		this.subURL = (String) configMap.get(AbstractSqlQueryUtil.SUB_URL);
		if (this.subURL == null) {
			this.subURL = "";
		}

		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if (this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix() + ":" + this.hostname + port + ";endpoint=" + this.endpoint
					+ ";protocol=" + this.protocol + ";project=" + projectId + ";insight=" + insightId;
			if (!subURL.isEmpty()) {
				this.connectionUrl += ";sub_url=" + subURL;
			}

			this.connectionUrl = appendAdditionalProps(this.connectionUrl);
		}

		return this.connectionUrl;
	}

	@Override
	public String setConnectionDetailsFromSMSS(CaseInsensitiveProperties prop) throws RuntimeException {
		if (prop == null || prop.isEmpty()) {
			throw new RuntimeException("Properties object is null or empty");
		}

		this.connectionUrl = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_URL);

		this.hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty()) && (hostname == null || hostname.isEmpty())) {
			throw new RuntimeException("Must pass in a hostname");
		}

		this.port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "443";
		}

		this.projectId = (String) prop.get(AbstractSqlQueryUtil.PROJECT);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty())
				&& (this.projectId == null || this.projectId.isEmpty())) {
			throw new RuntimeException("Must pass in project id");
		}

		this.insightId = (String) prop.get(AbstractSqlQueryUtil.INSIGHT);
		if ((this.connectionUrl == null || this.connectionUrl.isEmpty())
				&& (this.insightId == null || this.insightId.isEmpty())) {
			throw new RuntimeException("Must pass in insight id");
		}

		this.protocol = (String) prop.get(AbstractSqlQueryUtil.PROTOCOL);
		if (this.protocol == null || this.protocol.isEmpty()) {
			this.protocol = "https";
		}

		this.endpoint = (String) prop.get(AbstractSqlQueryUtil.ENDPOINT);
		if (this.endpoint == null || this.endpoint.isEmpty()) {
			this.endpoint = "Monolith";
		}

		this.subURL = (String) prop.get(AbstractSqlQueryUtil.SUB_URL);
		if (this.subURL == null) {
			this.subURL = "";
		}

		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if (this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix() + ":" + this.hostname + port + ";endpoint=" + this.endpoint
					+ ";protocol=" + this.protocol + ";project=" + projectId + ";insight=" + insightId;
			if (!subURL.isEmpty()) {
				this.connectionUrl += ";sub_url=" + subURL;
			}

			this.connectionUrl = appendAdditionalProps(this.connectionUrl);
		}

		return this.connectionUrl;
	}

	@Override
	public String buildConnectionString() {
		if (this.connectionUrl != null && !this.connectionUrl.isEmpty()) {
			return this.connectionUrl;
		}

		if (this.hostname == null || this.hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}

		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "443";
		}

		this.connectionUrl = this.dbType.getUrlPrefix() + ":" + this.hostname + port + ";endpoint=" + this.endpoint
				+ ";protocol=" + this.protocol + ";project=" + projectId + ";insight=" + insightId;
		if (!subURL.isEmpty()) {
			this.connectionUrl += ";sub_url=" + subURL;
		}

		this.connectionUrl = appendAdditionalProps(this.connectionUrl);

		return this.connectionUrl;
	}

	@Override
	public String getDatabaseMetadataCatalogFilter() {
		return this.projectId;
	}

	@Override
	public String getDatabaseMetadataSchemaFilter() {
		return this.insightId;
	}
}
