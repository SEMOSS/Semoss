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
package prerna.engine.impl.function;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.FunctionTypeEnum;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class RESTFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(RESTFunctionEngine.class);

	private String httpMethod;
	private String url;
	private Map<String, String> headers;

	private String contentType = "JSON";

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.httpMethod = smssProp.getProperty("HTTP_METHOD");
		if (this.httpMethod == null || (this.httpMethod = this.httpMethod.trim().toUpperCase()).isEmpty()
				|| (!this.httpMethod.equals("GET") && !this.httpMethod.equals("POST") && !this.httpMethod.equals("PUT")
						&& !this.httpMethod.equals("HEAD"))) {
			throw new IllegalArgumentException("RESTFunctionEngine only supports GET, HEAD, POST, or PUT requests");
		}

		this.url = smssProp.getProperty("URL");
		if (this.url == null || (this.url = this.url.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide a URL");
		}
		Utility.checkIfValidDomain(url);

		String headersStr = smssProp.getProperty("HEADERS");
		if (headersStr != null && !(headersStr = headersStr.trim()).isEmpty()) {
			this.headers = new GsonBuilder().disableHtmlEscaping().create().fromJson(headersStr,
					new TypeToken<Map<String, String>>() {
					}.getType());
		}

		if (smssProp.containsKey("CONTENT_TYPE")) {
			this.contentType = smssProp.getProperty("CONTENT_TYPE");
		}
	}

	@Override
	public void close() throws IOException {
		// i dont have anything to do here...

	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		if (parameterValues != null) {
			// remove insight
			parameterValues.remove(Constants.INSIGHT);
		}

		Object output = null;
		// validate all the required keys are set
		if (this.requiredParameters != null && !this.requiredParameters.isEmpty()) {
			Set<String> missingPs = new HashSet<>();
			for (String requiredP : this.requiredParameters) {
				if (!parameterValues.containsKey(requiredP)) {
					missingPs.add(requiredP);
				}
			}
			if (!missingPs.isEmpty()) {
				throw new IllegalArgumentException("Must define required keys = " + missingPs);
			}
		}

		if (httpMethod.equalsIgnoreCase("GET")) {
			StringBuffer queryString = new StringBuffer();
			boolean first = true;
			for (String k : parameterValues.keySet()) {
				if (!first) {
					queryString.append("&");
				}
				queryString.append(k).append("=").append(parameterValues.get(k));
				first = false;
			}
			String runTimeUrl = url + "?" + queryString;
			output = HttpHelperUtility.getRequest(runTimeUrl, this.headers, null, null, null);
		} else if (httpMethod.equalsIgnoreCase("HEAD")) {
			StringBuffer queryString = new StringBuffer();
			boolean first = true;
			for (String k : parameterValues.keySet()) {
				if (!first) {
					queryString.append("&");
				}
				queryString.append(k).append("=").append(parameterValues.get(k));
				first = false;
			}
			String runTimeUrl = url + "?" + queryString;
			output = HttpHelperUtility.headRequest(runTimeUrl, this.headers, null, null, null);
		} else if (httpMethod.equalsIgnoreCase("PUT")) {
			// for PUT, will assume we are constructing a JSON body
			if (this.contentType.equalsIgnoreCase("JSON")) {
				// gson the input as is
				output = HttpHelperUtility.putRequestStringBody(this.url, this.headers,
						new GsonBuilder().disableHtmlEscaping().create().toJson(parameterValues),
						ContentType.APPLICATION_JSON, null, null, null);
			} else {
				Map<String, String> bodyMap = new HashMap<>();
				for (String k : parameterValues.keySet()) {
					bodyMap.put(k, parameterValues.get(k) + "");
				}
				output = HttpHelperUtility.putRequestUrlEncodedBody(this.url, this.headers, bodyMap, null, null, null);
			}
		} else {
			// for POST, will assume we are constructing a JSON body
			if (this.contentType.equalsIgnoreCase("JSON")) {
				// gson the input as is
				output = HttpHelperUtility.postRequestStringBody(this.url, this.headers,
						new GsonBuilder().disableHtmlEscaping().create().toJson(parameterValues),
						ContentType.APPLICATION_JSON, null, null, null);
			} else {
				Map<String, String> bodyMap = new HashMap<>();
				for (String k : parameterValues.keySet()) {
					bodyMap.put(k, parameterValues.get(k) + "");
				}
				output = HttpHelperUtility.postRequestUrlEncodedBody(this.url, this.headers, bodyMap, null, null, null);
			}
		}
		return output;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.REST.getFunctionName();
	}

}
