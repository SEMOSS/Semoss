/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.project.impl;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpHeaders;

public class ProjectHeaderAuthEvaluator {

	private String projectId;
	private String method = "getAuthorizationHeader";
	private transient String accessKey;
	private transient String secretKey;

	public ProjectHeaderAuthEvaluator() {
	}

	// TODO: expand on this to allow other login types outside of basics

	public Map<String, String> eval() throws UnsupportedEncodingException {
		String concat = this.accessKey + ":" + this.secretKey;
		byte[] encoded = Base64.getEncoder().encode(concat.getBytes("UTF-8"));

		Map<String, String> headers = new HashMap<>();
		headers.put(HttpHeaders.AUTHORIZATION, "Basic " + new String(encoded));
		return headers;
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}
}
