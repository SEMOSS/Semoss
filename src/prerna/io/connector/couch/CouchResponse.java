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
package prerna.io.connector.couch;

public class CouchResponse {

	private final int statusCode;
	private final String responseBody;
	private final String revision;

	public CouchResponse(int statusCode, String responseBody, String revision) {
		this.statusCode = statusCode;
		this.responseBody = responseBody;
		this.revision = revision;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public String getResponseBody() {
		return responseBody;
	}

	public String getRevision() {
		return revision;
	}

	@Override
	public String toString() {
		return "CouchResponse [statusCode=" + statusCode + ", responseBody=" + responseBody + ", revision=" + revision
				+ "]";
	}
}
