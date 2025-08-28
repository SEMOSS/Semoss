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
package prerna.aws;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class AwsSecretsManagerUnitTests {

	private AwsSecretsManager aws;

	@BeforeEach
	public void setup() {
		aws = new AwsSecretsManager();
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = {" ", "\t", "\n"})
	public void testMakeRequestUrlIncorrect(String input) {
		aws.setUrl(input);
		NullPointerException e = assertThrows(NullPointerException.class, aws::makeRequest);
		assertEquals("Must define the url", e.getMessage());
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = {" ", "\t", "\n"})
	public void testMakeRequestSecretIdWrong(String input) {
		aws.setUrl("url");
		aws.setSecretId(input);
		NullPointerException e = assertThrows(NullPointerException.class, aws::makeRequest);
		assertEquals("Must define the ARN of the secret", e.getMessage());
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = {" ", "\t", "\n"})
	public void testMakeRequestOnlySecretHeader(String input) {
		aws.setUrl("url");
		aws.setSecretId("secret");
		aws.setVersionId(input);
		aws.setVersionStage(input);

		aws.setKeyPass("keypass");
		aws.setKeyStore("keystore");
		aws.setKeyStorePass("keystorepass");
		try (MockedStatic<HttpHelperUtility> http = Mockito.mockStatic(HttpHelperUtility.class)) {
			http.when(() -> HttpHelperUtility.getRequest(eq("url"), any(Map.class), eq("keystore"), eq("keystorepass"),
					eq("keypass"))).thenReturn("{response: \"data\"}");

			aws.makeRequest();

			ArgumentCaptor<Map<String, String>> mapCaptor = ArgumentCaptor.forClass(Map.class);
			http.verify(() -> HttpHelperUtility.getRequest(eq("url"), mapCaptor.capture(), eq("keystore"),
					eq("keystorepass"), eq("keypass")), times(1));
			Map<String, String> map = mapCaptor.getValue();
			assertEquals(1, map.size());
			assertEquals("secret", map.get("SecretId"));
		}

		assertEquals("{response: \"data\"}", aws.getResponseData());
		assertEquals("data", aws.getResponseJson().get("response"));
	}

	@Test
	public void testMakRequestSecretKeyNull() {
		aws.setUrl("url");
		aws.setSecretId("secret");

		aws.setAccessKey("hasAccessKey");

		aws.setKeyPass("keypass");
		aws.setKeyStore("keystore");
		aws.setKeyStorePass("keystorepass");
		try (MockedStatic<HttpHelperUtility> http = Mockito.mockStatic(HttpHelperUtility.class)) {
			http.when(() -> HttpHelperUtility.getRequest(eq("url"), any(Map.class), eq("keystore"), eq("keystorepass"),
					eq("keypass"))).thenReturn("{response: \"data\"}");

			aws.makeRequest();

			ArgumentCaptor<Map<String, String>> mapCaptor = ArgumentCaptor.forClass(Map.class);
			http.verify(() -> HttpHelperUtility.getRequest(eq("url"), mapCaptor.capture(), eq("keystore"),
					eq("keystorepass"), eq("keypass")), times(1));
			Map<String, String> map = mapCaptor.getValue();
			assertEquals(1, map.size());
			assertEquals("secret", map.get("SecretId"));
		}

		assertEquals("{response: \"data\"}", aws.getResponseData());
		assertEquals("data", aws.getResponseJson().get("response"));
	}

	@Test
	public void testMakeRequestAllHeaders() {
		aws.setUrl("url");
		aws.setSecretId("secret");
		aws.setVersionId("versionId");
		aws.setVersionStage("versionStage");
		aws.setAccessKey("ak");
		aws.setSecretKey("sk");

		aws.setKeyPass("keypass");
		aws.setKeyStore("keystore");
		aws.setKeyStorePass("keystorepass");
		try (MockedStatic<HttpHelperUtility> http = Mockito.mockStatic(HttpHelperUtility.class)) {
			http.when(() -> HttpHelperUtility.getRequest(eq("url"), any(Map.class), eq("keystore"), eq("keystorepass"),
					eq("keypass"))).thenReturn("{response: \"data\"}");

			aws.makeRequest();

			ArgumentCaptor<Map<String, String>> mapCaptor = ArgumentCaptor.forClass(Map.class);
			http.verify(() -> HttpHelperUtility.getRequest(eq("url"), mapCaptor.capture(), eq("keystore"),
					eq("keystorepass"), eq("keypass")), times(1));
			Map<String, String> map = mapCaptor.getValue();
			assertEquals(4, map.size());
			assertEquals("secret", map.get("SecretId"));
			assertEquals("versionId", map.get("VersionId"));
			assertEquals("versionStage", map.get("VersionStage"));
			assertEquals("Basic YWs6c2s=", map.get("Authorization"));
		}

		assertEquals("{response: \"data\"}", aws.getResponseData());
		assertEquals("data", aws.getResponseJson().get("response"));
	}

	@Test
	void setUseApplicationCertsTrue() {
		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE)).thenReturn("ks");
			util.when(() -> Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD)).thenReturn("ksp");
			util.when(() -> Utility.getDIHelperProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD)).thenReturn("csp");

			aws.setUseApplicationCerts(true);

			assertEquals("ks", aws.getKeyStore());
			assertEquals("ksp", aws.getKeyStorePass());
			assertEquals("csp", aws.getKeyPass());
			assertTrue(aws.isUseApplicationCerts());
		}
	}

	@Test
	void testNotUseApplicationCertsFalse() {
		aws.setUseApplicationCerts(false);
		assertFalse(aws.isUseApplicationCerts());
	}

	@Test
	void testSettersAndGetters() {
		aws.setUrl("test");
		assertEquals("test", aws.getUrl());

		aws.setAccessKey("ak");
		assertEquals("ak", aws.getAccessKey());

		aws.setSecretKey("sk");
		assertEquals("sk", aws.getSecretKey());

		aws.setKeyStore("keystore");
		assertEquals("keystore", aws.getKeyStore());

		aws.setKeyStorePass("keystorepass");
		assertEquals("keystorepass", aws.getKeyStorePass());

		aws.setKeyPass("keypass");
		assertEquals("keypass", aws.getKeyPass());

		aws.setSecretId("secret");
		assertEquals("secret", aws.getSecretId());

		aws.setVersionId("versionId");
		assertEquals("versionId", aws.getVersionId());

		aws.setVersionStage("versionStage");
		assertEquals("versionStage", aws.getVersionStage());

		aws.setResponseData("response");
		assertEquals("response", aws.getResponseData());

		Map<String, Object> map = new HashMap<>();
		aws.setResponseJson(map);
		assertEquals(map, aws.getResponseJson());
	}
}
