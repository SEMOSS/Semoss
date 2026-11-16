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
package prerna.security;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.util.Constants;
import prerna.util.InternalUtility;

class SensitiveKeysUnitTests {

	private Properties savedCoreProp;

	@BeforeEach
	void saveRdfMap() {
		this.savedCoreProp = InternalUtility.getCoreProp();
		InternalUtility.setCoreProp(new Properties());
	}

	@AfterEach
	void restoreRdfMap() {
		InternalUtility.setCoreProp(this.savedCoreProp);
	}

	@ParameterizedTest
	@ValueSource(strings = { "PASSWORD", "API_KEY", "OPEN_AI_KEY", "AZ_PRIMARY_KEY", "AZ_KEY", "HEADERS",
			"Authorization", "secretKey", "MCP_AUTH_TOKEN", "AWS_ACCESS_KEY", "AWS_SECRET_KEY", "AZ_CONN_STRING",
			"SAS_URL", "GCS_SERVICE_ACCOUNT_JSON", "GCP_SERVICE_ACCOUNT_KEY", "SERVICE_ACCOUNT_CREDENTIALS",
			"S3_ACCESS_KEY", "MINIO_SECRET_KEY" })
	void theKeysEveryEngineAlreadyHidIsSecret(String key) {
		assertTrue(SensitiveKeys.isSensitiveKey(key), key);
	}

	@ParameterizedTest
	@ValueSource(strings = { "SECRET_KEY", "ACCESS_KEY", "accessKey", "PWD", "oauthAccessToken", "oauthRefreshToken",
			"oauthClientSecret", "SP_CLIENT_SECRET", "MS_TEAMS_CLIENT_SECRET", "SMTP_PASSWORD",
			"EXCHANGE_CLIENT_SECRET", "IMAP_PASSWORD", "POP3_PASSWORD", "BASIC_PASSWORD", "OAUTH_SECRET", "1_PASSWORD",
			"COUCH_CREDS", "VAULT_TOKEN", "AWS_SECRETS_MANAGER_SESSION_TOKEN", "VIRUSTOTAL_API_KEY", "X-Vault-Token" })
	void keysEndingInASecretWordAreSecret(String key) {
		assertTrue(SensitiveKeys.isSensitiveKey(key), key);
	}

	@Test
	void caseAndSeparatorsDoNotMatter() {
		assertTrue(SensitiveKeys.isSensitiveKey("secret-key"));
		assertTrue(SensitiveKeys.isSensitiveKey("Secret.Key"));
		assertTrue(SensitiveKeys.isSensitiveKey("open_ai_key"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "ENGINE", "ENGINE_ALIAS", "CONNECTION_URL", "MODEL", "MAX_TOKENS", "CATALOG_MODEL_KEY",
			"SESSION_ID_KEY", "fileKey", "API_KEY_ID", "OAUTH_CLIENT", "UID", "VAULT_ADDR", "URL", "ENDPOINT" })
	void settingsThatAreNotSecretStayVisible(String key) {
		assertFalse(SensitiveKeys.isSensitiveKey(key), key);
	}

	@Test
	void anEmptyOrMissingKeyIsNotSecret() {
		assertFalse(SensitiveKeys.isSensitiveKey(null));
		assertFalse(SensitiveKeys.isSensitiveKey(""));
		assertFalse(SensitiveKeys.isSensitiveKey("___"));
	}

	@Test
	void theRdfMapCanNameMoreSecretKeys() {
		assertFalse(SensitiveKeys.isSensitiveKey("VENDOR_SIGNING_SEED"));

		Properties rdfMap = new Properties();
		rdfMap.setProperty(Constants.SENSITIVE_KEYS, " vendor_signing_seed , PARTNER-LICENSE,, ");
		InternalUtility.setCoreProp(rdfMap);

		assertTrue(SensitiveKeys.isSensitiveKey("VENDOR_SIGNING_SEED"));
		assertTrue(SensitiveKeys.isSensitiveKey("partnerLicense"));
		assertFalse(SensitiveKeys.isSensitiveKey("PARTNER"));

		rdfMap.setProperty(Constants.SENSITIVE_KEYS, "PARTNER_LICENSE");
		assertFalse(SensitiveKeys.isSensitiveKey("VENDOR_SIGNING_SEED"));
		assertTrue(SensitiveKeys.isSensitiveKey("PARTNER_LICENSE"));
	}
}
