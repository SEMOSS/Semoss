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
package prerna.io.connector.secrets;

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.sablecc2.om.execptions.InsightEncryptionException;
import prerna.security.BcFipsProvider;
import prerna.security.InsightCipher;

public class SecretsUtility {

	private static final Logger classLogger = LogManager.getLogger(SecretsUtility.class);

	private static final int SALT_BYTE_LENGTH = 16;
	// Written to cacheData only so HashiCorpVaultUtil's existing storage schema
	// (which always expects an "iv" entry) still round-trips; never read back
	// for decryption. InsightCipher generates and prepends a fresh IV to every
	// stream it wraps instead of reusing one IV for a whole insight - see its
	// javadoc for why reusing a GCM (key, IV) pair across the many independent
	// ciphertexts a multi-dataframe insight produces is a critical
	// vulnerability, not just a weakness.
	private static final int VESTIGIAL_IV_BYTE_LENGTH = 12;

	private static final SecureRandom RANDOM = new SecureRandom();

	private SecretsUtility() {

	}

	public static InsightCipher generateCipherForInsight(String projectId, String projectName, String insightId) {
		ISecrets secretsEngine = SecretsFactory.getSecretConnector();
		if (secretsEngine == null) {
			throw new InsightEncryptionException(
					"Encryption services have not been enabled on this instance. Caching will not occur for this insight");
		}

		String secret = UUID.randomUUID().toString();

		byte[] saltBytes = new byte[SALT_BYTE_LENGTH];
		RANDOM.nextBytes(saltBytes);
		String salt = Base64.getEncoder().encodeToString(saltBytes);

		byte[] vestigialIv = new byte[VESTIGIAL_IV_BYTE_LENGTH];
		RANDOM.nextBytes(vestigialIv);

		InsightCipher cipher;
		try {
			cipher = InsightCipher.forEncryption(deriveKey(secret, salt, insightId, projectId));
		} catch (NoSuchAlgorithmException | InvalidKeySpecException e1) {
			classLogger.error("Unable to build the encryption cipher for insight {} in project {}.", insightId,
					projectId, e1);
			throw new InsightEncryptionException("Unable to generate encryption details for the insight cache");
		}

		Map<String, Object> cacheData = new HashMap<>();
		cacheData.put(ISecrets.SECRET, secret);
		cacheData.put(ISecrets.SALT, salt);
		cacheData.put(ISecrets.IV, vestigialIv);
		secretsEngine.writeInsightEncryptionSecrets(projectId, projectName, insightId, cacheData);
		return cipher;
	}

	public static InsightCipher retrieveCipherForInsight(Insight in) {
		return retrieveCipherForInsight(in.getProjectId(), in.getProjectName(), in.getRdbmsId());
	}

	public static InsightCipher retrieveCipherForInsight(String projectId, String projectName, String insightId) {
		ISecrets secretsEngine = SecretsFactory.getSecretConnector();
		if (secretsEngine == null) {
			throw new InsightEncryptionException(
					"Encryption services have not been enabled on this instance. Cannot retrieve details to decrypt the insight");
		}

		Map<String, Object> cacheData = secretsEngine.getInsightEncryptionSecrets(projectId, projectName, insightId);
		String secret = (String) cacheData.get(ISecrets.SECRET);
		String salt = (String) cacheData.get(ISecrets.SALT);
		try {
			return InsightCipher.forDecryption(deriveKey(secret, salt, insightId, projectId));
		} catch (NoSuchAlgorithmException | InvalidKeySpecException e1) {
			classLogger.error("Unable to build the decryption cipher for insight {} in project {}.", insightId,
					projectId, e1);
			throw new InsightEncryptionException("Unable to generate encryption details for the insight cache");
		}
	}

	private static SecretKey deriveKey(String secret, String salt, String insightId, String projectId)
			throws NoSuchAlgorithmException, InvalidKeySpecException {
		Provider bcFips = BcFipsProvider.get();
		SecretKeyFactory factory = bcFips != null ? SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256", bcFips)
				: SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
		KeySpec spec = new PBEKeySpec(secret.toCharArray(), salt.getBytes(), 65536, 256);
		SecretKey tmp = factory.generateSecret(spec);
		return new SecretKeySpec(tmp.getEncoded(), "AES");
	}

}
