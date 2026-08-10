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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Password-based encryption using only FIPS 140-3 approved primitives:
 * PBKDF2-HMAC-SHA256 for key derivation (SP 800-132) and AES-256-GCM for
 * authenticated encryption (SP 800-38D).
 *
 * <p>
 * Replaces the previous jasypt {@code StandardPBEByteEncryptor} usage, which
 * defaulted to {@code PBEWithMD5AndDES}. Neither MD5 nor single DES is an
 * approved algorithm, and both are unavailable once the BouncyCastle FIPS
 * provider runs in approved-only mode.
 *
 * <p>
 * Payload layout, all big-endian, no separators:
 * 
 * <pre>
 *   [0]        version byte (currently 1)
 *   [1..16]    16-byte random salt for PBKDF2
 *   [17..28]   12-byte random IV for GCM
 *   [29..]     AES-256-GCM ciphertext with the 128-bit tag appended
 * </pre>
 * 
 * The salt and IV are generated fresh per call, so encrypting the same
 * plaintext twice yields different payloads. GCM authenticates the ciphertext,
 * so a wrong password or a tampered payload fails loudly rather than returning
 * garbage - unlike the CBC/DES scheme it replaces.
 *
 * <p>
 * Ciphertext produced by the old jasypt format is NOT readable here. The
 * version byte exists so a future format change can be detected rather than
 * silently misparsed.
 */
public class PBEncryptionUtility {

	/** Payload format version, occupying the first byte of every payload. */
	private static final byte FORMAT_VERSION = 1;

	private static final String KDF_ALGORITHM = "PBKDF2WithHmacSHA256";
	private static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
	private static final String KEY_ALGORITHM = "AES";

	private static final int SALT_LENGTH_BYTES = 16;
	private static final int IV_LENGTH_BYTES = 12;
	private static final int GCM_TAG_LENGTH_BITS = 128;
	private static final int DERIVED_KEY_LENGTH_BITS = 256;

	/**
	 * PBKDF2 iteration count. SP 800-132 sets a floor of 1000; this follows the
	 * higher OWASP guidance for PBKDF2-HMAC-SHA256. Deriving a key costs on the
	 * order of 100ms, which is the intended tradeoff - lower it only if a caller is
	 * on a hot path and the password is known to be high-entropy.
	 */
	private static final int PBKDF2_ITERATIONS = 210_000;

	private static final int HEADER_LENGTH_BYTES = 1 + SALT_LENGTH_BYTES + IV_LENGTH_BYTES;

	private static final SecureRandom RANDOM = new SecureRandom();

	private PBEncryptionUtility() {
		// static utility
	}

	/**
	 * Encrypt with a fresh random salt and IV.
	 *
	 * @param plaintext bytes to encrypt
	 * @param password  password to derive the key from
	 * @return a self-describing payload: version, salt, IV, then GCM ciphertext
	 * @throws GeneralSecurityException if the JCE providers cannot supply
	 *                                  PBKDF2-HMAC-SHA256 or AES/GCM
	 */
	public static byte[] encrypt(byte[] plaintext, String password) throws GeneralSecurityException {
		if (plaintext == null) {
			throw new IllegalArgumentException("Cannot encrypt null plaintext");
		}
		byte[] salt = new byte[SALT_LENGTH_BYTES];
		byte[] iv = new byte[IV_LENGTH_BYTES];
		RANDOM.nextBytes(salt);
		RANDOM.nextBytes(iv);

		Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
		cipher.init(Cipher.ENCRYPT_MODE, deriveKey(password, salt), new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
		byte[] ciphertext = cipher.doFinal(plaintext);

		return ByteBuffer.allocate(HEADER_LENGTH_BYTES + ciphertext.length).put(FORMAT_VERSION).put(salt).put(iv)
				.put(ciphertext).array();
	}

	/**
	 * Decrypt a payload produced by {@link #encrypt(byte[], String)}.
	 *
	 * @param payload  the versioned payload
	 * @param password password to derive the key from
	 * @return the decrypted bytes
	 * @throws IllegalArgumentException if the payload is too short or carries an
	 *                                  unrecognized version byte
	 * @throws GeneralSecurityException if the password is wrong, the payload was
	 *                                  tampered with, or the required algorithms
	 *                                  are unavailable
	 */
	public static byte[] decrypt(byte[] payload, String password) throws GeneralSecurityException {
		if (payload == null || payload.length <= HEADER_LENGTH_BYTES) {
			throw new IllegalArgumentException("Payload is too short to be a valid encrypted value");
		}
		ByteBuffer buffer = ByteBuffer.wrap(payload);
		byte version = buffer.get();
		if (version != FORMAT_VERSION) {
			throw new IllegalArgumentException("Unsupported encrypted payload version: " + version);
		}
		byte[] salt = new byte[SALT_LENGTH_BYTES];
		byte[] iv = new byte[IV_LENGTH_BYTES];
		buffer.get(salt);
		buffer.get(iv);
		byte[] ciphertext = new byte[buffer.remaining()];
		buffer.get(ciphertext);

		Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
		cipher.init(Cipher.DECRYPT_MODE, deriveKey(password, salt), new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
		// throws AEADBadTagException (a GeneralSecurityException) on a wrong
		// password or a modified payload
		return cipher.doFinal(ciphertext);
	}

	/**
	 * Convenience wrapper over {@link #encrypt(byte[], String)} for callers that
	 * need to move the payload as text. This is the transport format any external
	 * tool producing ciphertext for SEMOSS should emit.
	 *
	 * @param plaintext string to encrypt, read as UTF-8
	 * @param password  password to derive the key from
	 * @return the base64 encoded payload
	 * @throws GeneralSecurityException if encryption fails
	 */
	public static String encryptToBase64(String plaintext, String password) throws GeneralSecurityException {
		return Base64.getEncoder().encodeToString(encrypt(plaintext.getBytes(StandardCharsets.UTF_8), password));
	}

	/**
	 * Convenience wrapper over {@link #decrypt(byte[], String)}.
	 *
	 * @param base64Payload base64 encoded payload
	 * @param password      password to derive the key from
	 * @return the decrypted string, read as UTF-8
	 * @throws IllegalArgumentException if the input is not valid base64
	 * @throws GeneralSecurityException if decryption fails
	 */
	public static String decryptFromBase64(String base64Payload, String password) throws GeneralSecurityException {
		return new String(decrypt(Base64.getDecoder().decode(base64Payload), password), StandardCharsets.UTF_8);
	}

	/**
	 * Derive a 256-bit AES key from the password and salt via PBKDF2-HMAC-SHA256.
	 *
	 * @param password the password
	 * @param salt     the per-payload random salt
	 * @return the derived AES key
	 * @throws GeneralSecurityException if the KDF is unavailable or the spec is
	 *                                  rejected
	 */
	private static SecretKey deriveKey(String password, byte[] salt) throws GeneralSecurityException {
		if (password == null || password.isEmpty()) {
			throw new IllegalArgumentException("No encryption password is configured");
		}
		PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, PBKDF2_ITERATIONS, DERIVED_KEY_LENGTH_BITS);
		try {
			byte[] keyBytes = SecretKeyFactory.getInstance(KDF_ALGORITHM).generateSecret(spec).getEncoded();
			return new SecretKeySpec(keyBytes, KEY_ALGORITHM);
		} finally {
			spec.clearPassword();
		}
	}

}
