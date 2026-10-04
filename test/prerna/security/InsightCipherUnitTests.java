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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class InsightCipherUnitTests {

	private static final int IV_BYTE_LENGTH = 12;

	private static SecretKey key;

	@BeforeAll
	static void generateKey() throws Exception {
		KeyGenerator gen = KeyGenerator.getInstance("AES");
		gen.init(256, new SecureRandom());
		key = gen.generateKey();
	}

	@Test
	void roundTripsThroughStreams() throws Exception {
		byte[] plaintext = "insight cache payload".getBytes(StandardCharsets.UTF_8);

		ByteArrayOutputStream encrypted = new ByteArrayOutputStream();
		try (OutputStream out = InsightCipher.forEncryption(key).wrap(encrypted)) {
			out.write(plaintext);
		}

		byte[] decrypted;
		try (InputStream in = InsightCipher.forDecryption(key).wrap(new ByteArrayInputStream(encrypted.toByteArray()))) {
			decrypted = in.readAllBytes();
		}

		assertArrayEquals(plaintext, decrypted);
	}

	@Test
	void roundTripsThroughByteArrayConvenienceMethods() throws Exception {
		byte[] plaintext = "insight cache payload".getBytes(StandardCharsets.UTF_8);

		byte[] encrypted = InsightCipher.forEncryption(key).encryptToBytes(plaintext);
		byte[] decrypted = InsightCipher.forDecryption(key).decryptFromBytes(encrypted);

		assertArrayEquals(plaintext, decrypted);
	}

	/**
	 * Direct proof of the fix: encrypting many independent segments under the
	 * SAME key (mirroring N dataframes cached under one insight) must never
	 * reuse an IV. Each segment gets its own fresh {@link InsightCipher}
	 * instance, exactly as each call site in the redesign now does.
	 */
	@Test
	void everySegmentEncryptedUnderTheSameKeyGetsADistinctIv() throws Exception {
		int segments = 50;
		Set<String> seenIvs = new HashSet<>();

		for (int i = 0; i < segments; i++) {
			byte[] plaintext = ("segment-" + i).getBytes(StandardCharsets.UTF_8);
			byte[] encrypted = InsightCipher.forEncryption(key).encryptToBytes(plaintext);

			byte[] iv = Arrays.copyOf(encrypted, IV_BYTE_LENGTH);
			assertTrue(seenIvs.add(Base16.encode(iv)), "IV reused across segments: " + Base16.encode(iv));

			byte[] decrypted = InsightCipher.forDecryption(key).decryptFromBytes(encrypted);
			assertArrayEquals(plaintext, decrypted);
		}

		assertEquals(segments, seenIvs.size());
	}

	@Test
	void tamperingWithCiphertextFailsDecryption() {
		byte[] plaintext = "insight cache payload".getBytes(StandardCharsets.UTF_8);
		byte[] encrypted = assertDoesNotThrow(() -> InsightCipher.forEncryption(key).encryptToBytes(plaintext));

		// flip a byte after the clear-text IV prefix, inside the actual ciphertext/tag
		encrypted[encrypted.length - 1] ^= 0x01;

		// CipherInputStream wraps the GCM auth-tag failure (AEADBadTagException, a
		// GeneralSecurityException) as a plain IOException once it surfaces through
		// doFinal() during a read() - so either exception type is a valid signal
		// that tampering was detected; what matters is that decryption refuses to
		// hand back unauthenticated plaintext.
		Exception thrown = assertThrows(Exception.class,
				() -> InsightCipher.forDecryption(key).decryptFromBytes(encrypted));
		boolean isSecurityFailure = thrown instanceof GeneralSecurityException
				|| (thrown instanceof IOException && thrown.getCause() instanceof GeneralSecurityException);
		assertTrue(isSecurityFailure, "Expected a GeneralSecurityException (possibly IOException-wrapped), got: " + thrown);
	}

	@Test
	void cannotUseAnEncryptionCipherToDecrypt() {
		InsightCipher cipher = InsightCipher.forEncryption(key);
		assertThrows(IllegalStateException.class, () -> cipher.wrap(new ByteArrayInputStream(new byte[0])));
	}

	@Test
	void cannotUseADecryptionCipherToEncrypt() {
		InsightCipher cipher = InsightCipher.forDecryption(key);
		assertThrows(IllegalStateException.class, () -> cipher.wrap(new ByteArrayOutputStream()));
	}

	/** Tiny local hex encoder so the test has no extra dependency. */
	private static final class Base16 {
		static String encode(byte[] bytes) {
			StringBuilder sb = new StringBuilder(bytes.length * 2);
			for (byte b : bytes) {
				sb.append(String.format("%02x", b));
			}
			return sb.toString();
		}
	}
}
