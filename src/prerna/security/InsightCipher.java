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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

/**
 * Wraps streams (or, via {@link #encryptToBytes(byte[])}/
 * {@link #decryptFromBytes(byte[])}, byte arrays) with AES-GCM encryption,
 * generating a FRESH, randomly generated IV for every stream/call - never the
 * same (key, IV) pair twice - and prepending it to the output in the clear
 * (an IV is not secret; only the key is).
 *
 * <p>
 * Replaces passing a single {@code javax.crypto.Cipher} around, already
 * {@code init()}'d with one IV, and reusing it across many independent
 * {@code doFinal()}/stream-close operations under an insight's encrypted
 * cache - for an insight with N dataframes, that meant N+ independent
 * ciphertexts under one (key, IV) pair. Reusing a GCM (key, IV) pair for two
 * different messages recovers the authentication subkey, breaking both
 * confidentiality and integrity for every message encrypted under that key.
 * This is not merely a theoretical concern here: confirmed empirically that
 * BC-FIPS's {@code BouncyCastleFipsProvider}, unlike stock SunJCE, does not
 * throw/guard against a second {@code doFinal()} on an already-finalized GCM
 * {@code Cipher} - it silently re-encrypts under the same (key, IV),
 * verified by decrypting the resulting second ciphertext with the original
 * parameters.
 */
public final class InsightCipher {

	private static final int IV_BYTE_LENGTH = 12;
	private static final int GCM_TAG_LENGTH_BITS = 128;
	private static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
	private static final SecureRandom RANDOM = new SecureRandom();

	private final SecretKey key;
	private final boolean forEncryption;

	private InsightCipher(SecretKey key, boolean forEncryption) {
		this.key = key;
		this.forEncryption = forEncryption;
	}

	public static InsightCipher forEncryption(SecretKey key) {
		return new InsightCipher(key, true);
	}

	public static InsightCipher forDecryption(SecretKey key) {
		return new InsightCipher(key, false);
	}

	/**
	 * Wraps {@code out} so every byte subsequently written to the result is
	 * AES-GCM encrypted under a fresh IV generated right now, written first (in
	 * the clear) so {@link #wrap(InputStream)} can recover it.
	 */
	public OutputStream wrap(OutputStream out) throws GeneralSecurityException, IOException {
		requireMode(true);
		byte[] iv = new byte[IV_BYTE_LENGTH];
		RANDOM.nextBytes(iv);
		out.write(iv);
		return new CipherOutputStream(out, newCipher(Cipher.ENCRYPT_MODE, iv));
	}

	/**
	 * Wraps {@code in}, first reading the IV {@link #wrap(OutputStream)}
	 * prepended, then AES-GCM decrypting everything read after it.
	 */
	public InputStream wrap(InputStream in) throws GeneralSecurityException, IOException {
		requireMode(false);
		byte[] iv = new byte[IV_BYTE_LENGTH];
		readFully(in, iv);
		return new CipherInputStream(in, newCipher(Cipher.DECRYPT_MODE, iv));
	}

	/** One-shot convenience over {@link #wrap(OutputStream)} for a whole byte array. */
	public byte[] encryptToBytes(byte[] plaintext) throws GeneralSecurityException, IOException {
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		try (OutputStream wrapped = wrap(buffer)) {
			wrapped.write(plaintext);
		}
		return buffer.toByteArray();
	}

	/** One-shot convenience over {@link #wrap(InputStream)} for a whole byte array. */
	public byte[] decryptFromBytes(byte[] payload) throws GeneralSecurityException, IOException {
		try (InputStream wrapped = wrap(new ByteArrayInputStream(payload))) {
			return wrapped.readAllBytes();
		}
	}

	private void requireMode(boolean expectedEncryption) {
		if (forEncryption != expectedEncryption) {
			throw new IllegalStateException("This InsightCipher was built for "
					+ (forEncryption ? "encryption" : "decryption") + ", not "
					+ (expectedEncryption ? "encryption" : "decryption"));
		}
	}

	private Cipher newCipher(int mode, byte[] iv) throws GeneralSecurityException {
		Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
		cipher.init(mode, key, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
		return cipher;
	}

	private static void readFully(InputStream in, byte[] buffer) throws IOException {
		int offset = 0;
		while (offset < buffer.length) {
			int read = in.read(buffer, offset, buffer.length - offset);
			if (read < 0) {
				throw new IOException("Stream ended before the full IV could be read");
			}
			offset += read;
		}
	}
}
