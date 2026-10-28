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
package prerna.util.k8s;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PEM certificates and unencrypted private keys, read with the JDK alone.
 *
 * The JDK reads PKCS#8 ({@code PRIVATE KEY}) but not the SEC1
 * ({@code EC PRIVATE KEY}) that k3s writes, nor PKCS#1
 * ({@code RSA PRIVATE KEY}); both become PKCS#8 by wrapping them in a few DER
 * bytes. BouncyCastle's PEMParser would do it, but SEMOSS ships both of its
 * families (FIPS and jdk18on), and when the FIPS pkix jar loads first it fails
 * on an EC key with a NoSuchMethodError.
 */
final class PemKeys {

	static final int INTEGER = 0x02;
	static final int BIT_STRING = 0x03;
	static final int OCTET_STRING = 0x04;
	static final int SEQUENCE = 0x30;
	static final int CONTEXT_0 = 0xA0;

	/** 1.2.840.10045.2.1 */
	private static final byte[] OID_EC_PUBLIC_KEY = { 0x06, 0x07, 0x2A, (byte) 0x86, 0x48, (byte) 0xCE, 0x3D, 0x02,
			0x01 };
	/** 1.2.840.113549.1.1.1 */
	private static final byte[] OID_RSA_ENCRYPTION = { 0x06, 0x09, 0x2A, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xF7,
			0x0D, 0x01, 0x01, 0x01 };
	private static final byte[] DER_NULL = { 0x05, 0x00 };
	private static final byte[] VERSION_0 = { INTEGER, 0x01, 0x00 };

	private static final Pattern BLOCK = Pattern.compile("-----BEGIN ([A-Z0-9 ]+)-----(.*?)-----END \\1-----",
			Pattern.DOTALL);
	private static final List<String> KEY_ALGORITHMS = List.of("EC", "RSA", "Ed25519");

	private PemKeys() {
	}

	private record Block(String type, byte[] der, boolean encrypted) {
	}

	static List<X509Certificate> certificates(String pem, String what) {
		List<X509Certificate> certificates = new ArrayList<>();
		try {
			CertificateFactory factory = CertificateFactory.getInstance("X.509");
			for (Certificate certificate : factory
					.generateCertificates(new ByteArrayInputStream(pem.getBytes(StandardCharsets.US_ASCII)))) {
				certificates.add((X509Certificate) certificate);
			}
		} catch (GeneralSecurityException e) {
			throw new IllegalArgumentException("Cannot read the " + what + ": " + e.getMessage(), e);
		}
		if (certificates.isEmpty()) {
			throw new IllegalArgumentException("The " + what + " contains no certificate");
		}
		return certificates;
	}

	static PrivateKey privateKey(String pem, String what) {
		byte[] curve = null;
		for (Block block : blocks(pem, what)) {
			if (block.encrypted() || block.type().equals("ENCRYPTED PRIVATE KEY")) {
				throw new IllegalArgumentException("The " + what + " is encrypted, which is not supported");
			}
			switch (block.type()) {
			case "EC PARAMETERS" -> curve = block.der();
			case "PRIVATE KEY" -> {
				return pkcs8(block.der(), what);
			}
			case "EC PRIVATE KEY" -> {
				return pkcs8(ecToPkcs8(block.der(), curve, what), what);
			}
			case "RSA PRIVATE KEY" -> {
				return pkcs8(der(SEQUENCE, VERSION_0, der(SEQUENCE, OID_RSA_ENCRYPTION, DER_NULL),
						der(OCTET_STRING, block.der())), what);
			}
			default -> {
				// certificates and other blocks next to the key
			}
			}
		}
		throw new IllegalArgumentException("The " + what + " contains no PEM private key");
	}

	private static List<Block> blocks(String pem, String what) {
		List<Block> blocks = new ArrayList<>();
		Matcher matcher = BLOCK.matcher(pem);
		while (matcher.find()) {
			StringBuilder base64 = new StringBuilder();
			boolean encrypted = false;
			for (String line : matcher.group(2).split("\\r?\\n")) {
				if (line.contains(":")) {
					// RFC 1421 headers, as in "Proc-Type: 4,ENCRYPTED"
					encrypted |= line.contains("ENCRYPTED");
				} else {
					base64.append(line.strip());
				}
			}
			try {
				blocks.add(new Block(matcher.group(1), Base64.getMimeDecoder().decode(base64.toString()), encrypted));
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("The " + what + " has invalid base64", e);
			}
		}
		return blocks;
	}

	/**
	 * SEC1 {@code ECPrivateKey ::= SEQUENCE { version, privateKey, [0] parameters, [1] publicKey }}
	 * becomes PKCS#8 with the curve, taken from [0] or from a separate EC PARAMETERS
	 * block, in the algorithm identifier.
	 */
	private static byte[] ecToPkcs8(byte[] sec1, byte[] curveFromParameters, String what) {
		byte[] curve = curveFromParameters;
		try {
			for (Der field : read(sec1, 0).children()) {
				if (field.tag() == CONTEXT_0) {
					curve = field.content();
				}
			}
		} catch (RuntimeException e) {
			throw new IllegalArgumentException("The " + what + " is not a valid EC private key", e);
		}
		if (curve == null) {
			throw new IllegalArgumentException("The " + what + " does not name its elliptic curve");
		}
		return der(SEQUENCE, VERSION_0, der(SEQUENCE, OID_EC_PUBLIC_KEY, curve), der(OCTET_STRING, sec1));
	}

	private static PrivateKey pkcs8(byte[] der, String what) {
		for (String algorithm : KEY_ALGORITHMS) {
			try {
				return KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(der));
			} catch (GeneralSecurityException e) {
				// not this algorithm
			}
		}
		throw new IllegalArgumentException("The " + what + " is not an EC, RSA or Ed25519 private key");
	}

	/** A DER element: its tag and where its content lies in the source bytes. */
	record Der(int tag, byte[] source, int start, int length) {

		byte[] content() {
			return Arrays.copyOfRange(this.source, this.start, this.start + this.length);
		}

		List<Der> children() {
			List<Der> children = new ArrayList<>();
			int position = this.start;
			while (position < this.start + this.length) {
				Der child = read(this.source, position);
				children.add(child);
				position = child.start() + child.length();
			}
			return children;
		}
	}

	static Der read(byte[] data, int offset) {
		if (offset + 2 > data.length) {
			throw new IllegalArgumentException("Truncated DER");
		}
		int tag = data[offset] & 0xFF;
		int first = data[offset + 1] & 0xFF;
		int position = offset + 2;
		int length;
		if (first < 0x80) {
			length = first;
		} else {
			int count = first & 0x7F;
			if (count == 0 || count > 4 || position + count > data.length) {
				throw new IllegalArgumentException("Unsupported DER length");
			}
			length = 0;
			for (int index = 0; index < count; index++) {
				length = (length << 8) | (data[position++] & 0xFF);
			}
		}
		if (length < 0 || position + length > data.length) {
			throw new IllegalArgumentException("Truncated DER");
		}
		return new Der(tag, data, position, length);
	}

	/** Encode a DER element from its already-encoded parts. */
	static byte[] der(int tag, byte[]... parts) {
		ByteArrayOutputStream content = new ByteArrayOutputStream();
		for (byte[] part : parts) {
			content.writeBytes(part);
		}
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.write(tag);
		int length = content.size();
		if (length < 0x80) {
			out.write(length);
		} else {
			byte[] bytes = java.math.BigInteger.valueOf(length).toByteArray();
			int skip = bytes[0] == 0 ? 1 : 0;
			out.write(0x80 | (bytes.length - skip));
			out.write(bytes, skip, bytes.length - skip);
		}
		out.writeBytes(content.toByteArray());
		return out.toByteArray();
	}
}
