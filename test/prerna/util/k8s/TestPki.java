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

import static prerna.util.k8s.PemKeys.BIT_STRING;
import static prerna.util.k8s.PemKeys.CONTEXT_0;
import static prerna.util.k8s.PemKeys.INTEGER;
import static prerna.util.k8s.PemKeys.OCTET_STRING;
import static prerna.util.k8s.PemKeys.SEQUENCE;
import static prerna.util.k8s.PemKeys.der;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPrivateKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;

/**
 * A throwaway CA issuing EC P-256 certificates, the kind k3s uses for its API
 * server and its admin kubeconfig. Built with the JDK alone (DER by hand), for
 * the same reason the production code avoids BouncyCastle: which of SEMOSS's two
 * BouncyCastle families loads first changes from one classpath to the next.
 */
final class TestPki {

	private static final AtomicLong SERIAL = new AtomicLong(1);
	private static final char[] PASSWORD = "test".toCharArray();
	private static final DateTimeFormatter UTC_TIME = DateTimeFormatter.ofPattern("yyMMddHHmmss'Z'")
			.withZone(ZoneOffset.UTC);
	/** 1.2.840.10045.4.3.2 */
	private static final byte[] ECDSA_WITH_SHA256 = { 0x06, 0x08, 0x2A, (byte) 0x86, 0x48, (byte) 0xCE, 0x3D, 0x04,
			0x03, 0x02 };
	/** 1.2.840.10045.3.1.7 (P-256) */
	private static final byte[] PRIME256V1 = { 0x06, 0x08, 0x2A, (byte) 0x86, 0x48, (byte) 0xCE, 0x3D, 0x03, 0x01,
			0x07 };
	private static final byte[] BASIC_CONSTRAINTS = { 0x06, 0x03, 0x55, 0x1D, 0x13 };
	private static final byte[] SUBJECT_ALT_NAME = { 0x06, 0x03, 0x55, 0x1D, 0x11 };
	private static final byte[] TRUE = { 0x01, 0x01, (byte) 0xFF };

	final KeyPair caKeys;
	final X509Certificate ca;

	TestPki() throws Exception {
		this.caKeys = ecKeys();
		this.ca = certificate("CN=test-ca", this.caKeys.getPublic(), this.caKeys.getPrivate(), true, null);
	}

	Issued issue(String subject, String ipAddress) throws Exception {
		KeyPair keys = ecKeys();
		return new Issued(keys, certificate(subject, keys.getPublic(), this.caKeys.getPrivate(), false, ipAddress));
	}

	/** A server context presenting the certificate and requiring clients signed by this CA. */
	SSLContext serverContext(Issued server) throws Exception {
		KeyStore keyStore = KeyStore.getInstance("PKCS12");
		keyStore.load(null, null);
		keyStore.setKeyEntry("server", server.keys().getPrivate(), PASSWORD,
				new X509Certificate[] { server.certificate(), this.ca });
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(keyStore, PASSWORD);
		KeyStore trustStore = KeyStore.getInstance("PKCS12");
		trustStore.load(null, null);
		trustStore.setCertificateEntry("ca", this.ca);
		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(trustStore);
		SSLContext context = SSLContext.getInstance("TLS");
		context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
		return context;
	}

	static KeyPair ecKeys() throws Exception {
		KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
		generator.initialize(new ECGenParameterSpec("secp256r1"));
		return generator.generateKeyPair();
	}

	private X509Certificate certificate(String subject, PublicKey key, PrivateKey signer, boolean isCa,
			String ipAddress) throws Exception {
		Instant now = Instant.now();
		byte[] algorithm = der(SEQUENCE, ECDSA_WITH_SHA256);
		byte[] constraints = der(SEQUENCE, BASIC_CONSTRAINTS, TRUE,
				der(OCTET_STRING, isCa ? der(SEQUENCE, TRUE) : der(SEQUENCE)));
		byte[] extensions = constraints;
		if (ipAddress != null) {
			byte[] ip = InetAddress.getByName(ipAddress).getAddress();
			extensions = concat(extensions,
					der(SEQUENCE, SUBJECT_ALT_NAME, der(OCTET_STRING, der(SEQUENCE, der(0x87, ip)))));
		}
		byte[] tbs = der(SEQUENCE, der(CONTEXT_0, der(INTEGER, new byte[] { 2 })),
				der(INTEGER, BigInteger.valueOf(SERIAL.getAndIncrement()).toByteArray()), algorithm,
				new X500Principal("CN=test-ca").getEncoded(),
				der(SEQUENCE, utcTime(now.minus(Duration.ofMinutes(5))), utcTime(now.plus(Duration.ofDays(1)))),
				new X500Principal(subject).getEncoded(), key.getEncoded(), der(0xA3, der(SEQUENCE, extensions)));
		Signature signature = Signature.getInstance("SHA256withECDSA");
		signature.initSign(signer);
		signature.update(tbs);
		byte[] certificate = der(SEQUENCE, tbs, algorithm, der(BIT_STRING, concat(new byte[] { 0 }, signature.sign())));
		return (X509Certificate) CertificateFactory.getInstance("X.509")
				.generateCertificate(new ByteArrayInputStream(certificate));
	}

	private static byte[] utcTime(Instant instant) {
		return der(0x17, UTC_TIME.format(instant).getBytes(StandardCharsets.US_ASCII));
	}

	private static byte[] concat(byte[] first, byte[] second) {
		byte[] result = Arrays.copyOf(first, first.length + second.length);
		System.arraycopy(second, 0, result, first.length, second.length);
		return result;
	}

	/** The key as k3s writes it: SEC1 {@code EC PRIVATE KEY} naming its curve. */
	static String sec1Pem(KeyPair keys) {
		byte[] scalar = ((ECPrivateKey) keys.getPrivate()).getS().toByteArray();
		byte[] fixed = new byte[32];
		int length = Math.min(scalar.length, 32);
		System.arraycopy(scalar, scalar.length - length, fixed, 32 - length, length);
		return pem("EC PRIVATE KEY",
				der(SEQUENCE, der(INTEGER, new byte[] { 1 }), der(OCTET_STRING, fixed), der(CONTEXT_0, PRIME256V1)));
	}

	/** OpenSSL's older form: the curve in a separate EC PARAMETERS block, SEC1 without it. */
	static String sec1PemWithSeparateParameters(KeyPair keys) {
		byte[] scalar = ((ECPrivateKey) keys.getPrivate()).getS().toByteArray();
		byte[] fixed = new byte[32];
		int length = Math.min(scalar.length, 32);
		System.arraycopy(scalar, scalar.length - length, fixed, 32 - length, length);
		return pem("EC PARAMETERS", PRIME256V1)
				+ pem("EC PRIVATE KEY", der(SEQUENCE, der(INTEGER, new byte[] { 1 }), der(OCTET_STRING, fixed)));
	}

	static String pkcs8Pem(PrivateKey key) {
		return pem("PRIVATE KEY", key.getEncoded());
	}

	static String certificatePem(X509Certificate certificate) throws Exception {
		return pem("CERTIFICATE", certificate.getEncoded());
	}

	static String pem(String type, byte[] der) {
		return "-----BEGIN " + type + "-----\n"
				+ Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(der)
				+ "\n-----END " + type + "-----\n";
	}

	static String base64(String text) {
		return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.US_ASCII));
	}

	record Issued(KeyPair keys, X509Certificate certificate) {
	}
}
