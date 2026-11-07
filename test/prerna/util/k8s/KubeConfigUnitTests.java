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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.RSAPrivateKey;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KubeConfigUnitTests {

	@TempDir
	Path dir;

	/** The shape k3s writes to /etc/rancher/k3s/k3s.yaml */
	private Path k3sStyleKubeconfig(TestPki pki, TestPki.Issued client, String server) throws Exception {
		String clientPem = TestPki.certificatePem(client.certificate()) + TestPki.certificatePem(pki.ca);
		String yaml = """
				apiVersion: v1
				clusters:
				- cluster:
				    certificate-authority-data: %s
				    server: %s
				  name: default
				contexts:
				- context:
				    cluster: default
				    user: default
				  name: default
				current-context: default
				kind: Config
				users:
				- name: default
				  user:
				    client-certificate-data: %s
				    client-key-data: %s
				""".formatted(TestPki.base64(TestPki.certificatePem(pki.ca)), server, TestPki.base64(clientPem),
				TestPki.base64(TestPki.sec1Pem(client.keys())));
		return write("k3s.yaml", yaml);
	}

	private Path write(String name, String content) throws Exception {
		Path file = this.dir.resolve(name);
		Files.writeString(file, content, StandardCharsets.UTF_8);
		return file;
	}

	@Test
	void readsK3sKubeconfigWithEcClientKey() throws Exception {
		TestPki pki = new TestPki();
		TestPki.Issued client = pki.issue("CN=system:admin,O=system:masters", null);

		KubeConfig config = KubeConfig.load(k3sStyleKubeconfig(pki, client, "https://127.0.0.1:6443"), null);

		assertEquals("default", config.getContextName());
		assertEquals("https://127.0.0.1:6443", config.getServer());
		assertNull(config.getNamespace());
		assertNull(config.getToken());
		assertTrue(config.getClientKeyPem().startsWith("-----BEGIN EC PRIVATE KEY-----"));
		PrivateKey key = PemKeys.privateKey(config.getClientKeyPem(), "test key");
		assertEquals(scalar(client.keys().getPrivate()), scalar(key), "the same key, read back");
		assertEquals(2, PemKeys.certificates(config.getClientCertificatePem(), "test chain").size());
		assertNotNull(KubeConnection.fromKubeConfig(config).getSslContext());
	}

	@Test
	void readsEveryUnencryptedKeyFormat() throws Exception {
		KeyPair ec = TestPki.ecKeys();
		assertEquals(scalar(ec.getPrivate()), scalar(PemKeys.privateKey(TestPki.pkcs8Pem(ec.getPrivate()), "pkcs8")));
		assertEquals(scalar(ec.getPrivate()),
				scalar(PemKeys.privateKey(TestPki.sec1PemWithSeparateParameters(ec), "sec1 + parameters")));

		KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
		generator.initialize(2048);
		KeyPair rsa = generator.generateKeyPair();
		// PKCS#1 is the OCTET STRING inside the JDK's PKCS#8 encoding
		PemKeys.Der pkcs8 = PemKeys.read(rsa.getPrivate().getEncoded(), 0);
		byte[] pkcs1 = pkcs8.children().get(2).content();
		PrivateKey read = PemKeys.privateKey(TestPki.pem("RSA PRIVATE KEY", pkcs1), "pkcs1");
		assertEquals("RSA", read.getAlgorithm());
		RSAPrivateKey original = (RSAPrivateKey) rsa.getPrivate();
		assertEquals(original.getModulus(), ((RSAPrivateKey) read).getModulus());
		assertEquals(original.getPrivateExponent(), ((RSAPrivateKey) read).getPrivateExponent());
	}

	private static BigInteger scalar(PrivateKey key) {
		return ((ECPrivateKey) key).getS();
	}

	@Test
	void rejectsEncryptedAndMissingKeys() {
		String encrypted = "-----BEGIN RSA PRIVATE KEY-----\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES-128-CBC,00\n\nAAAA\n-----END RSA PRIVATE KEY-----\n";
		assertTrue(assertThrows(IllegalArgumentException.class, () -> PemKeys.privateKey(encrypted, "key"))
				.getMessage().contains("encrypted"));
		assertTrue(assertThrows(IllegalArgumentException.class,
				() -> PemKeys.privateKey("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n", "key"))
				.getMessage().contains("no PEM private key"));
	}

	@Test
	void selectsNamedContextAndResolvesRelativeTokenFile() throws Exception {
		write("token", "  sa-token-value\n");
		Path kubeconfig = write("config", """
				apiVersion: v1
				kind: Config
				current-context: other
				clusters:
				- name: dev
				  cluster:
				    server: http://127.0.0.1:8001
				- name: prod
				  cluster:
				    server: https://prod.example:6443
				contexts:
				- name: other
				  context: {cluster: prod, user: robot}
				- name: dev
				  context: {cluster: dev, user: robot, namespace: models}
				users:
				- name: robot
				  user:
				    tokenFile: token
				""");

		KubeConfig config = KubeConfig.load(kubeconfig, "dev");

		assertEquals("http://127.0.0.1:8001", config.getServer());
		assertEquals("models", config.getNamespace());
		assertEquals(this.dir.resolve("token"), config.getTokenFile());
		KubeConnection connection = KubeConnection.fromKubeConfig(config);
		assertNull(connection.getSslContext(), "plain http needs no TLS");
		assertEquals("sa-token-value", connection.bearerToken());
	}

	@Test
	void rejectsCredentialPlugins() throws Exception {
		Path kubeconfig = write("config", """
				current-context: eks
				clusters: [{name: c, cluster: {server: "https://example:443"}}]
				contexts: [{name: eks, context: {cluster: c, user: u}}]
				users:
				- name: u
				  user:
				    exec: {command: aws, args: [eks, get-token]}
				""");
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> KubeConfig.load(kubeconfig, null));
		assertTrue(error.getMessage().contains("exec"), error.getMessage());
	}

	@Test
	void rejectsInsecureSkipTlsVerify() throws Exception {
		Path kubeconfig = write("config", """
				current-context: x
				clusters: [{name: c, cluster: {server: "https://example:443", insecure-skip-tls-verify: true}}]
				contexts: [{name: x, context: {cluster: c, user: u}}]
				users: [{name: u, user: {token: abc}}]
				""");
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> KubeConfig.load(kubeconfig, null));
		assertTrue(error.getMessage().contains("insecure-skip-tls-verify"), error.getMessage());
	}

	@Test
	void explainsMissingPieces() throws Exception {
		Path noContext = write("no-context", """
				clusters: [{name: c, cluster: {server: "https://example:443"}}]
				users: [{name: u, user: {token: abc}}]
				""");
		assertTrue(assertThrows(IllegalArgumentException.class, () -> KubeConfig.load(noContext, null)).getMessage()
				.contains("KSERVE_KUBE_CONTEXT"));

		Path certWithoutKey = write("cert-only", """
				current-context: x
				clusters: [{name: c, cluster: {server: "https://example:443"}}]
				contexts: [{name: x, context: {cluster: c, user: u}}]
				users: [{name: u, user: {client-certificate-data: QUJD}}]
				""");
		assertTrue(assertThrows(IllegalArgumentException.class, () -> KubeConfig.load(certWithoutKey, null))
				.getMessage().contains("without its key"));

		assertTrue(assertThrows(IllegalArgumentException.class,
				() -> KubeConfig.load(this.dir.resolve("absent"), null)).getMessage().contains("not found"));
	}

	@Test
	void readsThePodsServiceAccount() throws Exception {
		TestPki pki = new TestPki();
		Path account = Files.createDirectories(this.dir.resolve("serviceaccount"));
		Files.writeString(account.resolve("token"), "eyJ.token\n");
		Files.writeString(account.resolve("ca.crt"), TestPki.certificatePem(pki.ca));
		Files.writeString(account.resolve("namespace"), "semoss");

		KubeConnection connection = KubeConnection.inCluster("10.43.0.1", "443", account);

		assertEquals(URI.create("https://10.43.0.1:443"), connection.getServer());
		assertEquals("eyJ.token", connection.bearerToken());
		assertEquals("semoss", connection.getNamespace());
		assertNotNull(connection.getSslContext());
		assertEquals(URI.create("https://[fd00::1]:6443"),
				KubeConnection.inCluster("fd00::1", "6443", account).getServer());
	}

	@Test
	void inClusterNeedsAHostAndAToken() {
		assertThrows(IllegalStateException.class, () -> KubeConnection.inCluster(null, "443", this.dir));
		IllegalStateException error = assertThrows(IllegalStateException.class,
				() -> KubeConnection.inCluster("10.43.0.1", "443", this.dir.resolve("none")));
		assertTrue(error.getMessage().contains("automountServiceAccountToken"), error.getMessage());
	}
}
