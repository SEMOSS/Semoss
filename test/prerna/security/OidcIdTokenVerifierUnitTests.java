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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.Date;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpServer;

class OidcIdTokenVerifierUnitTests {

	private static final String ISSUER = "https://adfs.example.com/adfs";
	private static final String AUDIENCE = "test-client-id";

	private static HttpServer server;
	private static String jwksUrl;
	private static RSAKey jwk;

	@BeforeAll
	static void startJwksServer() throws Exception {
		KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
		gen.initialize(2048);
		KeyPair pair = gen.generateKeyPair();
		jwk = new RSAKey.Builder((RSAPublicKey) pair.getPublic()).privateKey((RSAPrivateKey) pair.getPrivate())
				.keyID("test-key-1").build();
		byte[] jwksBytes = new JWKSet(jwk.toPublicJWK()).toString().getBytes(StandardCharsets.UTF_8);

		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/jwks", exchange -> {
			exchange.sendResponseHeaders(200, jwksBytes.length);
			exchange.getResponseBody().write(jwksBytes);
			exchange.close();
		});
		server.start();
		jwksUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/jwks";
	}

	@AfterAll
	static void stopJwksServer() {
		server.stop(0);
	}

	private static String sign(String issuer, String audience, Date expiration) throws Exception {
		JWTClaimsSet claims = new JWTClaimsSet.Builder().issuer(issuer).audience(audience).subject("user-123")
				.issueTime(new Date()).expirationTime(expiration).build();
		SignedJWT jwt = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(jwk.getKeyID()).build(), claims);
		jwt.sign(new RSASSASigner(jwk));
		return jwt.serialize();
	}

	private static String validToken() throws Exception {
		return sign(ISSUER, AUDIENCE, new Date(System.currentTimeMillis() + 60_000));
	}

	@Test
	void verifiesAValidToken() throws Exception {
		JWTClaimsSet claims = OidcIdTokenVerifier.verify(validToken(), jwksUrl, ISSUER, AUDIENCE);
		assertEquals("user-123", claims.getSubject());
	}

	@Test
	void rejectsWrongAudience() throws Exception {
		String token = validToken();
		assertThrows(Exception.class, () -> OidcIdTokenVerifier.verify(token, jwksUrl, ISSUER, "wrong-client-id"));
	}

	@Test
	void rejectsWrongIssuer() throws Exception {
		String token = validToken();
		assertThrows(Exception.class,
				() -> OidcIdTokenVerifier.verify(token, jwksUrl, "https://not-the-real-issuer.example.com", AUDIENCE));
	}

	@Test
	void rejectsExpiredToken() throws Exception {
		String expired = sign(ISSUER, AUDIENCE, new Date(System.currentTimeMillis() - 600_000));
		assertThrows(Exception.class, () -> OidcIdTokenVerifier.verify(expired, jwksUrl, ISSUER, AUDIENCE));
	}

	@Test
	void rejectsATamperedSignature() throws Exception {
		String[] parts = validToken().split("\\.");
		String tampered = parts[0] + "." + parts[1] + ".tamperedSignatureXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
		assertThrows(Exception.class, () -> OidcIdTokenVerifier.verify(tampered, jwksUrl, ISSUER, AUDIENCE));
	}

	@Test
	void rejectsAnUnsignedAlgNoneToken() {
		String header = base64Url("{\"alg\":\"none\",\"typ\":\"JWT\"}");
		String payload = base64Url(String.format("{\"iss\":\"%s\",\"aud\":\"%s\",\"sub\":\"user-123\",\"exp\":%d}",
				ISSUER, AUDIENCE, (System.currentTimeMillis() / 1000) + 60));
		String noneAlgToken = header + "." + payload + ".";
		assertThrows(Exception.class, () -> OidcIdTokenVerifier.verify(noneAlgToken, jwksUrl, ISSUER, AUDIENCE));
	}

	@Test
	void failsClosedWhenJwksUrlIsNotConfigured() throws Exception {
		String token = validToken();
		assertThrows(IllegalStateException.class, () -> OidcIdTokenVerifier.verify(token, null, ISSUER, AUDIENCE));
	}

	@Test
	void failsClosedWhenIssuerIsNotConfigured() throws Exception {
		String token = validToken();
		assertThrows(IllegalStateException.class, () -> OidcIdTokenVerifier.verify(token, jwksUrl, null, AUDIENCE));
	}

	private static String base64Url(String s) {
		return Base64.getUrlEncoder().withoutPadding().encodeToString(s.getBytes(StandardCharsets.UTF_8));
	}
}
