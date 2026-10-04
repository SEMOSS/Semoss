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

import java.net.URI;
import java.util.Set;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;

/**
 * Verifies an OIDC {@code id_token} - signature, issuer, audience, and
 * expiry/not-before - against a provider's published JWKS, using
 * {@code com.nimbusds:nimbus-jose-jwt} (already on the classpath transitively
 * via the existing {@code com.nimbusds:oauth2-oidc-sdk} dependency; no new
 * dependency was added for this).
 *
 * <p>
 * Signature verification deliberately only accepts the FIPS-approved
 * asymmetric families - RSASSA-PKCS1-v1_5, RSASSA-PSS, and ECDSA - never
 * {@code none} and never the HMAC (HS*) family. An HMAC "signature" is keyed
 * by a shared secret; if a verifier naively used whatever's in a JSON Web Key
 * as that secret, an attacker who can read the provider's (public) JWKS could
 * forge a validly-"signed" token entirely on their own.
 *
 * <p>
 * No explicit JCA provider is pinned for the verification operation itself
 * (unlike {@link BcFipsProvider}'s callers): RSA/EC signature verification is
 * an algorithm both IL4 variants' own default-registered provider already
 * implements (BCFIPS is globally registered and first on the BC-FIPS variant;
 * ACCP is globally registered and implements RSA on the ACCP variant), so a
 * bare JCA lookup already resolves to the right one on each - unlike PBKDF2,
 * which ACCP does not implement at all.
 */
public final class OidcIdTokenVerifier {

	private OidcIdTokenVerifier() {
	}

	private static final Set<JWSAlgorithm> ALLOWED_ALGORITHMS = Set.of(JWSAlgorithm.RS256, JWSAlgorithm.RS384,
			JWSAlgorithm.RS512, JWSAlgorithm.PS256, JWSAlgorithm.PS384, JWSAlgorithm.PS512, JWSAlgorithm.ES256,
			JWSAlgorithm.ES384, JWSAlgorithm.ES512);

	/**
	 * @param idToken the compact JWS id_token to verify
	 * @param jwksUrl the provider's JWKS endpoint (its published public keys)
	 * @param issuer  the expected {@code iss} claim
	 * @param clientId the expected {@code aud} claim (the client/application ID
	 *                registered with the provider)
	 * @return the verified claim set
	 * @throws Exception if {@code jwksUrl}/{@code issuer} are not configured, the
	 *                    JWKS can't be fetched, the signature does not verify
	 *                    with an allowed algorithm, or the issuer/audience/
	 *                    expiry/not-before checks fail
	 */
	public static JWTClaimsSet verify(String idToken, String jwksUrl, String issuer, String clientId)
			throws Exception {
		if (idToken == null || idToken.isBlank()) {
			throw new IllegalArgumentException("No id_token to verify");
		}
		if (jwksUrl == null || jwksUrl.isBlank() || issuer == null || issuer.isBlank()) {
			throw new IllegalStateException(
					"id_token verification requires both a configured jwks_url and issuer; refusing to trust an unverified token");
		}

		JWKSource<SecurityContext> keySource = JWKSourceBuilder.<SecurityContext>create(URI.create(jwksUrl).toURL())
				.cache(true).build();
		JWSVerificationKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(ALLOWED_ALGORITHMS,
				keySource);

		ConfigurableJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
		processor.setJWSKeySelector(keySelector);
		processor.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier<>(
				new JWTClaimsSet.Builder().issuer(issuer).audience(clientId).build(), Set.of("sub", "exp", "iat")));

		return processor.process(idToken, null);
	}
}
