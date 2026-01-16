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
package prerna.auth.mcp;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Manages RSA keypair for signing and verifying JWT tokens in MCP OAuth flow.
 * Generates a single keypair on first use and caches it for the lifetime of the application.
 */
public class MCPKeyManager {
    private static final Logger classLogger = LogManager.getLogger(MCPKeyManager.class);
    private static MCPKeyManager instance;

    private KeyPair keyPair;
    private String keyId = "mcp-key-1";

    private MCPKeyManager() {
        generateKeyPair();
    }

    public static synchronized MCPKeyManager getInstance() {
        if (instance == null) {
            instance = new MCPKeyManager();
        }
        return instance;
    }

    /**
     * Generate RSA keypair for JWT signing
     */
    private void generateKeyPair() {
        try {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
            keyGen.initialize(2048);
            this.keyPair = keyGen.generateKeyPair();
            classLogger.info("Generated RSA keypair for MCP JWT signing");
        } catch (Exception e) {
            classLogger.error("Failed to generate RSA keypair", e);
            throw new RuntimeException("Failed to generate RSA keypair", e);
        }
    }

    /**
     * Get the private key for signing JWTs
     */
    public PrivateKey getPrivateKey() {
        return keyPair.getPrivate();
    }

    /**
     * Get the public key for verifying JWTs
     */
    public PublicKey getPublicKey() {
        return keyPair.getPublic();
    }

    /**
     * Get the key ID (kid) for JWT header
     */
    public String getKeyId() {
        return keyId;
    }

    /**
     * Get JWKS (JSON Web Key Set) representation of the public key
     * Used by /.well-known/jwks.json endpoint
     */
    public Map<String, Object> getJWKS() {
        RSAPublicKey rsaPublicKey = (RSAPublicKey) keyPair.getPublic();

        // Encode modulus and exponent in Base64 URL-safe format
        String n = Base64.getUrlEncoder().withoutPadding()
            .encodeToString(rsaPublicKey.getModulus().toByteArray());
        String e = Base64.getUrlEncoder().withoutPadding()
            .encodeToString(rsaPublicKey.getPublicExponent().toByteArray());

        Map<String, Object> jwk = new HashMap<>();
        jwk.put("kty", "RSA");
        jwk.put("use", "sig");
        jwk.put("alg", "RS256");
        jwk.put("kid", keyId);
        jwk.put("n", n);
        jwk.put("e", e);

        Map<String, Object> jwks = new HashMap<>();
        jwks.put("keys", new Object[] { jwk });

        return jwks;
    }
}
