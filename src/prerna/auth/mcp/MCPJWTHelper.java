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

import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.auth.User;

/**
 * Helper class for creating and verifying JWT tokens for MCP OAuth.
 * Uses RS256 (RSA Signature with SHA-256) algorithm.
 */
public class MCPJWTHelper {
    private static final Logger classLogger = LogManager.getLogger(MCPJWTHelper.class);
    private static final Gson gson = new Gson();

    /**
     * Create a JWT token for an authenticated user
     *
     * @param user The authenticated user
     * @param clientId The OAuth client ID
     * @param issuer The token issuer (our server URL)
     * @param audience The token audience (resource server URL)
     * @param expiresInSeconds Token expiration time in seconds
     * @return JWT token string
     */
    public static String createJWT(User user, String clientId, String issuer,
                                  String audience, long expiresInSeconds) {
        try {
            long nowSeconds = System.currentTimeMillis() / 1000;
            long expSeconds = nowSeconds + expiresInSeconds;

            // Build JWT header
            Map<String, Object> header = new HashMap<>();
            header.put("alg", "RS256");
            header.put("typ", "JWT");
            header.put("kid", MCPKeyManager.getInstance().getKeyId());

            // Build JWT payload (claims)
            Map<String, Object> payload = new HashMap<>();
            payload.put("iss", issuer);
            payload.put("sub", user.getPrimaryLoginToken().getEmail());
            payload.put("aud", audience);
            payload.put("exp", expSeconds);
            payload.put("iat", nowSeconds);
            payload.put("client_id", clientId);
            payload.put("user_id", user.getPrimaryLoginToken().getId());
            payload.put("scope", "mcp:read mcp:write mcp:tools:call");

            // Encode header and payload
            String encodedHeader = base64UrlEncode(gson.toJson(header));
            String encodedPayload = base64UrlEncode(gson.toJson(payload));

            // Create signature
            String signatureInput = encodedHeader + "." + encodedPayload;
            String signature = signRS256(signatureInput);

            // Return complete JWT
            String jwt = signatureInput + "." + signature;
            classLogger.debug("Created JWT token for user: " + user.getPrimaryLoginToken().getEmail());
            return jwt;

        } catch (Exception e) {
            classLogger.error("Failed to create JWT", e);
            throw new RuntimeException("Failed to create JWT", e);
        }
    }

    /**
     * Verify and parse a JWT token
     *
     * @param token The JWT token string
     * @param expectedIssuer Expected issuer claim
     * @param expectedAudience Expected audience claim (can be null to skip check)
     * @return JWT claims if valid, null if invalid
     */
    public static Map<String, Object> verifyJWT(String token, String expectedIssuer, String expectedAudience) {
        try {
            String[] parts = token.split("\\.");
            if (parts.length != 3) {
                classLogger.warn("Invalid JWT format - expected 3 parts");
                return null;
            }

            String encodedHeader = parts[0];
            String encodedPayload = parts[1];
            String signature = parts[2];

            // Verify signature
            String signatureInput = encodedHeader + "." + encodedPayload;
            if (!verifyRS256(signatureInput, signature)) {
                classLogger.warn("JWT signature verification failed");
                return null;
            }

            // Parse payload
            String payloadJson = base64UrlDecode(encodedPayload);
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = gson.fromJson(payloadJson, Map.class);

            // Verify expiration
            Number expNum = (Number) payload.get("exp");
            if (expNum == null) {
                classLogger.warn("JWT missing exp claim");
                return null;
            }
            long exp = expNum.longValue();
            long now = System.currentTimeMillis() / 1000;
            if (now >= exp) {
                classLogger.warn("JWT expired");
                return null;
            }

            // Verify issuer
            String iss = (String) payload.get("iss");
            if (iss == null || !iss.equals(expectedIssuer)) {
                classLogger.warn("JWT issuer mismatch - expected: " + expectedIssuer + ", got: " + iss);
                return null;
            }

            // Verify audience (if provided)
            if (expectedAudience != null) {
                String aud = (String) payload.get("aud");
                if (aud == null || !aud.equals(expectedAudience)) {
                    classLogger.warn("JWT audience mismatch - expected: " + expectedAudience + ", got: " + aud);
                    return null;
                }
            }

            classLogger.debug("JWT verified successfully");
            return payload;

        } catch (Exception e) {
            classLogger.error("JWT verification failed", e);
            return null;
        }
    }

    /**
     * Sign data using RS256 (RSA with SHA-256)
     */
    private static String signRS256(String data) throws Exception {
        Signature signer = Signature.getInstance("SHA256withRSA");
        PrivateKey privateKey = MCPKeyManager.getInstance().getPrivateKey();
        signer.initSign(privateKey);
        signer.update(data.getBytes(StandardCharsets.UTF_8));
        byte[] signatureBytes = signer.sign();
        return base64UrlEncode(signatureBytes);
    }

    /**
     * Verify RS256 signature
     */
    private static boolean verifyRS256(String data, String signatureStr) throws Exception {
        Signature verifier = Signature.getInstance("SHA256withRSA");
        PublicKey publicKey = MCPKeyManager.getInstance().getPublicKey();
        verifier.initVerify(publicKey);
        verifier.update(data.getBytes(StandardCharsets.UTF_8));
        byte[] signatureBytes = base64UrlDecodeByte(signatureStr);
        return verifier.verify(signatureBytes);
    }

    /**
     * Base64 URL-safe encode string
     */
    private static String base64UrlEncode(String str) {
        return base64UrlEncode(str.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Base64 URL-safe encode bytes
     */
    private static String base64UrlEncode(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    /**
     * Base64 URL-safe decode to string
     */
    private static String base64UrlDecode(String str) {
        byte[] bytes = base64UrlDecodeByte(str);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Base64 URL-safe decode to bytes
     */
    private static byte[] base64UrlDecodeByte(String str) {
        return Base64.getUrlDecoder().decode(str);
    }
}
