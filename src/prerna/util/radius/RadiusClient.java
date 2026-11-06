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
package prerna.util.radius;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A minimal RADIUS (RFC 2865) client implementing PAP-style Access-Request /
 * Access-Accept / Access-Reject / Access-Challenge, sufficient for a
 * two-step (pin, then OTP) authentication exchange against a RADIUS server
 * such as LinOTP's RADIUS token.
 *
 * <p>
 * This is implemented directly against {@link java.net.DatagramSocket}
 * rather than pulling in a third-party RADIUS library: the protocol surface
 * needed here is small, and for a FIPS/IL4-hardened build, a from-scratch
 * implementation that is fully auditable in this file is preferable to
 * trusting an external dependency's exact handling of password encryption
 * and response-authenticator verification.
 * </p>
 *
 * <p>
 * <b>FIPS note:</b> RFC 2865's {@code User-Password} attribute encryption and
 * its response-authenticator scheme are both defined in terms of MD5; this is
 * mandated by the RADIUS protocol itself; not a choice made by this client.
 * MD5 is used here only as specified by RFC 2865, not for any other security
 * purpose. See the LinOTP RADIUS documentation in the IL4 README for the
 * accepted-risk writeup.
 * </p>
 */
public final class RadiusClient {

	private final String hostname;
	private final int port;
	private final byte[] sharedSecret;
	private final String nasIdentifier;
	private final int timeoutMillis;
	private final int retries;
	private final SecureRandom random = new SecureRandom();

	/**
	 * @param hostname      RADIUS server hostname or IP address
	 * @param port          RADIUS authentication port (standard is 1812)
	 * @param sharedSecret  the shared secret configured on both this client and
	 *                      the RADIUS server
	 * @param nasIdentifier value sent as the NAS-Identifier attribute, identifying
	 *                      this application to the RADIUS server
	 * @param timeoutMillis socket receive timeout for a single attempt
	 * @param retries       number of additional attempts after the first, if a
	 *                      request times out (RADIUS runs over UDP, which does
	 *                      not guarantee delivery)
	 */
	public RadiusClient(String hostname, int port, String sharedSecret, String nasIdentifier, int timeoutMillis,
			int retries) {
		if (hostname == null || hostname.isEmpty()) {
			throw new IllegalArgumentException("RADIUS hostname is required");
		}
		if (sharedSecret == null || sharedSecret.isEmpty()) {
			throw new IllegalArgumentException("RADIUS shared secret is required");
		}
		this.hostname = hostname;
		this.port = port;
		this.sharedSecret = sharedSecret.getBytes(StandardCharsets.UTF_8);
		this.nasIdentifier = nasIdentifier == null || nasIdentifier.isEmpty() ? "SEMOSS" : nasIdentifier;
		this.timeoutMillis = timeoutMillis;
		this.retries = Math.max(0, retries);
	}

	/**
	 * The outcome of a single RADIUS Access-Request/response exchange.
	 */
	public static final class RadiusResult {
		public enum Status {
			ACCEPT, REJECT, CHALLENGE
		}

		private final Status status;
		private final String replyMessage;
		private final byte[] state;

		private RadiusResult(Status status, String replyMessage, byte[] state) {
			this.status = status;
			this.replyMessage = replyMessage;
			this.state = state == null ? null : state.clone();
		}

		public Status getStatus() {
			return status;
		}

		/** The server-supplied prompt/explanation, if any (Reply-Message attribute). */
		public String getReplyMessage() {
			return replyMessage;
		}

		/**
		 * The opaque State attribute that must be echoed back in the follow-up
		 * Access-Request carrying the OTP, present only when
		 * {@link #getStatus()} is {@link Status#CHALLENGE}.
		 */
		public byte[] getState() {
			return state == null ? null : state.clone();
		}
	}

	/**
	 * Sends an initial Access-Request with the user's name and PIN/password.
	 */
	public RadiusResult authenticate(String username, String password) throws IOException {
		return doAccessRequest(username, password, null);
	}

	/**
	 * Sends a follow-up Access-Request carrying the user's OTP and the State
	 * attribute from a prior {@link RadiusResult.Status#CHALLENGE} response.
	 */
	public RadiusResult authenticateChallengeResponse(String username, String otp, byte[] state) throws IOException {
		if (state == null || state.length == 0) {
			throw new IllegalArgumentException("A State value from the prior Access-Challenge is required");
		}
		return doAccessRequest(username, otp, state);
	}

	private RadiusResult doAccessRequest(String username, String secretValue, byte[] state) throws IOException {
		byte[] requestAuthenticator = new byte[RadiusPacket.AUTHENTICATOR_LENGTH];
		random.nextBytes(requestAuthenticator);

		List<RadiusAttribute> attributes = new ArrayList<>();
		attributes.add(RadiusAttribute.ofString(RadiusAttribute.TYPE_USER_NAME, username));
		attributes.add(new RadiusAttribute(RadiusAttribute.TYPE_USER_PASSWORD,
				encryptUserPassword(secretValue, requestAuthenticator)));
		attributes.add(RadiusAttribute.ofString(RadiusAttribute.TYPE_NAS_IDENTIFIER, nasIdentifier));
		if (state != null) {
			attributes.add(new RadiusAttribute(RadiusAttribute.TYPE_STATE, state));
		}

		int identifier = random.nextInt(256);
		RadiusPacket request = new RadiusPacket(RadiusPacket.CODE_ACCESS_REQUEST, identifier, requestAuthenticator,
				attributes);

		IOException lastFailure = null;
		for (int attempt = 0; attempt <= retries; attempt++) {
			try {
				RadiusPacket response = sendAndReceive(request);
				return interpret(request, response);
			} catch (SocketTimeoutException e) {
				lastFailure = e;
			}
		}
		throw new IOException(
				"No RADIUS response from " + hostname + ":" + port + " after " + (retries + 1) + " attempt(s)",
				lastFailure);
	}

	private RadiusPacket sendAndReceive(RadiusPacket request) throws IOException {
		byte[] requestBytes = request.encode();
		try (DatagramSocket socket = new DatagramSocket()) {
			socket.setSoTimeout(timeoutMillis);
			InetAddress address = InetAddress.getByName(hostname);
			DatagramPacket outgoing = new DatagramPacket(requestBytes, requestBytes.length, address, port);
			socket.send(outgoing);

			byte[] receiveBuffer = new byte[RadiusPacket.MAX_PACKET_LENGTH];
			DatagramPacket incoming = new DatagramPacket(receiveBuffer, receiveBuffer.length);
			socket.receive(incoming);

			RadiusPacket response = RadiusPacket.decode(incoming.getData(), incoming.getLength());
			if (response.getIdentifier() != request.getIdentifier()) {
				// Not the response we were waiting for (a stray or stale UDP datagram);
				// treat it the same as not having received anything usable yet.
				throw new SocketTimeoutException(
						"Received a RADIUS response with mismatched identifier " + response.getIdentifier()
								+ " (expected " + request.getIdentifier() + ")");
			}
			if (!isAuthenticSourceAndResponse(request, response)) {
				throw new IOException(
						"RADIUS response authenticator did not match - discarding a response that does not prove "
								+ "knowledge of the configured shared secret (possible spoofed/corrupted packet)");
			}
			return response;
		}
	}

	private RadiusResult interpret(RadiusPacket request, RadiusPacket response) throws IOException {
		RadiusAttribute replyMessageAttribute = response.getAttribute(RadiusAttribute.TYPE_REPLY_MESSAGE);
		String replyMessage = replyMessageAttribute == null ? null : replyMessageAttribute.getValueAsString();

		switch (response.getCode()) {
		case RadiusPacket.CODE_ACCESS_ACCEPT:
			return new RadiusResult(RadiusResult.Status.ACCEPT, replyMessage, null);
		case RadiusPacket.CODE_ACCESS_REJECT:
			return new RadiusResult(RadiusResult.Status.REJECT, replyMessage, null);
		case RadiusPacket.CODE_ACCESS_CHALLENGE:
			RadiusAttribute stateAttribute = response.getAttribute(RadiusAttribute.TYPE_STATE);
			if (stateAttribute == null) {
				throw new IOException("RADIUS server sent Access-Challenge without a State attribute to echo back");
			}
			return new RadiusResult(RadiusResult.Status.CHALLENGE, replyMessage, stateAttribute.getValue());
		default:
			throw new IOException("Unexpected RADIUS response code " + response.getCode());
		}
	}

	/**
	 * Verifies the response authenticator (RFC 2865 section 3): proof that the
	 * reply was produced by a party that knows the shared secret, computed over
	 * the response's own code/identifier/length/attributes but with the
	 * <em>request's</em> authenticator substituted in, followed by the secret.
	 */
	private boolean isAuthenticSourceAndResponse(RadiusPacket request, RadiusPacket response) {
		byte[] expected = computeAuthenticatorDigest(response.encodeHeaderAndAttributesOnly(),
				request.getAuthenticator(), sharedSecret);
		return MessageDigest.isEqual(expected, response.getAuthenticator());
	}

	/**
	 * MD5(codeIdLength[0..3] + authenticatorToEmbed[16] + attributes + secret),
	 * shared by response-authenticator verification (RFC 2865 section 3). Package
	 * visible so unit tests can check it directly against hand-computed vectors.
	 */
	static byte[] computeAuthenticatorDigest(byte[] codeIdLengthAndAttributes, byte[] authenticatorToEmbed,
			byte[] secret) {
		MessageDigest md5 = newMd5();
		md5.update(codeIdLengthAndAttributes, 0, 4);
		md5.update(authenticatorToEmbed);
		md5.update(codeIdLengthAndAttributes, 4, codeIdLengthAndAttributes.length - 4);
		md5.update(secret);
		return md5.digest();
	}

	/**
	 * Encrypts a password per RFC 2865 section 5.2: the password is null-padded
	 * to a 16-byte boundary, split into 16-byte blocks, and each block is XORed
	 * with MD5(secret + previous-ciphertext-block), chaining from
	 * MD5(secret + RequestAuthenticator) for the first block. Package visible so
	 * unit tests can check it directly against hand-computed vectors.
	 */
	static byte[] encryptUserPassword(String password, byte[] requestAuthenticator, byte[] secret) {
		byte[] passwordBytes = password == null ? new byte[0] : password.getBytes(StandardCharsets.UTF_8);
		int blockCount = Math.max(1, (passwordBytes.length + 15) / 16);
		byte[] padded = Arrays.copyOf(passwordBytes, blockCount * 16);

		byte[] ciphertext = new byte[padded.length];
		byte[] previousBlock = requestAuthenticator;
		for (int block = 0; block < blockCount; block++) {
			MessageDigest md5 = newMd5();
			md5.update(secret);
			md5.update(previousBlock);
			byte[] b = md5.digest();

			int offset = block * 16;
			for (int i = 0; i < 16; i++) {
				ciphertext[offset + i] = (byte) (padded[offset + i] ^ b[i]);
			}
			previousBlock = Arrays.copyOfRange(ciphertext, offset, offset + 16);
		}
		return ciphertext;
	}

	private byte[] encryptUserPassword(String password, byte[] requestAuthenticator) {
		return encryptUserPassword(password, requestAuthenticator, sharedSecret);
	}

	private static MessageDigest newMd5() {
		try {
			return MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// Every JDK provider set includes MD5; this cannot happen in practice.
			throw new IllegalStateException("MD5 is required by RFC 2865 and is expected to always be available", e);
		}
	}
}
