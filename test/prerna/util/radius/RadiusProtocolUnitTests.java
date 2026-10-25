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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * No live RADIUS server is available in this environment (see the IL4 README's
 * LinOTP RADIUS writeup), so correctness here rests on two complementary
 * techniques instead of comparing against a real server: (1) algorithms are
 * checked against an independent, from-scratch re-implementation of the RFC
 * 2865 math written directly in these tests, rather than re-using
 * {@link RadiusClient}'s own code to check itself; and (2) a minimal, hermetic
 * UDP "RADIUS server" bound to loopback drives {@link RadiusClient} through a
 * full two-step (pin, then OTP) exchange, exercising the real socket I/O,
 * retry, and response-authenticator-verification paths end to end.
 */
class RadiusProtocolUnitTests {

	private static final String SECRET = "test-shared-secret";

	// ---------------------------------------------------------------------
	// RadiusAttribute
	// ---------------------------------------------------------------------

	@Test
	void attribute_encodeDecodeRoundTrip() {
		RadiusAttribute original = RadiusAttribute.ofString(RadiusAttribute.TYPE_USER_NAME, "alice");
		byte[] buffer = new byte[original.getWireLength()];
		original.writeTo(buffer, 0);

		RadiusAttribute parsed = RadiusAttribute.parse(buffer, 0);
		assertEquals(original.getType(), parsed.getType());
		assertEquals("alice", parsed.getValueAsString());
		assertEquals(7, parsed.getWireLength()); // 2 header bytes + 5-byte "alice"
	}

	@Test
	void attribute_rejectsValueOverWireLimit() {
		byte[] tooLong = new byte[RadiusAttribute.MAX_VALUE_LENGTH + 1];
		assertThrows(IllegalArgumentException.class, () -> new RadiusAttribute(RadiusAttribute.TYPE_USER_PASSWORD, tooLong));
	}

	@Test
	void attribute_parseRejectsTruncatedBuffer() {
		byte[] buffer = { (byte) RadiusAttribute.TYPE_STATE, 10 }; // declares 10 bytes, provides 0 more
		assertThrows(IllegalArgumentException.class, () -> RadiusAttribute.parse(buffer, 0));
	}

	// ---------------------------------------------------------------------
	// RadiusPacket
	// ---------------------------------------------------------------------

	@Test
	void packet_encodeDecodeRoundTrip() {
		byte[] authenticator = randomBytes(16);
		List<RadiusAttribute> attributes = new ArrayList<>();
		attributes.add(RadiusAttribute.ofString(RadiusAttribute.TYPE_USER_NAME, "bob"));
		attributes.add(new RadiusAttribute(RadiusAttribute.TYPE_STATE, new byte[] { 1, 2, 3, 4 }));

		RadiusPacket original = new RadiusPacket(RadiusPacket.CODE_ACCESS_REQUEST, 42, authenticator, attributes);
		byte[] wire = original.encode();

		RadiusPacket decoded = RadiusPacket.decode(wire, wire.length);
		assertEquals(RadiusPacket.CODE_ACCESS_REQUEST, decoded.getCode());
		assertEquals(42, decoded.getIdentifier());
		assertArrayEquals(authenticator, decoded.getAuthenticator());
		assertEquals("bob", decoded.getAttribute(RadiusAttribute.TYPE_USER_NAME).getValueAsString());
		assertArrayEquals(new byte[] { 1, 2, 3, 4 }, decoded.getAttribute(RadiusAttribute.TYPE_STATE).getValue());
	}

	@Test
	void packet_getAttributeReturnsNullWhenAbsent() {
		RadiusPacket packet = new RadiusPacket(RadiusPacket.CODE_ACCESS_ACCEPT, 1, randomBytes(16), List.of());
		assertNull(packet.getAttribute(RadiusAttribute.TYPE_REPLY_MESSAGE));
	}

	@Test
	void packet_decodeRejectsLengthLongerThanBytesReceived() {
		byte[] authenticator = randomBytes(16);
		RadiusPacket packet = new RadiusPacket(RadiusPacket.CODE_ACCESS_REQUEST, 1, authenticator, List.of());
		byte[] wire = packet.encode();
		// Claim the packet is a few bytes longer than what was actually received.
		wire[3] = (byte) (wire[3] + 5);
		assertThrows(IllegalArgumentException.class, () -> RadiusPacket.decode(wire, wire.length));
	}

	// ---------------------------------------------------------------------
	// RFC 2865 section 5.2 User-Password encryption, checked against an
	// independently written reference implementation (not RadiusClient's own
	// code checking itself).
	// ---------------------------------------------------------------------

	@Test
	void userPasswordEncryption_decryptsBackToOriginalViaIndependentImplementation() throws Exception {
		byte[] requestAuthenticator = randomBytes(16);
		String password = "correct horse battery staple"; // 29 chars -> 2 RFC blocks

		byte[] ciphertext = RadiusClient.encryptUserPassword(password, requestAuthenticator,
				SECRET.getBytes(StandardCharsets.UTF_8));

		String recovered = referenceDecryptUserPassword(ciphertext, requestAuthenticator, SECRET);
		assertEquals(password, recovered);
	}

	@Test
	void userPasswordEncryption_isPaddedToSixteenByteBlocks() {
		byte[] requestAuthenticator = randomBytes(16);
		byte[] ciphertext = RadiusClient.encryptUserPassword("short", requestAuthenticator,
				SECRET.getBytes(StandardCharsets.UTF_8));
		assertEquals(16, ciphertext.length);

		byte[] ciphertextLong = RadiusClient.encryptUserPassword("this password is definitely over sixteen characters",
				requestAuthenticator, SECRET.getBytes(StandardCharsets.UTF_8));
		assertEquals(0, ciphertextLong.length % 16, "ciphertext must be a whole number of 16-byte blocks");
		assertTrue(ciphertextLong.length >= 48);
	}

	@Test
	void userPasswordEncryption_emptyPasswordProducesOneBlock() throws Exception {
		byte[] requestAuthenticator = randomBytes(16);
		byte[] ciphertext = RadiusClient.encryptUserPassword("", requestAuthenticator,
				SECRET.getBytes(StandardCharsets.UTF_8));
		assertEquals(16, ciphertext.length);
		assertEquals("", referenceDecryptUserPassword(ciphertext, requestAuthenticator, SECRET));
	}

	@Test
	void userPasswordEncryption_differentSecretsProduceDifferentCiphertext() {
		byte[] requestAuthenticator = randomBytes(16);
		byte[] a = RadiusClient.encryptUserPassword("hunter2", requestAuthenticator, "secretA".getBytes(StandardCharsets.UTF_8));
		byte[] b = RadiusClient.encryptUserPassword("hunter2", requestAuthenticator, "secretB".getBytes(StandardCharsets.UTF_8));
		assertFalse(Arrays.equals(a, b));
	}

	/**
	 * An independent, from-scratch implementation of RFC 2865 section 5.2
	 * decryption (the inverse of {@link RadiusClient#encryptUserPassword}),
	 * written directly against the RFC text rather than by inverting the
	 * production code, so that agreement between the two is real evidence of
	 * correctness rather than a test that would pass even if both sides shared
	 * the same bug.
	 */
	private static String referenceDecryptUserPassword(byte[] ciphertext, byte[] requestAuthenticator, String secret)
			throws NoSuchAlgorithmException {
		byte[] secretBytes = secret.getBytes(StandardCharsets.UTF_8);
		byte[] plaintext = new byte[ciphertext.length];
		byte[] previousBlock = requestAuthenticator;
		for (int block = 0; block * 16 < ciphertext.length; block++) {
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			md5.update(secretBytes);
			md5.update(previousBlock);
			byte[] b = md5.digest();

			int offset = block * 16;
			for (int i = 0; i < 16; i++) {
				plaintext[offset + i] = (byte) (ciphertext[offset + i] ^ b[i]);
			}
			previousBlock = Arrays.copyOfRange(ciphertext, offset, offset + 16);
		}
		// Strip the RFC 2865 null padding.
		int end = plaintext.length;
		while (end > 0 && plaintext[end - 1] == 0) {
			end--;
		}
		return new String(plaintext, 0, end, StandardCharsets.UTF_8);
	}

	// ---------------------------------------------------------------------
	// Response authenticator (RFC 2865 section 3)
	// ---------------------------------------------------------------------

	@Test
	void responseAuthenticator_matchesIndependentReferenceComputation() throws Exception {
		byte[] requestAuthenticator = randomBytes(16);
		RadiusPacket response = new RadiusPacket(RadiusPacket.CODE_ACCESS_ACCEPT, 7, new byte[16],
				List.of(RadiusAttribute.ofString(RadiusAttribute.TYPE_REPLY_MESSAGE, "ok")));

		byte[] actual = RadiusClient.computeAuthenticatorDigest(response.encodeHeaderAndAttributesOnly(),
				requestAuthenticator, SECRET.getBytes(StandardCharsets.UTF_8));

		MessageDigest md5 = MessageDigest.getInstance("MD5");
		md5.update(response.encodeHeaderAndAttributesOnly(), 0, 4);
		md5.update(requestAuthenticator);
		byte[] headerAndAttrs = response.encodeHeaderAndAttributesOnly();
		md5.update(headerAndAttrs, 4, headerAndAttrs.length - 4);
		md5.update(SECRET.getBytes(StandardCharsets.UTF_8));
		byte[] expected = md5.digest();

		assertArrayEquals(expected, actual);
	}

	// ---------------------------------------------------------------------
	// End-to-end against a hermetic, loopback test RADIUS server.
	// ---------------------------------------------------------------------

	@Test
	@Timeout(value = 10, unit = TimeUnit.SECONDS)
	void endToEnd_challengeThenAccept() throws Exception {
		try (FakeRadiusServer server = FakeRadiusServer.start(SECRET, FakeRadiusServer.Script.CHALLENGE_THEN_ACCEPT)) {
			RadiusClient client = new RadiusClient("127.0.0.1", server.getPort(), SECRET, "SEMOSS-TEST", 2000, 1);

			RadiusClient.RadiusResult first = client.authenticate("alice", "1234");
			assertEquals(RadiusClient.RadiusResult.Status.CHALLENGE, first.getStatus());
			assertEquals("Enter your OTP", first.getReplyMessage());

			RadiusClient.RadiusResult second = client.authenticateChallengeResponse("alice", "999111", first.getState());
			assertEquals(RadiusClient.RadiusResult.Status.ACCEPT, second.getStatus());
		}
	}

	@Test
	@Timeout(value = 10, unit = TimeUnit.SECONDS)
	void endToEnd_reject() throws Exception {
		try (FakeRadiusServer server = FakeRadiusServer.start(SECRET, FakeRadiusServer.Script.REJECT)) {
			RadiusClient client = new RadiusClient("127.0.0.1", server.getPort(), SECRET, "SEMOSS-TEST", 2000, 1);
			RadiusClient.RadiusResult result = client.authenticate("mallory", "wrong-pin");
			assertEquals(RadiusClient.RadiusResult.Status.REJECT, result.getStatus());
		}
	}

	@Test
	@Timeout(value = 10, unit = TimeUnit.SECONDS)
	void endToEnd_forgedResponseAuthenticatorIsRejected() throws Exception {
		try (FakeRadiusServer server = FakeRadiusServer.start("wrong-secret-the-server-thinks-it-shares",
				FakeRadiusServer.Script.CHALLENGE_THEN_ACCEPT)) {
			// Client is configured with a DIFFERENT secret than the server actually
			// signs its responses with, simulating a misconfigured or spoofed server.
			RadiusClient client = new RadiusClient("127.0.0.1", server.getPort(), SECRET, "SEMOSS-TEST", 1500, 0);
			IOException thrown = assertThrows(IOException.class, () -> client.authenticate("alice", "1234"));
			assertTrue(thrown.getMessage().contains("authenticator"),
					"expected a message about the authenticator mismatch, got: " + thrown.getMessage());
		}
	}

	@Test
	@Timeout(value = 10, unit = TimeUnit.SECONDS)
	void noServerListening_retriesThenThrows() {
		RadiusClient client = new RadiusClient("127.0.0.1", findUnusedPort(), SECRET, "SEMOSS-TEST", 300, 1);
		IOException thrown = assertThrows(IOException.class, () -> client.authenticate("alice", "1234"));
		assertTrue(thrown.getMessage().contains("No RADIUS response"));
	}

	private static byte[] randomBytes(int length) {
		byte[] bytes = new byte[length];
		new Random().nextBytes(bytes);
		return bytes;
	}

	private static int findUnusedPort() {
		try (DatagramSocket socket = new DatagramSocket(0)) {
			return socket.getLocalPort();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * A minimal, scripted RADIUS server used only by these tests: it decrypts the
	 * incoming User-Password attribute with the real RFC 2865 algorithm
	 * (independently implemented here, not delegated to {@link RadiusClient}),
	 * and plays back a fixed scenario (challenge-then-accept, or reject),
	 * signing every response with its own configured secret exactly as a real
	 * RADIUS server would.
	 */
	private static final class FakeRadiusServer implements AutoCloseable {
		enum Script {
			CHALLENGE_THEN_ACCEPT, REJECT
		}

		private final DatagramSocket socket;
		private final Thread thread;
		private final byte[] secret;
		private final byte[] state = { 'S', 'T', 'A', 'T', 'E', '-', '1' };

		private FakeRadiusServer(DatagramSocket socket, String secret, Script script) {
			this.socket = socket;
			this.secret = secret.getBytes(StandardCharsets.UTF_8);
			this.thread = new Thread(() -> run(script), "fake-radius-server");
			this.thread.setDaemon(true);
		}

		static FakeRadiusServer start(String secret, Script script) throws IOException {
			DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress());
			socket.setSoTimeout(5000);
			FakeRadiusServer server = new FakeRadiusServer(socket, secret, script);
			server.thread.start();
			return server;
		}

		int getPort() {
			return socket.getLocalPort();
		}

		private void run(Script script) {
			try {
				boolean sawChallenge = false;
				while (!socket.isClosed()) {
					byte[] buffer = new byte[RadiusPacket.MAX_PACKET_LENGTH];
					DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
					socket.receive(incoming);
					RadiusPacket request = RadiusPacket.decode(incoming.getData(), incoming.getLength());

					RadiusPacket response;
					if (script == Script.REJECT) {
						response = buildResponse(request, RadiusPacket.CODE_ACCESS_REJECT, null, null);
					} else if (!sawChallenge) {
						sawChallenge = true;
						response = buildResponse(request, RadiusPacket.CODE_ACCESS_CHALLENGE, "Enter your OTP", state);
					} else {
						RadiusAttribute echoedState = request.getAttribute(RadiusAttribute.TYPE_STATE);
						if (echoedState == null || !Arrays.equals(echoedState.getValue(), state)) {
							response = buildResponse(request, RadiusPacket.CODE_ACCESS_REJECT, "state mismatch", null);
						} else {
							response = buildResponse(request, RadiusPacket.CODE_ACCESS_ACCEPT, null, null);
						}
					}

					byte[] wire = response.encode();
					socket.send(new DatagramPacket(wire, wire.length, incoming.getAddress(), incoming.getPort()));
				}
			} catch (IOException e) {
				// Socket closed by close(), or the harness tore the test down; nothing to do.
			}
		}

		private RadiusPacket buildResponse(RadiusPacket request, int code, String replyMessage, byte[] stateValue) {
			List<RadiusAttribute> attributes = new ArrayList<>();
			if (replyMessage != null) {
				attributes.add(RadiusAttribute.ofString(RadiusAttribute.TYPE_REPLY_MESSAGE, replyMessage));
			}
			if (stateValue != null) {
				attributes.add(new RadiusAttribute(RadiusAttribute.TYPE_STATE, stateValue));
			}
			// Build the attributes first so we know their encoded bytes, then compute
			// the real RFC 2865 response authenticator over code+id+length+(request's
			// authenticator)+attributes+secret, and use that as this packet's own
			// authenticator - exactly what a real server does, and exactly what
			// RadiusClient independently re-derives and checks on receipt.
			RadiusPacket unsigned = new RadiusPacket(code, request.getIdentifier(), new byte[16], attributes);
			byte[] digest = RadiusClient.computeAuthenticatorDigest(unsigned.encodeHeaderAndAttributesOnly(),
					request.getAuthenticator(), secret);
			return new RadiusPacket(code, request.getIdentifier(), digest, attributes);
		}

		@Override
		public void close() {
			socket.close();
			try {
				thread.join(2000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}
}
