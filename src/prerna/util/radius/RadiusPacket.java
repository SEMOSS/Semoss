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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A RADIUS packet (RFC 2865 section 3): a 20-byte header (Code, Identifier,
 * Length, Authenticator) followed by a sequence of {@link RadiusAttribute}s.
 * This class only models the wire format; {@link RadiusClient} is responsible
 * for the authenticator's cryptographic meaning (request vs. response
 * authenticator, password encryption) since that depends on the shared
 * secret, which a packet does not carry.
 */
public final class RadiusPacket {

	public static final int CODE_ACCESS_REQUEST = 1;
	public static final int CODE_ACCESS_ACCEPT = 2;
	public static final int CODE_ACCESS_REJECT = 3;
	public static final int CODE_ACCESS_CHALLENGE = 11;

	/** Fixed header size: 1 (code) + 1 (identifier) + 2 (length) + 16 (authenticator). */
	public static final int HEADER_LENGTH = 20;
	public static final int AUTHENTICATOR_LENGTH = 16;
	/** RFC 2865 section 3: "The minimum length is 20 and maximum length is 4096." */
	public static final int MAX_PACKET_LENGTH = 4096;

	private final int code;
	private final int identifier;
	private final byte[] authenticator;
	private final List<RadiusAttribute> attributes;

	public RadiusPacket(int code, int identifier, byte[] authenticator, List<RadiusAttribute> attributes) {
		if (code < 0 || code > 255) {
			throw new IllegalArgumentException("RADIUS code must fit in one byte: " + code);
		}
		if (identifier < 0 || identifier > 255) {
			throw new IllegalArgumentException("RADIUS identifier must fit in one byte: " + identifier);
		}
		if (authenticator == null || authenticator.length != AUTHENTICATOR_LENGTH) {
			throw new IllegalArgumentException("RADIUS authenticator must be exactly " + AUTHENTICATOR_LENGTH
					+ " bytes, got " + (authenticator == null ? "null" : authenticator.length));
		}
		this.code = code;
		this.identifier = identifier;
		this.authenticator = authenticator.clone();
		this.attributes = new ArrayList<>(attributes);
	}

	public int getCode() {
		return code;
	}

	public int getIdentifier() {
		return identifier;
	}

	public byte[] getAuthenticator() {
		return authenticator.clone();
	}

	public List<RadiusAttribute> getAttributes() {
		return new ArrayList<>(attributes);
	}

	/**
	 * Returns the first attribute of the given type, or {@code null} if none is
	 * present. RADIUS allows an attribute type to repeat; callers that need every
	 * occurrence should use {@link #getAttributes()} directly.
	 */
	public RadiusAttribute getAttribute(int type) {
		for (RadiusAttribute attribute : attributes) {
			if (attribute.getType() == type) {
				return attribute;
			}
		}
		return null;
	}

	/**
	 * Serializes this packet to its wire format.
	 */
	public byte[] encode() {
		int attributesLength = 0;
		for (RadiusAttribute attribute : attributes) {
			attributesLength += attribute.getWireLength();
		}
		int totalLength = HEADER_LENGTH + attributesLength;
		if (totalLength > MAX_PACKET_LENGTH) {
			throw new IllegalStateException(
					"Encoded RADIUS packet would be " + totalLength + " bytes, exceeding the " + MAX_PACKET_LENGTH
							+ "-byte RFC 2865 maximum");
		}

		byte[] buffer = new byte[totalLength];
		buffer[0] = (byte) code;
		buffer[1] = (byte) identifier;
		buffer[2] = (byte) ((totalLength >> 8) & 0xFF);
		buffer[3] = (byte) (totalLength & 0xFF);
		System.arraycopy(authenticator, 0, buffer, 4, AUTHENTICATOR_LENGTH);

		int offset = HEADER_LENGTH;
		for (RadiusAttribute attribute : attributes) {
			attribute.writeTo(buffer, offset);
			offset += attribute.getWireLength();
		}
		return buffer;
	}

	/**
	 * Parses a packet received over the wire. This only validates structural
	 * well-formedness (declared length matches the bytes actually received, each
	 * attribute's own length is self-consistent); it does not verify the response
	 * authenticator, which requires the shared secret and the original request -
	 * see {@link RadiusClient}.
	 *
	 * @param buffer     the received datagram payload
	 * @param byteCount  the number of valid bytes in {@code buffer} (a
	 *                   {@link java.net.DatagramPacket}'s backing array may be
	 *                   larger than the datagram actually received)
	 */
	public static RadiusPacket decode(byte[] buffer, int byteCount) {
		if (byteCount < HEADER_LENGTH) {
			throw new IllegalArgumentException(
					"RADIUS response is only " + byteCount + " bytes; a packet needs at least " + HEADER_LENGTH);
		}
		int code = buffer[0] & 0xFF;
		int identifier = buffer[1] & 0xFF;
		int declaredLength = ((buffer[2] & 0xFF) << 8) | (buffer[3] & 0xFF);
		if (declaredLength < HEADER_LENGTH || declaredLength > MAX_PACKET_LENGTH) {
			throw new IllegalArgumentException("RADIUS response declares an invalid length " + declaredLength);
		}
		if (declaredLength > byteCount) {
			throw new IllegalArgumentException("RADIUS response declares length " + declaredLength
					+ " but only " + byteCount + " bytes were received");
		}
		byte[] authenticator = new byte[AUTHENTICATOR_LENGTH];
		System.arraycopy(buffer, 4, authenticator, 0, AUTHENTICATOR_LENGTH);

		List<RadiusAttribute> attributes = new ArrayList<>();
		int offset = HEADER_LENGTH;
		while (offset < declaredLength) {
			RadiusAttribute attribute = RadiusAttribute.parse(buffer, offset);
			attributes.add(attribute);
			offset += attribute.getWireLength();
		}
		if (offset != declaredLength) {
			throw new IllegalArgumentException(
					"RADIUS response attributes overran the packet's declared length by " + (offset - declaredLength)
							+ " bytes");
		}
		return new RadiusPacket(code, identifier, authenticator, attributes);
	}

	/**
	 * The bytes a response authenticator is computed/verified over (RFC 2865
	 * section 3): Code + Identifier + Length + RequestAuthenticator + Attributes,
	 * with the response's own authenticator field in the RequestAuthenticator
	 * slot swapped out for the correlated request's authenticator by the caller -
	 * this method just serializes {@code this} packet's own header+attributes
	 * byte-for-byte, which {@link RadiusClient} then feeds alongside the request
	 * authenticator and shared secret into the MD5 computation.
	 */
	byte[] encodeHeaderAndAttributesOnly() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		int attributesLength = 0;
		for (RadiusAttribute attribute : attributes) {
			attributesLength += attribute.getWireLength();
		}
		int totalLength = HEADER_LENGTH + attributesLength;
		out.write(code);
		out.write(identifier);
		out.write((totalLength >> 8) & 0xFF);
		out.write(totalLength & 0xFF);
		for (RadiusAttribute attribute : attributes) {
			byte[] encoded = new byte[attribute.getWireLength()];
			attribute.writeTo(encoded, 0);
			out.write(encoded, 0, encoded.length);
		}
		return out.toByteArray();
	}
}
