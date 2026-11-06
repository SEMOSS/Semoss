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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A single RADIUS attribute (RFC 2865 section 5): a one-byte type, a one-byte
 * length (the wire length, including these two header bytes), and a value of
 * up to 253 bytes.
 */
public final class RadiusAttribute {

	public static final int TYPE_USER_NAME = 1;
	public static final int TYPE_USER_PASSWORD = 2;
	public static final int TYPE_NAS_IP_ADDRESS = 4;
	public static final int TYPE_REPLY_MESSAGE = 18;
	public static final int TYPE_STATE = 24;
	public static final int TYPE_NAS_IDENTIFIER = 32;

	/** Wire length is one byte, so a value cannot exceed 253 bytes (255 - 2 header bytes). */
	public static final int MAX_VALUE_LENGTH = 253;

	private final int type;
	private final byte[] value;

	public RadiusAttribute(int type, byte[] value) {
		if (type < 0 || type > 255) {
			throw new IllegalArgumentException("RADIUS attribute type must fit in one byte: " + type);
		}
		if (value == null) {
			throw new IllegalArgumentException("RADIUS attribute value cannot be null");
		}
		if (value.length > MAX_VALUE_LENGTH) {
			throw new IllegalArgumentException(
					"RADIUS attribute value exceeds the " + MAX_VALUE_LENGTH + "-byte wire limit: " + value.length);
		}
		this.type = type;
		this.value = value.clone();
	}

	public static RadiusAttribute ofString(int type, String value) {
		return new RadiusAttribute(type, value.getBytes(StandardCharsets.UTF_8));
	}

	public int getType() {
		return type;
	}

	public byte[] getValue() {
		return value.clone();
	}

	public String getValueAsString() {
		return new String(value, StandardCharsets.UTF_8);
	}

	/** Wire length of this attribute, including its own 2-byte type/length header. */
	public int getWireLength() {
		return value.length + 2;
	}

	public void writeTo(byte[] buffer, int offset) {
		buffer[offset] = (byte) type;
		buffer[offset + 1] = (byte) getWireLength();
		System.arraycopy(value, 0, buffer, offset + 2, value.length);
	}

	/**
	 * Parses one attribute starting at {@code offset} in {@code buffer}, which
	 * must have at least 2 bytes remaining there for the type/length header.
	 *
	 * @return the parsed attribute
	 */
	public static RadiusAttribute parse(byte[] buffer, int offset) {
		int type = buffer[offset] & 0xFF;
		int wireLength = buffer[offset + 1] & 0xFF;
		if (wireLength < 2) {
			throw new IllegalArgumentException("RADIUS attribute at offset " + offset
					+ " declares an impossible length " + wireLength + " (must be at least 2)");
		}
		if (offset + wireLength > buffer.length) {
			throw new IllegalArgumentException("RADIUS attribute at offset " + offset + " declares length "
					+ wireLength + " but only " + (buffer.length - offset) + " bytes remain");
		}
		byte[] value = Arrays.copyOfRange(buffer, offset + 2, offset + wireLength);
		return new RadiusAttribute(type, value);
	}
}
