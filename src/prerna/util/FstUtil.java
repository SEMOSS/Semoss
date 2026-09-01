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
package prerna.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

public final class FstUtil {

	private static final Logger classLogger = LogManager.getLogger(FstUtil.class);
	private static final int LENGTH_PREFIX_SIZE = Integer.BYTES;

	private FstUtil() {
		throw new IllegalStateException("Utility class");
	}

	public static byte[] serialize(Object input) {
		ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
		try (FSTObjectOutput output = new FSTObjectOutput(outputBytes)) {
			output.writeObject(input);
		} catch (IOException e) {
			classLogger.error("Failed to serialize object with FST", e);
			return null;
		}
		return outputBytes.toByteArray();
	}

	public static Object deserialize(byte[] data) {
		try (FSTObjectInput input = new FSTObjectInput(new ByteArrayInputStream(data))) {
			return input.readObject();
		} catch (Exception e) {
			classLogger.error("Failed to deserialize FST data", e);
			return null;
		}
	}

	public static byte[] packBytes(Object obj) {
		byte[] serialized = serialize(obj);
		if (serialized == null) {
			return null;
		}

		return ByteBuffer.allocate(LENGTH_PREFIX_SIZE + serialized.length).putInt(serialized.length).put(serialized)
				.array();
	}

}
