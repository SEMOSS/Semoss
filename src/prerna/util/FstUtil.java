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

public class FstUtil {

	private static final Logger classLogger = LogManager.getLogger(FstUtil.class);

	public static byte[] serialize(Object input) {
		ByteArrayOutputStream baos = null;
		FSTObjectOutput fo = null;
		try {
			// write it back
			baos = new ByteArrayOutputStream();
			// FST
			fo = new FSTObjectOutput(baos);
			fo.writeObject(input);
			fo.close();
			byte[] retArr = baos.toByteArray();
			return retArr;
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (fo != null) {
				try {
					fo.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if (baos != null) {
				try {
					baos.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return null;
	}

	public static Object deserialize(byte[] data) {
		ByteArrayInputStream bais = null;
		FSTObjectInput fi = null;
		try {
			bais = new ByteArrayInputStream(data);
			fi = new FSTObjectInput(bais);
			Object object = fi.readObject();
			return object;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (fi != null) {
				try {
					fi.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if (bais != null) {
				try {
					bais.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return null;
	}

	public static byte[] packBytes(Object obj) {
		byte[] psBytes = FstUtil.serialize(obj);

		if (psBytes == null) {
			return psBytes;
		}
		// get the length
		int length = psBytes.length;

		// make this into array
		byte[] lenBytes = ByteBuffer.allocate(4).putInt(length).array();

		// pack both of these
		byte[] finalByte = new byte[psBytes.length + lenBytes.length];

		for (int lenIndex = 0; lenIndex < lenBytes.length; lenIndex++) {
			finalByte[lenIndex] = lenBytes[lenIndex];
		}

		for (int lenIndex = 0; lenIndex < psBytes.length; lenIndex++) {
			finalByte[lenIndex + lenBytes.length] = psBytes[lenIndex];
		}

		return finalByte;
	}

}
