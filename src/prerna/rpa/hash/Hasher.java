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
package prerna.rpa.hash;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.util.Constants;

public class Hasher {

  private static final Logger classLogger = LogManager.getLogger(Hasher.class);

  private final int truncateLength; // Bytes
  private final Charset encoding;
  private final MessageDigest digester;

  private static final String DEFAULT_ENCODING = "UTF-8";
  private static final String DEFAULT_ALGORITHM = "SHA-256";
  private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

  public static Hasher getDefaultHasher(int truncateLength) {
    try {
      return new Hasher(DEFAULT_ALGORITHM, truncateLength, DEFAULT_ENCODING);
    } catch (NoSuchAlgorithmException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new IllegalStateException("The default algorithm should always exist.");
    } catch (UnsupportedEncodingException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new IllegalStateException("The default encoding should always be supported.");
    }
  }

  public Hasher(String algorithm, int truncateLength, String encoding)
      throws NoSuchAlgorithmException, UnsupportedEncodingException {
    if (!Charset.isSupported(encoding)) {
      throw new UnsupportedEncodingException();
    }
    if (truncateLength < 1) {
      throw new IllegalArgumentException("Truncate length must be greater than 1.");
    }
    this.truncateLength = truncateLength;
    this.encoding = Charset.forName(encoding);
    digester = MessageDigest.getInstance(algorithm);
  }

  public synchronized String hash(String text) {
    byte[] hash = digester.digest(text.getBytes(encoding));

    // Truncate if the hash length is greater than the desired truncated
    // length
    if (hash.length > truncateLength) {
      byte[] truncatedHash = new byte[truncateLength];
      for (int i = 0; i < truncateLength; i++) {
        truncatedHash[i] = hash[i];
      }
      return bytesToHexString(truncatedHash);
    } else {
      return bytesToHexString(hash);
    }
  }

  private static String bytesToHexString(byte[] bytes) {

    // See http://stackoverflow.com/q/9655181
    char[] hexChars = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      int v = bytes[i] & 0xFF;
      hexChars[i * 2] = HEX_ARRAY[v >>> 4];
      hexChars[i * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }
}
