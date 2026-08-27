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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;

/**
 * Single entry point for mime type detection.
 */
public class MimeTypeUtility {

	private static final Logger classLogger = LogManager.getLogger(MimeTypeUtility.class);

	public static final String DEFAULT_MIME_TYPE = "application/octet-stream";

	private MimeTypeUtility() {

	}

	/**
	 * Detect the mime type of a file
	 * 
	 * @param file
	 * @return the base mime type or null when it cannot be determined
	 */
	public static String detectMimeType(File file) {
		if (file == null) {
			return null;
		}
		return detectMimeType(file.toPath());
	}

	/**
	 * Detect the mime type of a file
	 * 
	 * @param filePath
	 * @return the base mime type or null when it cannot be determined
	 */
	public static String detectMimeType(String filePath) {
		if (filePath == null) {
			return null;
		}
		try {
			return detectMimeType(Paths.get(filePath));
		} catch (InvalidPathException e) {
			classLogger.error(Constants.ERROR_MESSAGE, e);
			return null;
		}
	}

	/**
	 * Detect the mime type of a file
	 * 
	 * @param path
	 * @return the base mime type or null when it cannot be determined
	 */
	public static String detectMimeType(Path path) {
		if (path == null) {
			return null;
		}

		// getting the stream from the path also records the file name on the metadata
		// so the detector can use both the contents and the extension
		Metadata metadata = new Metadata();
		try (TikaInputStream stream = TikaInputStream.get(path, metadata)) {
			return detect(stream, metadata);
		} catch (IOException e) {
			classLogger.error(Constants.ERROR_MESSAGE, e);
		}

		return null;
	}

	/**
	 * Detect the mime type of a stream. The stream is consumed and closed by this
	 * method
	 *
	 * @param inputStream
	 * @param fileName    optional, used as an additional detection hint
	 * @return the base mime type or null when it cannot be determined
	 */
	public static String detectMimeType(InputStream inputStream, String fileName) {
		if (inputStream == null) {
			return null;
		}

		Metadata metadata = new Metadata();
		if (fileName != null) {
			metadata.add(TikaCoreProperties.RESOURCE_NAME_KEY, fileName);
		}
		try (TikaInputStream stream = TikaInputStream.get(inputStream)) {
			return detect(stream, metadata);
		} catch (IOException e) {
			classLogger.error(Constants.ERROR_MESSAGE, e);
		}

		return null;
	}

	/**
	 *
	 * @param stream
	 * @param metadata
	 * @return the base mime type or null when it cannot be determined
	 * @throws IOException
	 */
	private static String detect(TikaInputStream stream, Metadata metadata) throws IOException {
		TikaConfig config = TikaConfig.getDefaultConfig();
		Detector detector = config.getDetector();
		MediaType mediaType = detector.detect(stream, metadata);
		if (mediaType == null) {
			return null;
		}
		return mediaType.getBaseType().toString();
	}

	/**
	 * Detect the mime type of a file and fall back to the file format when the
	 * contents of the file cannot be read
	 * 
	 * @param filePath
	 * @param format   the file extension, i.e. png, jpeg
	 * @return the base mime type, never null
	 */
	public static String guessMimeType(String filePath, String format) {
		String mimeType = detectMimeType(filePath);
		if (mimeType != null) {
			return mimeType;
		}
		return getMimeTypeForFormat(format);
	}

	/**
	 * Map a file extension to its mime type
	 * 
	 * @param format the file extension, i.e. png, jpeg
	 * @return the base mime type, never null
	 */
	public static String getMimeTypeForFormat(String format) {
		if (format == null) {
			return DEFAULT_MIME_TYPE;
		}
		switch (format.toLowerCase()) {
		case "jpg":
		case "jpeg":
			return "image/jpeg";
		case "png":
			return "image/png";
		case "gif":
			return "image/gif";
		default:
			return DEFAULT_MIME_TYPE;
		}
	}

}
