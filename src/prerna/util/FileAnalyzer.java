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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.fileupload2.core.DiskFileItem;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileAnalyzer {

	private static final Logger classLogger = LogManager.getLogger(FileAnalyzer.class);

	private static final List<Charset> COMMON_ENCODINGS = Arrays.asList(StandardCharsets.UTF_8,
			StandardCharsets.ISO_8859_1, // same as latin1
			Charset.forName("Windows-1252") // same as cp1252
	);

	private DiskFileItem item;
	private Charset charset = null;

	public FileAnalyzer(DiskFileItem item) {
		this.item = item;
	}

	/**
	 * 
	 * @return
	 * @throws IOException
	 */
	public boolean isTextContent() throws IOException {
		String filetype = FilenameUtils.getExtension(item.getName());
		String mimeType = null;

		try {
			mimeType = MimeTypeUtility.detectMimeType(this.item.getInputStream(), item.getName());
		} catch (IOException e) {
			classLogger.error(Constants.ERROR_MESSAGE, e);
		}

		if (mimeType != null) {
			if (mimeType.equals("application/zip")) {
				// zip
				return false;
			} else if (mimeType.startsWith("image/")) {
				// image
				return false;
			} else if (mimeType
					.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
					|| ((mimeType.equalsIgnoreCase("application/x-tika-ooxml")
							|| mimeType.equalsIgnoreCase("application/msword")
							|| mimeType.equalsIgnoreCase("application/x-tika-msoffice"))
							&& (filetype.equals("doc") || filetype.equals("docx")))) {
				// document
				return false;
			} else if (mimeType
					.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.presentationml.presentation")
					|| ((mimeType.equalsIgnoreCase("application/x-tika-ooxml")
							|| (mimeType.equalsIgnoreCase("application/vnd.ms-powerpoint")))
							&& (filetype.equals("ppt") || filetype.equals("pptx")))) {
				// powerpoint
				return false;
			} else if (mimeType.equalsIgnoreCase("application/vnd.ms-excel.sheet.macroenabled.12")
					|| mimeType.equalsIgnoreCase("application/vnd.ms-excel.sheet.binary.macroenabled.12")
					|| mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
					|| mimeType.equalsIgnoreCase("application/vnd.ms-excel")
					|| (mimeType.equalsIgnoreCase("application/x-tika-ooxml")
							&& (filetype.equals("xls") || filetype.equals("xlsx") || filetype.equals("xlsm")))) {
				// excel
				return false;
			} else if (mimeType.equalsIgnoreCase("application/pdf")) {
				// pdf
				return false;
			}
		}

		for (Charset charset : COMMON_ENCODINGS) {
			try (InputStream is = item.getInputStream();
					InputStreamReader isr = new InputStreamReader(is, charset);
					BufferedReader reader = new BufferedReader(isr)) {
				char[] buffer = new char[4096];
				int charsRead = reader.read(buffer);
				if (charsRead == -1) {
					return false; // Empty file
				}
				String contentSnippet = new String(buffer, 0, charsRead);
				if (isLikelyText(contentSnippet)) {
					this.charset = charset;
					return true;
				}
			} catch (IOException e) {
				// Ignore and try the next encoding
			}
		}
		return false;
	}

	/**
	 * 
	 * @param contentSnippet
	 * @return
	 */
	private boolean isLikelyText(String contentSnippet) {
		// Check for non-text characters and common text patterns
		boolean hasNonTextCharacters = contentSnippet.chars().anyMatch(c -> !(Character.isWhitespace(c)
				|| Character.isISOControl(c) || (c >= 32 && c <= 126) || (c >= 128 && c <= 255)));
		if (hasNonTextCharacters) {
			return false;
		}
		return contentSnippet.contains("\n") || contentSnippet.contains("\r") || contentSnippet.contains(",")
				|| contentSnippet.contains("\t");
	}

	/**
	 * 
	 * @return
	 */
	public Charset getCharset() {
		return charset;
	}
}
