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

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.fileupload2.core.FileItem;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.mozilla.universalchardet.UniversalDetector;

public class FileEncoderDetector {

	private static final Logger classLogger = LogManager.getLogger(FileEncoderDetector.class);

	private FileItem item;
	private Charset charset = null;

	public FileEncoderDetector(FileItem item) {
		this.item = item;
	}

	/**
	 * 
	 * @return
	 * @throws IOException
	 */
	public boolean isTextContent() throws IOException {
		// use tika to check if this is a file we should process
		{
			String filetype = FilenameUtils.getExtension(item.getName());
			String mimeType = null;

			TikaConfig config = TikaConfig.getDefaultConfig();
			Detector detector = config.getDetector();
			Metadata metadata = new Metadata();
			metadata.add(TikaCoreProperties.RESOURCE_NAME_KEY, item.getName());

			try (TikaInputStream stream = TikaInputStream.get(this.item.getInputStream())) {
				mimeType = detector.detect(stream, metadata).toString();
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
						|| mimeType
								.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
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
		}

		// use universal detector to determine the type
		byte[] buf = new byte[8192];
		try (java.io.InputStream fis = this.item.getInputStream()) {
			UniversalDetector detector = new UniversalDetector();
			int nread;
			while ((nread = fis.read(buf)) > 0 && !detector.isDone()) {
				detector.handleData(buf, 0, nread);
			}
			detector.dataEnd();

			String encoding = detector.getDetectedCharset();
			if (encoding != null) {
				// we got an encoding!
				this.charset = Charset.forName(encoding);
				return true;
			}
		}

		return false;
	}

	/**
	 * 
	 * @return
	 */
	public Charset getCharset() {
		return charset;
	}
}
