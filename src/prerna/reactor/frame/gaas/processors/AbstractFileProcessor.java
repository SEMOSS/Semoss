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
package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public abstract class AbstractFileProcessor implements IFileProcessor {

	private static final Logger classLogger = LogManager.getLogger(AbstractFileProcessor.class);

	protected String filePath = null;
	protected VectorDatabaseCSVWriter writer = null;

	public AbstractFileProcessor(String filePath, VectorDatabaseCSVWriter writer) {
		this.filePath = filePath;
		this.writer = writer;
	}

	/**
	 * 
	 * @param filePath
	 * @return
	 */
	protected String getSource(String filePath) {
		File file = new File(filePath);
		return file.getName();
	}

	/**
	 * 
	 * @param file
	 * @param writer
	 * @return
	 */
	public static IFileProcessor getFileProcessor(File file, VectorDatabaseCSVWriter writer) {
		// pick up the files and convert them to CSV
		classLogger.info("Processing file : " + file.getName());

		// process this file
		String filetype = FilenameUtils.getExtension(file.getAbsolutePath());
		String mimeType = null;

		// using tika for mime type check since it is more consistent across env + rhel
		// OS and macOS
		TikaConfig config = TikaConfig.getDefaultConfig();
		Detector detector = config.getDetector();
		Metadata metadata = new Metadata();
		metadata.add(TikaCoreProperties.RESOURCE_NAME_KEY, file.getName());
		try (TikaInputStream stream = TikaInputStream.get(new FileInputStream(file))) {
			mimeType = detector.detect(stream, metadata).toString();
		} catch (IOException e) {
			classLogger.error(Constants.ERROR_MESSAGE, e);
		}

		if (mimeType == null) {
			throw new NullPointerException("Unable to determine the mimType for file " + file.getName());
		}

		IFileProcessor processor = null;

		classLogger.info("Processing file : " + file.getName() + " mime type: " + mimeType);
		if (mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
				|| ((mimeType.equalsIgnoreCase("application/x-tika-ooxml")
						|| mimeType.equalsIgnoreCase("application/msword")
						|| mimeType.equalsIgnoreCase("application/x-tika-msoffice"))
						&& (filetype.equals("doc") || filetype.equals("docx")))) {
			// document
			processor = new DocProcessor(file.getAbsolutePath(), writer);
		} else if (mimeType
				.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.presentationml.presentation")
				|| ((mimeType.equalsIgnoreCase("application/x-tika-ooxml")
						|| (mimeType.equalsIgnoreCase("application/vnd.ms-powerpoint")))
						&& (filetype.equals("ppt") || filetype.equals("pptx")))) {
			// powerpoint
			processor = new PPTProcessor(file.getAbsolutePath(), writer);
		} else if (mimeType.equalsIgnoreCase("application/pdf")) {
			processor = new PDFProcessor(file.getAbsolutePath(), writer);
		} else if (mimeType.equalsIgnoreCase("message/rfc822")
				|| (filetype.equals("eml"))) {
			// eml email
			processor = new EMLProcessor(file.getAbsolutePath(), writer);
		} else if (mimeType.equalsIgnoreCase("application/vnd.ms-outlook")
				|| (filetype.equals("msg"))) {
			// msg email
			processor = new MSGProcessor(file.getAbsolutePath(), writer);
		} else if (mimeType.equalsIgnoreCase("text/plain")
				|| mimeType.equalsIgnoreCase("application/rtf")
				|| mimeType.equalsIgnoreCase("text/txt")
				|| mimeType.equalsIgnoreCase("text/rtf")
				|| mimeType.equalsIgnoreCase("text/richtext")
				|| mimeType.equalsIgnoreCase("application/json")
				|| mimeType.equalsIgnoreCase("application/xml")) {
			// basic text
			processor = new TextFileProcessor(file.getAbsolutePath(), writer);
		} else {
			classLogger.warn("No support exists for parsing mime-type = " + mimeType);
			classLogger.warn("No support exists for parsing mime-type = " + mimeType);
			classLogger.warn("No support exists for parsing mime-type = " + mimeType);
			classLogger.warn("No support exists for parsing mime-type = " + mimeType);
			classLogger.warn("No support exists for parsing mime-type = " + mimeType);
			classLogger.warn("No support exists for parsing mime-type = " + mimeType);
			classLogger.warn("No support exists for parsing mime-type = " + mimeType);
		}

		return processor;
	}

}
