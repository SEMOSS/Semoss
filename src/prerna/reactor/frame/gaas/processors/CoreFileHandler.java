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
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.io.FilenameUtils;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;

public class CoreFileHandler extends AbstractFileHandler {

	private static final Logger classLogger = LogManager.getLogger();

	public static final Set<String> SUPPORTED_TYPES = Set.of("csv", "doc", "docx", "json", "pdf", "ppt", "pptx", "txt",
			"xml", "rtf");

	@Override
	public boolean supportsFile(File file) {
		String fileType = FilenameUtils.getExtension(file.getAbsolutePath());
		return SUPPORTED_TYPES.contains(fileType);
	}

	@Override
	public IFileProcessor getFileProcessor(File file, VectorDatabaseCSVWriter writer) {
		String fileType = FilenameUtils.getExtension(file.getAbsolutePath());
		String filePath = file.getAbsolutePath();
		String mimeType = this.getMimeType(file);
		IFileProcessor subprocessor = null;
		// classLogger.info("Processing file : " + file.getName() + " mime type: " +
		// mimeType);
		if (mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
				|| ((mimeType.equalsIgnoreCase("application/x-tika-ooxml")
						|| mimeType.equalsIgnoreCase("application/msword")
						|| mimeType.equalsIgnoreCase("application/x-tika-msoffice"))
						&& (fileType.equals("doc") || fileType.equals("docx")))) {
			// document
			subprocessor = new DocProcessor(filePath, writer);
		} else if (mimeType
				.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.presentationml.presentation")
				|| ((mimeType.equalsIgnoreCase("application/x-tika-ooxml")
						|| (mimeType.equalsIgnoreCase("application/vnd.ms-powerpoint")))
						&& (fileType.equals("ppt") || fileType.equals("pptx")))) {
			// powerpoint
			subprocessor = new PPTProcessor(filePath, writer);
		} else if (mimeType.equalsIgnoreCase("application/pdf")) {
			subprocessor = new PDFProcessor(filePath, writer);
		} else if (mimeType.equalsIgnoreCase("message/rfc822")
				|| (fileType.equals("eml"))) {
			// eml email
			subprocessor = new EMLProcessor(filePath, writer);
		} else if (mimeType.equalsIgnoreCase("application/vnd.ms-outlook")
				|| (fileType.equals("msg"))) {
			// msg email
			subprocessor = new MSGProcessor(filePath, writer);
		} else if (mimeType.equalsIgnoreCase("text/plain")
				|| mimeType.equalsIgnoreCase("application/rtf")
				|| mimeType.equalsIgnoreCase("text/txt")
				|| mimeType.equalsIgnoreCase("text/rtf")
				|| mimeType.equalsIgnoreCase("text/richtext")
				|| mimeType.equalsIgnoreCase("application/json")
				|| mimeType.equalsIgnoreCase("application/xml")) {
			// basic text
			subprocessor = new TextFileProcessor(filePath, writer);
		} else {
			classLogger.warn("No support exists for parsing mime-type = " + mimeType);
		}
		return subprocessor;
	}
}