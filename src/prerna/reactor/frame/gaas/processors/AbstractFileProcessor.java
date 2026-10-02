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

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.MimeTypeUtility;

public abstract class AbstractFileProcessor implements IFileProcessor {

	private static final Logger classLogger = LogManager.getLogger(AbstractFileProcessor.class);

	/**
	 * The kinds of files the processors know how to parse
	 */
	public enum FILE_PROCESSOR_TYPE {
		DOC, PPT, PDF, EML, MSG, TEXT
	}

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
	 * Determine what kind of file this is based on its mime type
	 * 
	 * @param file
	 * @return the matching type or null when the file is not supported
	 */
	protected static FILE_PROCESSOR_TYPE getFileProcessorType(File file) {
		String filetype = FilenameUtils.getExtension(file.getAbsolutePath());
		String mimeType = MimeTypeUtility.detectMimeType(file);

		if (mimeType == null) {
			throw new NullPointerException("Unable to determine the mimType for file " + file.getName());
		}

		classLogger.info("Processing file : " + file.getName() + " mime type: " + mimeType);
		if (mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
				|| ((mimeType.equalsIgnoreCase("application/x-tika-ooxml")
						|| mimeType.equalsIgnoreCase("application/msword")
						|| mimeType.equalsIgnoreCase("application/x-tika-msoffice"))
						&& (filetype.equals("doc") || filetype.equals("docx")))) {
			// document
			return FILE_PROCESSOR_TYPE.DOC;
		} else if (mimeType
				.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.presentationml.presentation")
				|| ((mimeType.equalsIgnoreCase("application/x-tika-ooxml")
						|| (mimeType.equalsIgnoreCase("application/vnd.ms-powerpoint")))
						&& (filetype.equals("ppt") || filetype.equals("pptx")))) {
			// powerpoint
			return FILE_PROCESSOR_TYPE.PPT;
		} else if (mimeType.equalsIgnoreCase("application/pdf")) {
			return FILE_PROCESSOR_TYPE.PDF;
		} else if (mimeType.equalsIgnoreCase("message/rfc822") || (filetype.equals("eml"))) {
			// eml email
			return FILE_PROCESSOR_TYPE.EML;
		} else if (mimeType.equalsIgnoreCase("application/vnd.ms-outlook") || (filetype.equals("msg"))) {
			// msg email
			return FILE_PROCESSOR_TYPE.MSG;
		} else if (mimeType.equalsIgnoreCase("text/plain") || mimeType.equalsIgnoreCase("application/rtf")
				|| mimeType.equalsIgnoreCase("text/txt") || mimeType.equalsIgnoreCase("text/rtf")
				|| mimeType.equalsIgnoreCase("text/richtext") || mimeType.equalsIgnoreCase("application/json")
				|| mimeType.equalsIgnoreCase("application/xml")) {
			// basic text
			return FILE_PROCESSOR_TYPE.TEXT;
		}

		classLogger.warn("No support exists for parsing mime-type = " + mimeType);
		return null;
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

		FILE_PROCESSOR_TYPE fileProcessorType = getFileProcessorType(file);
		if (fileProcessorType == null) {
			return null;
		}

		switch (fileProcessorType) {
		case DOC:
			return new DocProcessor(file.getAbsolutePath(), writer);
		case PPT:
			return new PPTProcessor(file.getAbsolutePath(), writer);
		case PDF:
			return new PDFProcessor(file.getAbsolutePath(), writer);
		case EML:
			return new EMLProcessor(file.getAbsolutePath(), writer);
		case MSG:
			return new MSGProcessor(file.getAbsolutePath(), writer);
		case TEXT:
			return new TextFileProcessor(file.getAbsolutePath(), writer);
		default:
			return null;
		}
	}

}
