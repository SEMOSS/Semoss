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
package prerna.reactor.collaboration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;

import prerna.io.connector.mail.AttachmentText;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkAttachmentText(filePath=["/plan.docx"]);
// A file the owner attached in the composer reaches the model as this text copy, since most providers reject
// Office and mail files; source attachments get theirs from WorkDownloadAttachment.
public class WorkAttachmentTextReactor extends AbstractCollaborationReactor {

	private static final Logger classLogger = LogManager.getLogger(WorkAttachmentTextReactor.class);

	private static final String FILE_PATH = "filePath";

	public WorkAttachmentTextReactor() {
		this.keysToGet = new String[] { FILE_PATH };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		getUser();
		String path = getString(FILE_PATH);
		if (path == null) {
			throw new IllegalArgumentException("Must pass the filePath of a file in this chat");
		}
		Path root;
		Path file;
		try {
			root = Path.of(this.insight.getInsightFolder()).toRealPath();
			file = MCPUtility.resolveContainedMcpFile(root.toString(), path.replaceFirst("^/+", ""));
		} catch (IOException | IllegalArgumentException e) {
			throw new IllegalArgumentException("No file at " + path + " in this chat.", e);
		}
		String name = file.getFileName().toString();
		if (!AttachmentText.supports(name)) {
			throw new IllegalArgumentException(name + " is not a Word, Excel, PowerPoint, .msg or .eml file.");
		}
		Path copy = file.resolveSibling(name + ".txt");
		try {
			// a text document reaches a model without its file name
			AttachmentText.Extracted text = AttachmentText.extract(file, name, "Attachment: " + name + "\n\n",
					AttachmentText.MAX_CHARS);
			Files.writeString(copy, text.text(), StandardCharsets.UTF_8);
		} catch (Exception e) {
			// POI flags a password on a .docx, .xlsx or .pptx only by this message; older files get the exception
			if (e instanceof EncryptedDocumentException || "Invalid password specified".equals(e.getMessage())) {
				throw new IllegalArgumentException(name + " is password protected, so its text could not be read.", e);
			}
			classLogger.warn("Could not read the text of an attached file", e);
			throw new IllegalArgumentException("The text of " + name + " could not be read.", e);
		}
		return mapResult(Map.of("textPath", "/" + root.relativize(copy).toString().replace('\\', '/')));
	}

	@Override
	public String getReactorDescription() {
		return "Writes <file>.txt beside a Word, Excel, PowerPoint, .msg or .eml file in this chat's folder, holding its "
				+ "text, and returns its path as textPath. Fails, naming the file, when the text cannot be read.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (FILE_PATH.equals(key)) {
			return "Path of the file in this chat's folder, as the upload returned it, for example /plan.docx";
		}
		return super.getDescriptionForKey(key);
	}
}
