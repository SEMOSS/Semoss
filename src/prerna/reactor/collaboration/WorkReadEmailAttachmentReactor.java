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
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;

import prerna.collaboration.EmailAttachmentFiles;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Editor-only file transport. Not registered as an agent tool. */
public class WorkReadEmailAttachmentReactor extends AbstractCollaborationReactor {

	public WorkReadEmailAttachmentReactor() {
		this.keysToGet = new String[] { "path", "sha256" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		getUser();
		if (this.insight.getRoomId() == null) {
			throw new IllegalArgumentException("Open the email's room first.");
		}
		String reference = getString("path");
		if (reference == null || !reference.startsWith(EmailAttachmentFiles.DIRECTORY + "/")) {
			throw new IllegalArgumentException("Expected a prepared email attachment.");
		}
		try {
			Path root = Path.of(this.insight.getInsightFolder()).toRealPath();
			Path file = MCPUtility.resolveContainedMcpFile(root.toString(), reference);
			if (!file.startsWith(root.resolve(EmailAttachmentFiles.DIRECTORY))) {
				throw new IllegalArgumentException("Expected a prepared email attachment.");
			}
			byte[] bytes = EmailAttachmentFiles.read(file);
			if (!EmailAttachmentFiles.digest(bytes).equals(getString("sha256"))) {
				throw new IllegalArgumentException(
						"The email attachment changed. Ask the assistant to attach it again.");
			}
			return mapResult(Map.of("path", reference, "name", file.getFileName().toString(), "size", bytes.length,
					"contentBase64", Base64.getEncoder().encodeToString(bytes)));
		} catch (IOException e) {
			throw new IllegalArgumentException(
					"The email attachment could not be read. Ask the assistant to attach it again.", e);
		}
	}
}
