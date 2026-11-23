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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import prerna.auth.User;
import prerna.collaboration.WorkTwinPersona;
import prerna.io.connector.mail.MailRecipientRules;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Retrieves the signed-in owner's enabled persona before writing email or a Teams message as them.
 */
public class BrainGetWritingStyleReactor extends AbstractCollaborationReactor {

	private static final String RECIPIENTS = "recipients";

	public BrainGetWritingStyleReactor() {
		this.keysToGet = new String[] { RECIPIENTS };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		MailRecipientRules.validate(getNounAsStringList(RECIPIENTS));
		// Older saved routines read these fields. Only the persona supplies writing guidance.
		Map<String, Object> style = new LinkedHashMap<>(Map.of("confirmed", false, "audience", "unknown",
				"summary", "", "examples", List.of()));
		String persona = WorkTwinPersona.forAssistant(user);
		if (persona != null) {
			style.put("persona", persona);
		}
		return mapResult(style);
	}

	@Override
	public JSONObject getMcpProperties() {
		JSONObject properties = super.getMcpProperties();
		properties.getJSONObject(RECIPIENTS).put("type", "array").put("items", new JSONObject().put("type", "string"));
		return properties;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Read the owner's enabled persona before writing or rewriting email content (a new email, reply, "
						+ "forwarding note, subject line, or suggested email wording) or a Teams message as them. "
						+ "Call with {} and read its result before generating the text, even for a short draft. "
						+ "Do not call for other writing, ordinary answers, or only reading, searching, summarizing, "
						+ "saving, sending, or attaching files. Fetch again for each writing request.")
				.key(RECIPIENTS, "Optional list of resolved bare email addresses, accepted for existing callers.")
				.output(Map.class, "The persona field contains the owner's writing guidance when enabled. "
						+ "When absent, use the owner's current request and ordinary correspondence conventions.");
	}
}
