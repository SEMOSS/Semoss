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

import java.util.List;
import java.util.Map;

import prerna.collaboration.WorkTwinSuggest;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkSuggestTwinKnowledge(prompt=["The Apollo migration and the Contoso contract"]);
// WorkSuggestTwinKnowledge(prompt=["The Apollo migration"], kinds=["email", "file"], sinceDays=[30]);
// takes no model on purpose: the text model is the one an admin set in RDF_Map
public class WorkSuggestTwinKnowledgeReactor extends AbstractCollaborationReactor {

	private static final String PROMPT = "prompt";
	private static final String KINDS = "kinds";
	private static final String SINCE_DAYS = "sinceDays";

	public WorkSuggestTwinKnowledgeReactor() {
		this.keysToGet = new String[] { PROMPT, KINDS, SINCE_DAYS };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		return mapResult(WorkTwinSuggest.suggest(getUser(), getString(PROMPT), getNounAsStringList(KINDS),
				getInt(SINCE_DAYS)));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Suggests emails, Teams chats and files for the signed-in user's Digital Twin: searches their own "
						+ "Outlook mailbox, Teams chats and OneDrive for what the prompt describes, and returns "
						+ "the ones that fit with the reason for each. Nothing is copied or saved; the caller "
						+ "copies in the ones the user picks")
				.key(PROMPT, "What the Digital Twin should know, in the user's words, up to 1000 characters")
				.key(KINDS, key -> key.type(List.class).description("Optional kinds to look for: email, chat and "
						+ "file. Only the places that hold them are searched: Outlook, Teams chats and OneDrive. "
						+ "All three when left out"))
				.key(SINCE_DAYS, key -> key.type(Integer.class)
						.description("Optional number of days back to look. Any time when left out"))
				.output(Map.class, "suggestions, the best first, each with kind (email, chat or file), id (an "
						+ "email's conversationId, a chatId, or a drive item id with its driveId), title, from, "
						+ "date, preview and reason, and where it is known person, whose picture to show: an "
						+ "email's sender by email, a one on one chat's other person by userId or email; and "
						+ "notes, one for each place that could not be searched or was searched only in part");
	}
}
