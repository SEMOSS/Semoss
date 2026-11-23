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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.collaboration.BrainAgentEdits;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainEditTopic(topic=["Air Force AIMES"], addPeople=["Rose Amado"], removePeople=["x@y.com"], description=["..."]);
public class BrainEditTopicReactor extends AbstractCollaborationReactor {

	private static final String TOPIC_ID = "topicId";
	private static final String TOPIC = "topic";
	private static final String NAME = "name";
	private static final String DESCRIPTION = "description";
	private static final String STATUS = "status";
	private static final String ADD_PEOPLE = "addPeople";
	private static final String REMOVE_PEOPLE = "removePeople";
	private static final String ADD_NOTE = "addNote";
	private static final String DELETE_NOTE_ID = "deleteNoteId";
	private static final String ADD_EMAILS = "addEmails";

	public BrainEditTopicReactor() {
		this.keysToGet = new String[] { TOPIC_ID, TOPIC, NAME, DESCRIPTION, STATUS, ADD_PEOPLE, REMOVE_PEOPLE, ADD_NOTE,
				DELETE_NOTE_ID, ADD_EMAILS };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		return mapResult(BrainAgentEdits.editTopic(user, getString(TOPIC_ID), getString(TOPIC), getString(NAME),
				getString(DESCRIPTION), getString(STATUS), people(ADD_PEOPLE), people(REMOVE_PEOPLE),
				getString(ADD_NOTE), getString(DELETE_NOTE_ID), getNounAsStringList(ADD_EMAILS)));
	}

	// each entry a string; a model that sends {name, email, personId} objects is
	// read the same way
	private List<String> people(String key) {
		List<String> out = new ArrayList<>();
		GenRowStruct grs = this.store.getGenRowStruct(key);
		for (int i = 0; grs != null && i < grs.size(); i++) {
			Object entry = grs.get(i);
			if (entry instanceof Map) {
				Map<?, ?> map = (Map<?, ?>) entry;
				Object ref = firstOf(map, "personId", "id", "email", "name");
				if (ref != null) {
					out.add(String.valueOf(ref));
				}
			} else if (entry != null) {
				out.add(String.valueOf(entry));
			}
		}
		return out;
	}

	private static Object firstOf(Map<?, ?> map, String... keys) {
		for (String k : keys) {
			Object v = map.get(k);
			if (v != null && !String.valueOf(v).isBlank()) {
				return v;
			}
		}
		return null;
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		// changes the owner's Brain, so it waits for their approval
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		return meta;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Changes one of the owner's Brain topics, as the owner would on the topic page: rename it, edit its "
						+ "description, set its status, add or remove its people, put emails in it, or add or delete a "
						+ "note. "
						+ "Several changes can go in one call. To create a topic, leave out topicId and topic and pass its "
						+ "name with any other changes; a name another topic has is refused. It waits for the owner to "
						+ "approve. Read the topic with ListTopics first so you pass the right names")
				.key(TOPIC_ID, "Topic id from ListTopics; pass this or topic, or neither to create a topic named name")
				.key(TOPIC, "Topic name, when the id is not known; must match one topic")
				.key(NAME, "New topic name, or the name of the topic to create")
				.key(DESCRIPTION, "New description; replaces the old one").key(STATUS, "active, dormant, or archived")
				.key(ADD_PEOPLE, key -> key.type(List.class).description(
						"People to add to the topic: a list of plain strings, each an email, a name, or a person id (not objects); a suggested person becomes a member"))
				.key(REMOVE_PEOPLE, key -> key.type(List.class).description(
						"People to remove from the topic: a list of plain strings, each an email, a name, or a person id (not objects); their threads stay linked"))
				.key(ADD_NOTE, "Text of a note to add; it is kept as the owner's memory about the topic")
				.key(DELETE_NOTE_ID, "Id of a note to delete, from ListTopics")
				.key(ADD_EMAILS, key -> key.type(List.class).description(
						"Emails to put in the topic: a list of plain strings, each a threadId from SearchMail or ReadThread, "
								+ "an emailRef from ReadThread, or an email's id from ListM365Mail. Any email in the owner's "
								+ "mailbox can be added, with its whole conversation"));
	}
}
