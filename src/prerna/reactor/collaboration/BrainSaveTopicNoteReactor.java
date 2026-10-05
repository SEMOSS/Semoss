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

import prerna.auth.User;
import prerna.collaboration.BrainTopicUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainSaveTopicNote(topicId=["..."], noteId=["..."], kind=["goal"], text=["..."], state=["open"]);
public class BrainSaveTopicNoteReactor extends AbstractCollaborationReactor {

	private static final String TOPIC_ID = "topicId";
	private static final String NOTE_ID = "noteId";
	private static final String KIND = "kind";
	private static final String TEXT = "text";
	private static final String STATE = "state";

	public BrainSaveTopicNoteReactor() {
		this.keysToGet = new String[] { TOPIC_ID, NOTE_ID, KIND, TEXT, STATE };
		this.keyRequired = new int[] { 1, 0, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String topicId = getString(TOPIC_ID);
		if (topicId == null) {
			throw new IllegalArgumentException("Must pass a topicId");
		}
		return mapResult(BrainTopicUtils.saveTopicNote(user, topicId, getString(NOTE_ID), getString(KIND),
				getString(TEXT), getString(STATE)));
	}

	@Override
	public String getReactorDescription() {
		return "Adds or edits a goal or note on a Brain topic";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (TOPIC_ID.equals(key)) {
			return "Topic id";
		} else if (NOTE_ID.equals(key)) {
			return "Note id to edit; omit to create";
		} else if (KIND.equals(key)) {
			return "goal or note";
		} else if (TEXT.equals(key)) {
			return "Goal or note text";
		} else if (STATE.equals(key)) {
			return "open or done for a goal; draft or confirmed for a note";
		}
		return super.getDescriptionForKey(key);
	}
}
