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

import prerna.auth.User;
import prerna.collaboration.BrainThreadClassifier;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainClassifyThreads(); or BrainClassifyThreads(threadIds=["...", "..."], engine=["..."], dryRun=[true]);
public class BrainClassifyThreadsReactor extends AbstractCollaborationReactor {

	private static final String THREAD_IDS = "threadIds";
	private static final String ENGINE = "engine";
	private static final String DRY_RUN = "dryRun";

	public BrainClassifyThreadsReactor() {
		this.keysToGet = new String[] { THREAD_IDS, ENGINE, DRY_RUN };
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		GenRowStruct ids = this.store.getNoun(THREAD_IDS);
		List<String> threadIds = ids == null ? null : ids.getAllStrValues();
		return mapResult(BrainThreadClassifier.classify(user, this.insight, threadIds, getString(ENGINE),
				Boolean.TRUE.equals(getBoolean(DRY_RUN))));
	}

	@Override
	public String getReactorDescription() {
		return "Files threads under topics and creates work items with the Brain classifier (v0, pluggable model)";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (THREAD_IDS.equals(key)) {
			return "Threads to classify; omit for every unmuted thread with no work item yet";
		} else if (ENGINE.equals(key)) {
			return "Model engine id (a Jev/TypeSafe model or any chat model); omit to use Brain settings";
		} else if (DRY_RUN.equals(key)) {
			return "true to return scores for every unmuted thread without writing anything";
		}
		return super.getDescriptionForKey(key);
	}
}
