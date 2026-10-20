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

// BrainClassifyThreads(); BrainClassifyThreads(threadIds=["...", "..."], dryRun=[true]); or BrainClassifyThreads(topics=[true],
// async=[true]) after onboarding picks topics; the model is always
// COLLAB_CLASSIFIER_ENGINE_ID
public class BrainClassifyThreadsReactor extends AbstractCollaborationReactor {

	private static final String THREAD_IDS = "threadIds";
	private static final String DRY_RUN = "dryRun";
	private static final String ASYNC = "async";
	private static final String TOPICS = "topics";

	public BrainClassifyThreadsReactor() {
		this.keysToGet = new String[] { THREAD_IDS, DRY_RUN, ASYNC, TOPICS };
		this.keyRequired = new int[] { 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		GenRowStruct ids = this.store.getNoun(THREAD_IDS);
		List<String> threadIds = ids == null ? null : ids.getAllStrValues();
		boolean dryRun = Boolean.TRUE.equals(getBoolean(DRY_RUN));
		if (Boolean.TRUE.equals(getBoolean(TOPICS))) {
			if (dryRun || !Boolean.TRUE.equals(getBoolean(ASYNC))) {
				throw new IllegalArgumentException("Topic filing runs only as a background job (async=[true])");
			}
			return mapResult(BrainThreadClassifier.startTopics(user));
		}
		if (Boolean.TRUE.equals(getBoolean(ASYNC))) {
			if (dryRun) {
				throw new IllegalArgumentException("A dry run returns its scores and cannot run in the background");
			}
			return mapResult(BrainThreadClassifier.start(user, threadIds));
		}
		return mapResult(BrainThreadClassifier.classify(user, this.insight, threadIds, dryRun));
	}

	@Override
	public String getReactorDescription() {
		return "Files threads under topics and creates work items with the platform classifier model "
				+ "(COLLAB_CLASSIFIER_ENGINE_ID, a Jev/TypeSafe or chat model)";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (THREAD_IDS.equals(key)) {
			return "Threads to classify; omit for every unmuted thread with no work item yet";
		} else if (DRY_RUN.equals(key)) {
			return "true to return scores for every unmuted thread without writing anything";
		} else if (TOPICS.equals(key)) {
			return "true to only file real mail that has no topic yet against the kept topics, after onboarding picks them";
		} else if (ASYNC.equals(key)) {
			return "true to run as a background job and return it; poll with BrainGetJob(kind=classify)";
		}
		return super.getDescriptionForKey(key);
	}
}
