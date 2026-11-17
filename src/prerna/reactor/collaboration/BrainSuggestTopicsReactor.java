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

import prerna.collaboration.BrainTopicSuggest;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainSuggestTopics(dryRun=[true]) previews the standard onboarding proposals;
// BrainSuggestTopics(fresh=[true]) looks for new topics in recent mail no topic holds.
public class BrainSuggestTopicsReactor extends AbstractCollaborationReactor {

	public BrainSuggestTopicsReactor() {
		this.keysToGet = new String[] { "dryRun", "fresh" };
	}

	@Override
	public NounMetadata execute() {
		boolean dryRun = Boolean.TRUE.equals(getBoolean("dryRun"));
		return mapResult(Boolean.TRUE.equals(getBoolean("fresh")) ? BrainTopicSuggest.fresh(getUser(), dryRun)
				: BrainTopicSuggest.topics(getUser(), dryRun));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Onboarding topic proposals from imported headers using structure clustering and three complete model votes. "
						+ "Sorted mail uses the wider pool; otherwise proposals require engagement. dryRun returns proposals without "
						+ "writing topics. A successful write replaces undecided brain suggestions; owner-accepted topics remain. "
						+ "Accept by saving with status active. Model or header failure returns modelError")
				.key("dryRun", "true to preview without replacing or writing topics; default false")
				.key("fresh", key -> key.type(Boolean.class).description("true to look only at recent conversations no "
						+ "topic holds, for a new project or event: a group the owner takes part in is saved as a suggested "
						+ "topic with a new_topic review, and existing suggestions stay; default false"));
	}
}
