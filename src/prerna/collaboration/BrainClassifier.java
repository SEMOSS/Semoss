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
package prerna.collaboration;

import java.util.List;
import java.util.Map;

import prerna.engine.api.IModelEngine;
import prerna.engine.api.ITypeSafeEngine;
import prerna.om.Insight;

// A pluggable thread classifier: any model that can score a thread against the owner's topics. The
// policy that turns scores into topic links and work items lives in BrainThreadClassifier, so a new model
// only has to fill in Scores.
public interface BrainClassifier {

	/** What a classifier sees: the gated thread, never more. */
	record ThreadInput(String threadId, String ownerName, String subject, List<String> participants,
			List<Message> messages) {
	}

	/** One message, newest last; "me" marks the owner in from, to, and cc. */
	record Message(String from, List<String> to, List<String> cc, String at, String text) {
	}

	/** A topic the thread can be filed under. */
	record TopicOption(String id, String name, String description) {
	}

	/**
	 * Scores in [0, 1] (urgency in [0, 3]); topics maps topic id to probability.
	 * raw keeps the model's own answer for audit and later re-tuning.
	 */
	record Scores(Map<String, Double> topics, double fyi, double automated, double urgency, Map<String, Object> raw) {
	}

	/**
	 * Where scores turn into work: fyi at or above fyiAt is FYI, below asksAt asks
	 * the owner, in between the owner confirms; automated at or above automatedAt
	 * skips the thread. Models score on different scales, so RDF_Map can override
	 * these per engine (COLLAB_CLASSIFIER_CUTOFFS {engineId: {...}}).
	 */
	record Cutoffs(double fyiAt, double asksAt, double automatedAt) {
	}

	// defaults tuned on brain-mail-v1 with a Jev (TypeSafe) model
	default Cutoffs cutoffs() {
		return new Cutoffs(0.6, 0.4, 0.8);
	}

	/** Stored as CLASSIFIER_VERSION, for example "jev-v1:<engineId>". */
	String version();

	Scores score(ThreadInput thread, List<TopicOption> topics, Insight insight);

	// picks the classifier for an engine by its model type: TypeSafe (Jev) or any
	// chat model
	static BrainClassifier forEngine(String engineId, IModelEngine model) {
		if (model instanceof ITypeSafeEngine jev) {
			return new JevBrainClassifier(engineId, jev);
		}
		return new ChatBrainClassifier(engineId, model);
	}
}
