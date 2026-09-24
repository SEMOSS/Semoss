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
package prerna.reactor.memory;

import java.util.Locale;
import java.util.Set;

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Query-required memory recall for agents. Reuses {@link ListMemoriesReactor}
 * so search and browse always apply identical visibility and scope rules.
 */
public class SearchMemoriesReactor extends ListMemoriesReactor {

	private static final Set<String> STOP_WORDS = Set.of("a", "an", "and", "are", "as", "at", "be", "by", "can",
			"could", "did", "do", "does", "for", "from", "has", "have", "he", "her", "his", "i", "in", "is", "it",
			"its", "me", "memory", "memories", "my", "of", "on", "or", "our", "please", "remember", "she", "should",
			"that", "the", "their", "them", "they", "this", "to", "was", "we", "were", "what", "when", "where",
			"which", "who", "why", "with", "would", "you", "your");

	public SearchMemoriesReactor() {
		super();
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String search = this.keyValue.get(ReactorKeysEnum.SEARCH.getKey());
		if (!hasMeaningfulSearchTerm(search)) {
			throw new IllegalArgumentException("Search must include at least one meaningful non-stopword term.");
		}
		return super.execute();
	}

	static boolean hasMeaningfulSearchTerm(String search) {
		if (search == null || search.isBlank()) {
			return false;
		}
		String[] tokens = search.toLowerCase(Locale.ROOT).split("[^\\p{L}\\p{N}_-]+");
		for (String token : tokens) {
			if (!token.isBlank() && !STOP_WORDS.contains(token)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public String getReactorDescription() {
		return """
				Searches memories relevant to a required natural-language question or \
				topic using the configured vector engine's hybrid semantic and BM25 \
				ranking. Before answering any question that may depend on information \
				the user shared previously - including preferences, plans, decisions, \
				names, dates, or requests to remember - call this tool rather than \
				assuming no memory exists. Use ListMemories only to browse or manage \
				memories by recency and filters. Omit roomId to search every conversation \
				with the current room's attached agent. Every result still passes the \
				same user, agent, workspace, project, event type, metadata, deletion, and \
				superseded-memory checks as ListMemories.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.SEARCH.getKey())) {
			return "Required natural-language question or topic used for hybrid semantic and BM25 memory recall";
		}
		return super.getDescriptionForKey(key);
	}
}
