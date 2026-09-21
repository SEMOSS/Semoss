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
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty. See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.memory;

import prerna.sablecc2.om.ReactorKeysEnum;

/**
 * Query-required memory recall for agents. Reuses {@link ListMemoriesReactor}
 * so search and browse always apply identical visibility and scope rules.
 */
public class SearchMemoriesReactor extends ListMemoriesReactor {

	public SearchMemoriesReactor() {
		super();
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public String getReactorDescription() {
		return """
				Searches memories relevant to a required natural-language question or \
				topic using the configured vector engine's hybrid semantic and BM25 \
				ranking. Use this for agent recall; use ListMemories to browse or manage \
				memories by recency and filters. The search spans every conversation \
				with the current room's attached agent unless roomId is explicitly \
				supplied. Every result still passes the same user, agent, workspace, \
				project, event type, metadata, deletion, and superseded-memory checks as \
				ListMemories.\
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
