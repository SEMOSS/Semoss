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

import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.MemoryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Combines two or more memories into a single summary memory (event type
 * "session_summary"). Source memories are kept but marked as superseded by the
 * new summary; only the creator of every source memory may compact them.
 */
public class CompactMemoriesReactor extends AbstractReactor {

	public CompactMemoriesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.MEMORY_IDS.getKey(),
				ReactorKeysEnum.REASONING_ENGINE_ID.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public String getReactorDescription() {
		return """
				Combines two or more memories into a single summary memory (a session \
				summary), using a reasoning model to write the summary when \
				reasoningEngineId is supplied. The source memories are kept, not deleted, \
				but are marked as superseded by the new summary.\
				""";
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String userId = user.getPrimaryLoginToken().getId();

		List<String> memoryIds = getListStringFromKeyOrCurRow(ReactorKeysEnum.MEMORY_IDS.getKey());
		String reasoningEngineId = this.keyValue.get(ReactorKeysEnum.REASONING_ENGINE_ID.getKey());

		Map<String, Object> result = MemoryUtils.compactMemories(userId, memoryIds, reasoningEngineId, this.insight);
		return new NounMetadata(result, PixelDataType.MAP);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.MEMORY_IDS.getKey())) {
			return "Two or more memory ids to combine into a single summary memory";
		}
		if (key.equals(ReactorKeysEnum.REASONING_ENGINE_ID.getKey())) {
			return "Engine id used to generate the summary text (falls back to a simple concatenation if omitted)";
		}
		return super.getDescriptionForKey(key);
	}

}
