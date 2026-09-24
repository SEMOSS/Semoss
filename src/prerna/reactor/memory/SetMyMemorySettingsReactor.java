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

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.MemoryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Sets (or clears) the current user's personal vector engine preference for
 * memory dedup/embedding (see {@link MemoryUtils#resolveVectorEngineId}).
 * Deliberately does not validate that the engine exists or is a vector engine -
 * every consumer of the stored value already treats a bad/missing engine as
 * "skip dedup/indexing silently", so a stale or mistaken value here can never
 * turn into a hard error for the user later, only a quieter fallback to the
 * platform default.
 */
public class SetMyMemorySettingsReactor extends AbstractReactor {

	public SetMyMemorySettingsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.EMBEDDING_ENGINE_ID.getKey() };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public String getReactorDescription() {
		return "Sets the current user's personal vector engine to use for memory duplicate detection/embedding. Pass an empty/omitted embeddingEngineId to clear the preference and fall back to the platform default.";
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String vectorEngineId = this.keyValue.get(ReactorKeysEnum.EMBEDDING_ENGINE_ID.getKey());
		MemoryUtils.setUserVectorEngineId(userId, vectorEngineId);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.EMBEDDING_ENGINE_ID.getKey())) {
			return "Vector engine id to use for this user's future memory operations, or blank to clear the preference and use the platform default";
		}
		return super.getDescriptionForKey(key);
	}

}
