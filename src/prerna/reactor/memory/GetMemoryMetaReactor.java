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
 * Fetches a memory's custom metadata (MEMORY_META rows) as metakey -> ordered
 * list of values, for display alongside a memory (e.g. tags in the Settings
 * UI's memory detail view). Kept separate from ListMemoriesReactor since most
 * callers filtering/browsing memories don't need per-row metadata fetched
 * eagerly.
 */
public class GetMemoryMetaReactor extends AbstractReactor {

	public GetMemoryMetaReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.MEMORY_ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public String getReactorDescription() {
		return "Fetches a memory's custom metadata (metakey -> value(s)) previously attached via AddMemory's metaFilters.";
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		String memoryId = this.keyValue.get(ReactorKeysEnum.MEMORY_ID.getKey());
		Map<String, List<String>> meta = MemoryUtils.getMemoryMeta(memoryId);
		return new NounMetadata(meta, PixelDataType.MAP);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.MEMORY_ID.getKey())) {
			return "Id of the memory to fetch custom metadata for";
		}
		return super.getDescriptionForKey(key);
	}

}
