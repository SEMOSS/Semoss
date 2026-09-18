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
 * Lists MEMORY_AUDIT rows for a memory (created on every edit/delete). Kept
 * separate from ListMemoriesReactor so audit history never leaks into normal
 * memory listings.
 */
public class ListMemoryAuditReactor extends AbstractReactor {

	public ListMemoryAuditReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.MEMORY_ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public String getReactorDescription() {
		return "Lists the audit history (edits and deletes) for one memory, most recent first.";
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}

		String memoryId = this.keyValue.get(ReactorKeysEnum.MEMORY_ID.getKey());
		List<Map<String, Object>> audit = MemoryUtils.listAuditTrail(memoryId);
		return new NounMetadata(audit, PixelDataType.VECTOR);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.MEMORY_ID.getKey())) {
			return "Id of the memory to fetch audit history for";
		}
		return super.getDescriptionForKey(key);
	}

}
