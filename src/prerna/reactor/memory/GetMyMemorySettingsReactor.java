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

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.MemoryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Fetches the current user's personal memory settings - currently just which
 * vector engine to use for dedup/embedding (see
 * {@link MemoryUtils#resolveVectorEngineId}). Returns
 * {@code vectorEngineId: null} when the user has not configured one, meaning
 * the platform default is used; this is never an error state.
 */
public class GetMyMemorySettingsReactor extends AbstractReactor {

	@Override
	public String getReactorDescription() {
		return "Fetches the current user's personal memory settings (currently: which vector engine to use for duplicate detection/embedding). A null vectorEngineId means the platform default is used - not an error.";
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String userId = user.getPrimaryLoginToken().getId();

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("vectorEngineId", MemoryUtils.getUserVectorEngineId(userId));
		result.put("defaultVectorEngineId", MemoryUtils.DEFAULT_VECTOR_ENGINE_ID);
		return new NounMetadata(result, PixelDataType.MAP);
	}

}
