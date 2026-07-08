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
package prerna.reactor.model.batch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.gson.GsonUtility;

/**
 * Shared helpers for the batch model-call reactors. Each call carries an ENGINE
 * (the routing + security key) and delegates to {@link prerna.engine.impl.model.batch.ModelBatchManager}.
 */
public abstract class AbstractModelBatchReactor extends AbstractReactor {

	protected static final Gson GSON = GsonUtility.getDefaultGson();

	protected User getUser() {
		return this.insight.getUser();
	}

	/**
	 * @return a fresh, mutable copy of the optional caller-supplied param map
	 */
	@SuppressWarnings("unchecked")
	protected Map<String, Object> baseParams() {
		Map<String, Object> out = new HashMap<>();
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		List<NounMetadata> mapInputs = null;
		if (mapGrs != null && !mapGrs.isEmpty()) {
			mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
		}
		if (mapInputs == null || mapInputs.isEmpty()) {
			mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		}
		if (mapInputs != null && !mapInputs.isEmpty()) {
			Object val = mapInputs.get(0).getValue();
			if (val instanceof Map) {
				out.putAll((Map<String, Object>) val);
			}
		}
		return out;
	}
}
