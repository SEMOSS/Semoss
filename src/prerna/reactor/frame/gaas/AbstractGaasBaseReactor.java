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
package prerna.reactor.frame.gaas;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;

public abstract class AbstractGaasBaseReactor extends AbstractReactor {

	/**
	 * The project id passed in the noun store, else the insight's context project,
	 * else the insight's own project. Null when there is none - callers here treat
	 * the project as optional.
	 *
	 * @return
	 */
	public String getProjectId() {
		String projectId = null;
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.PROJECT.getKey());
		if (grs != null && !grs.isEmpty()) {
			projectId = grs.get(0).toString();
		}
		return resolveContextEngineIdOrNull(projectId);
	}

	/**
	 * 
	 * @return
	 */
	public Map processParamMap() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (grs != null) {
			List maps = grs.getValuesOfType(PixelDataType.MAP);
			if (maps != null && maps.size() > 0) {
				return (Map) maps.get(0);
			}
		}
		return null;
	}

	/**
	 * 
	 * @param inputMap
	 * @return
	 */
	public String processMapToString(Map<String, Object> inputMap) {
		StringBuilder buf = new StringBuilder("");
		Iterator<String> keys = inputMap.keySet().iterator();
		while (keys.hasNext()) {
			String thisKey = keys.next();
			Object value = inputMap.get(thisKey);

			if (buf.length() != 0) {
				buf.append(", ");
			}
			// add the key
			buf.append(thisKey).append("=");
			// add the value
			if (value instanceof String) {
				buf.append("\"").append(value).append("\"");
			} else {
				buf.append(value);
			}
		}
		return buf.toString();
	}

}
