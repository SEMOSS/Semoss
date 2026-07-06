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
package prerna.reactor.insights;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.PixelList;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PositionInsightRecipeReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PositionInsightRecipeReactor.class);

	private static final String POSITION = "positionMap";

	public PositionInsightRecipeReactor() {
		this.keysToGet = new String[] { POSITION };
	}

	@Override
	public NounMetadata execute() {
		PixelList pixelList = this.insight.getPixelList();

		List<Map<String, Object>> positionMap = getPosition();
		if (positionMap == null || positionMap.isEmpty()) {
			classLogger.info("No positions defined for setting the position map");
			return new NounMetadata(false, PixelDataType.BOOLEAN);
		}

		int size = positionMap.size();
		for (int i = 0; i < size; i++) {
			Map<String, Object> position = positionMap.get(i);
			if (position != null && !position.isEmpty()) {
				pixelList.get(i).setPositionMap(position);
			}
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * Get the list of position maps
	 * 
	 * @return
	 */
	private List<Map<String, Object>> getPosition() {
		List<Map<String, Object>> positionList = new Vector<>();
		// grab from noun store
		GenRowStruct grs = this.store.getGenRowStruct(POSITION);
		if (grs != null && grs.isEmpty()) {
			for (int i = 0; i < grs.size(); i++) {
				Object val = grs.get(i);
				if (val == null) {
					positionList.add(null);
				} else {
					positionList.add((Map<String, Object>) val);
				}
			}
			return positionList;
		}

		// else grab from current row
		for (int i = 0; i < this.curRow.size(); i++) {
			Object val = this.curRow.get(i);
			if (val == null) {
				positionList.add(null);
			} else {
				positionList.add((Map<String, Object>) val);
			}
		}

		// return the list
		return positionList;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(POSITION)) {
			return "The list of position maps containing {'top'=[value1], 'left'=[value2]} where value1,value2 are integers";
		}
		return super.getDescriptionForKey(key);
	}

}
