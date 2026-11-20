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
package prerna.reactor.masterdatabase;

import java.util.List;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetSpecificConceptPropertiesReactor extends AbstractReactor {

	public GetSpecificConceptPropertiesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		GenRowStruct conceptNamesGrs = this.store.getGenRowStruct(keysToGet[0]);
		if (conceptNamesGrs == null) {
			throw new IllegalArgumentException("Need to define the concepts to find relations");
		}
		String conceptLogicals = conceptNamesGrs.get(0).toString();

		// account for optional engine filter
		GenRowStruct engineFilterGrs = this.store.getGenRowStruct(keysToGet[1]);
		if (engineFilterGrs == null) {
			throw new IllegalArgumentException("Need to define the engine filter");
		}
		String engineId = engineFilterGrs.get(0).toString();
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
		List<String> dbFilters = SecurityEngineUtils.getFullUserEngineIds(this.insight.getUser());
		if (!dbFilters.contains(engineId)) {
			throw new IllegalArgumentException(
					"Databases " + engineId + " does not exist or user does not have access");
		}

		List<String> conceptProperties = MasterDatabaseUtility.getSpecificConceptProperties(conceptLogicals, engineId);
		return new NounMetadata(conceptProperties, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.DATABASE_CONCEPT_PROPERTIES);
	}

	@Override
	public String getReactorDescription() {
		return "Returns the properties/columns for a specific concept/table within a database";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.CONCEPT.getKey())) {
			return "The concept/table name to get properties for";
		} else if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "The database id";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
