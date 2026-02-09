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
package prerna.reactor.security;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.sql.DatabaseCategoryEnum;

/**
 * Reactor that determines the category (SQL or NoSQL) of a database engine
 * Takes an engine ID and returns the database category
 */
public class GetDatabaseCategoryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetDatabaseCategoryReactor.class);

	public GetDatabaseCategoryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		DatabaseCategoryEnum category = DatabaseCategoryEnum.UNKNOWN;

		GetEngineMetadataReactor metadataReactor = new GetEngineMetadataReactor();
		metadataReactor.setInsight(this.insight);
		metadataReactor.setNounStore(this.store);
		NounMetadata engineMetadataResult = metadataReactor.execute();
		if (engineMetadataResult != null && engineMetadataResult.getValue() instanceof Map) {
			Map<String, Object> engineMetadata = (Map<String, Object>) engineMetadataResult.getValue();
			Object rdbmsType = engineMetadata.get("database_subtype");
			classLogger.info("rdbms type: {}", rdbmsType);
			if (rdbmsType != null) {
				category = DatabaseCategoryEnum.getCategoryFromRdbmsType(rdbmsType.toString());
			}
		}

		return new NounMetadata(category.getCategoryName(), PixelDataType.CONST_STRING, PixelOperationType.ENGINE_INFO);
	}

}