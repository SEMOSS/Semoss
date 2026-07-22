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
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.query.parsers.SqlTranslator;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class QueryTranslatorReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(QueryTranslatorReactor.class);

	public QueryTranslatorReactor() {
		this.keysToGet = new String[] { "query", "sourceDB", "targetDB" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String query = this.keyValue.get(this.keysToGet[0]);
		System.out.println(query);
		String sourceDbId = this.keyValue.get(this.keysToGet[1]);
		String targetDbId = this.keyValue.get(this.keysToGet[2]);
		sourceDbId = MasterDatabaseUtility.testDatabaseIdIfAlias(sourceDbId);
		targetDbId = MasterDatabaseUtility.testDatabaseIdIfAlias(targetDbId);
		sourceDbId = testDatabaseId(sourceDbId, false);
		targetDbId = testDatabaseId(targetDbId, false);
		// get physical to physical translation from sourceDB to targetDB
		Map<String, List<String>> translation = MasterDatabaseUtility.databaseTranslator(sourceDbId, targetDbId);
		// generate translated queries
		SqlTranslator translator = new SqlTranslator(translation);
		Set<String> queries = null;
		try {
			queries = translator.processQuery(query);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		return new NounMetadata(queries, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
}
