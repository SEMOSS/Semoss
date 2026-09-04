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
package prerna.reactor.frame.sql;

import java.util.HashMap;
import java.util.Map;

import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.parsers.SqlParser;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ParseSQL2Wrapper extends AbstractReactor {

	// fairly straight forward.. parses it
	// stores the wrapper in the insight
	// creates a json structure
	// query : full query
	// paramMap : map of param names to the replacement

	// main reactors needed
	// parse
	// replace param
	// get param default values
	// generate query

	public ParseSQL2Wrapper() {
		this.keysToGet = new String[] { ReactorKeysEnum.SQL.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		try {
			String sql = keyValue.get(keysToGet[0]);

			SqlParser sqlParser = new SqlParser();
			sqlParser.parameterize = true;
			GenExpressionWrapper wrapper = sqlParser.processQuery(sql);

			Map<String, Object> returnMap = new HashMap<String, Object>();
			returnMap.put("query", sql);

			// generate the sql
			String generatedSQL = wrapper.generateQuery(false);
			generatedSQL = generatedSQL.replace("\n", " ").trim();

			returnMap.put("generated_query", generatedSQL);

			// get the param string list to embed
			returnMap.put("params", wrapper.getAllParamNames());

			String id = this.insight.setSQLWrapper(sql, wrapper);
			returnMap.put("ID", id);

			return new NounMetadata(returnMap, PixelDataType.MAP);
		} catch (Exception e) {
			return NounMetadata.getErrorNounMessage(e.getLocalizedMessage());
		}
	}

}
