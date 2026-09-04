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

public class ReplaceParamReactor extends AbstractReactor {

	public ReplaceParamReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SQL.getKey(), ReactorKeysEnum.PARAM_KEY.getKey(),
				ReactorKeysEnum.VALUE.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		// replace the parameter
		// fill it
		// generate the sql
		// generate wrapper from it
		// parameterize and regenerate - need to know if front end needs it like that
		organizeKeys();

		try {
			String id = keyValue.get(keysToGet[0]); // this is really the id
			String param = keyValue.get(keysToGet[1]);
			String value = keyValue.get(keysToGet[2]);

			GenExpressionWrapper wrapper = this.insight.getSQLWrapper(id);
			if (wrapper.setCurrentValueOfParam(param, value)) {
				SqlParser sqlParser = new SqlParser();

				Map<String, Object> returnMap = new HashMap<String, Object>();
				// generate the sql
				wrapper.fillParameters();
				// this will be the new sql
				String newSql = sqlParser.generateQuery(wrapper.root);
				newSql = newSql.replace("\n", " ").trim();
				returnMap.put("query", newSql);

				sqlParser.parameterize = true;
				wrapper = sqlParser.processQuery(newSql);
				String generatedSQL = wrapper.generateQuery(false);
				returnMap.put("generated_query", generatedSQL);

				// get the param string list to embed
				returnMap.put("params", wrapper.getAllParamNames());

				// remove the old one
				this.insight.replaceWrapper(id, newSql, wrapper);

				return new NounMetadata(returnMap, PixelDataType.MAP);
			} else {
				return NounMetadata.getErrorNounMessage("No such parameter found");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			return NounMetadata.getErrorNounMessage(e.getLocalizedMessage());
		}
	}

}
