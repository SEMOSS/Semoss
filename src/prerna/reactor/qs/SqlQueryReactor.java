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
package prerna.reactor.qs;

import org.json.JSONObject;

import prerna.sablecc2.om.ReactorKeysEnum;

/**
 * Executes a SQL query against a database.
 */
public class SqlQueryReactor extends AbstractSqlQueryReactor {

	@Override
	protected String getDecodedQuery() {
		return this.keyValue.get(ReactorKeysEnum.QUERY_KEY.getKey());
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.QUERY_KEY.getKey())) {
			return """
					One or more SQL statements to execute. Multiple statements are parsed, \
					checked for database access, executed in order, and returned as an ordered result array. \
					For convenience, instead of escaping quotes or backslashes you can wrap \
					the input within "<encode>your_text</encode>" and the system will encode it for you.
					""";
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	public JSONObject getMcpProperties() {
		JSONObject properties = super.getMcpProperties();
		properties.getJSONObject(ReactorKeysEnum.QUERY_KEY.getKey()).put("description",
				"One or more SQL statements to parse, check for database access, and execute in order.");
		return properties;
	}

}
