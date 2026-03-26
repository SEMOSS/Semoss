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

import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Utility;

/**
 * Unified SQL Query Reactor that: 1. Parses SQL to detect query type (SELECT vs
 * modification) 2. Validates user permissions based on query type 3. Delegates
 * to appropriate existing reactors
 * 
 * Usage: SqlQuery(database=["myDb"], query=["SELECT * FROM table"],
 * limit=[100], commit=[true])
 */

public class SqlQueryReactor extends AbstractSqlQueryReactor {

	public SqlQueryReactor() {

	}

	@Override
	protected String getDecodedQuery() {
		return Utility.decodeURIComponent(this.keyValue.get(ReactorKeysEnum.QUERY_KEY.getKey()));
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.QUERY_KEY.getKey())) {
			return "The SQL query to execute, provided as a URL-encoded UTF-8 string.";
		} else if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "The database id";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Limits the number of rows retrieved by the SQL query";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Execute a URL-encoded SQL query against a database with pagination support (limit and offset). ";
	}
}
