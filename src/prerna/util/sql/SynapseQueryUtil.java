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
package prerna.util.sql;

public class SynapseQueryUtil extends MicrosoftSqlServerQueryUtil {

	SynapseQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.SYNAPSE);
	}

	SynapseQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.SYNAPSE);
	}

	@Override
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset) {

		if (limit > 0) {
			String strquery = query.toString();
			if (strquery.startsWith("SELECT DISTINCT")) {
				strquery = strquery.replaceFirst("SELECT DISTINCT", "SELECT DISTINCT TOP " + limit + " ");
			} else {
				strquery = strquery.replaceFirst("SELECT", "SELECT TOP " + limit + " ");
			}
			query = new StringBuilder();
			query.append(strquery);
		}

		// TODO there is no offset for now
//		if(offset > 0) {
//			query = query.append(" OFFSET "+offset);
//		}
		return query;
	}

	@Override
	public StringBuffer addLimitOffsetToQuery(StringBuffer query, long limit, long offset) {

		if (limit > 0) {
			String strquery = query.toString();
			query = new StringBuffer();
			if (strquery.startsWith("SELECT DISTINCT")) {
				strquery = strquery.replaceFirst("SELECT DISTINCT", "SELECT DISTINCT TOP " + limit + " ");
			} else {
				strquery = strquery.replaceFirst("SELECT", "SELECT TOP " + limit + " ");
			}
			query.append(strquery);
		}

		// TODO there is no offset for now
//		if(offset > 0) {
//			query = query.append(" OFFSET "+offset);
//		}
		return query;
	}

//	//this creates the temp table to select top from the entire list of distinct selectors. 
//	//this is only used with distinct
//	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset, String tempTable) {
//
//		if(limit > 0) {
//			query=query.insert(0, "SELECT TOP " + limit + " * from (");
//			query=query.append(") as "+ tempTable);
//		}
//		
//		//TODO there is no offset for now
////		if(offset > 0) {
////			query = query.append(" OFFSET "+offset);
////		}
//		return query;
//	}

}
