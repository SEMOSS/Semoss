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

import java.util.List;

public class H2QueryUtil extends SQLQueryUtil {
	
	public static final String DATABASE_DRIVER = "org.h2.Driver";
	
	public H2QueryUtil(){
		super.setDialectAllTables(" SHOW TABLES FROM PUBLIC ");
		super.setResultAllTablesTableName("TABLE_NAME");
		super.setDialectAllIndexesInDB("SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_SCHEMA = 'PUBLIC' ORDER BY INDEX_NAME");
		super.setDialectIndexInfo("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_SCHEMA = 'PUBLIC' AND INDEX_NAME = ");
		super.setDialectForceGraph("SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC'");
		super.setDialectOuterJoinLeft(" LEFT OUTER JOIN ");
		super.setDialectOuterJoinRight(" RIGHT OUTER JOIN ");
		super.setDefaultDbUserName("sa");
		super.setDefaultDbPassword("");
	}
	
	@Override
	public SQLQueryUtil.DB_TYPE getDatabaseType(){
		return SQLQueryUtil.DB_TYPE.H2_DB;
	}
	@Override
	public String getDialectAllIndexesInDB(String schema){
		return super.getDialectAllIndexesInDB(); //dont plop schema into here
	}
	@Override
	public String getDefaultOuterJoins(){
		return SQLQueryUtil.USE_OUTER_JOINS_FALSE;
	}
	@Override
	public String getConnectionURL(String baseFolder, String dbname){
		String engineDirectoryName = "db" + System.getProperty("file.separator") + dbname;
		return "jdbc:h2:nio:" + baseFolder + System.getProperty("file.separator") + engineDirectoryName
				+ System.getProperty("file.separator") + "database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
	}
	@Override
	public String getDatabaseDriverClassName(){
		return DATABASE_DRIVER;
	}
	@Override
	public String getDialectIndexInfo(String indexName, String dbName){
		return super.getDialectIndexInfo() + "'" + indexName+ "'"; //h2 doesnt need the dbName 
	}
	
	
	//"full outer join"
	//this is definetly going to be db specific, (so abstracting)
	public String getDialectFullOuterJoinQuery(boolean distinct, String selectors, List<String> rightJoinsArr, List<String> leftJoinsArr, 
			List<String> joinsArr, String filters, int limit, String groupBy){
		String joins = "";
		String rightOuterJoins = "";
		String leftOuterJoins = "";

		for(String singleJoin: joinsArr){
			if(joins.length()>0) joins+= " AND ";
			joins += singleJoin;
		}
		
		for(String singleJoin: rightJoinsArr){
			rightOuterJoins += singleJoin;
		}
		
		for(String singleJoin: leftJoinsArr){
			leftOuterJoins += singleJoin;
		}
		
		if(joins.length() > 0 && filters.length() > 0)
			joins = joins + " AND " + filters;
		else if(filters.length() > 0)
			joins = filters;
		if(joins.length() > 0)
			joins = " WHERE " + joins;
		
		String selectStr = super.dialectSelect;
		if(distinct) selectStr+= super.dialectDistinct;
		
		String query = selectStr + selectors + "  FROM  " + leftOuterJoins + joins; //+ " LIMIT " + limit ;
		if(groupBy !=null && groupBy.length()>0)
			query += " GROUP BY " + groupBy;
		query += " UNION ";
		query += selectStr + selectors + "  FROM  " + rightOuterJoins + joins; //+ " LIMIT " + limit ;
		
		if(groupBy !=null && groupBy.length()>0)
			query += " GROUP BY " + groupBy;
		
		if(limit!=-1){
			query += " LIMIT " + limit;
		}

		return query;
	}

	@Override
	public String getEngineNameFromConnectionURL(String connectionURL) {
		int indexOfSemiColon = connectionURL.indexOf(";");
		String connUrlSubStr = connectionURL.substring(0, indexOfSemiColon);
		int lastIndexOfSlashAfterEngineNm = connUrlSubStr.lastIndexOf("/");
		connUrlSubStr = connUrlSubStr.substring(0, lastIndexOfSlashAfterEngineNm);
		int lastIndexOfSlashBeforeEngineNm = connUrlSubStr.lastIndexOf("/");
		String engineName = connectionURL.substring(lastIndexOfSlashBeforeEngineNm+1, lastIndexOfSlashAfterEngineNm);
		return engineName;
	}	
	
}