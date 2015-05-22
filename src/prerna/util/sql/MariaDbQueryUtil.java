/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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

import java.util.ArrayList;

//interchangable with mysql
public class MariaDbQueryUtil extends SQLQueryUtil {
	
	public static final String DATABASE_DRIVER = "org.mariadb.jdbc.Driver";
	private static String connectionBase = "jdbc:mysql://localhost:3306";
	
	public MariaDbQueryUtil(){
		super.setDialectAllTables(" SHOW TABLES ");
		super.setResultAllTablesTableName("Tables_in_");
		super.setResultAllColumnsColumnName("Field");
		super.setResultAllColumnsColumnType("Type");
		super.setDialectAllIndexesInDB("SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ");
		super.setDialectIndexInfo("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE INDEX_NAME = {indexName} "
				+ "and TABLE_SCHEMA = {dbName} ");  
		//super.setResultAllIndexesInDBColumnName("Column_name");
		//super.setResultAllIndexesInDBTableName("Table");
		super.setDialectOuterJoinLeft(" LEFT JOIN ");
		super.setDialectOuterJoinRight(" RIGHT JOIN ");
		super.setDefaultDbUserName("root");
		super.setDefaultDbPassword("");
	}
	
	@Override
	public SQLQueryUtil.DB_TYPE getDatabaseType(){
		return SQLQueryUtil.DB_TYPE.MARIA_DB;
	}
	@Override
	public String getDefaultOuterJoins(){
		return SQLQueryUtil.USE_OUTER_JOINS_YES;
	}
	@Override
	public String getConnectionURL(String baseFolder,String dbname){
		return connectionBase + System.getProperty("file.separator") + dbname;
	}
	public static String getTempConnectionURL(){
		return connectionBase;
	}
	@Override
	public String getDatabaseDriverClassName(){
		return DATABASE_DRIVER;
	}
	@Override
	public String getDialectDropIndex(String indexName, String tableName){
		return super.getDialectDropIndex() + indexName + " ON " + tableName;
	}
	@Override
	public String getDialectIndexInfo(String indexName, String dbName){
		String qry =  super.getDialectIndexInfo().replace("{indexName}", "'" + indexName + "'");
		qry =  qry.replace("{dbName}", "'" + dbName + "'");
		return qry;
	}
	
	//"full outer join"
	@Override
	public String getDialectDistinctFullOuterJoinQuery(String selectors, ArrayList<String> rightJoinsArr, 
			ArrayList<String> leftJoinsArr, ArrayList<String> joinsArr, String filters, int limit){
		
		String rightOuterJoins = "";
		String leftOuterJoins = "";
		
		if(rightJoinsArr.size() == leftJoinsArr.size() && rightJoinsArr.size() == (joinsArr.size()-1)){
			System.out.println("getDialectDistinctFullOuterJoinQuery: can continue");
		} else {
			System.out.println("getDialectDistinctFullOuterJoinQuery: cant continue");
		}
		
		for(int i = 0; i < rightJoinsArr.size(); i++){
			rightOuterJoins += rightJoinsArr.get(i);
			leftOuterJoins += leftJoinsArr.get(i);
			if(i!=0){
				rightOuterJoins += " ON " + joinsArr.get(i-1);
				leftOuterJoins += " ON " + joinsArr.get(i-1);
			}
		}
		
		if(filters.length() > 0)
			filters = " WHERE " + filters;
		
		String query = super.dialectSelectDistinct + selectors + "  FROM  " + leftOuterJoins + filters;
		query += " UNION ";
		query += super.dialectSelectDistinct + selectors + "  FROM  " + rightOuterJoins + filters;
		if(limit!=-1){
			query += " LIMIT " + limit;
		}
		return query;

	}
	
}