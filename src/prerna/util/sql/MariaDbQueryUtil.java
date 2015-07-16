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
	private static String indexNameBind = "{indexName}";
	private static String dbNameBind = "{dbName}";
	private static String dialectForceGraphMaria = "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = " + dbNameBind;

	
	public MariaDbQueryUtil(){
		super.setDialectAllTables(" SHOW TABLES ");
		super.setResultAllTablesTableName("Tables_in_");
		super.setResultAllColumnsColumnName("Field");
		super.setResultAllColumnsColumnType("Type");
		super.setDialectAllIndexesInDB("SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ");
		super.setDialectIndexInfo("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE INDEX_NAME = " + indexNameBind
				+ " and TABLE_SCHEMA = " + dbNameBind);  
		super.setDialectForceGraph(dialectForceGraphMaria);

		//super.setResultAllIndexesInDBColumnName("Column_name");
		//super.setResultAllIndexesInDBTableName("Table");
		super.setDialectOuterJoinLeft(" LEFT JOIN ");
		super.setDialectOuterJoinRight(" RIGHT JOIN ");
		super.setDefaultDbUserName("root");
		super.setDefaultDbPassword("");
		super.setDialectDeleteDBSchema("DROP DATABASE IF EXISTS ");
	}
	

	
	@Override
	public SQLQueryUtil.DB_TYPE getDatabaseType(){
		return SQLQueryUtil.DB_TYPE.MARIA_DB;
	}
	@Override
	public String getDefaultOuterJoins(){
		return SQLQueryUtil.USE_OUTER_JOINS_TRUE;
	}
	@Override
	public String getConnectionURL(String baseFolder,String dbname){
		return connectionBase + System.getProperty("file.separator") + dbname;
	}
	@Override
	public String getTempConnectionURL(){
		return connectionBase;
	}
	@Override
	public String getDatabaseDriverClassName(){
		return DATABASE_DRIVER;
	}
	@Override
	public String getDialectIndexInfo(String indexName, String dbName){
		String qry =  super.getDialectIndexInfo().replace(indexNameBind, "'" + indexName + "'");
		qry =  qry.replace(dbNameBind, "'" + dbName + "'");
		return qry;
	}
	@Override 
	public String getDialectForceGraph(String dbName){
		String qry = dialectForceGraphMaria.replace(dbNameBind, "'" + dbName + "'");
		return qry;
	}
	
	
	@Override
	public String getDialectDropIndex(String indexName, String tableName){
		return super.getDialectDropIndex() + indexName + " ON " + tableName;
	}
	
	//"full outer join"
	//this is definetly going to be db specific, (so abstracting)
	@Override
	public String getDialectFullOuterJoinQuery(boolean distinct, String selectors, ArrayList<String> rightJoinsArr, 
			ArrayList<String> leftJoinsArr, ArrayList<String> joinsArr, String filters, int limit, String groupBy){
		
		String rightOuterJoins = "";
		String leftOuterJoins = "";
		String joins = "";
		
		//if(rightJoinsArr.size() == leftJoinsArr.size() && (rightJoinsArr.size()!=0 && (rightJoinsArr.size()-1) <= joinsArr.size())){
		//	System.out.println("getDialectDistinctFullOuterJoinQuery: can continue");
		//} else {
		//	System.out.println("getDialectDistinctFullOuterJoinQuery: cant continue");
		//}
		
		for(int i = 0; i < rightJoinsArr.size(); i++){
			rightOuterJoins += rightJoinsArr.get(i);
			leftOuterJoins += leftJoinsArr.get(i);
			if(i!=0){
				rightOuterJoins += " ON " + joinsArr.get(i-1);
				leftOuterJoins += " ON " + joinsArr.get(i-1);
			}
		}
		// if the joinsArray still has more
		if(rightJoinsArr.size()!=0 && ((rightJoinsArr.size()-1) < joinsArr.size())){
			for(int i = rightJoinsArr.size()-1; i < joinsArr.size(); i++){
				if(joins.length()>0) joins += " AND ";
				joins += joinsArr.get(i);
			}
		}
		
		String queryJoinsAndFilters = "";
		if(joins.length() > 0 && filters.length() > 0)
			queryJoinsAndFilters = joins + " AND " + filters;
		else if(joins.length() > 0){
			queryJoinsAndFilters = joins;
		}
		else if(filters.length() > 0){
			queryJoinsAndFilters = filters;
		}
		if(queryJoinsAndFilters.length() > 0)
			queryJoinsAndFilters = " WHERE " + queryJoinsAndFilters;
		
		String selectStr = super.dialectSelect;
		if(distinct) selectStr+= super.dialectDistinct;
		
		String query = selectStr + selectors + "  FROM  " + leftOuterJoins + queryJoinsAndFilters; //+ " LIMIT " + limit ;
		if(groupBy !=null && groupBy.length()>0){
			query += " GROUP BY " + groupBy;
		}
		
		query += " UNION ";
		query += selectStr + selectors + "  FROM  " + rightOuterJoins + queryJoinsAndFilters; //+ " LIMIT " + limit ;
		
		if(groupBy !=null && groupBy.length()>0){
			query += " GROUP BY " + groupBy;
		}
		
		if(limit!=-1){
			query += " LIMIT " + limit;
		}
		return query;

	}

	@Override
	public String getEngineNameFromConnectionURL(String connectionURL) {
		String engineName = "";
		String splitConnectionURL[] = connectionURL.split("/");
		engineName = splitConnectionURL[splitConnectionURL.length - 1];
		return engineName;
	}	
	
}