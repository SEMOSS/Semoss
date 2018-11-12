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

public class H2QueryUtil extends SQLQueryUtil {
	
	public H2QueryUtil(){
		super.setDialectAllIndexesInDB("SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_SCHEMA = 'PUBLIC' ORDER BY INDEX_NAME");
		super.setDialectIndexInfo("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_SCHEMA = 'PUBLIC' AND INDEX_NAME = ");
		super.setDefaultDbUserName("sa");
		super.setDefaultDbPassword("");
	}
	
	@Override
	public RdbmsTypeEnum getDatabaseType(){
		return RdbmsTypeEnum.H2_DB;
	}
	
	@Override
	public String getDialectAllIndexesInDB(String schema){
		return super.getDialectAllIndexesInDB(); //dont plop schema into here
	}
	
	@Override
	public String getConnectionURL(String baseFolder, String dbname){
		String engineDirectoryName = "db" + System.getProperty("file.separator") + dbname;
		return "jdbc:h2:nio:" + baseFolder + System.getProperty("file.separator") + engineDirectoryName
				+ System.getProperty("file.separator") + "database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
	}
	
	@Override
	public String getDatabaseDriverClassName(){
		return RdbmsTypeEnum.H2_DB.getDriver();
	}
	
	@Override
	public String getDialectIndexInfo(String indexName, String dbName){
		return super.getDialectIndexInfo() + "'" + indexName+ "'"; //h2 doesnt need the dbName 
	}
}