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

import prerna.util.Constants;
import prerna.util.DIHelper;

//interchangable with mysql
public class MariaDbQueryUtil extends SQLQueryUtil {
	
	private static String connectionBase = "jdbc:mysql://localhost:"+DIHelper.getInstance().getProperty(Constants.MARIADB_PORT);
	private static String indexNameBind = "{indexName}";
	private static String dbNameBind = "{dbName}";

	public MariaDbQueryUtil(){
		super.setDialectAllIndexesInDB("SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ");
		super.setDialectIndexInfo("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE INDEX_NAME = " + indexNameBind
				+ " and TABLE_SCHEMA = " + dbNameBind);  

		super.setDefaultDbUserName("root");
		super.setDefaultDbPassword("");
	}
	
	@Override
	public RdbmsTypeEnum getDatabaseType(){
		return RdbmsTypeEnum.MARIADB;
	}
	@Override
	public String getConnectionURL(String baseFolder,String dbname){
		return connectionBase + System.getProperty("file.separator") + dbname;
	}

	@Override
	public String getDatabaseDriverClassName(){
		return RdbmsTypeEnum.MARIADB.getDriver();
	}
	@Override
	public String getDialectIndexInfo(String indexName, String dbName){
		String qry =  super.getDialectIndexInfo().replace(indexNameBind, "'" + indexName + "'");
		qry =  qry.replace(dbNameBind, "'" + dbName + "'");
		return qry;
	}
	
	@Override
	public String getDialectDropIndex(String indexName, String tableName){
		return super.getDialectDropIndex() + indexName + " ON " + tableName;
	}
}