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
package prerna.query.querystruct.update;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import prerna.date.SemossDate;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.interpreters.sql.SqlInterpreter;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.sql.AbstractSqlQueryUtil;

@Deprecated
public class UpdateSqlInterpreter extends SqlInterpreter {

	private StringBuilder sets = new StringBuilder();
	private String updateFrom = null;
	private String userId = null;

	@Deprecated
	public UpdateSqlInterpreter(UpdateQueryStruct qs) {
		this.qs = qs;
		if (this.qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE
				|| this.qs.getQsType() == AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			this.engine = qs.retrieveQueryStructEngine();
			if (this.engine != null) {
				this.queryUtil = ((IRDBMSEngine) this.engine).getQueryUtil();
			}
		} else {
			this.frame = qs.getFrame();
			if (this.frame != null) {
				this.queryUtil = ((AbstractRdbmsFrame) this.frame).getQueryUtil();
			}
		}
	}

	//////////////////////////////////////////// Compose Query
	//////////////////////////////////////////// //////////////////////////////////////////////

	@Deprecated
	@Override
	public String composeQuery() {
		addSets();
		addFilters();

		// Initiate String
		StringBuilder query = new StringBuilder("UPDATE ");
		// Add sets depending on...
		query.append(this.updateFrom);
		query.append(" SET ").append(this.sets);

		int numFilters = this.filterStatements.size();
		for (int i = 0; i < numFilters; i++) {
			if (i == 0) {
				query.append(" WHERE ");
			} else {
				query.append(" AND ");
			}
			query.append(this.filterStatements.get(i).toString());
		}

		return query.toString();
	}

	//////////////////////////////////////////// End Compose Query
	//////////////////////////////////////////// //////////////////////////////////////////

	private void addSets() {
		Set<String> tableList = new HashSet<String>();

		// determine if we can insert booleans as true/false
		boolean allowBooleanType = queryUtil.allowBooleanDataType();

		List<IQuerySelector> selectors = qs.getSelectors();
		List<Object> values = ((UpdateQueryStruct) qs).getValues();
		int numSelectors = selectors.size();
		for (int i = 0; i < numSelectors; i++) {
			if (i != 0) {
				sets.append(", ");
			}
			QueryColumnSelector s = (QueryColumnSelector) selectors.get(i);
			String table = s.getTable();
			String column = s.getColumn();
			if (column.equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
				column = getPrimKey4Table(table);
			}
			Object v = values.get(i);
			if (v == null) {
				sets.append(column + "= NULL");
			} else if (v instanceof String) {
				if (v.equals("<UUID>")) {
					sets.append(column + "=" + "'"
							+ AbstractSqlQueryUtil.escapeForSQLStatement(UUID.randomUUID().toString()) + "'");
				} else if (v.equals("<USER_ID>") && this.userId != null) {
					sets.append(column + "=" + "'" + AbstractSqlQueryUtil.escapeForSQLStatement(userId) + "'");
				} else {
					sets.append(column + "=" + "'" + AbstractSqlQueryUtil.escapeForSQLStatement(v + "") + "'");
				}
			} else if (v instanceof SemossDate) {
				String dateValue = ((SemossDate) v).getFormattedDate();
				if (dateValue == null || dateValue.isEmpty() || dateValue.equals("null")) {
					sets.append(column + "= NULL");
				} else {
					sets.append(column + "=" + "'" + dateValue + "'");
				}
			} else if (v instanceof Boolean) {
				if (allowBooleanType) {
					sets.append(column + "=" + v);
				} else {
					// append 1 or 0 based on true/false
					if (Boolean.parseBoolean(v + "")) {
						sets.append(column + "=1");
					} else {
						sets.append(column + "=0");
					}
				}
			} else {
				sets.append(column + "=" + v);
			}

			tableList.add(table);
		}

		if (tableList.size() > 1) {
			throw new IllegalArgumentException("Cannot update multiple tables in the same statement");
		}
		this.updateFrom = tableList.iterator().next();
	}

	@Deprecated
	public String getUserId() {
		return userId;
	}

	@Deprecated
	public void setUserId(String userId) {
		this.userId = userId;
	}

	//////////////////////////////////////////// Main function to test
	//////////////////////////////////////////// //////////////////////////////////////

//	public static void main(String[] args) {
//		// load engine
////		TestUtilityMethods.loadDIHelper("C:/Users/laurlai/workspace/Semoss/RDF_Map.prop");
////		
////		String engineProp = "C:/Users/laurlai/workspace/Semoss/db/LocalMasterDatabase.smss";
////		IEngine coreEngine = new RDBMSNativeEngine();
////		coreEngine.setEngineName(Constants.LOCAL_MASTER_DB_NAME);
////		coreEngine.open(engineProp);
////		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);
////
////		engineProp = "C:/Users/laurlai/workspace/Semoss/db/MovieDB.smss";
////		coreEngine = new RDBMSNativeEngine();
////		coreEngine.setEngineName("MovieDB");
////		coreEngine.open(engineProp);
////		DIHelper.getInstance().setLocalProperty("MovieDB", coreEngine);
//		
//		
//		// Create qs object
//		UpdateQueryStruct qs = new UpdateQueryStruct();
//		
//		/**
//		 * Update one column on one table
//		 */
//		qs.addSelector("Nominated", "Nominated");
//		List<Object> values = new ArrayList<Object>();
//		values.add("N");
//		qs.setValues(values);
//		QueryColumnSelector tab = new QueryColumnSelector("Nominated__Title_FK");
//		NounMetadata fil1 = new NounMetadata(tab, PixelDataType.COLUMN);
//		NounMetadata fil2 = new NounMetadata("Chocolat", PixelDataType.CONST_STRING);
//		SimpleQueryFilter filter1 = new SimpleQueryFilter(fil2, "=", fil1);
//		qs.addExplicitFilter(filter1);
//		
//		/**
//		 * Update one table using values of another for reference
//		 * UPDATE Genre SET Genre.Genre='Comedy' WHERE Genre.Title_FK IN (SELECT Title.Title FROM Title WHERE Title = 'Avatar')
//		 */
////		qs.addSelector("Genre", "Genre");
////		List<Object> values = new ArrayList<Object>();
////		values.add("Drama");
////		qs.setValues(values);
////		
////		// Making subquery
////		QueryStruct2 subQuery = new QueryStruct2();
////		QueryColumnSelector title = new QueryColumnSelector("Title__Title");
////		subQuery.addSelector(title);
////		NounMetadata fil3 = new NounMetadata(title, PixelDataType.COLUMN);
////		NounMetadata fil4 = new NounMetadata("Avatar", PixelDataType.CONST_STRING);
////		SimpleQueryFilter subQueryFilter = new SimpleQueryFilter(fil3, "=", fil4);
////		subQuery.addExplicitFilter(subQueryFilter);
////		
//////		// Add to qs
////		NounMetadata col = new NounMetadata(new QueryColumnSelector("Genre__Title_FK"), PixelDataType.COLUMN);
////		NounMetadata filquery = new NounMetadata(subQuery, PixelDataType.QUERY_STRUCT);
////		SimpleQueryFilter filter5 = new SimpleQueryFilter(col, "==", filquery);
////		qs.addExplicitFilter(filter5);
////				
//		// Create interpreter and compose query
//		UpdateSqlInterpreter interpreter = new UpdateSqlInterpreter(qs);
//		String s = interpreter.composeQuery();
//		System.out.println(s);
//		
//		// run query on engine
////		coreEngine.insertData(s);
////		
////		// viewing results
//////		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(coreEngine, "select * from Nominated");
////		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(coreEngine, "select * from Genre");
////		while(it.hasNext()) {
////			System.out.println(Arrays.toString(it.next().getValues()));
////		}
//	}
//	

}
