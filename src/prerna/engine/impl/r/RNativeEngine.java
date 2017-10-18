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
package prerna.engine.impl.r;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.tools.DeleteDbFiles;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import prerna.algorithm.api.IMetaData;
import prerna.ds.QueryStruct;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RIterator2;
import prerna.ds.util.CsvFileIterator;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.query.interpreters.IQueryInterpreter2;
import prerna.query.interpreters.RInterpreter2;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.rdf.util.AbstractQueryParser;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PersistentHash;
import prerna.util.Utility;
import prerna.util.sql.RDBMSUtility;
import prerna.util.sql.SQLQueryUtil;

public class RNativeEngine extends AbstractEngine {

	public static final String STATEMENT_OBJECT = "STATEMENT_OBJECT";
	public static final String RESULTSET_OBJECT = "RESULTSET_OBJECT";
	public static final String CONNECTION_OBJECT = "CONNECTION_OBJECT";
	public static final String ENGINE_CONNECTION_OBJECT = "ENGINE_CONNECTION_OBJECT";
	

	static final Logger logger = LogManager.getLogger(RNativeEngine.class.getName());
	DriverManager manager = null;
	boolean engineConnected = false;
	boolean datasourceConnected = false;
	private SQLQueryUtil.DB_TYPE dbType;
	private BasicDataSource dataSource = null;
	Connection engineConn = null;
	private boolean useConnectionPooling = false;
	int tableCount = 0;
	PersistentHash conceptIdHash = null;
	Hashtable tablePresent = new Hashtable();
	private String fileDB = null;
	
	private String password = null;
	private String driver = null;
	private String connectionURL = null;
	private String userName = null;
	private String createString = null;
	
	String rvarName = null;
	String fakeHeader = null;
	
	RDataTable dt = new RDataTable();

	@Override
	public void openDB(String propFile)
	{
		if(propFile == null && prop == null){
			logger.fatal("Cannot load this R Engine No Property file found");
		} else {
			// will mostly be sent the connection string and I will connect here
			// I need to see if the connection pool has been initiated
			// if not initiate the connection pool
			if(prop == null) {
				prop = loadProp(propFile);
				if(!prop.containsKey("TEMP")) { // if this is not a temp then open the super
					super.openDB(propFile); // this primarily loads the metahelper as well as the insights to get it going
				}
			}


			// make a check to see if it is asking to use file
			boolean useFile = false;
			if(prop.containsKey(USE_FILE))
				useFile = Boolean.valueOf(prop.getProperty(USE_FILE));
			if(useFile)
			{
				// load everything from file
				fileDB = prop.getProperty(DATA_FILE);
				Map <String, String> paramHash = new Hashtable<String, String>();
				
				paramHash.put("BaseFolder", DIHelper.getInstance().getProperty("BaseFolder"));
				paramHash.put("ENGINE", getEngineName());
				fileDB = Utility.fillParam2(fileDB, paramHash);
							
				Vector <String> concepts = this.getConcepts();
				// usually there should be only one, but just a check again
				// need to account for concept being itself
				if(concepts.size() > 2)
				{
					logger.fatal("RDBMS Engine suggests to use file, but there are more than one concepts i.e. this is not a flat file");
					return;
				}

				String [] conceptsArray = concepts.toArray(new String[concepts.size()]);
				Map <String,String> conceptAndType = this.getDataTypes(conceptsArray);
				for(int conceptIndex = 0;conceptIndex < conceptsArray.length;conceptIndex++)
				{
					List <String> propList = getProperties4Concept(conceptsArray[conceptIndex], false);
					if(propList.size() > 0)
						fakeHeader = conceptsArray[conceptIndex];
					String [] propArray = propList.toArray(new String[propList.size()]);
					
					Map<String, String> typeMap = getDataTypes(propArray);
					conceptAndType.putAll(typeMap);
				}
				
				// convert it to a data type
				Map <String, IMetaData.DATA_TYPES> conceptMetaMap = new Hashtable<String, IMetaData.DATA_TYPES>();
				Iterator <String> conceptKeys = conceptAndType.keySet().iterator();

				// I need to create a CSVFileIterator
				// and then let RBuilder primarily load it
				// I may not need these metadata types
				// not sure why I am doing it twice - CSVQueryStruct and then the conceptMap
				// it is just there
				
				while(conceptKeys.hasNext())
				{
					String thisKey = conceptKeys.next();
					String thisType = conceptAndType.get(thisKey);
					
					// this is the keys
					thisKey = Utility.getClassName(thisKey);
					thisKey = thisKey.toUpperCase();
					
					IMetaData.DATA_TYPES newType = IMetaData.convertToDataTypeEnum(thisType);
					conceptMetaMap.put(thisKey, newType);
				}
				
				CsvQueryStruct cqs = new CsvQueryStruct();
				cqs.setCsvFilePath(fileDB);
				cqs.setColumnTypes(conceptAndType);
				
				// the filte iterator will rip through the whole thing anyways
				CsvFileIterator fit = new CsvFileIterator(cqs);
				
				

				String dbName = fileDB.replace(".csv", "");
				dbName = dbName.replace(".tsv", "");
				// delete the database if it exists to start with

				rvarName = Utility.getRandomString(6);
				
				//rvarName = Utility.cleanString(fileDB, true);
//				builder.createTableViaIterator(rvarName, fit, conceptMetaMap);
				dt = new RDataTable(rvarName);
				dt.addRowsViaIterator(fit, rvarName, conceptMetaMap);
				fakeHeader = Utility.getInstanceName(fakeHeader);
				dt.generateRowIdWithName();//fakeHeader);
				//fakeHeader = null;
				
				// process is complete at this point
				// at this point I will just have a builder and I am good to go
			}
		}
	}	
		
	@Override
	// need to clean up the exception it will never be thrown
	public void insertData(String query) 
	{
		// there is no insert data on R not with queries
	}

	private void closeConnections(Connection conn, ResultSet rs, Statement stmt){
		if(isConnected()){
			dt.closeConnection();
		}
		// just setting the varname to null 
		// need a method to close JRI
	}

	@Override
	public ENGINE_TYPE getEngineType()
	{
		return IEngine.ENGINE_TYPE.R;
	}

	@Override
	public Vector<Object> getEntityOfType(String type)
	{
		// need to confirm if it is only legacy or we should implement
		// need a way to maintain 
		
		return null;

	}

	
	public Vector<Object> getCleanSelect(String query){
	
		Vector <Object> retObject = null;
		
		RIterator2 it = (RIterator2) dt.query(query);
		if(it.getHeaders() != null && it.getHeaders().length > 0)
		{
			String headerName = it.getHeaders()[0];
		
			retObject = new Vector();
			while(it.hasNext())
				retObject.add(it.next().getField(headerName));
		}
		return retObject;
	}

	public Object execQuery(String query)
	{
		return dt.query(query);
	}

	@Override
	public boolean isConnected()
	{
		return this.engineConnected;
	}

	@Override
	public void closeDB() {
		dt.closeConnection();
	}

	public IQueryInterpreter getQueryInterpreter(){
		RInterpreter retInterp = new RInterpreter();
		retInterp.setDataTableName(rvarName);
		return retInterp;
	}

	public AbstractQueryParser getQueryParser() {
		// not implemented for R
		return null;
	}

	@Override
	public void removeData(String query) {
		// not implemented for R
	}

	@Override
	public void commit() {
		// not implemented for R
	}

	// traverse from a type to a type
	public String traverseOutputQuery(String fromType, String toType, List <String> fromInstances)
	{
		/*
		 * 1. Get the relation for the type
		 * 2. For every relation create a join
		 * 3. If Properties are included get the properties
		 * 4. Add the properties
		 * 5. For every, type 
		 */
		IQueryInterpreter builder = getQueryInterpreter();
		QueryStruct qs = new QueryStruct();
		
		String fromTableName = Utility.getInstanceName(fromType);
		String toTableName = Utility.getInstanceName(toType);
		qs.addSelector(fromTableName, QueryStruct.PRIM_KEY_PLACEHOLDER);
		qs.addSelector(toTableName, QueryStruct.PRIM_KEY_PLACEHOLDER);

		// determine relationship order
		String relationQuery = "SELECT ?relation WHERE {"
				+ "{" + "<" + fromType + "> ?relation <" + toType +">}"
				+ "{?relation <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>}"
				+ "}";

		String relationName = getRelation(relationQuery);
		if(relationName != null && relationName.length() != 0) {
			qs.addRelation(fromTableName, toTableName, "inner.join");
		} else {
			qs.addRelation(toTableName, fromTableName, "inner.join");
		}

		if(fromInstances != null) {
			// convert instances to simple instance
			Vector <String> simpleFromInstances = new Vector<String>();
			for(int fromIndex = 0;fromIndex < fromInstances.size();fromIndex++) {
				simpleFromInstances.add(Utility.getInstanceName(fromInstances.get(fromIndex)));
			}
			qs.addFilter(fromTableName, "=", simpleFromInstances);
		}
		
		String retQuery = builder.composeQuery();
		return retQuery;
	}

	private String getRelation(String query)
	{
		String relation = null;
		try {
			TupleQueryResult tqr = (TupleQueryResult)execOntoSelectQuery(query);
			while(tqr.hasNext())
			{
				BindingSet bs = tqr.next();
				relation = bs.getBinding("relation").getValue() + "";
				if(!relation.equalsIgnoreCase("http://semoss.org/ontologies/Relation"))
					break;
			}
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return relation;
	}


	public void deleteDB() {
		logger.debug("Deleting RDBMS Engine: " + this.engineName);

		// Close the Insights RDBMS connection, the actual connection, and delete the folders
		try {
			//this.insightRDBMS.getConnection().close();
			closeDB();
			DeleteDbFiles.execute(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + this.engineName, "database", false);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Clean up SMSS and DB files/folder
		super.deleteDB();
	}



	private String resetH2ConnectionURL() {
		String baseH2URL = RDBMSUtility.getH2BaseConnectionURL();
		return RDBMSUtility.fillH2ConnectionURL(baseH2URL, engineName);
	}

	
	
	@Override
	public IQueryInterpreter2 getQueryInterpreter2(){
		RInterpreter2 retInterp = new RInterpreter2();
		if(fakeHeader != null)
			retInterp.addHeaderToRemove(fakeHeader);
		retInterp.setDataTableName(rvarName);
		return retInterp;
	} 
}
