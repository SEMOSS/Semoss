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

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.tools.DeleteDbFiles;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RIterator;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.om.Insight;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.rdf.util.AbstractQueryParser;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RNativeEngine extends AbstractEngine {

	private static final Logger LOGGER = LogManager.getLogger(RNativeEngine.class.getName());
	
	private String fileDB = null;

	String rvarName = null;
	String fakeHeader = null;
	
	// we use a dummy insight that stores this information
	AbstractRJavaTranslator rJavaTranslator = RJavaTranslatorFactory.getRJavaTranslator(new Insight(), LOGGER);
	RDataTable dt = new RDataTable(rJavaTranslator);

	@Override
	public void openDB(String propFile)
	{
		if(propFile == null && prop == null){
			LOGGER.fatal("Cannot load this R Engine. No Property file found");
		} else {
			// will mostly be sent the connection string and I will connect here
			// I need to see if the connection pool has been initiated
			// if not initiate the connection pool
			if(prop == null) {
				prop = Utility.loadProperties(propFile);
				if(!prop.containsKey("TEMP")) { // if this is not a temp then open the super
					super.openDB(propFile); // this primarily loads the metahelper as well as the insights to get it going
				}
			}

			// load everything from file
			fileDB = prop.getProperty(DATA_FILE);
			Map <String, String> paramHash = new Hashtable<String, String>();

			paramHash.put("BaseFolder", DIHelper.getInstance().getProperty("BaseFolder"));
			paramHash.put("ENGINE", getEngineId());
			fileDB = Utility.fillParam2(fileDB, paramHash);

			Vector <String> concepts = this.getConcepts();
			// usually there should be only one, but just a check again
			// need to account for concept being itself
			if(concepts.size() > 2)
			{
				LOGGER.fatal("RDBMS Engine suggests to use file, but there are more than one concepts i.e. this is not a flat file");
				return;
			}

			String [] conceptsArray = concepts.toArray(new String[concepts.size()]);
			Map <String,String> conceptAndType = this.getDataTypes(conceptsArray);
			for(int conceptIndex = 0;conceptIndex < conceptsArray.length;conceptIndex++)
			{
				List <String> propList = getPropertyUris4PhysicalUri(conceptsArray[conceptIndex]);
				if(propList.size() > 0) {
					fakeHeader = conceptsArray[conceptIndex];
				}
				String [] propArray = propList.toArray(new String[propList.size()]);

				Map<String, String> typeMap = getDataTypes(propArray);
				conceptAndType.putAll(typeMap);
			}

			// convert it to a data type
			Map <String, SemossDataType> conceptMetaMap = new Hashtable<String, SemossDataType>();
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

				SemossDataType newType = SemossDataType.convertStringToDataType(thisType);
				conceptMetaMap.put(thisKey, newType);
			}

			CsvQueryStruct cqs = new CsvQueryStruct();
			cqs.setFilePath(fileDB);
			cqs.setColumnTypes(conceptAndType);

			// the filte iterator will rip through the whole thing anyways
			CsvFileIterator fit = new CsvFileIterator(cqs);
			String dbName = fileDB.replace(".csv", "");
			dbName = dbName.replace(".tsv", "");
			// delete the database if it exists to start with

			rvarName = Utility.getRandomString(6);

			//rvarName = Utility.cleanString(fileDB, true);
			//				builder.createTableViaIterator(rvarName, fit, conceptMetaMap);
			dt = new RDataTable(rJavaTranslator, rvarName);
			dt.addRowsViaIterator(fit, rvarName, conceptMetaMap);
			fakeHeader = Utility.getInstanceName(fakeHeader);
			dt.generateRowIdWithName();//fakeHeader);
			//fakeHeader = null;

			// process is complete at this point
			// at this point I will just have a builder and I am good to go
		}
	}	

	@Override
	// need to clean up the exception it will never be thrown
	public void insertData(String query) 
	{
		// there is no insert data on R not with queries
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

		RIterator it = (RIterator) dt.query(query);
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
	public boolean isConnected() {
		return true;
	}

	@Override
	public void closeDB() {
		dt.closeConnection();
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

	public void deleteDB() {
		LOGGER.debug("Deleting RDBMS Engine: " + this.engineName);
		try {
			closeDB();
			DeleteDbFiles.execute(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + this.engineName, "database", false);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Clean up SMSS and DB files/folder
		super.deleteDB();
	}

	@Override
	public IQueryInterpreter getQueryInterpreter(){
		RInterpreter retInterp = new RInterpreter();
		if(fakeHeader != null) {
			retInterp.addHeaderToRemove(fakeHeader);
		}
		retInterp.setDataTableName(rvarName);
		return retInterp;
	}
}
