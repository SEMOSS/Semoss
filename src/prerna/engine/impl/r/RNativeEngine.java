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

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RIterator;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.sablecc2.reactor.imports.RImporter;
import prerna.util.Utility;

public class RNativeEngine extends AbstractEngine {

	private static final Logger LOGGER = LogManager.getLogger(RNativeEngine.class.getName());

	private File file = null;
	private String fileLocation = null;
	private Map<String, String> columnToType = null;
	private Map<String, String> additionalDataType = null;
	// to store based on columnToType
	private Map<String, SemossDataType> columnTypes = null;
	
	private String dtName;
	private Insight in;
	private AbstractRJavaTranslator rJavaTranslator;
	private RDataTable dt;
	
	@Override
	public void openDB(String propFile) {
		super.openDB(propFile);

		this.file = SmssUtilities.getDataFile(this.prop);
		this.fileLocation = this.file.getAbsolutePath().replace('\\', '/');

		List<String> concepts = this.getConcepts();
		// usually there should be only one, but just a check again
		// need to account for concept being itself
		if(concepts.size() > 2) {
			throw new IllegalArgumentException("Cannot support more than 1 table in R Native Engine");
		}

		String tableUri = concepts.get(0);
		this.dtName = Utility.getInstanceName(tableUri);

		List<String> propertyUris = this.getPropertyUris4PhysicalUri(tableUri);
		String[] propertyUriArr = propertyUris.toArray(new String[] {});
		this.columnToType = this.getDataTypes(propertyUriArr);
		this.additionalDataType = this.getAdtlDataTypes(propertyUriArr);
		
		this.in = new Insight();
		this.rJavaTranslator = RJavaTranslatorFactory.getRJavaTranslator(this.in, LOGGER);
		
		CsvQueryStruct cqs = new CsvQueryStruct();
		cqs.setFilePath(this.fileLocation);
		cqs.setColumnTypes(this.columnToType);
		cqs.setAdditionalTypes(additionalDataType);
		CsvFileIterator iterator = new CsvFileIterator(cqs);
		
		this.dt = new RDataTable(rJavaTranslator, this.dtName);
		RImporter importer = new RImporter(dt, cqs, iterator);
		importer.insertData();
		
		// store the data types
		this.columnTypes = this.dt.getMetaData().getHeaderToTypeMap();
	}

	@Override
	// need to clean up the exception it will never be thrown
	public void insertData(String query) {
		// there is no insert data on R not with queries
	}

	@Override
	public ENGINE_TYPE getEngineType() {
		return IEngine.ENGINE_TYPE.R;
	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		// need to confirm if it is only legacy or we should implement
		// need a way to maintain 

		return null;
	}

	/**
	 * This returns a RIterator
	 */
	@Override
	public Object execQuery(String query) {
		RIterator exec = new RIterator(this.dt.getBuilder(), query);
		return exec;
	}
	
	public Object execQuery(String query, SelectQueryStruct qs) {
		RIterator exec = new RIterator(this.dt.getBuilder(), query, qs);
		return exec;
	}

	@Override
	public boolean isConnected() {
		return true;
	}

	@Override
	public void closeDB() {
		this.dt.close();
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
		LOGGER.debug("Deleting R Engine: " + this.engineName + "__" + this.engineId);
		try {
			closeDB();
		} catch (Exception e) {
			e.printStackTrace();
		}
		// clean up SMSS and DB files/folder
		super.deleteDB();
	}

	@Override
	public IQueryInterpreter getQueryInterpreter(){
		RInterpreter retInterp = new RInterpreter();
		retInterp.setDataTableName(this.dtName);
		retInterp.setColDataTypes(this.columnTypes);
		retInterp.setAdditionalTypes(this.additionalDataType);
		return retInterp;
	}
	
	/**
	 * Load data from this R env to another R env
	 * This is used for performance enhancements when moving from engine to frame
	 * @param otherTranslator
	 * @param assignVar
	 * @param rScript
	 */
	public void directLoad(AbstractRJavaTranslator otherTranslator, String assignVar, String rScript) {
		AbstractRJavaTranslator.loadDataBetweenEnv(otherTranslator, assignVar, this.rJavaTranslator, rScript);
	}
}
