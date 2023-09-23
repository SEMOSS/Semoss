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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RIterator;
import prerna.ds.util.flatfile.CsvFileIterator;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.sablecc2.reactor.imports.RImporter;
import prerna.util.CSVToOwlMaker;
import prerna.util.Constants;
import prerna.util.Utility;

public class RNativeEngine extends AbstractDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(RNativeEngine.class.getName());

	private File file = null;
	private String fileLocation = null;
	private Map<String, String> columnToType = null;
	private Map<String, String> additionalDataType = null;
	private Map<String, String> newHeaders = null;
	// to store based on columnToType
	private Map<String, SemossDataType> columnTypes = null;
	
	private String dtName;
	private Insight in;
	private AbstractRJavaTranslator rJavaTranslator;
	private RDataTable dt;
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.file = SmssUtilities.getDataFile(this.smssProp);
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
		String[] propertyUriArr = propertyUris.toArray(new String[propertyUris.size()]);
		String typeMapStr = this.smssProp.getProperty(Constants.SMSS_DATA_TYPES);
		if (typeMapStr != null && !typeMapStr.trim().isEmpty()) {
			try {
				this.columnToType = new ObjectMapper().readValue(typeMapStr, Map.class);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		} else {
			this.columnToType = this.getDataTypes(propertyUriArr);
		}
		
		String addTypeStr = this.smssProp.getProperty(Constants.ADDITIONAL_DATA_TYPES);
		if (addTypeStr != null && !addTypeStr.trim().isEmpty()) {
			try {
				this.additionalDataType = new ObjectMapper().readValue(addTypeStr, Map.class);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		} else {
			this.additionalDataType = this.getAdtlDataTypes(propertyUriArr);
		}

		String newHeadersStr = this.smssProp.getProperty(Constants.NEW_HEADERS);
		if (newHeadersStr != null && !newHeadersStr.trim().isEmpty()) {
			try {
				this.newHeaders = new ObjectMapper().readValue(newHeadersStr, Map.class);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		
		this.in = new Insight();
		this.rJavaTranslator = RJavaTranslatorFactory.getRJavaTranslator(this.in, classLogger);
		
		CsvQueryStruct cqs = new CsvQueryStruct();
		cqs.setFilePath(this.fileLocation);
		cqs.setColumnTypes(this.columnToType);
		cqs.setAdditionalTypes(this.additionalDataType);
		cqs.setNewHeaderNames(this.newHeaders);
		CsvFileIterator iterator = new CsvFileIterator(cqs);
		
		this.dt = new RDataTable(rJavaTranslator, this.dtName);
		RImporter importer = new RImporter(dt, cqs, iterator);
		importer.insertData();
		
		// store the data types
		this.columnTypes = this.dt.getMetaData().getHeaderToTypeMap();
	}
	
	/**
	 * Generate the OWL based on a flat file
	 * @param dataFile
	 * @param owlFile
	 * @param owlFileName
	 * @return
	 * @throws Exception 
	 */
	protected String generateOwlFromFlatFile(String engineId, String dataFile, String owlFile, String owlFileName) throws Exception {
		CSVToOwlMaker maker = new CSVToOwlMaker();
		maker.makeFlatOwl(engineId, dataFile, owlFile, getDatabaseType(), false);
		if(owlFile.equals("REMAKE")) {
			try {
				Utility.changePropertiesFileValue(this.smssFilePath, Constants.OWL, owlFileName);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return owlFile;
	}

	@Override
	// need to clean up the exception it will never be thrown
	public void insertData(String query) {
		// there is no insert data on R not with queries
	}

	@Override
	public DATABASE_TYPE getDatabaseType() {
		return IDatabaseEngine.DATABASE_TYPE.R;
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
	public void close() throws IOException {
		this.dt.close();
		super.close();
	}

	@Override
	public void removeData(String query) {
		// not implemented for R
	}

	@Override
	public void commit() {
		// not implemented for R
	}

	@Override
	public IQueryInterpreter getQueryInterpreter(){
		RInterpreter retInterp = new RInterpreter();
		retInterp.setDataTableName(this.dtName);
		retInterp.setColDataTypes(this.columnTypes);
//		retInterp.setAdditionalTypes(this.additionalDataType);
		return retInterp;
	}
	
	/**
	 * Reload the file to generate the frame
	 */
	public void reloadFile() {
		CsvQueryStruct cqs = new CsvQueryStruct();
		cqs.setFilePath(this.fileLocation);
		cqs.setColumnTypes(this.columnToType);
		cqs.setAdditionalTypes(this.additionalDataType);
		cqs.setNewHeaderNames(this.newHeaders);
		CsvFileIterator iterator = new CsvFileIterator(cqs);
		
		// the insertData will generate a new variable
		RImporter importer = new RImporter(this.dt, cqs, iterator);
		importer.insertData();
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
	
	@Override
	public boolean holdsFileLocks() {
		return false;
	}
	
}
