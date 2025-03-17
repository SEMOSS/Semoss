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
package prerna.engine.impl.rdf;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BooleanQuery;
import org.eclipse.rdf4j.query.GraphQuery;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.inferencer.fc.SchemaCachingRDFSInferencer;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDFDatabase;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * References the RDF source and uses the Eclipse RDF4J API to query a database stored in an RDF file
 */
public class EclipseRDF4JFileEngine extends AbstractDatabaseEngine implements IRDFDatabase {

	private static final Logger classLogger = LogManager.getLogger(EclipseRDF4JFileEngine.class);

	private Repository myRepository = null;
	private RepositoryConnection rc = null;
	private ValueFactory vf = null;
	
	private String rdfFileType = "RDF/XML";
	private String baseURI = "http://semoss.org/ontologies";
	private String filePath = null;
	private boolean connected = false;
	
	/**
	 * Opens a database as defined by its properties file.  What is included in the properties file is dependent on the type of 
	 * engine that is being initiated.  This is the function that first initializes an engine with the property file at the very 
	 * least defining the data store.
	 * @param smssFilePath contains all information regarding the data store and how the engine should be instantiated.  Dependent on 
	 * what type of engine is being instantiated.
	 * @throws Exception 
	 */
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		String dataDir = EngineUtility.getSpecificEngineBaseFolder(this.getCatalogType(), this.getEngineId(), this.getEngineName());
		dataDir += "/data";
		this.myRepository = new SailRepository(new SchemaCachingRDFSInferencer (new NativeStore(new File(dataDir))));
		this.myRepository.init();

		// you can technically set the filename directly
		// so we will still check that the smssProp is not null/empty
//		if(this.smssProp != null && !this.smssProp.isEmpty()) {
//			if(this.smssProp.containsKey(Constants.RDF_FILE_PATH)) {
//				this.filePath = this.smssProp.getProperty(Constants.RDF_FILE_PATH);
//			} else {
//				File f = SmssUtilities.getRdfFile(this.smssProp);
//				if(f != null) {
//					this.filePath = f.getAbsolutePath();
//				}
//			}
//			
//			if(this.smssProp.containsKey(Constants.RDF_FILE_TYPE)) {
//				this.rdfFileType = this.smssProp.getProperty(Constants.RDF_FILE_TYPE);
//			}
//
//			if(this.smssProp.containsKey(Constants.RDF_FILE_BASE_URI)) {
//				this.baseURI = this.smssProp.getProperty(Constants.RDF_FILE_BASE_URI);
//			}
//		}

		rc = myRepository.getConnection();
		vf = rc.getValueFactory();

//		loadFile();
		this.connected = true;
	}
	
	private void loadFile() throws IOException, RDFParseException, RepositoryException {
		if(this.filePath != null) {
			File file = new File(Utility.normalizePath(filePath));
			if(!(file.exists() && file.isFile())) {
				classLogger.warn("Calling open for RDFFileSesameEngine with file path set but file does not exist");
			} else {
				if(rdfFileType.equalsIgnoreCase("RDF/XML")) rc.add(file, baseURI, RDFFormat.RDFXML);
				else if(rdfFileType.equalsIgnoreCase("TURTLE")) rc.add(file, baseURI, RDFFormat.TURTLE);
				else if(rdfFileType.equalsIgnoreCase("BINARY")) rc.add(file, baseURI, RDFFormat.BINARY);
				else if(rdfFileType.equalsIgnoreCase("N3")) rc.add(file, baseURI, RDFFormat.N3);
				else if(rdfFileType.equalsIgnoreCase("NTRIPLES")) rc.add(file, baseURI, RDFFormat.NTRIPLES);
				else if(rdfFileType.equalsIgnoreCase("TRIG")) rc.add(file, baseURI, RDFFormat.TRIG);
				else if(rdfFileType.equalsIgnoreCase("TRIX")) rc.add(file, baseURI, RDFFormat.TRIX);
				else throw new IOException("Unable to load RDF file type = " + rdfFileType);
			}
		}
	}

	/**
	 * Closes the data base associated with the engine.  This will prevent further changes from being made in the data store and 
	 * safely ends the active transactions and closes the engine.
	 * @throws IOException 
	 */
	@Override
	public void close() throws IOException {
		super.close();
		try {
			this.rc.close();
			this.myRepository.shutDown();
			this.connected = false;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	public void reloadFile() throws Exception {
		close();
		open(this.smssProp);
	}
	
	@Override
	public boolean holdsFileLocks() {
		return true;
	}

	/**
	 * Runs the passed string query against the engine as a SELECT query.  The query passed must be in the structure of a SELECT 
	 * SPARQL query and the result format will depend on the engine type.
	 * @param query the string version of the SELECT query to be run against the engine

	 * @return triple query results that can be displayed as a grid */
	@Override
	public Object execQuery(String query) {
		try {
			Query fullQuery = rc.prepareQuery(QueryLanguage.SPARQL, query);
			classLogger.debug("\nSPARQL: " + Utility.cleanLogString(query));
			fullQuery.setIncludeInferred(true /* includeInferred */);
			if(fullQuery instanceof TupleQuery){
				TupleQueryResult sparqlResults = ((TupleQuery) fullQuery).evaluate();
				return sparqlResults;
			}
			else if (fullQuery instanceof GraphQuery){
				GraphQueryResult res = ((GraphQuery) fullQuery).evaluate();
				return res;
			}
			else if (fullQuery instanceof BooleanQuery){
				Boolean bool = ((BooleanQuery) fullQuery).evaluate();
				return bool;
			}
		} catch (RepositoryException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (MalformedQueryException mqe) {
			classLogger.error(Constants.STACKTRACE, mqe);
		} catch (QueryEvaluationException qee) {
			classLogger.error(Constants.STACKTRACE, qee);
		}
		return null;
	}

	@Override
	public DATABASE_TYPE getDatabaseType() {
		return IDatabaseEngine.DATABASE_TYPE.RDF4J;
	}

	/**
	 * Processes a SELECT query just like {@link #execSelectQuery(String)} but gets the results in the exact format that the database stores them.
	 * This is important for things like param values so that we can take the returned value and fill the main query without needing modification
	 * @param sparqlQuery the SELECT SPARQL query to be run against the engine
	 * @return the Vector of Strings representing the full uris of all of the query results */
	public Vector<Object> getCleanSelect(String sparqlQuery)
	{
		try {
			TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
			classLogger.debug("\nSPARQL: " + sparqlQuery);
			tq.setIncludeInferred(true /* includeInferred */);
			TupleQueryResult sparqlResults = tq.evaluate();
			Vector<Object> retVec = new Vector<>();
			while(sparqlResults.hasNext()) {
				Value val = sparqlResults.next().getValue(Constants.ENTITY);
				Object next = null;
				if(val instanceof Literal){
					Literal literal = ((Literal)val);
					IRI dataType = literal.getDatatype();
					if(dataType.getLocalName().equals("integer")) {
						next = literal.intValue();
					} else if(dataType.getLocalName().equals("double")) {
						next = literal.doubleValue();
					} else if(dataType.getLocalName().equals("float")) {
				        next = literal.floatValue();
					} else if(dataType.getLocalName().equalsIgnoreCase("boolean")) {
				        next = literal.booleanValue();
					} else if(dataType.getLocalName().equalsIgnoreCase("dateTime")) {
						next = Date.from(literal.calendarValue().toGregorianCalendar().toInstant());
					} else if(dataType.getLocalName().equalsIgnoreCase("date")) {
						next = Date.from(literal.calendarValue().toGregorianCalendar().toInstant());
					} else {
						next = ((Literal)val).getLabel();
					}
				} else {
					next = "" + val;
				}
				retVec.add(next);
			}
			return retVec;
		} catch (RepositoryException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (MalformedQueryException mqe) {
			classLogger.error(Constants.STACKTRACE, mqe);
		} catch (QueryEvaluationException qee) {
			classLogger.error(Constants.STACKTRACE, qee);
		}
		return null;
	}

	/**
	 * Uses a type URI to get the URIs of all instances of that type. These instance URIs are returned as the Vector of Strings.
	 * @param type The full URI of the node type that we want to get the instances of
	 * @return the Vector of Strings representing the full uris of all of the instances of the passed in type */
	public Vector<Object> getEntityOfType(String type)
	{
		// Get query from smss
		// If the query is not there, get from RDFMap
		// Fill query with type
		// run through getCleanSelect()
		String query = this.getProperty(Constants.TYPE_QUERY);
		if(query==null){
			query = Utility.getDIHelperProperty(Constants.TYPE_QUERY);
		}
		Map<String, List<Object>> paramHash = new Hashtable<>();
		List<Object> retList = new ArrayList<>();
		retList.add(type);
		paramHash.put("entity", retList);
		query = Utility.fillParam(query, paramHash);

		return getCleanSelect(query);
	}

	/**
	 * Returns whether or not an engine is currently connected to the data store.  The connection becomes true when {@link #open(String)} 
	 * is called and the connection becomes false when {@link #close()} is called.

	 * @return true if the engine is connected to its data store and false if it is not */
	@Override
	public boolean isConnected() {
		return connected;
	}

	@Override
	public void bulkInsert(List<Object[]> args) {
		try {
			if(!rc.isActive()) {
				rc.begin();
			}
			for(Object[] obj : args) {
				processTriple(obj, true);
			}
			rc.commit();
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			rc.rollback();
		}
	}

	@Override
	public void bulkRemoval(List<Object[]> args) {
		try {
			if(!rc.isActive()) {
				rc.begin();
			}
			for(Object[] obj : args) {
				processTriple(obj, false);
			}
			rc.commit();
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			rc.rollback();
		}
	}
	
	@Override
	public void addStatement(Object[] args) {
		processStatement(args, true);
	}

	@Override
	public void removeStatement(Object[] args) {
		processStatement(args, false);
	}
	
	/**
	 * Handles the transaction for a single triple to be added/removed from the dataset
	 * @param args array contains the following
	 * 				subject String - RDF Subject
	 * 				predicate String - RDF Predicate
	 * 				object Object - RDF Object
	 * 				concept boolean - True if the statement is a concept (URI), False if it is a property (Literal)
	 * @param add	if we are adding or removing the triple
	 */
	private void processStatement(Object[] args, boolean add) {
		try {
			if(!rc.isActive()) {
				rc.begin();
			}
			processTriple(args, add);
			rc.commit();
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			rc.rollback();
		}
	}
	
	/**
	 * Adds or removes a single triple - no transaction management
	 * @param args array contains the following
	 * 				subject String - RDF Subject
	 * 				predicate String - RDF Predicate
	 * 				object Object - RDF Object
	 * 				concept boolean - True if the statement is a concept (URI), False if it is a property (Literal)
	 * @param add	if we are adding or removing the triple
	 */
	private void processTriple(Object[] args, boolean add) {
		String subject = args[0]+"";
		String predicate = args[1]+"";
		Object object = args[2];
		Boolean concept = (Boolean) args[3];

		IRI newSub = null;
		IRI newPred = null;
		String subString = null;
		String predString = null;
		String sub = subject.trim();
		String pred = predicate.trim();

		subString = Utility.cleanString(sub, false);
		newSub = vf.createIRI(subString);

		predString = Utility.cleanString(pred, false);
		newPred = vf.createIRI(predString);

		Value newObj = null;
		if(concept) {
			newObj = vf.createIRI(Utility.cleanString((object + "").trim(), false));
		} else {
			if(object.getClass() == new Double(1).getClass())
			{
				classLogger.debug("Found Double " + object);
				newObj = vf.createLiteral(((Double)object).doubleValue());
			}
			else if(object.getClass() == new Date(1).getClass())
			{
				classLogger.debug("Found Date " + object);
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				String date = df.format(object);
				IRI datatype = vf.createIRI("http://www.w3.org/2001/XMLSchema#dateTime");
				newObj = vf.createLiteral(date, datatype);
			}
			else
			{
				classLogger.debug("Found String " + object);
				String value = object + "";
				newObj = vf.createLiteral(value);
			}
		}
		if(add) {
			rc.add(newSub, newPred, newObj);
		} else {
			rc.remove(newSub, newPred, newObj);
		}
	}
	

	/**
	 * Runs the passed string query against the engine as an INSERT query.  The query passed must be in the structure of an INSERT 
	 * SPARQL query or an INSERT DATA SPARQL query 
	 * and there are no returned results.  The query will result in the specified triples getting added to the 
	 * data store.
	 * @param query the INSERT or INSERT DATA SPARQL query to be run against the engine
	 */
	@Override
	public void insertData(String query) {
		Update up;
		try {
			up = rc.prepareUpdate(QueryLanguage.SPARQL, query);
			classLogger.debug("\nSPARQL: " + query);
			rc.begin();
			up.execute();
			rc.commit();
		} catch (RepositoryException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (MalformedQueryException mqe) {
			classLogger.error(Constants.STACKTRACE, mqe);
		} catch (SailException se) {
			classLogger.error(Constants.STACKTRACE, se);
		} catch (UpdateExecutionException uee) {
			classLogger.error(Constants.STACKTRACE, uee);
		}
	}

	/**
	 * Method exportDB.  Exports the repository connection to the RDF database.
	 * @throws IOException 
	 * @throws RDFHandlerException 
	 * @throws RepositoryException 
	 */
	//TODO: need to combine this with commit()
//	public void exportDB() throws Exception {
//		classLogger.info("Exporting database " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
//		RDFXMLWriter rdfWriter = null;
//		try (OutputStreamWriter writer = new OutputStreamWriter(
//				new FileOutputStream(Utility.normalizePath(filePath)), StandardCharsets.UTF_8)){
//			rdfWriter = new RDFXMLWriter(writer);
//			rc.export(rdfWriter);
//		}
//	}
	
	public void setFilePath(String filePath){
		this.filePath = filePath;
	}

	public String getFilePath() {
		return filePath;
	}
	
	public void deleteFile() {
		File f = new File(this.filePath);
		if(f.exists() && f.isFile()) {
			f.delete();
		}
	}
	
	@Override
	public void removeData(String query) {
		insertData(query);
	}

	@Override
	public void commit() {
		try {
			rc.commit();
			classLogger.info("Exporting database " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
//			RDFXMLWriter rdfWriter = null;
//			try (OutputStreamWriter writer = new OutputStreamWriter(
//					new FileOutputStream(Utility.normalizePath(filePath)), StandardCharsets.UTF_8)){
//				rdfWriter = new RDFXMLWriter(writer);
//				rc.export(rdfWriter);
//			}
		} catch (SailException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
//		catch (IOException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
	}
	
	/**
	 * Method infer.	
	 */
	public void infer() {
		// do nothing
	}

	@Override
	public void exportDB() throws Exception {
		// TODO Auto-generated method stub
		
	}

}
