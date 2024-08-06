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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
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
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.rdfxml.RDFXMLWriter;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISesameRdfEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * References the RDF source and uses the Sesame API to query a database stored in an RDF file
 */
public class RDFFileSesameEngine extends AbstractDatabaseEngine implements ISesameRdfEngine {

	private static final Logger classLogger = LogManager.getLogger(RDFFileSesameEngine.class);

	private RepositoryConnection rc = null;
	private SailConnection sc = null;
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
		Repository myRepository = new SailRepository(
				new ForwardChainingRDFSInferencer(
						new MemoryStore()));
		myRepository.initialize();
		
		// you can technically set the filename directly
		// so we will still check that the smssProp is not null/empty
		if(this.smssProp != null && !this.smssProp.isEmpty()) {
			if(this.smssProp.containsKey(Constants.RDF_FILE_PATH)) {
				this.filePath = this.smssProp.getProperty(Constants.RDF_FILE_PATH);
			} else {
				File f = SmssUtilities.getRdfFile(this.smssProp);
				if(f != null) {
					this.filePath = f.getAbsolutePath();
				}
			}
			
			if(this.smssProp.containsKey(Constants.RDF_FILE_TYPE)) {
				this.rdfFileType = this.smssProp.getProperty(Constants.RDF_FILE_TYPE);
			}

			if(this.smssProp.containsKey(Constants.RDF_FILE_BASE_URI)) {
				this.baseURI = this.smssProp.getProperty(Constants.RDF_FILE_BASE_URI);
			}
		}

		rc = myRepository.getConnection();
		sc = ((SailRepositoryConnection) rc).getSailConnection();
		vf = rc.getValueFactory();

		loadFile();
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
			rc.close();
			connected = false;
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
	public DATABASE_TYPE getDatabaseType()
	{
		return IDatabaseEngine.DATABASE_TYPE.SESAME;
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
					URI dataType = literal.getDatatype();
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

	/**
	 * Method addStatement. Processes a given subject, predicate, object triple and adds the statement to the SailConnection.
	 * @param subject String - RDF Subject
	 * @param predicate String - RDF Predicate
	 * @param object Object - RDF Object
	 * @param concept boolean - True if the statement is a concept
	 */
	//	public void addStatement(String subject, String predicate, Object object, boolean concept)
	public void addStatement(Object[] args)
	{
		String subject = args[0]+"";
		String predicate = args[1]+"";
		Object object = args[2];
		Boolean concept = (Boolean) args[3];
		//logger.info("Updating Triple " + subject + "<>" + predicate + "<>" + object);
		try {
			URI newSub = null;
			URI newPred = null;
			String subString = null;
			String predString = null;
			String sub = subject.trim();
			String pred = predicate.trim();

			//System.err.println("VF is " + vf);
			if(!rc.isActive()) {
				rc.begin();
			}

			subString = Utility.cleanString(sub, false);
			newSub = vf.createURI(subString);

			predString = Utility.cleanString(pred, false);
			newPred = vf.createURI(predString);

			if(!concept)
			{
				if(object.getClass() == new Double(1).getClass())
				{
					classLogger.debug("Found Double " + object);
					sc.addStatement(newSub, newPred, vf.createLiteral(((Double)object).doubleValue()));
				}
				else if(object.getClass() == new Date(1).getClass())
				{
					classLogger.debug("Found Date " + object);
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
					String date = df.format(object);
					URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
					sc.addStatement(newSub, newPred, vf.createLiteral(date, datatype));
				}
				else
				{
					classLogger.debug("Found String " + object);
					String value = object + "";
					// try to see if it already has properties then add to it
					//String cleanValue = value.replaceAll("/", "-").replaceAll("\"", "'");			
					sc.addStatement(newSub, newPred, vf.createLiteral(value));
				}
			}
			else {
				URI newObj = vf.createURI(Utility.cleanString((object + "").trim(), false));
				sc.addStatement(newSub, newPred, newObj);
			}
			rc.commit();
		} catch (SailException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (RepositoryException re) {
			classLogger.error(Constants.STACKTRACE, re);
		}
	}

	/**
	 * Method removeStatement. Processes a given subject, predicate, object triple and adds the statement to the SailConnection.
	 * @param subject String - RDF Subject
	 * @param predicate String - RDF Predicate
	 * @param object Object - RDF Object
	 * @param concept boolean - True if the statement is a concept
	 */
	//	public void removeStatement(String subject, String predicate, Object object, boolean concept)
	public void removeStatement(Object[] args)
	{
		String subject = args[0]+"";
		String predicate = args[1]+"";
		Object object = args[2];
		Boolean concept = (Boolean) args[3];
		//logger.info("Updating Triple " + subject + "<>" + predicate + "<>" + object);
		try {
			URI newSub = null;
			URI newPred = null;
			String subString = null;
			String predString = null;
			String sub = subject.trim();
			String pred = predicate.trim();

			//System.err.println("VF is " + vf);
			sc.begin();


			subString = Utility.cleanString(sub, false);
			newSub = vf.createURI(subString);

			predString = Utility.cleanString(pred, false);
			newPred = vf.createURI(predString);

			URI uriObj = null;
			try{
				uriObj = vf.createURI(object+"");
			}catch(IllegalArgumentException e){
				// ignore exception
			}

			if(!concept || uriObj == null)
			{
				if(object.getClass() == new Double(1).getClass())
				{
					classLogger.debug("Found Double " + object);
					sc.removeStatements(newSub, newPred, vf.createLiteral(((Double)object).doubleValue()));
				}
				else if(object.getClass() == new Date(1).getClass())
				{
					classLogger.debug("Found Date " + object);
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
					String date = df.format(object);
					URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
					sc.removeStatements(newSub, newPred, vf.createLiteral(date, datatype));
				}
				else
				{
					classLogger.debug("Found String " + object);
					String value = object + "";
					// try to see if it already has properties then add to it
					//String cleanValue = value.replaceAll("/", "-").replaceAll("\"", "'");			
					sc.removeStatements(newSub, newPred, vf.createLiteral(value));
				}
			}
			else
			{
				sc.removeStatements(newSub, newPred, uriObj);
			}
			sc.commit();
		} catch (SailException e) {
			classLogger.error(Constants.STACKTRACE, e);
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
			//sc.addStatement(vf.createURI("<http://semoss.org/ontologies/Concept/Service/tom2>"),vf.createURI("<http://semoss.org/ontologies/Relation/Exposes>"),vf.createURI("<http://semoss.org/ontologies/Concept/BusinessLogicUnit/tom1>"));
			classLogger.debug("\nSPARQL: " + query);
			//tq.setIncludeInferred(true /* includeInferred */);
			//tq.evaluate();
			//rc.setAutoCommit(false);
			sc.begin();
			up.execute();
			//rc.commit();
			sc.commit();
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
	@Override
	@Deprecated
	//TODO: need to combine this with commit()
	public void exportDB() throws Exception {
		classLogger.info("Exporting database " + SmssUtilities.getUniqueName(this.engineName, this.engineId));
		RDFXMLWriter rdfWriter = null;
		try (OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(Utility.normalizePath(filePath)), StandardCharsets.UTF_8)){
			rdfWriter = new RDFXMLWriter(writer);
			rc.export(rdfWriter);
		}
	}
	
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
			sc.commit();
		} catch (SailException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	@Override
	public void infer() {
		// do nothing
	}
	
	@Override
	public RepositoryConnection getRc() {
		return this.rc;
	}

	@Override
	public void setRc(RepositoryConnection rc) {
		this.rc = rc;
	}

	@Override
	public void setSc(SailConnection sc) {
		this.sc = sc;
	}
	
	@Override
	public SailConnection getSc() {
		return this.sc;
	}


	@Override
	public ValueFactory getVf() {
		return this.vf;
	}

	@Override
	public void setVf(ValueFactory vf) {
		this.vf = vf;
	}
}
