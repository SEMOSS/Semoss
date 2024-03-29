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

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * References the RDF source and uses the Jena API to query a database stored in an RDF file
 */
public class RDFFileJenaEngine extends AbstractDatabaseEngine {
	
	private static final Logger classLogger = LogManager.getLogger(RDFFileJenaEngine.class);

	private Model jenaModel = null;
	private String propFile = null;
	private boolean connected = false;
	
	private String fileLocation = null;
	private String baseURI = null;
	private String rdfFileType = null;
	private Lang type = null;
	
	/**
	 * Opens a database as defined by its properties file.  What is included in the properties file is dependent on the type of 
	 * engine that is being initiated.  This is the function that first initializes an engine with the property file at the very 
	 * least defining the data store.
	 * @param smssProp contains all information regarding the data store and how the engine should be instantiated.  Dependent on 
	 * what type of engine is being instantiated.
	 * @throws Exception 
	 */
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		this.fileLocation = smssProp.getProperty(Constants.RDF_FILE_NAME);
		this.baseURI = smssProp.getProperty(Constants.RDF_FILE_BASE_URI);
		this.rdfFileType = smssProp.getProperty(Constants.RDF_FILE_TYPE);
		this.type = determineLang(rdfFileType);
		this.jenaModel = ModelFactory.createDefaultModel();
		RDFDataMgr.read(this.jenaModel, this.fileLocation, this.baseURI, this.type);
		this.connected = true;
	}
	
	/**
	 * Closes the data base associated with the engine.  This will prevent further changes from being made in the data store and 
	 * safely ends the active transactions and closes the engine.
	 * @throws IOException 
	 */
	@Override
	public void close() throws IOException {
		super.close();
		this.jenaModel.close();
		classLogger.info("Closing the database to the file " + Utility.cleanLogString(propFile));		
	}

	/**
	 * Runs the passed string query against the engine as a SELECT query.  The query passed must be in the structure of a SELECT 
	 * SPARQL query and the result format will depend on the engine type.
	 * @param query the string version of the SELECT query to be run against the engine
	
	 * @return triple query results that can be displayed as a grid */
	@Override
	public Object execQuery(String query) {
		Query q2 = QueryFactory.create(query); 
		QueryExecution qexec = QueryExecutionFactory.create(q2, jenaModel) ;
		if(q2.isSelectType()){
			ResultSet rs = qexec.execSelect();
			return rs;
		}
		else if(q2.isConstructType()){
			Model resultModel = qexec.execConstruct() ;
			classLogger.info("Executing the RDF File Graph Query " + Utility.cleanLogString(query));
			return resultModel;
		}
		else if(q2.isAskType()){
			Boolean bool = qexec.execAsk() ;
			classLogger.info("Executing the RDF File ASK Query " + Utility.cleanLogString(query));
			return bool;
		}
		else {
			return null;
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
		UpdateRequest request = UpdateFactory.create();
		request.add(query);
		UpdateAction.execute(request, this.jenaModel);
	}

	@Override
	public void removeData(String query) {
		UpdateRequest request = UpdateFactory.create();
		request.add(query);
		UpdateAction.execute(request, this.jenaModel);
	}
	
	@Override
	public DATABASE_TYPE getDatabaseType() {
		return IDatabaseEngine.DATABASE_TYPE.JENA;
	}
	
	@Override
	public boolean holdsFileLocks() {
		return true;
	}

	/**
	 * Processes a SELECT query just like {@link #execSelectQuery(String)} but gets the results in the exact format that the database stores them.
	 * This is important for things like param values so that we can take the returned value and fill the main query without needing modification
	 * @param sparqlQuery the SELECT SPARQL query to be run against the engine
	 * @return the Vector of Strings representing the full uris of all of the query results */
	public Vector<Object> getCleanSelect(String sparqlQuery)
	{
		// run the query 
		// convert to string
		Vector <Object> retString = new Vector<Object>();
		ResultSet rs = (ResultSet)execQuery(sparqlQuery);
		
		// gets only the first variable
		Iterator<String> varIterator = rs.getResultVars().iterator();
		String varName = varIterator.next();
		while(rs.hasNext())
		{
			QuerySolution row = rs.next();
			retString.addElement(row.get(varName)+"");
		}
		return retString;
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
		if(query == null){
			query = DIHelper.getInstance().getProperty(Constants.TYPE_QUERY);
		}
		Map<String, List<Object>> paramHash = new Hashtable<String, List<Object>>();
		List<Object> retList = new ArrayList<Object>();
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
	 * 
	 * @param rdfFileType
	 * @return
	 */
	private Lang determineLang(String rdfFileType) {
		if(rdfFileType.equalsIgnoreCase("RDF/XML")) return Lang.RDFXML;
		else if(rdfFileType.equalsIgnoreCase("TURTLE")) return Lang.TURTLE;
		else if(rdfFileType.equalsIgnoreCase("N3")) return Lang.N3;
		else if(rdfFileType.equalsIgnoreCase("NTRIPLES")) return Lang.NTRIPLES;
		else if(rdfFileType.equalsIgnoreCase("TRIG")) return Lang.TRIG;
		else if(rdfFileType.equalsIgnoreCase("TRIX")) return Lang.TRIX;
		
		return null;
	}
	
	@Override
	public void commit() {
		this.jenaModel.commit();
	}

	
	/**
	 * Method addStatement. Processes a given subject, predicate, object triple and adds the statement to the SailConnection.
	 * @param subject String - RDF Subject
	 * @param predicate String - RDF Predicate
	 * @param object Object - RDF Object
	 * @param concept boolean - True if the statement is a concept
	 */
	public void addStatement(Object[] args) {
		processStatement(args, true);
	}
	
	/**
	 * Method removeStatement. Processes a given subject, predicate, object triple and adds the statement to the SailConnection.
	 * @param subject String - RDF Subject
	 * @param predicate String - RDF Predicate
	 * @param object Object - RDF Object
	 * @param concept boolean - True if the statement is a concept
	 */
	public void removeStatement(Object[] args) {
		processStatement(args, false);
	}
	
	/**
	 * 
	 * @param args
	 * @param add
	 */
	private void processStatement(Object[] args, boolean add) {
		String subject = args[0]+"";
		String predicate = args[1]+"";
		Object object = args[2];
		Boolean concept = (Boolean) args[3];
			
		Resource newSub = null;
		Property newPred = null;
		String subString = null;
		String predString = null;
		String sub = subject.trim();
		String pred = predicate.trim();

		subString = Utility.cleanString(sub, false);
		newSub = this.jenaModel.createResource(subString);

		predString = Utility.cleanString(pred, false);
		newPred = this.jenaModel.createProperty(predString);

		RDFNode newObject = null;

		if(concept) {
			String objString = Utility.cleanString((object + "").trim(), false);
			newObject = this.jenaModel.createResource(objString);
		} else {
			if(object instanceof Number) {
				classLogger.debug("Found Double " + object);
		        newObject = ResourceFactory.createTypedLiteral( ((Number) object).doubleValue() );
			} else if(object instanceof Date) {
				classLogger.debug("Found Date " + object);
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				String date = df.format(object);
		        newObject = ResourceFactory.createTypedLiteral(date, XSDDatatype.XSDdateTime);
			} else if(object instanceof Boolean) {
				classLogger.debug("Found Boolean " + object);
		        newObject = ResourceFactory.createTypedLiteral((Boolean) object);
			} else {
				classLogger.debug("Found String " + object);
				newObject = ResourceFactory.createTypedLiteral(object+"");
			}
		}
		
		if(add) {
			this.jenaModel.add(newSub, newPred, newObject);
		} else {
			this.jenaModel.remove(newSub, newPred, newObject);
		}
	}
}
