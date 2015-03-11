/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
package prerna.rdf.engine.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Vector;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
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
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriter;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * References the RDF source and uses the Sesame API to query a database stored in an RDF file (.jnl file).
 */
public class RDFFileSesameEngine extends AbstractEngine implements IEngine {

	Properties rdfMap = null;
	RepositoryConnection rc = null;
	ValueFactory vf = null;
	String rdfFileType = "RDF/XML";
	public String baseURI = null;
	public String fileName = null;
	SailConnection sc = null;
	boolean connected = false;
	
	/**
	 * Opens a database as defined by its properties file.  What is included in the properties file is dependent on the type of 
	 * engine that is being initiated.  This is the function that first initializes an engine with the property file at the very 
	 * least defining the data store.
	 * @param propFile contains all information regarding the data store and how the engine should be instantiated.  Dependent on 
	 * what type of engine is being instantiated.
	 */
	@Override
	public void openDB(String propFile)
	{
		super.openDB(propFile);
		try
		{
			Repository myRepository;
				super.openDB(propFile);
				myRepository = new SailRepository(
						new ForwardChainingRDFSInferencer(
						new MemoryStore()));
				myRepository.initialize();
				
			//System.err.println("Prop File is " + propFile2);
			
			if(prop != null)
			{
				
				fileName = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + prop.getProperty(Constants.RDF_FILE_NAME);
				rdfFileType = prop.getProperty(Constants.RDF_FILE_TYPE);
				baseURI = prop.getProperty(Constants.RDF_FILE_BASE_URI);
			}

			File file = new File( fileName);
			rc = myRepository.getConnection();
			sc = ((SailRepositoryConnection) rc).getSailConnection();
			vf = rc.getValueFactory();
			
			if(file != null)
			{
				if(rdfFileType.equalsIgnoreCase("RDF/XML")) rc.add(file, baseURI, RDFFormat.RDFXML);
				else if(rdfFileType.equalsIgnoreCase("TURTLE")) rc.add(file, baseURI, RDFFormat.TURTLE);
				else if(rdfFileType.equalsIgnoreCase("BINARY")) rc.add(file, baseURI, RDFFormat.BINARY);
				else if(rdfFileType.equalsIgnoreCase("N3")) rc.add(file, baseURI, RDFFormat.N3);
				else if(rdfFileType.equalsIgnoreCase("NTRIPLES")) rc.add(file, baseURI, RDFFormat.NTRIPLES);
				else if(rdfFileType.equalsIgnoreCase("TRIG")) rc.add(file, baseURI, RDFFormat.TRIG);
				else if(rdfFileType.equalsIgnoreCase("TRIX")) rc.add(file, baseURI, RDFFormat.TRIX);
			}
		    this.connected = true;
		}catch(RuntimeException ignored)
		{
			this.connected = false;
			ignored.printStackTrace();
		} catch (RDFParseException e) {
			this.connected = false;
			e.printStackTrace();
		} catch (RepositoryException e) {
			this.connected = false;
			e.printStackTrace();
		} catch (IOException e) {
			this.connected = false;
			e.printStackTrace();
		}
	}

	/**
	 * Closes the data base associated with the engine.  This will prevent further changes from being made in the data store and 
	 * safely ends the active transactions and closes the engine.
	 */
	@Override
	public void closeDB() {
		// ng.stopTransaction(Conclusion.SUCCESS);
		try {
			rc.close();
			connected = false;
		} catch (Exception e) {
			e.printStackTrace();
		}
		// ng.shutdown();
	}

	/**
	 * Runs the passed string query against the engine and returns graph query results.  The query passed must be in the structure 
	 * of a CONSTRUCT SPARQL query.  The exact format of the results will be 
	 * dependent on the type of the engine, but regardless the results are able to be graphed.
	 * @param query the string version of the query to be run against the engine
	
	 * @return the graph query results */
	@Override
	public GraphQueryResult execGraphQuery(String query) {
		 GraphQueryResult res = null;
		 
		try {
			GraphQuery sagq = rc.prepareGraphQuery(QueryLanguage.SPARQL,
						query);
				res = sagq.evaluate();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		return res;	
	}

	/**
	 * Runs the passed string query against the engine as a SELECT query.  The query passed must be in the structure of a SELECT 
	 * SPARQL query and the result format will depend on the engine type.
	 * @param query the string version of the SELECT query to be run against the engine
	
	 * @return triple query results that can be displayed as a grid */
	@Override
	public TupleQueryResult execSelectQuery(String query) {
		
		TupleQueryResult sparqlResults = null;
		
		try {
			TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, query);
			logger.debug("\nSPARQL: " + query);
			tq.setIncludeInferred(true /* includeInferred */);
			sparqlResults = tq.evaluate();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		return sparqlResults;
	}
	
	/**
	 * Gets the type of the engine.  The engine type is often used to determine what API to use while running queries against the 
	 * engine.
	
	 * @return the type of the engine */
	public ENGINE_TYPE getEngineType()
	{
		return IEngine.ENGINE_TYPE.SESAME;
	}
	
	/**
	 * Processes a SELECT query just like {@link #execSelectQuery(String)} but then parses the results to get only their 
	 * instance names.  These instance names are then returned as the Vector of Strings.
	 * @param sparqlQuery the SELECT SPARQL query to be run against the engine
	
	 * @return the Vector of Strings representing the instance names of all of the query results */
	public Vector<String> getEntityOfType(String sparqlQuery)
	{
		try {
			TupleQuery tq = rc.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);
			logger.debug("\nSPARQL: " + sparqlQuery);
			tq.setIncludeInferred(true /* includeInferred */);
			TupleQueryResult sparqlResults = tq.evaluate();
			Vector<String> strVector = new Vector();
			while(sparqlResults.hasNext())
				strVector.add(sparqlResults.next().getValue(Constants.ENTITY)+ "");

			return strVector;
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Returns whether or not an engine is currently connected to the data store.  The connection becomes true when {@link #openDB(String)} 
	 * is called and the connection becomes false when {@link #closeDB()} is called.
	
	 * @return true if the engine is connected to its data store and false if it is not */
	@Override
	public boolean isConnected()
	{
		return connected;
	}

	/**
	 * Processes the passed ASK SPARQL query against the engine.  The query must be in the structure of an ASK query and the 
	 * result will be a boolean indicating whether or not the data store connected to the engine has triples matching the 
	 * pattern of the ASK query.
	 * @param query the ASK SPARQL query to be run against the engine
	
	 * @return true if the data store connected to the engine contains triples that match the pattern of the query and false 
	 * if it does not. */
	@Override
	public Boolean execAskQuery(String query) {
		BooleanQuery bq;
		boolean response = false;
		try {
			bq = rc.prepareBooleanQuery(QueryLanguage.SPARQL, query);
			logger.debug("\nSPARQL: " + query);
			response = bq.evaluate();
		} catch (MalformedQueryException e) {
			e.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		
		return response;
	}

	/**
	 * Method addStatement. Processes a given subject, predicate, object triple and adds the statement to the SailConnection.
	 * @param subject String - RDF Subject
	 * @param predicate String - RDF Predicate
	 * @param object Object - RDF Object
	 * @param concept boolean - True if the statement is a concept
	 */
	@Override
	public void addStatement(String subject, String predicate, Object object, boolean concept)
	{
		//logger.info("Updating Triple " + subject + "<>" + predicate + "<>" + object);
		try {
			URI newSub = null;
			URI newPred = null;
			Value newObj = null;
			String subString = null;
			String predString = null;
			String objString = null;
			String sub = subject.trim();
			String pred = predicate.trim();
			
			//System.err.println("VF is " + vf);
			
			rc.begin();
			
			subString = Utility.cleanString(sub, false);
			newSub = vf.createURI(subString);
			
			predString = Utility.cleanString(pred, false);
			newPred = vf.createURI(predString);
			
			if(!concept)
			{
				if(object.getClass() == new Double(1).getClass())
				{
					logger.debug("Found Double " + object);
					sc.addStatement(newSub, newPred, vf.createLiteral(((Double)object).doubleValue()));
				}
				else if(object.getClass() == new Date(1).getClass())
				{
					logger.debug("Found Date " + object);
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
					String date = df.format(object);
					URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
					sc.addStatement(newSub, newPred, vf.createLiteral(date, datatype));
				}
				else
				{
					logger.debug("Found String " + object);
					String value = object + "";
					// try to see if it already has properties then add to it
					String cleanValue = value;//.replaceAll("/", "-").replaceAll("\"", "'");			
					sc.addStatement(newSub, newPred, vf.createLiteral(cleanValue));
				} 
			}
			else
				sc.addStatement(newSub, newPred, vf.createURI(object+""));
			rc.commit();
		} catch (SailException e) {
			e.printStackTrace();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Method removeStatement. Processes a given subject, predicate, object triple and adds the statement to the SailConnection.
	 * @param subject String - RDF Subject
	 * @param predicate String - RDF Predicate
	 * @param object Object - RDF Object
	 * @param concept boolean - True if the statement is a concept
	 */
	@Override
	public void removeStatement(String subject, String predicate, Object object, boolean concept)
	{
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
					logger.debug("Found Double " + object);
					sc.removeStatements(newSub, newPred, vf.createLiteral(((Double)object).doubleValue()));
				}
				else if(object.getClass() == new Date(1).getClass())
				{
					logger.debug("Found Date " + object);
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
					String date = df.format(object);
					URI datatype = vf.createURI("http://www.w3.org/2001/XMLSchema#dateTime");
					sc.removeStatements(newSub, newPred, vf.createLiteral(date, datatype));
				}
				else
				{
					logger.debug("Found String " + object);
					String value = object + "";
					// try to see if it already has properties then add to it
					String cleanValue = value;//.replaceAll("/", "-").replaceAll("\"", "'");			
					sc.removeStatements(newSub, newPred, vf.createLiteral(cleanValue));
				} 
			}
			else
			{
				sc.removeStatements(newSub, newPred, uriObj);
			}
			sc.commit();
		} catch (SailException e) 
		{
			e.printStackTrace();
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
	public void execInsertQuery(String query) throws SailException, UpdateExecutionException, RepositoryException, MalformedQueryException {

		Update up = rc.prepareUpdate(QueryLanguage.SPARQL, query);
		//sc.addStatement(vf.createURI("<http://semoss.org/ontologies/Concept/Service/tom2>"),vf.createURI("<http://semoss.org/ontologies/Relation/Exposes>"),vf.createURI("<http://semoss.org/ontologies/Concept/BusinessLogicUnit/tom1>"));
		logger.debug("\nSPARQL: " + query);
		//tq.setIncludeInferred(true /* includeInferred */);
		//tq.evaluate();
		//rc.setAutoCommit(false);
		sc.begin();
		up.execute();
		//rc.commit();
		sc.commit();
	}
	
	/**
	 * Method exportDB.  Exports the repository connection to the RDF database.
	 * @throws IOException 
	 * @throws RDFHandlerException 
	 * @throws RepositoryException 
	 */
	public void exportDB() throws RepositoryException, RDFHandlerException, IOException
	{
		FileWriter writer = null;
		try{
			System.err.println("Exporting database");
			writer = new FileWriter(fileName);
			rc.export(new RDFXMLPrettyWriter(writer));
		}catch(IOException e) {
			e.printStackTrace();
		} finally {
			try{
				if(writer!=null)
					writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Method getRC.  Gets the repository connection.
	
	 * @return RepositoryConnection - The repository connection. */
	public RepositoryConnection getRC() {
		return this.rc;
	}
	
	/**
	 * Method getRc.  Gets the repository connection.
	
	 * @return RepositoryConnection - The repository connection. */
	public RepositoryConnection getRc() {
		return rc;
	}

	public void setRC(RepositoryConnection rc2) {
		this.rc = rc2;
		
	}
	/**
	 * Method getRc.  Gets the repository connection.
	
	 * @return RepositoryConnection - The repository connection. */
	public SailConnection getSC() {
		return sc;
	}
	/**
	 * Method getRc.  Gets the repository connection.
	
	 * @return RepositoryConnection - The repository connection. */
	public void setSC(SailConnection sc) {
		this.sc = sc;
	}
	/**
	 * Method getRc.  Gets the repository connection.
	
	 * @return RepositoryConnection - The repository connection. */
	public void setVF(ValueFactory vf) {
		this.vf = vf;
	}
	
	public ValueFactory getVF() {
		return this.vf;
	}
	
}
