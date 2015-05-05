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
package prerna.engine.impl.rdf;

import java.io.IOException;
import java.io.StringBufferInputStream;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.impl.AbstractEngine;
import prerna.rdf.engine.wrappers.SesameConstructWrapper;
import prerna.rdf.engine.wrappers.SesameSelectWrapper;
import prerna.ui.components.RDFEngineHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.google.gson.Gson;

public class RemoteSemossSesameEngine extends AbstractEngine {

	private static final Logger logger = LogManager.getLogger(RemoteSemossSesameEngine.class.getName());
	// things this needs to override
	// OpenDB
	// CloseDB
	// Querying of the result
	boolean connected = false;
	String api = null;
	String database = null;
	
	
	
	public void openDB(String propFile)
	{
		// the propfile will typically have 
		// the remote base URI
		// the name of the database
		try {
			
				// get the URI for the remote
				System.err.println("Loading file ENGINE " + propFile);
				
				
				if(propFile != null)
				{
					String baseFolder = DIHelper.getInstance().getProperty(
					"BaseFolder");
					String fileName = baseFolder + "/" + propFile;
					prop = loadProp(propFile);
					
					api = prop.getProperty(Constants.URI);
					database = prop.getProperty("DATABASE");
				}
				engineName = database;
				String insights = Utility.retrieveResult(api + "/s-" + database + "/getInsightDefinition", null);
				
				this.createInsightBase();
				insightBaseXML.rc = getNewRepository();
				engineURI2 = engineBaseURI + "/" + engineName;
				System.out.println("Engine URI is " + engineURI2);
				
				System.out.println("Insights is " + insights);


				// add it to the core connection
				insightBaseXML.rc.add(new StringBufferInputStream(insights), "http://semoss.org", RDFFormat.RDFXML);
				
				// need to do the same with the owl
				String owl = Utility.retrieveResult(api + "/s-" + database + "/getOWLDefinition", null);
				
				RepositoryConnection rc = getNewRepository();

				rc.add(new StringBufferInputStream(owl), "http://semoss.org", RDFFormat.RDFXML);
				baseDataEngine = new RDFFileSesameEngine();
				
				baseDataEngine.rc = rc;
				
				try {
					this.setBaseHash(RDFEngineHelper.createBaseFilterHash(rc));
				} catch (MalformedQueryException e) {
					e.printStackTrace();
				} catch (QueryEvaluationException e) {
					e.printStackTrace();
				}
				
				// yup.. we are open for business
				connected = true;
		}catch(RuntimeException ex)
		{
			// will do something later
			ex.printStackTrace();
			connected = false;
		} catch (RDFParseException e) {
			e.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
		
	public RepositoryConnection getNewRepository() {
		try {
			RepositoryConnection rc = null;
			Repository myRepository = new SailRepository(
					new ForwardChainingRDFSInferencer(new MemoryStore()));
			myRepository.initialize();
			rc = myRepository.getConnection();
			return rc;
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Object execQuery(String query) {
		// this will call the engine and gets then flushes it into sesame jena wrapper
		Hashtable <String,String> params = new Hashtable<String, String>();
		params.put("query", query);
		// get the result
		Gson gson = new Gson();
		query = query.trim();
		if(query.toLowerCase().startsWith("construct")){
			String output = Utility.retrieveResult(api + "/s-" + database + "/execGraphQuery", params);
			SesameConstructWrapper sjcw = gson.fromJson(output, SesameConstructWrapper.class);
			return sjcw;
		}
		else if(query.toLowerCase().startsWith("select")){
			String output = Utility.retrieveResult(api + "/s-" + database + "/execSelectQuery", params);
			SesameSelectWrapper sjcw = gson.fromJson(output, SesameSelectWrapper.class);
			return sjcw;
		}
		else if(query.toLowerCase().startsWith("ask")){
			String output = Utility.retrieveResult(api + "/s-" + database + "/execSelectQuery", params);
			SesameSelectWrapper sjcw = gson.fromJson(output, SesameSelectWrapper.class);
			return sjcw;
		}
		else{
			return null;
		}

	}

	public Object execCheaterQuery(String query)
	{
		// this will call the engine and gets then flushes it into sesame jena construct wrapper
		Hashtable <String,String> params = new Hashtable<String, String>();
		params.put("query", query);
		// get the result
		String output = Utility.retrieveResult(api + "/s-" + database + "/execCheaterQuery", params);

		Gson gson = new Gson();
		SesameJenaSelectCheater sjcw = gson.fromJson(output, SesameJenaSelectCheater.class);
		
		return sjcw;
	}

	
	// gets the from neighborhood for a given node
	public Vector <String> getFromNeighbors(String nodeType, int neighborHood)
	{
		Hashtable <String,String> params = new Hashtable<String, String>();
		params.put("nodeType", nodeType);
		params.put("neighborHood", neighborHood+"");
		// get the result
		String output = Utility.retrieveResult(api + "/s-" + database + "/getFromNeighbors", params);

		Gson gson = new Gson();
		Vector <String> fromN = gson.fromJson(output, Vector.class);
		
		return fromN;
	}
	
	// gets the to nodes
	public Vector <String> getToNeighbors(String nodeType, int neighborHood)
	{
		Hashtable <String,String> params = new Hashtable<String, String>();
		params.put("nodeType", nodeType);
		params.put("neighborHood", neighborHood+"");
		// get the result
		String output = Utility.retrieveResult(api + "/s-" + database + "/getToNeighbors", params);

		Gson gson = new Gson();
		Vector <String> toN = gson.fromJson(output, Vector.class);
		
		return toN;

	}
	
	// gets the from and to nodes
	public Vector <String> getNeighbors(String nodeType, int neighborHood)
	{
		Hashtable <String,String> params = new Hashtable<String, String>();
		params.put("nodeType", nodeType);
		params.put("neighborHood", neighborHood+"");
		// get the result
		String output = Utility.retrieveResult(api + "/s-" + database + "/getNeighbors", params);

		Gson gson = new Gson();
		Vector <String> nN = gson.fromJson(output, Vector.class);
		
		return nN;
	}

	public ENGINE_TYPE getEngineType()
	{
		return IEngine.ENGINE_TYPE.SEMOSS_SESAME_REMOTE;
	}
	
	
	public void setAPI(String api)
	{
		this.api = api;
	}
	
	public void setDatabase(String database)
	{
		this.database = database;
	}

	/**
	 * Processes a SELECT query just like {@link #execSelectQuery(String)} but gets the results in the exact format that the database stores them.
	 * This is important for things like param values so that we can take the returned value and fill the main query without needing modification
	 * @param sparqlQuery the SELECT SPARQL query to be run against the engine
	 * @return the Vector of Strings representing the full uris of all of the query results */
	public Vector<String> getCleanSelect(String sparqlQuery)
	{
		logger.debug("\nSPARQL: " + sparqlQuery);
		SesameSelectWrapper ssw = (SesameSelectWrapper) this.execQuery(sparqlQuery);
		Vector<String> strVector = new Vector<String>();
		while(ssw.hasNext()){
			ISelectStatement sss = ssw.next();
			strVector.add(sss.getRawVar(Constants.ENTITY)+ "");
		}

		return null;
	}
	
	/**
	 * Uses a type URI to get the URIs of all instances of that type. These instance URIs are returned as the Vector of Strings.
	 * @param type The full URI of the node type that we want to get the instances of
	 * @return the Vector of Strings representing the full uris of all of the instances of the passed in type */
	public Vector<String> getEntityOfType(String type)
	{
		Hashtable <String,String> params = new Hashtable<String, String>();
		params.put("sparqlQuery", type);
		// get the result
		String output = Utility.retrieveResult(api + "/s-" + database + "/getEntityOfType", params);

		Gson gson = new Gson();
		Vector <String> retVector = gson.fromJson(output, Vector.class);
		
		return retVector;
	}
	
	public String getProperty(String key)
	{
		Hashtable params = new Hashtable();
		params.put("key", key);
		String output = Utility.retrieveResult(api + "/s-" + database + "/getProperty", params);
		return output;
	}
	
	// needs
	// get Entity of type
	
	// is connected
	public boolean isConnected()
	{
		return connected;
	}

	@Override
	public void deleteDB(){
		// this does nothing
		logger.info("cannot delete remote engine");
	}

	@Override
	public void closeDB() {
		// this does nothing
		logger.info("cannot close remote engine");
	}
	
	@Override
	public void insertData(String query) {
		// this does nothing
		logger.info("cannot insert into remote engine");
	}

	@Override
	public void removeData(String query) {
		// this does nothing
		logger.info("cannot remove from remote engine");
	}

	@Override
	public void commit() {
		// this does nothing
		logger.info("cannot commit remote engine");
	}
	
}
