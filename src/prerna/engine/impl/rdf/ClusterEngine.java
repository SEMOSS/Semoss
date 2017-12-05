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

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Constants;
import prerna.util.sql.H2QueryUtil;

public class ClusterEngine extends AbstractEngine {

	private static final Logger logger = LogManager.getLogger(ClusterEngine.class.getName());
	// for every class type and relation it tells you which
	// database does this type belong in
	// database-URL has-entity classtype
	// database-URL has-entity relation
	// database-URL search-index URL for search index
	// entity has insight - How do we do entity combinations
	// insight has:label question description
	// insight has:sparql sparql
	// insight uses-database InMemory-DB / URL

	// keeps an in memory store which would be utilized for traverse freely
	RepositoryConnection rc = null;
		
	// database names
	Hashtable <String, IEngine> engineHash = new Hashtable<String, IEngine>();
	
	// You register a database with the name server
	// in this case you register an engine
	// when registered all the questions are parsed out
	// and saved in the above format
	// to be queried

	// there is a separate pane which starts giving you the interested questions
	// very similar to the amazon piece where it says
	// you might be interested in these questions too
	// the question needs to be replaced with actual pieces
	
	

	public void addEngine(AbstractEngine engine) {
		// put it in the hash
		engineHash.put(engine.getEngineName(), engine);
		
		// get the base owl file
		// get the name of the engine
		// get the ontology / base DB for this engine
		// load it into the in memory
		RepositoryConnection con = engine.getBaseDataEngine().getRc();
		if(con != null)
		{
			try {
				baseDataEngine.rc.add(con.getStatements(null, null, null, true));				
			}catch(RepositoryException ex)
			{
				logger.debug(ex);
				//ignored
			}
		}	
		
		// do the same with insights
		initializeInsightBase();
		
		//TODO: after moving to RDBMS insights, this has not been tested
		String insights = engine.getInsightDefinition();
		logger.info("Have insights string::: " + insights);
		String[] insightBuilderQueries = insights.split("%!%");
		
		for (String insightBuilderQuery : insightBuilderQueries)
		{
			logger.info("running query " +  insightBuilderQuery);
			this.insightRDBMS.insertData(insightBuilderQuery);
		}
		
//		con = engine.getInsightDB();
//		if(con != null)
//		{
//			try {
//				insightBaseXML.rc.add(con.getStatements(null, null, null, true));				
//			}catch(RepositoryException ex)
//			{
//				logger.debug(ex);
//				//ignored
//			}
//		}	
	}	
	
	public void initializeInsightBase()
	{
		Properties dbProp = writePropFile();
		this.insightRDBMS = new RDBMSNativeEngine();
		this.insightRDBMS.setProperties(dbProp);;
		this.insightRDBMS.openDB(null);
	}
	
	
	// the only other thing I really need to be able to do is
	// say pull data from multiple of these engines
	
	// I will leave the question for now
	// gets all the questions tagged for this question type
	public Vector getQuestions(String... entityType) 
	{
		// get the insights and convert
		Vector finalVector = new Vector();
		for(int entityIndex = 0;entityIndex < entityType.length;entityIndex++)
		{
			Iterator <IEngine> engines = engineHash.values().iterator();
			for(int engineIndex = 0;engines.hasNext();engineIndex++)
			{
				Vector engineInsights = engines.next().getInsight4Type(entityType[entityIndex]);
				// need to capture this so that I can relate this back to the engine when selected
				if(engineInsights == null)
					finalVector.add(engineInsights);					
			}
		}
		return finalVector;
	}

	@Override
	public void closeDB() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object execQuery(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertData(String query) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ENGINE_TYPE getEngineType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeData(String query) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Vector<Object> getCleanSelect(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	// TODO: do we really need this?
	private Properties writePropFile() {
		H2QueryUtil queryUtil = new H2QueryUtil();
		Properties prop = new Properties();
		String connectionURL = "jdbc:h2:mem:temp";
		prop.put(Constants.CONNECTION_URL, connectionURL);
		prop.put(Constants.USERNAME, queryUtil.getDefaultDBUserName());
		prop.put(Constants.PASSWORD, queryUtil.getDefaultDBPassword());
		prop.put(Constants.DRIVER,queryUtil.getDatabaseDriverClassName());
		prop.put(Constants.RDBMS_TYPE,queryUtil.getDatabaseType().toString());
		prop.put("TEMP", "TRUE");
		return prop;
	}
}
