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
import java.util.Properties;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.util.Constants;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class ClusterEngine extends AbstractDatabaseEngine {

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
	Hashtable <String, IDatabaseEngine> engineHash = new Hashtable<String, IDatabaseEngine>();
	
	// You register a database with the name server
	// in this case you register an engine
	// when registered all the questions are parsed out
	// and saved in the above format
	// to be queried

	// there is a separate pane which starts giving you the interested questions
	// very similar to the amazon piece where it says
	// you might be interested in these questions too
	// the question needs to be replaced with actual pieces
	
	

	public void addEngine(AbstractDatabaseEngine engine) {
		// put it in the hash
		engineHash.put(engine.getEngineId(), engine);
		
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
//		initializeInsightBase();
//		
//		//TODO: after moving to RDBMS insights, this has not been tested
//		String insights = engine.getInsightDefinition();
//		logger.info("Have insights string::: " + insights);
//		String[] insightBuilderQueries = insights.split("%!%");
//		
//		for (String insightBuilderQuery : insightBuilderQueries)
//		{
//			logger.info("running query " +  insightBuilderQuery);
//			try {
//				this.insightRdbms.insertData(insightBuilderQuery);
//			} catch (SQLException e) {
//				classLogger.error(Constants.STACKTRACE, e);
//			}
//		}
		
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
	
//	public void initializeInsightBase()
//	{
//		Properties dbProp = writePropFile();
//		this.insightRdbms = new RDBMSNativeEngine();
//		this.insightRdbms.setProp(dbProp);;
//		this.insightRdbms.open(null);
//	}
//	
	
	// the only other thing I really need to be able to do is
	// say pull data from multiple of these engines
	
	@Override
	public void close() {
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
	public DATABASE_TYPE getDatabaseType() {
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

	// TODO: do we really need this?
	private Properties writePropFile() {
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);
		Properties prop = new Properties();
		String connectionURL = "jdbc:h2:mem:temp";
		prop.put(Constants.CONNECTION_URL, connectionURL);
		prop.put(Constants.USERNAME, queryUtil.getUsername());
		prop.put(Constants.PASSWORD, queryUtil.getPassword());
		prop.put(Constants.RDBMS_TYPE, queryUtil.getDbType().toString());
		prop.put("TEMP", "TRUE");
		return prop;
	}
	
	@Override
	public boolean holdsFileLocks() {
		return true;
	}

}
