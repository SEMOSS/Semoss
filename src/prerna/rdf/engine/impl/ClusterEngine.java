/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.rdf.engine.impl;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.rdf.engine.api.IEngine;

public class ClusterEngine extends AbstractEngine {

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
	
	

	public void addEngine(IEngine engine) {
		// put it in the hash
		engineHash.put(engine.getEngineName(), engine);
		
		// get the base owl file
		// get the name of the engine
		// get the ontology / base DB for this engine
		// load it into the in memory
		RepositoryConnection con = engine.getOWL();
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
		con = engine.getInsightDB();
		if(con != null)
		{
			try {
				insightBaseXML.rc.add(con.getStatements(null, null, null, true));				
			}catch(RepositoryException ex)
			{
				logger.debug(ex);
				//ignored
			}
		}	
	}	
	
	public void initializeInsightBase()
	{
		if(insightBaseXML.rc == null)
		{
			try {
				Repository myRepository = new SailRepository(
						new ForwardChainingRDFSInferencer(new MemoryStore()));
				myRepository.initialize();
				insightBaseXML.rc = myRepository.getConnection();
			} catch (RepositoryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
}
