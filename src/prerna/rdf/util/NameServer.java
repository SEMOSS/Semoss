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
package prerna.rdf.util;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import org.openrdf.repository.RepositoryConnection;

import prerna.om.Insight;
import prerna.rdf.engine.api.IEngine;

public class NameServer {

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

	// hashtable to keep questions for a given entity type
	// the output is always a vector of questions
	// Vector of strings
	Hashtable<String, Vector> insightHash = new Hashtable<String, Vector>();

	// question id to insight 
	Hashtable <String, Insight> idInsightHash = new Hashtable <String, Insight>();
	
	// label to id
	Hashtable <String, String> labelIdHash = new Hashtable<String, String>();
	
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
		// get the name of the engine
		// get the ontology / base DB for this engine
		// load it into the in memory
	}	
	

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
	
	// gets the insight for the given question
	public Insight getInsight(String question)
	{
		// go to the labelIdHash and get the id
		// use the id to get the insight
		
		String id = labelIdHash.get(question);
		Insight reti = idInsightHash.get(id);
		
		return reti;
	}
}
