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
package prerna.rdf.engine.api;

import java.util.Hashtable;
import java.util.Vector;

import org.openrdf.repository.RepositoryConnection;

import prerna.om.Insight;

public interface IExplorable {
	// gets the perspectives for this engine
	public Vector getPerspectives();
	
	// gets the questions for a given perspective
	public Vector getInsights(String perspective);
	
	// get all the insights irrespective of perspective
	public Vector getInsights();

	// get the insight for a given question description
	public Insight getInsight(String label);

	// get the insight for a given question description
	public Vector<Hashtable<String, String>> getOutputs4Insights(Vector<String> insightLabels);
	
	// gets insights for a given type of entity
	public Vector <String> getInsight4Type(String type);
	
	// get insights for a tag
	public Vector<String> getInsight4Tag(String tag) ;
	
	// gets the from neighborhood for a given node
	public Vector <String> getFromNeighbors(String nodeType, int neighborHood);
	
	// gets the to nodes
	public Vector <String> getToNeighbors(String nodeType, int neighborHood);
	
	// gets the from and to nodes
	public Vector <String> getNeighbors(String nodeType, int neighborHood);
	
	// gets the insight database
	public RepositoryConnection getInsightDB();
	
	// gets all the params
	public Vector getParams(String insightName);

	// gets all the insights
	public String getInsightDefinition();

}
