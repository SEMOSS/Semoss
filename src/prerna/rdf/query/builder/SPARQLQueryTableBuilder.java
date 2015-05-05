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
package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.query.util.SPARQLConstants;
import prerna.rdf.query.util.TriplePart;

public class SPARQLQueryTableBuilder extends AbstractSPARQLQueryBuilder{
	static final Logger logger = LogManager.getLogger(SPARQLQueryTableBuilder.class.getName());

	@Override
	public void buildQuery() 
	{
		semossQuery.setQueryType(SPARQLConstants.SELECT);
		semossQuery.setDisctinct(true);
		parsePath();
		// we are assuming properties are passed in now based on user selection
//		parsePropertiesFromPath(); 
		configureQuery();	
	}
	
	protected void addRelationshipTriples (ArrayList<Hashtable<String,String>> predV) {
		for(Hashtable<String, String> predHash : predV){
			String predName = predHash.get(QueryBuilderHelper.varKey);
			String predURI = predHash.get(QueryBuilderHelper.uriKey);
			TriplePart relationVar = new TriplePart(predHash.get("SubjectVar"), TriplePart.VARIABLE);
			TriplePart subPropURI = new TriplePart(predURI, TriplePart.URI);
			TriplePart relationTypeURI = new TriplePart(predHash.get("ObjectVar"), TriplePart.VARIABLE);
			semossQuery.addTriple(relationVar, subPropURI, relationTypeURI);
		}
	}

	@Override
	protected void addReturnVariables(
			ArrayList<Hashtable<String, String>> predV2) {
		// TODO Auto-generated method stub
		
	}
}
