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
package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import prerna.rdf.query.util.ISPARQLReturnModifier;
import prerna.rdf.query.util.SEMOSSQuery;
import prerna.rdf.query.util.SEMOSSQueryHelper;
import prerna.rdf.query.util.SPARQLAbstractReturnModifier;

public abstract class AbstractSpecificQueryBuilder {
	protected ArrayList<Hashtable<String, String>> parameters;
	protected SEMOSSQuery baseQuery;
	protected String query;
	
	public AbstractSpecificQueryBuilder(ArrayList<Hashtable<String, String>> parameters, SEMOSSQuery baseQuery) {
		this.baseQuery = baseQuery;
		this.parameters = parameters;
	}
	
	protected void addParam(String clauseKey){
		SEMOSSQueryHelper.addParametersToQuery(parameters, baseQuery, clauseKey);
	}
	
	protected void createQuery() {
		baseQuery.createQuery();
		query = baseQuery.getQuery();
	}
	
	protected void addReturnVariable(String colName, String varName, SEMOSSQuery semossQuery, String mathFunc){
		if(!mathFunc.equals("false")){
			SEMOSSQueryHelper.addMathFuncToQuery(mathFunc, colName, semossQuery, varName); // add one to account for label
		}
		else if (colName.equals(varName)){
			SEMOSSQueryHelper.addSingleReturnVarToQuery(colName, semossQuery);
		}
		else { 
			ISPARQLReturnModifier mod = SEMOSSQueryHelper.createReturnModifier(colName, SPARQLAbstractReturnModifier.NONE);
			SEMOSSQueryHelper.addSingleReturnVarToQuery(varName, mod, semossQuery);
		}
	}
	
	protected ArrayList<String> uniqifyColNames(List<String> colNames){
		ArrayList<String> varNames = new ArrayList<String>();
		for (int i=0; i < colNames.size(); i++) {
			String nameInQuestion = colNames.get(i);
			int counter = 1;
			if(nameInQuestion != null){
				//check to see if same var is selected multiple times; then concatenate counter based on number of times dupe var is used
				for(int k=0; k < i; k++){
					String previousName = colNames.get(k);
					if(previousName != null){
						if(nameInQuestion.equals(previousName)) {
							counter++;
						}
					}
				}
			}
			if(counter>1)
				varNames.add(nameInQuestion + counter);
			else
				varNames.add(nameInQuestion);
		}
		return varNames;
	}
	
	public String getQuery() {
		return query;
	}
	
	public abstract void buildQuery();
}
