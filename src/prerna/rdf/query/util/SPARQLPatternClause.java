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
package prerna.rdf.query.util;

import java.util.ArrayList;

public class SPARQLPatternClause{

	ArrayList<SPARQLTriple> triples = new ArrayList<SPARQLTriple>();
	ArrayList<SPARQLBind> binds = new ArrayList<SPARQLBind>();
	ArrayList<SPARQLFilter> filters = new ArrayList<SPARQLFilter>();
	ArrayList<SEMOSSParameter> params = new ArrayList<SEMOSSParameter>();
	String clauseString;
	
	public void addTriple(SPARQLTriple triple)
	{
		triples.add(triple);
	}
	
	public void addBind(SPARQLBind bind)
	{
		binds.add(bind);
	}
	
	public void addFilter(SPARQLFilter filter)
	{
		filters.add(filter);
	}
	
	public void addParameter(SEMOSSParameter param){
		params.add(param);
	}
	
	public String getClauseString()
	{
		clauseString ="";
		addAllParametersToClause();
		addAllBindsToClause();
		addAllTriplesToClause();
		addAllFiltersToClause();
		return clauseString;
	}
	
	private void addAllTriplesToClause()
	{
		for (int triIdx=0; triIdx<triples.size();triIdx++)
		{
			//space out the variables
			clauseString = clauseString + triples.get(triIdx).getTripleString()+" ";
		}
	}
	
	private void addAllBindsToClause()
	{
		for (int bindIdx=0; bindIdx<binds.size();bindIdx++)
		{
			//space out the variables
			clauseString = clauseString + binds.get(bindIdx).getBindString()+" ";
		}
	}
	
	public boolean hasTriple(SPARQLTriple triple)
	{
		boolean retBoolean = false;
		getClauseString();
		if(clauseString.contains(triple.getTripleString()))
			retBoolean=true;
		
		return retBoolean;
	}
	
	private void addAllFiltersToClause()
	{
		for (int filterIdx=0; filterIdx<filters.size();filterIdx++)
		{
			//space out the variables
			clauseString = clauseString + filters.get(filterIdx).getFilterString()+" ";
		}
	}
	
	private void addAllParametersToClause()	{
		for(int paramIndex=0; paramIndex < params.size(); paramIndex++) {
			String paramName = params.get(paramIndex).getParamName();
			clauseString = clauseString + params.get(paramIndex).getParamString() + " ";
		}
	}
}
