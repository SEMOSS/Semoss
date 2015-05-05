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
package prerna.rdf.engine.wrappers;

import java.util.List;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.util.Utility;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

public class JenaSelectWrapper extends AbstractWrapper implements ISelectWrapper {
	
	transient ResultSet rs = null;
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return rs.hasNext();
	}

	@Override
	public ISelectStatement next() 
	{
		ISelectStatement thisSt = new SelectStatement();
	    QuerySolution row = rs.nextSolution();
		String [] values = new String[var.length];
		for(int colIndex = 0;colIndex < var.length;colIndex++)
		{
			String value = row.get(var[colIndex])+"";
			RDFNode node = row.get(var[colIndex]);
			if(node.isAnon())
			{
				logger.debug("Ok.. an anon node");
				String id = Utility.getNextID();
				thisSt.setVar(var[colIndex], id);
			}
			else
			{
				
				logger.debug("Raw data JENA For Column " +  var[colIndex]+" >>  " + value);
				String instanceName = Utility.getInstanceName(value);
				thisSt.setVar(var[colIndex], instanceName);
			}
			thisSt.setRawVar(var[colIndex], value);
			logger.debug("Binding Name " + var[colIndex]);
			logger.debug("Binding Value " + value);
		}	
		return thisSt;
	}

	@Override
	public String[] getVariables() {
		var = new String[rs.getResultVars().size()];
		List <String> names = rs.getResultVars();
		for(int colIndex = 0;colIndex < names.size();var[colIndex] = names.get(colIndex), colIndex++);
		return var;
	}

	@Override
	public void execute() {
		rs = (ResultSet) engine.execQuery(query);		
	}

}
