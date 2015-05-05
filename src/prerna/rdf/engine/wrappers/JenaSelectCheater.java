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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IConstructWrapper;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

public class JenaSelectCheater extends AbstractWrapper implements IConstructWrapper {
	
	transient int count = 0;
	transient String [] var = null;
	transient int triples;
	transient int tqrCount=0;
	String queryVar[];
	transient ResultSet rs = null;


	@Override
	public IConstructStatement next() {
		
		IConstructStatement thisSt = new ConstructStatement();
	    logger.debug("Adding a JENA statement ");
	    QuerySolution row = rs.nextSolution();
	    thisSt.setSubject(row.get(var[0])+"");
	    thisSt.setPredicate(row.get(var[1])+"");
	    thisSt.setObject(row.get(var[2]));
	    
	    return thisSt;
	    
	}

	@Override
	public void execute() {
		try {
			rs = (ResultSet)engine.execQuery(query);
			getVariables();
			
			processSelectVar();
			count=0;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return 	rs.hasNext();

	}
	
	private String [] getVariables()
	{
		var = new String[rs.getResultVars().size()];
		List <String> names = rs.getResultVars();
		for(int colIndex = 0;
				colIndex < names.size();
				var[colIndex] = names.get(colIndex), colIndex++);
		return var;
	}
	
	public void processSelectVar()
	{
		if(query.contains("DISTINCT"))
		{
			Pattern pattern = Pattern.compile("SELECT DISTINCT(.*?)WHERE");
		    Matcher matcher = pattern.matcher(query);
		    String varString = null;
		    while (matcher.find()) 
		    {
		    	varString = matcher.group(1);
		    }
		    varString = varString.trim();
		    queryVar = varString.split(" ");
		    int num = queryVar.length+1;
		    triples = (int) Math.floor(num/3);
		}
		else
		{
			Pattern pattern = Pattern.compile("SELECT (.*?)WHERE");
		    Matcher matcher = pattern.matcher(query);
		    String varString = null;
		    while (matcher.find()) {
		        varString = matcher.group(1);
		    }
		    varString = varString.trim();
		    queryVar = varString.split(" ");
		    int num = queryVar.length+1;
		    triples = (int) Math.floor(num/3);
		}
	}


}
