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
package prerna.rdf.engine.wrappers;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.hp.hpl.jena.vocabulary.RDFS;

import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IConstructWrapper;
import prerna.util.Utility;

public class SesameSelectCheater extends AbstractWrapper implements IConstructWrapper {

	private static final Logger LOGGER = LogManager.getLogger(SesameSelectCheater.class.getName());
	
	public transient TupleQueryResult tqr = null;
	transient int count = 0;
	transient String [] var = null;
	transient BindingSet bs = null;
	transient int triples;
	transient int tqrCount=0;
	String queryVar[];
	String nextPred;
	
	@Override
	public IConstructStatement next() {
		IConstructStatement thisSt = new ConstructStatement();
		
		if(nextPred != null){
			// need to create triple saying pred is a sub prop of relationship
			thisSt.setSubject(nextPred);
			thisSt.setPredicate(RDFS.subPropertyOf +"");
			thisSt.setObject("http://semoss.org/ontologies/Relation");
			this.nextPred = null;
			return thisSt;
		}
		
		try {
			if(count==0)
			{
				bs = tqr.next();
			}
			LOGGER.debug("Adding a sesame statement ");
			
			// there should only be three values
			Object sub=null;
			Object pred = null;
			Object obj = null;
			while (sub==null || pred==null || obj==null)
			{
				if (count==triples)
				{
					count=0;
					bs = tqr.next();
					tqrCount++;
					//logger.info(tqrCount);
				}
				sub = bs.getValue(queryVar[count*3].substring(1));
				pred = bs.getValue(queryVar[count*3+1].substring(1));
				if(bs.size()>2){
					obj = bs.getValue(queryVar[count*3+2].substring(1));
				}
				else {
					obj = pred;
					pred = "http://semoss.org/Relation/" + Utility.getInstanceName(sub + "") + ":" + Utility.getInstanceName(obj + "");
					nextPred =  pred + "";
				}
				count++;
			}
			thisSt.setSubject(sub+"");
			thisSt.setPredicate(pred+"");
			thisSt.setObject(obj);
			if (count==triples)
			{
				count=0;
			}
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return thisSt;
	}
	
	protected void processSelectVar()
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

	protected String[] getVariables()
	{
		try {
			var = new String[tqr.getBindingNames().size()];
			List <String> names = tqr.getBindingNames();
			for(int colIndex = 0;colIndex < names.size();var[colIndex] = names.get(colIndex), colIndex++);
			return var;
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void execute() {
		tqr = (TupleQueryResult) engine.execQuery(query);
		getVariables();
		
		processSelectVar();
		count=0;
	}

	@Override
	public boolean hasNext() {
		if(this.nextPred != null){
			return true;
		}
		boolean retBool = false;
		try {
			retBool = tqr.hasNext();
			if(!retBool)
				tqr.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retBool;
	}

}
