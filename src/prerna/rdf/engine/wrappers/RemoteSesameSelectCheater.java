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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Hashtable;

import org.openrdf.query.BindingSet;

import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IConstructWrapper;
import prerna.engine.impl.rdf.RemoteSemossSesameEngine;
import prerna.util.Utility;

public class RemoteSesameSelectCheater extends SesameSelectCheater implements IConstructWrapper {
	
	SesameSelectCheater proxy = null;
	String [] var = null;
	ObjectInputStream ris = null;
	IConstructStatement retSt = null;

	@Override
	public IConstructStatement next() {
		// TODO Auto-generated method stub
		IConstructStatement thisSt = retSt;
		retSt = null;
		return thisSt;
	}

	@Override
	public void execute() {
		System.out.println("Trying to get the wrapper remotely now");
		processSelectVar();
		count = 0;
		proxy = (SesameSelectCheater)((RemoteSemossSesameEngine)(engine)).execCheaterQuery(query);		
	}

	@Override
	public boolean hasNext() {
		
		boolean retBool = false;
		
		if(retSt != null) // they have not taken the previous one yet
			return true;
		retSt = new ConstructStatement();
		
		// I need to pull from remote
		// this is just so stupid to call its own
		try {
			if(ris == null)
			{
				Hashtable params = new Hashtable<String,String>();
				params.put("id", proxy.getRemoteID());
				ris = new ObjectInputStream(Utility.getStream(proxy.getRemoteAPI() + "/next", params));
			}	

			if(count==0)
			{
				Object myObject = ris.readObject();
				if(!myObject.toString().equalsIgnoreCase("null"))
				{
					bs = (BindingSet)myObject;
					retBool = true;
				}
				//tqrCount++;
				//logger.info(tqrCount);
			}
			logger.debug("Adding a sesame statement ");
			
			// there should only be three values

			Object sub=null;
			Object pred = null;
			Object obj = null;
			while (sub==null || pred==null || obj==null)
			{
				if (count==triples)
				{
					count=0;
					Object myObject = ris.readObject();
					if(!myObject.toString().equalsIgnoreCase("null"))
					{
						bs = (BindingSet)myObject;
						tqrCount++;
					}
					else
					{
						try{
							if(ris!=null) {
								ris.close();
							}
						} catch(IOException e) {
							e.printStackTrace();
						}
					}

					//logger.info(tqrCount);
				}
				sub = bs.getValue(queryVar[count*3].substring(1));
				pred = bs.getValue(queryVar[count*3+1].substring(1));
				obj = bs.getValue(queryVar[count*3+2].substring(1));
				count++;
			}
			retSt.setSubject(sub+"");
			retSt.setPredicate(pred+"");
			retSt.setObject(obj);
			if (count==triples)
			{
				count=0;
			}
			retBool = true;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retBool;
	}

	public String [] getVariables()
	{
		var = proxy.getVariables();
		return var;
	}
}
