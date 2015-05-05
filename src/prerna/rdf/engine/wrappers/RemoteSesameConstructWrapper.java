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

import org.openrdf.model.Statement;

import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IConstructWrapper;
import prerna.util.Utility;

public class RemoteSesameConstructWrapper extends AbstractWrapper implements IConstructWrapper {

	transient SesameConstructWrapper remoteWrapperProxy = null;
	transient IConstructStatement retSt = null;
	transient ObjectInputStream ris = null;


	@Override
	public void execute() {
		remoteWrapperProxy = (SesameConstructWrapper)engine.execQuery(query);
	}

	@Override
	public boolean hasNext() {
		boolean retBool = false;
		
		if(retSt != null) // they have not picked it up yet
			return true;
		retSt = new ConstructStatement();
		// I need to pull from remote
		// this is just so stupid to call its own
		try {
		if(ris == null)
		{
			Hashtable params = new Hashtable<String,String>();
			params.put("id", remoteWrapperProxy.getRemoteID());
			ris = new ObjectInputStream(Utility.getStream(remoteWrapperProxy.getRemoteAPI() + "/next", params));
		}					
			Object myObject = ris.readObject();
			
			if(!myObject.toString().equalsIgnoreCase("null"))
			{
				Statement stmt = (Statement)myObject;
				retSt.setSubject(stmt.getSubject()+"");
				retSt.setObject(stmt.getObject());
				retSt.setPredicate(stmt.getPredicate() + "");
				//System.out.println("Abile to get the object appropriately here " + retSt.getSubject());
				retBool = true;
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

		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			retSt = null;
			retBool = false;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			retSt = null;
			retBool = false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			retSt = null;
			retBool = false;
		}
		return retBool;
	}

	@Override
	public IConstructStatement next() {
		// TODO Auto-generated method stub
		IConstructStatement thisSt = retSt;
		retSt = null;
		return thisSt;
	}

}
