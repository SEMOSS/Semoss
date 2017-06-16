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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;

import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.util.Utility;

public class RemoteSesameSelectWrapper extends SesameSelectWrapper implements ISelectWrapper, IRawSelectWrapper {

	private static final Logger LOGGER = LogManager.getLogger(RemoteSesameSelectWrapper.class.getName());
	
	transient SesameSelectWrapper remoteWrapperProxy = null;
	transient ISelectStatement retSt = null;
	transient ObjectInputStream ris = null;
	transient BindingSet bs = null;


	@Override
	public void execute() {
		System.out.println("Trying to get the wrapper remotely now");
		remoteWrapperProxy = (SesameSelectWrapper)engine.execQuery(query);
		this.displayVar = remoteWrapperProxy.displayVar;
		this.var = remoteWrapperProxy.var;
				
//		var = remoteWrapperProxy.getVariables();
//		System.out.println("Output variables is " + remoteWrapperProxy.getVariables());
	}

	@Override
	public boolean hasNext() {
		boolean retBool = false;
		if(retSt != null) // this means they have not picked it up yet
			return true;
		//retSt = new SelectStatement();//

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
				bs = (BindingSet)myObject;
				//getDisplayVariables();
				//System.out.println("Proceeded to first");
				retSt = getSelectFromBinding(bs);			
				retBool = true;
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retBool;
	}

	@Override
	public ISelectStatement next() {
		// TODO Auto-generated method stub
		ISelectStatement thisSt = retSt;
		retSt = null;
		return thisSt;
	}

	@Override
	public String[] getVariables() {
		// TODO Auto-generated method stub
		var = remoteWrapperProxy.getVariables();
		System.out.println("Output variables is " + remoteWrapperProxy.getVariables());
		return var;
	}
	
	public String [] getDisplayVariables() {
		if(displayVar == null)
		{
			displayVar = remoteWrapperProxy.displayVar;
			/*
			try {
				ObjectInputStream displayStream;
				Hashtable params = new Hashtable<String,String>();
				params.put("id", remoteWrapperProxy.getRemoteID());
				displayStream = new ObjectInputStream(Utility.getStream(remoteWrapperProxy.getRemoteAPI() + "/getDisplayVariables", params));
				displayVar = (String [])displayStream.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		}
		return displayVar;
	}
	
	protected ISelectStatement getSelectFromBinding(BindingSet bs)
	{
		getDisplayVariables();
		String [] variableArr = displayVar;
		
		ISelectStatement sjss = new SelectStatement();
		for(int colIndex = 0;colIndex < variableArr.length;colIndex++)
		{
			Object val = bs.getValue(displayVar[colIndex]);
			Object parsedVal = getRealValue(val);

			sjss.setVar(variableArr[colIndex], parsedVal);
			if(val!=null){
				sjss.setRawVar(variableArr[colIndex], val);
			}
			LOGGER.debug("Binding Name " + variableArr[colIndex]);
		}
		return sjss;
	}
}
