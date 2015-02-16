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

import prerna.rdf.engine.api.IConstructWrapper;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.IEngineWrapper;
import prerna.rdf.engine.api.ISelectWrapper;

public class WrapperManager {

	// main job of this class is to take an engine
	// find the type of the engine
	// and the type of query
	// and then give back a wrapper
	// I need to make this completely through reflection
	// I will do that later
	
	public static WrapperManager manager = null;
	
	protected WrapperManager()
	{
		
	}
	
	public static WrapperManager getInstance()
	{
		// cant get lazier than this :)
		if(manager == null)
		{
			manager = new WrapperManager();
			// some other routine to load it
		}
		return manager;
	}
	
	public ISelectWrapper getSWrapper(IEngine engine, String query)
	{
		ISelectWrapper returnWrapper = null;
			switch(engine.getEngineType())
			{
			case SESAME: {
				returnWrapper = new SesameSelectWrapper();
				break;
			}
			case JENA: {
				returnWrapper = new JenaSelectWrapper();
				break;
			}
			case SEMOSS_SESAME_REMOTE:{
				returnWrapper = new RemoteSesameSelectWrapper();
				break;
			}
			case RDBMS:{
				returnWrapper = new RDBMSSelectWrapper();
				break;
				//TBD
			}

			default: {
				
			}
			}
			returnWrapper.setEngine(engine);
			returnWrapper.setQuery(query);
			returnWrapper.execute();
			//ISelectWrapper doh = (ISelectWrapper)returnWrapper;
			returnWrapper.getVariables();
			
			return returnWrapper;
	}

	public IConstructWrapper getCWrapper(IEngine engine, String query)
	{
		IConstructWrapper returnWrapper = null;
			switch(engine.getEngineType())
			{
			case SESAME: {
				returnWrapper = new SesameConstructWrapper();
				break;
			}
			case JENA: {
				returnWrapper = new JenaConstructWrapper();
				break;
			}
			case SEMOSS_SESAME_REMOTE:{
				returnWrapper = new RemoteSesameConstructWrapper();
				break;
			}
			case RDBMS:{
				//TBD
			}

			default: {
				
			}
			}
			returnWrapper.setEngine(engine);
			returnWrapper.setQuery(query);
			returnWrapper.execute();
			
			return returnWrapper;
	}

	public IConstructWrapper getChWrapper(IEngine engine, String query)
	{
		IConstructWrapper returnWrapper = null;
			switch(engine.getEngineType())
			{
			case SESAME: {
				returnWrapper = new SesameSelectCheater();
				break;
			}
			case JENA: {
				returnWrapper = new JenaSelectCheater();
				break;
			}
			case SEMOSS_SESAME_REMOTE:{
				returnWrapper = new RemoteSesameSelectCheater();
				break;
			}
			case RDBMS:{
				//TBD
			}

			default: {
				
			}
			}
			returnWrapper.setEngine(engine);
			returnWrapper.setQuery(query);
			returnWrapper.execute();
			//ISelectWrapper doh = (ISelectWrapper)returnWrapper;
			//returnWrapper.getVariables();
			
			return returnWrapper;
	}
	
}
