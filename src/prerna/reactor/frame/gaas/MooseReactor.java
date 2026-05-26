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
package prerna.reactor.frame.gaas;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.gaas.ner.FillFormReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;


public class MooseReactor extends AbstractGaasBaseReactor {

	private static final Logger classLogger = LogManager.getLogger(MooseReactor.class);

	// we could move this to RDF Map also later
	Map <String, Class> commandReactorMap = new HashMap<String, Class>(); 
	
	public MooseReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.PROJECT.getKey()};
		this.keyRequired = new int[] {1, 0};
		
		commandReactorMap.put("text2sql", NLPQuery3Reactor.class);
		commandReactorMap.put("fillform", FillFormReactor.class);
		commandReactorMap.put("text2viz", NLPQuery3Reactor.class); // need to replace this
	}
	
	@Override
	public NounMetadata execute() 
	{
		// TODO Auto-generated method stub
		// some key things
		
		// command
		// project_id
		// other data - optional
		String command = this.store.getGenRowStruct(keysToGet[0]).get(0).toString();
		String realCommand = command.substring(0, command.indexOf(":")).toLowerCase();
		String newCommand = command.substring(command.indexOf(":") + 1);
		
		
		if(commandReactorMap.containsKey(realCommand))
		{
			try {
				AbstractReactor reactor = (AbstractReactor)commandReactorMap.get(realCommand).newInstance();
				GenRowStruct commandStruct = new GenRowStruct();
				commandStruct.addLiteral(newCommand);
				this.store.removeNoun(keysToGet[0]);
				this.store.addNoun(keysToGet[0], commandStruct);

				reactor.setNounStore(this.store);
				reactor.setInsight(insight);
				return reactor.execute();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				classLogger.error(Constants.STACKTRACE, e);
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return null;
	}
	
	
	

}
