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
package prerna.algorithm.impl.rl;

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * 
 * @author ksmart
 */
public class SingleDirectionAction extends MultiDirectionAction{
	static final Logger LOGGER = LogManager.getLogger(SingleDirectionAction.class.getName());
	
	public SingleDirectionAction(String id, int xSize, int ySize) {
		super(id,xSize,ySize);
		
		if(!id.equals(LEFT_KEY) && !id.equals(RIGHT_KEY) && !id.equals(UP_KEY) && !id.equals(DOWN_KEY)) {
			LOGGER.error("SingleDirectionAction cannot have the id specified: "+id);
			LOGGER.error("ID names for this are limited to: "+LEFT_KEY+", "+RIGHT_KEY+", "+UP_KEY+", "+DOWN_KEY);
			return;
		}
	}

	@Override
	public Hashtable<String,State> calculateNextStates(ArrayList<State> states, State currState) {
		Hashtable<String,State> nextStateHash = new Hashtable<String,State>();
		
		State leftState = calculateNextState(states,currState,id);
		nextStateHash.put(id,leftState);
		
		return nextStateHash;
	}

}
