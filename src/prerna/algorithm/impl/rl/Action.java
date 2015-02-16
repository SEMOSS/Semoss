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

/**
 * Action is part of the Reinforcement Learning module.
 * It specifies a specific type of movement between states that occur in the problem.
 * A single action can have 1 or more options for movement that occur with a specified probability.
 * Each option leads to a single next state, but multiple options may lead to the same state.
 * @author ksmart
 */
public class Action {

	protected String id;
	private ArrayList<String> optionsList; //list of all the possible options that can be taken
	private Hashtable<String, Double> probabilityHash; //probability of selecting each option. all should add up to 1

	public Action(String id) {
		this.id = id;
		this.optionsList = new ArrayList<String>();
		this.probabilityHash = new Hashtable<String, Double>();
	}
	
	public void addOption(String optionName, double probability) {
		optionsList.add(optionName);
		probabilityHash.put(optionName, probability);
	}

	public Hashtable<String,State> calculateNextStates(ArrayList<State> states, State currState) {
		return null;
	}
	
	public double getProbablity(String option) {
		return probabilityHash.get(option);
	}
	
	public String getID() {
		return id;
	}
	
	@Override
	public boolean equals(Object obj) {
		return this.id.equals(((Action)obj).getID());
	}
}
