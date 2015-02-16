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

/**
 * State is part of the Reinforcement Learning module.
 * It specifies a single state that can occur in the problem.
 * It stores the id of the state, the reward received for reaching this state, and whether it is terminal.
 * @author ksmart
 */
public class State {

	private String id;
	private double reward; //reward for reaching this state.
	private Boolean terminal;
	
	/**
	 * Creates a new state with an id and a reward
	 * @param id String representing the states id. Should be unique.
	 * @param reward Double representing the value of reaching this state
	 */
	public State(String id, double reward, Boolean terminal) {
		this.id = id;
		this.reward = reward;
		this.terminal = terminal;
	}
	
	public String getID() {
		return id;
	}
	
	public double getReward() {
		return reward;
	}
	
	public Boolean isTerminal() {
		return terminal;
	}
	
	@Override
	public boolean equals(Object obj) {
		return this.id.equals(((State)obj).getID());
	}
}
