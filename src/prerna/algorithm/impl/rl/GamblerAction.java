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
 * GamblerAction is part of the Reinforcement Learning module.
 * It specifies the action a gambler can take at any point during the game
 * by representing a certain amount the gambler can bid.
 * It determines the next states given this bid.
 * @author ksmart
 */
public class GamblerAction extends Action{

	static final Logger LOGGER = LogManager.getLogger(GamblerAction.class.getName());
	
	private int betAmount;
	private static final String WIN_KEY = "win";
	private static final String LOSE_KEY = "lose";
	
	public GamblerAction(String id, int betAmount) {
		super(id);
		this.betAmount = betAmount;
	}
	
	@Override
	public void addOption(String optionName, double probability) {
		if(!optionName.equals(WIN_KEY) && !optionName.equals(LOSE_KEY)) {
			LOGGER.error("Action "+id + "cannot have the option specified: "+optionName);
			LOGGER.error("Option names are limited to: "+WIN_KEY+", "+LOSE_KEY);
			return;
		}
		super.addOption(optionName, probability);
	}
	
	public void setWinProbability(double probability) {
		if(probability<0.0 || probability > 1.0) {
			LOGGER.error("Action "+id + "cannot have a win probability of "+probability);
			LOGGER.error("Probability values must be between 0 and 1");
		}
		super.addOption(WIN_KEY,probability);
		super.addOption(LOSE_KEY,1-probability);	
	}

	public Hashtable<String,State> calculateNextStates(ArrayList<State> states, State currState) {

		Hashtable<String,State> nextStateHash = new Hashtable<String,State>();
		
		int size = states.size() - 1;
		int currStateNumber = ((NumericalState)currState).getX();
		int maxAllowedBet = Math.min(currStateNumber,size-currStateNumber);
		if(betAmount > maxAllowedBet )
			return nextStateHash;
		else {
			State winState = findState(states,currStateNumber + betAmount);
			State loseState = findState(states,currStateNumber - betAmount);
			
			nextStateHash.put(WIN_KEY,winState);
			nextStateHash.put(LOSE_KEY,loseState);
		}
		
		return nextStateHash;
	}
	
	private State findState(ArrayList<State> states,Integer x) {
		for(State state : states)
			if(((NumericalState)state).getX().equals(x))
				return state;
		return null;		
	}
	public int getBetAmount() {
		return betAmount;
	}
}
