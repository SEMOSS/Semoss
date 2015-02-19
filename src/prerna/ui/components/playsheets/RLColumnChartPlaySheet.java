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
package prerna.ui.components.playsheets;

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.rl.Action;
import prerna.algorithm.impl.rl.GamblerAction;
import prerna.algorithm.impl.rl.NumericalState;
import prerna.algorithm.impl.rl.State;
import prerna.algorithm.impl.rl.ValueIterationAlgorithm;

public class RLColumnChartPlaySheet extends ColumnChartPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(RLColumnChartPlaySheet.class.getName());
	private ArrayList<State> stateList;
	private ArrayList<Action> actionList;
	private double probWin;
	private double discountRate;
	
	public RLColumnChartPlaySheet() 
	{
		super();
	}
	
	@Override
	public void createData()
	{
		ValueIterationAlgorithm alg = new ValueIterationAlgorithm(discountRate,stateList,actionList);
		alg.findOptimalPolicy();
		Hashtable<State, ArrayList<Action>> optimalActionHash = alg.getOptimalActionHash();
		
		names = new String[]{"Dollar Amount","Suggested Bid Amount for win p="+probWin};
		list = new ArrayList<Object []>();
		
		for(State state : stateList) {
			int dollarAmt = ((NumericalState)state).getX();

			int minBet = 0;
			if(optimalActionHash.containsKey(state) && optimalActionHash.get(state).size() > 0 ) {
				ArrayList<Action> actions = optimalActionHash.get(state);
				minBet = ((GamblerAction)actions.get(0)).getBetAmount();
				for(Action action : actions) {
					int nextBet = ((GamblerAction)action).getBetAmount();
					minBet = Math.min(minBet,nextBet);
				}
//				GamblerAction action = (GamblerAction)optimalActionHash.get(state).get(0);
//				betAmt = action.getBetAmount();
			}
			Object[] row = new Object[]{dollarAmt,minBet};
			list.add(row);
		}
		dataHash = processQueryData();
	}
	
	public void setStateList(ArrayList<State> stateList) {
		this.stateList = stateList;
	}
	public void setActionList(ArrayList<Action> actionList) {
		this.actionList = actionList;	
	}
	public void setProbWin(double probWin) {
		this.probWin = probWin;	
	}
	public void setDiscountRate(double discountRate) {
		this.discountRate = discountRate;
	}
}
