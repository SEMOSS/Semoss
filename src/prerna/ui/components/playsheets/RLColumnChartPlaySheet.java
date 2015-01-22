/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
	
	public RLColumnChartPlaySheet() 
	{
		super();
	}
	
	@Override
	public void createData()
	{
		ValueIterationAlgorithm alg = new ValueIterationAlgorithm(stateList,actionList);
		alg.findOptimalPolicy();
		Hashtable<State, ArrayList<Action>> optimalActionHash = alg.getOptimalActionHash();
		
		names = new String[]{"Dollar Amount","Suggested Bid Amount"};
		list = new ArrayList<Object []>();
		
		for(State state : stateList) {
			int dollarAmt = ((NumericalState)state).getX();

			int betAmt = 0;
			if(optimalActionHash.containsKey(state) && optimalActionHash.get(state).size() > 0 ) {
				ArrayList<Action> actions =optimalActionHash.get(state);
				int minBet = ((GamblerAction)actions.get(0)).getBetAmount();
				for(Action action : actions) {
					int nextBet = ((GamblerAction)action).getBetAmount();
					minBet = Math.min(minBet,nextBet);
				}
				GamblerAction action = (GamblerAction)optimalActionHash.get(state).get(0);
				betAmt = action.getBetAmount();
			}
			Object[] row = new Object[]{dollarAmt,betAmt};
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
}
