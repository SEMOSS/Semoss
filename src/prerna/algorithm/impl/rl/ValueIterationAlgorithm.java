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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Hashtable;

public class ValueIterationAlgorithm {
	
	private double discountRate = 1.0;
	private double errorApprox = 0.1; //if this becomes >=1, need to adjust while loop.
	private BigDecimal initialPolicyVal = new BigDecimal(0.0);
	private ArrayList<State> stateList;
	private ArrayList<Action> actionList;
	private Hashtable<State, BigDecimal> optimalPolicyValHash;
	private Hashtable<State, ArrayList<Action>> optimalActionHash;

	
	public ValueIterationAlgorithm(ArrayList<State> stateList, ArrayList<Action> actionList) {
		this.stateList = stateList;
		this.actionList = actionList;
	}
	
	public ValueIterationAlgorithm(double discountRate,ArrayList<State> stateList, ArrayList<Action> actionList) {
		this.discountRate = discountRate;
		this.stateList = stateList;
		this.actionList = actionList;
	}

	public void findOptimalPolicy() {
		initialize();
		
		int count = 1;
		double delta = (1-discountRate) / discountRate + 1;
		while(delta > (1-discountRate) / discountRate* errorApprox) {
			Hashtable<State, BigDecimal> fvHash =  calculateMaxPolicyValueIteration();
			delta = calculateFVAndPolicyValDelta(fvHash);
			optimalPolicyValHash = fvHash;
//			System.out.println("ITERATION "+count+" RESULTS:");
//			System.out.println("MAXIMUM DELTA: "+delta);
//			printStateActionPolicyVal();
//			System.out.println();
			
			count++;
		}
		System.out.println("ITERATION "+count+" RESULTS:");
		System.out.println("MAXIMUM DELTA: "+delta);
		printStateActionPolicyVal();
		System.out.println();
	}
	
	public void findNormalPolicy() {
		initialize();
		
		int count = 1;
		double delta = (1-discountRate) / discountRate + 1;
		while(delta> (1-discountRate) / discountRate * errorApprox) {
			Hashtable<State, BigDecimal> fvHash =  calculateNormalPolicyValueIteration();
			delta = calculateFVAndPolicyValDelta(fvHash);
			optimalPolicyValHash = fvHash;
			System.out.println("ITERATION "+count+" RESULTS:");
			System.out.println("MAXIMUM DELTA: "+delta);
			printStateActionPolicyVal();
			System.out.println();
			
			count++;
		}
	}
	
	public void initialize() {
		//initialize policyValHash
		optimalPolicyValHash = new Hashtable<State, BigDecimal>();
		for(State state : stateList) {
			optimalPolicyValHash.put(state,initialPolicyVal);
		}
		optimalActionHash = new Hashtable<State, ArrayList<Action>>();
	}
	
	private Hashtable<State, BigDecimal> calculateMaxPolicyValueIteration() {
		Hashtable<State, BigDecimal> fvHash = new Hashtable<State, BigDecimal> ();
		for(State state : stateList) {
			ArrayList<Action> maxActionList = new ArrayList<Action>();
			if(!state.isTerminal()) {				
				Action firstAction = actionList.get(0);
				BigDecimal maxPolicyVal = calculatePolicyValue(state,firstAction);
				for(Action action : actionList) {
					BigDecimal policyVal = calculatePolicyValue(state,action);
					if(policyVal.equals(maxPolicyVal)) {
						maxActionList.add(action);
					}
					else 
						if(policyVal.compareTo(maxPolicyVal) > 0) {
						maxPolicyVal = policyVal;
						maxActionList = new ArrayList<Action>();
						maxActionList.add(action);
					}
				}			
				fvHash.put(state, maxPolicyVal);
			}else {
				fvHash.put(state, new BigDecimal(state.getReward()));
			}
			optimalActionHash.put(state, maxActionList);
		}

		return fvHash;
	}

	
	private Hashtable<State, BigDecimal> calculateNormalPolicyValueIteration() {
		Hashtable<State, BigDecimal> fvHash = new Hashtable<State, BigDecimal> ();
		for(State state : stateList) {
			if(!state.isTerminal()) {
				Action action = actionList.get(0);
				BigDecimal policyVal = calculateRandomValue(state,action);
				fvHash.put(state, policyVal);
			} else
				fvHash.put(state, new BigDecimal(state.getReward()));
		}

		return fvHash;
	}
	
	private BigDecimal calculateRandomValue(State state,Action action) {
		Hashtable<String,State> nextStates = action.calculateNextStates(stateList,state);
		
		if(nextStates.isEmpty())
			return new BigDecimal(0.0);
		
		BigDecimal expectedValue = new BigDecimal(0.0);
		
		for(String option : nextStates.keySet()) {
			State nextState = nextStates.get(option);
			
			BigDecimal nextStatePolVal = optimalPolicyValHash.get(nextState);
			expectedValue = expectedValue.add(nextStatePolVal);
			expectedValue = expectedValue.add(new BigDecimal(state.getReward()));
		}

		expectedValue = expectedValue.divide(new BigDecimal(nextStates.keySet().size()));
		
		return expectedValue;
	}
	
	private BigDecimal calculatePolicyValue(State state,Action action) {
		Hashtable<String,State> nextStates = action.calculateNextStates(stateList,state);
		
		if(nextStates.isEmpty())
			return new BigDecimal(0.0);
		
		BigDecimal optionSum = new BigDecimal(0.0);
		BigDecimal expectedValue = new BigDecimal(0.0);
		
		for(String option : nextStates.keySet()) {
			State nextState = nextStates.get(option);
			
			BigDecimal prob = new BigDecimal(action.getProbablity(option));
			BigDecimal nextStatePolVal = optimalPolicyValHash.get(nextState);
			BigDecimal mult = prob.multiply(nextStatePolVal);
			optionSum = optionSum.add(mult);
			
			BigDecimal reward = new BigDecimal(state.getReward());
			mult = prob.multiply(reward);
			expectedValue = expectedValue.add(mult);
		}

		expectedValue = expectedValue.divide(new BigDecimal(nextStates.keySet().size()));
		
		BigDecimal mult = optionSum.multiply(new BigDecimal(discountRate));
		BigDecimal policyVal = expectedValue.add(mult);
		return policyVal;
	}
	
	private double calculateFVAndPolicyValDelta(Hashtable<State, BigDecimal> fvHash) {
		State state0 = stateList.get(0);
		double maxDelta = Math.abs(optimalPolicyValHash.get(state0).doubleValue() - fvHash.get(state0).doubleValue());
		
		for(State state : stateList) {
			double delta = Math.abs(optimalPolicyValHash.get(state).doubleValue() - fvHash.get(state).doubleValue());
			maxDelta = Math.max(maxDelta, delta);
		}
		
		return maxDelta;
	}
	
	private void printStateActionPolicyVal() {
		for(State state : stateList) {
			System.out.print("State: "+state.getID()+" value: "+optimalPolicyValHash.get(state).doubleValue());
			if(optimalActionHash.size()>0) {
				ArrayList<Action> actionList = optimalActionHash.get(state);
				System.out.print(" actions:");
				for(Action action : actionList) {
					System.out.print(" "+action.getID());
				}
			}
			System.out.println();
		}
	}
	
	public Hashtable<State, ArrayList<Action>> getOptimalActionHash() {
		return optimalActionHash;
	}
	
	public Hashtable<State, BigDecimal> getOptimalPolicyValHash() {
		return optimalPolicyValHash;
	}

}
