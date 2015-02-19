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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.rl.Action;
import prerna.algorithm.impl.rl.State;
import prerna.algorithm.impl.rl.ValueIterationAlgorithm;

/**
 * The RLGridPlaySheet class creates the panel and table for a grid view of data from RL examples.
 */
@SuppressWarnings("serial")
public class RLGridPlaySheet extends GridPlaySheet{
	
	private static final Logger LOGGER = LogManager.getLogger(RLGridPlaySheet.class.getName());
	private ArrayList<State> stateList;
	private ArrayList<Action> actionList;
	private int xSize;
	private int ySize;
	private double discountRate;
	
	@Override
	public void createData() {
		ValueIterationAlgorithm alg = new ValueIterationAlgorithm(discountRate,stateList,actionList);
		alg.findOptimalPolicy();
		Hashtable<State,BigDecimal> optimalActionHash = alg.getOptimalPolicyValHash();
		
		names = new String[xSize+1];
		names[0] = "";
		for(int i=0;i<xSize;i++) {
			names[i+1]="Col"+i;
		}
		
		list = new ArrayList<Object []>();
		
		for(int row=0;row<ySize;row++) {
			Object[] newRow = new Object[xSize+1];
			newRow[0]="Row"+row;
			for(int col=0;col<xSize;col++) {
				State currState;
				if(row==ySize - 1 && col==xSize - 1)
					currState = stateList.get(0);
				else
					currState = stateList.get(row*xSize+col);
				newRow[col+1] = optimalActionHash.get(currState).doubleValue();
			}
			list.add(newRow);
		}
	}
	
	public void setStateList(ArrayList<State> stateList) {
		this.stateList = stateList;
	}
	public void setActionList(ArrayList<Action> actionList) {
		this.actionList = actionList;	
	}
	public void setGridDimensions(int xSize,int ySize) {
		this.xSize = xSize;
		this.ySize = ySize;
	}
	public void setDiscountRate(double discountRate) {
		this.discountRate = discountRate;
	}
}
