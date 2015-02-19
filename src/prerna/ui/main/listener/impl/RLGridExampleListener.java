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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.ArrayList;

import javax.swing.JComponent;
import javax.swing.JDesktopPane;
import javax.swing.JTextField;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.rl.Action;
import prerna.algorithm.impl.rl.GridState;
import prerna.algorithm.impl.rl.MultiDirectionAction;
import prerna.algorithm.impl.rl.SingleDirectionAction;
import prerna.algorithm.impl.rl.State;
import prerna.algorithm.impl.rl.ValueIterationAlgorithm;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.playsheets.RLColumnChartPlaySheet;
import prerna.ui.components.playsheets.RLGridPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This is the listener that runs the reinforcement learning example of moving in a grid.
 * @author ksmart
 *
 */
public class RLGridExampleListener implements IChakraListener {
	
	static final Logger LOGGER = LogManager.getLogger(RLGridExampleListener.class.getName());
	
	private ArrayList<State> stateList;
	private ArrayList<Action> actionList;
	private int xSize = 4;
	private int ySize = 4;
	
	/**
	 * Uses Reinforcement Learning to create an optimal strategy for moving in a grid.
	 * @param actionevent ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		LOGGER.info("RL Grid example button pushed ...");
		
		JTextField xSizeField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.RL_X_SIZE_TEXT_FIELD);
		String xSizeTextValue = xSizeField.getText();
		xSize = 0;
		JTextField ySizeField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.RL_Y_SIZE_TEXT_FIELD);
		String ySizeTextValue = ySizeField.getText();
		ySize = 0;
		JTextField discountRateField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.RL_DISCOUNT_RATE_TEXT_FIELD);
		String discountRateTextValue = discountRateField.getText();
		double discountRate = 0.0;
		
		try{
			xSize = Integer.parseInt(xSizeTextValue);
			ySize = Integer.parseInt(ySizeTextValue);
			discountRate = Double.parseDouble(discountRateTextValue);
		}catch(RuntimeException e){
			Utility.showError("X and Y sizes must be integers greater than or equal to 2 and the discount rate must be a decimal greater than 0 and less than or equal to 1");
			return;
		}
		if(xSize<2 || ySize <2 || discountRate<=0.0 || discountRate > 1.0) {
			Utility.showError("X and Y sizes must be integers greater than or equal to 2 and the discount rate must be a decimal between 0 and 1");
			return;
		}
		
		//builds test data so that the user has the option to choose to move in 4 directions (up, down, left, right)
		//finds the optimal strategy for how the user should move at each state to maximize reward
		fillDirectionData();
		
		RLGridPlaySheet playSheet = new RLGridPlaySheet();
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		playSheet.setJDesktopPane(pane);
		playSheet.setStateList(stateList);
		playSheet.setActionList(actionList);
		playSheet.setGridDimensions(xSize,ySize);
		playSheet.setTitle("Reinforcement Learning Grid Example Results");
		playSheet.setDiscountRate(discountRate);
		
		PlaysheetCreateRunner runner = new PlaysheetCreateRunner(playSheet);
		Thread playThread = new Thread(runner);
		playThread.start();		
		
//		//builds test data so that the action is to move, but the direction is random.
//		//uses random strategy
//		fillMoveData();
//		ValueIterationAlgorithm alg2 = new ValueIterationAlgorithm(stateList,actionList);
//		alg2.findNormalPolicy();
	}

	
	/**
	 * Builds test data with single action move that has 4 options (left, right, down, up) with equal probability.
	 */
	public void fillMoveData() {
		stateList = new ArrayList<State>();
		actionList = new ArrayList<Action>();
		
		MultiDirectionAction move = new MultiDirectionAction("move",xSize,ySize);
		move.addOption("left", 0.25);
		move.addOption("right", 0.25);
		move.addOption("up", 0.25);
		move.addOption("down", 0.25);
		
		actionList.add(move);
		
		stateList.add(new GridState("s0",0.0,0,0,true));
		for(int i=1;i<xSize*ySize - 1;i++) {
			stateList.add(new GridState("s"+i,-1.0,i%xSize,i/xSize,false));
		}		
	}
	
	/**
	 * Builds test data with 4 actions (left, right down, up) that the user can choose between.
	 */
	public void fillDirectionData() {
		stateList = new ArrayList<State>();
		actionList = new ArrayList<Action>();
		
		SingleDirectionAction left = new SingleDirectionAction("left",xSize,ySize);
		left.addOption("left",1.0);	
		actionList.add(left);

		SingleDirectionAction right = new SingleDirectionAction("right",xSize,ySize);
		right.addOption("right",1.0);	
		actionList.add(right);

		SingleDirectionAction up = new SingleDirectionAction("up",xSize,ySize);
		up.addOption("up",1.0);	
		actionList.add(up);

		SingleDirectionAction down = new SingleDirectionAction("down",xSize,ySize);
		down.addOption("down",1.0);		
		actionList.add(down);
		
		stateList.add(new GridState("s0",0.0,0,0,true));
		for(int i=1;i<xSize*ySize - 1;i++) {
			stateList.add(new GridState("s"+i,-1.0,i%xSize,i/xSize,false));
		}		
	}

	/**
	 * Override method from IChakraListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {
		
	}
}
