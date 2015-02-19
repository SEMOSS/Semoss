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
import javax.swing.JSlider;
import javax.swing.JTextField;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.rl.Action;
import prerna.algorithm.impl.rl.GamblerAction;
import prerna.algorithm.impl.rl.NumericalState;
import prerna.algorithm.impl.rl.State;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.playsheets.RLGridScatterSheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RLGamblingExampleListener  implements IChakraListener {
	
	private static final Logger LOGGER = LogManager.getLogger(RLGamblingExampleListener.class.getName());
	
	private ArrayList<State> stateList;
	private ArrayList<Action> actionList;
	private double probWin;
	
	/**
	 * Uses Reinforcement Learning to create an optimal strategy for gambling example.
	 * @param actionevent ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		LOGGER.info("RL Gambling example button pushed ...");
		
		JTextField discountRateField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.RL_DISCOUNT_RATE_TEXT_FIELD);
		String discountRateTextValue = discountRateField.getText();
		double discountRate = 0.0;
		
		try{
			discountRate = Double.parseDouble(discountRateTextValue);
		}catch(RuntimeException e){
			Utility.showError("The discount rate must be a decimal between 0 and 1");
			return;
		}
		if(discountRate<0.0 || discountRate > 1.0) {
			Utility.showError("The discount rate must be a decimal between 0 and 1");
			return;
		}
		
		JSlider probWinSlider = (JSlider) DIHelper.getInstance().getLocalProp(Constants.RL_PROB_WIN_SLIDER);
		int probWinInt = probWinSlider.getValue();
		probWin = probWinInt / 100.0;
		
		fillTestData();
				
		RLGridScatterSheet playSheet = new RLGridScatterSheet();
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		playSheet.setJDesktopPane(pane);
		playSheet.setStateList(stateList);
		playSheet.setActionList(actionList);
		playSheet.setProbWin(probWin);
		playSheet.setDiscountRate(discountRate);
		
		PlaysheetCreateRunner runner = new PlaysheetCreateRunner(playSheet);
		Thread playThread = new Thread(runner);
		playThread.start();
		
	}
	
	/**
	 * Builds test data for example.
	 * 101 states are created representing each possible numerical value the gambler can have.
	 * Reward of 1 is received only if the gambler reaches $100.
	 * 50 actions are created representing each possible amount that the gambler can bet on a turn.
	 * There are 2 possible results of each action, winning with 0.4% chance and losing with 0.6% chance
	 */
	public void fillTestData() {
		stateList = new ArrayList<State>();		
		stateList.add(new NumericalState("s0",0.0,0,true));
		for(int i=1;i<100;i++) {
			stateList.add(new NumericalState("s"+i,0.0,i,false));
		}
		stateList.add(new NumericalState("s100",1.0,100,true));
		
		actionList = new ArrayList<Action>();
		for(int i=1;i<=50;i++) {
			GamblerAction action = new GamblerAction("a"+i,i);
			action.setWinProbability(probWin);
			actionList.add(action);
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
