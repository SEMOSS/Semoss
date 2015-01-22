package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.ArrayList;

import javax.swing.JComponent;
import javax.swing.JDesktopPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.rl.Action;
import prerna.algorithm.impl.rl.GamblerAction;
import prerna.algorithm.impl.rl.NumericalState;
import prerna.algorithm.impl.rl.State;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.playsheets.RLColumnChartPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RLGamblingExampleListener  implements IChakraListener {
	
	private static final Logger LOGGER = LogManager.getLogger(RLGamblingExampleListener.class.getName());
	
	private ArrayList<State> stateList;
	private ArrayList<Action> actionList;
	
	/**
	 * Uses Reinforcement Learning to create an optimal strategy for gambling example.
	 * @param actionevent ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		LOGGER.info("RL Gambling example button pushed ...");
		
		fillTestData();
				
		RLColumnChartPlaySheet playSheet = new RLColumnChartPlaySheet();
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		playSheet.setJDesktopPane(pane);
		playSheet.setStateList(stateList);
		playSheet.setActionList(actionList);
		
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
			action.setWinProbability(0.4);
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
