/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JToggleButton;

import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Listener for btnAdvancedFinancialFunctions on MHS TAP Tab
 * Results in showing the Transition Cost Calculations buttons to calculate GL Items in TAP Cost database
 */
public class ShowHideAdvancedFinancials implements IChakraListener{

	/**
	 * This is executed when the btnAdvancedFinancialFunctions is pressed by the user
	 * Press when user wants to get additional buttons to add base transition and additional transition costs for the TAP Cost database
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		JToggleButton button = (JToggleButton) arg0.getSource();
		JPanel advancedPanel = (JPanel) DIHelper.getInstance().getLocalProp(Constants.ADVANCED_TRANSITION_FUNCTIONS_PANEL);
		
		//if button currently says "Show", show panel and set text of button to Hide
		if(button.isSelected()){
			button.setText("Hide");
			advancedPanel.setVisible(true);
		}
		
		//if button currently says "Hide", hide panel and set text of button to Show
		else{
			button.setText("Show");
			advancedPanel.setVisible(false);
		}
	}

	/**
	 * Override method from IChakraListener
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {
		
	}


}
