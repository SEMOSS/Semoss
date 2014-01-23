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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JPanel;

import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Controls what to do when the "Show Advanced Features" button is selected in the DB modification tab.  Hides/Unhides the options.
 */
public class AdvancedImportOptionsListener implements IChakraListener{

	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JButton button = (JButton) e.getSource();
		JPanel advancedPanel = (JPanel) DIHelper.getInstance().getLocalProp(Constants.ADVANCED_IMPORT_OPTIONS_PANEL);
		
		//if button currently says "Show", show panel and set text of button to Hide
		if(button.getText().contains("Show")){
			button.setText(button.getText().replace("Show", "Hide"));
			advancedPanel.setVisible(true);
		}
		
		//if button currently says "Hide", hide panel and set text of button to Show
		else if(button.getText().contains("Hide")){
			button.setText(button.getText().replace("Hide", "Show"));
			advancedPanel.setVisible(false);
		}
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		
	}


}
