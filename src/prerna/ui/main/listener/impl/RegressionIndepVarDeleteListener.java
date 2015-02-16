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

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.util.List;

import javax.swing.DefaultListModel;
import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import prerna.ui.components.api.IChakraListener;

/**
 * Controls the deletion of independent variables from the input.
 */
public class RegressionIndepVarDeleteListener implements IChakraListener {
	
	/**
	 * 
	 * Method actionPerformed.
	 * Pulls the selected items from the possible input list and deletes them from the regressors list.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		Component source = (Component) e.getSource();
		JPanel regPanel = (JPanel) source.getParent().getParent();
		
		JScrollPane possibleInputScrollPane = (JScrollPane)(regPanel.getComponent(9));
		JList possibleInputList = (JList)((possibleInputScrollPane.getViewport()).getView());
		List<String> indepVarToDelete = (possibleInputList.getSelectedValuesList());
		
		JScrollPane indepVarScrollPane = (JScrollPane)(regPanel.getComponent(9));
		JList indepVarList = (JList)((indepVarScrollPane.getViewport()).getView());
		DefaultListModel indepVarModelList = (DefaultListModel)indepVarList.getModel();
		for(String toDelete : indepVarToDelete)
		{
			if(indepVarModelList.contains(toDelete))
				indepVarModelList.removeElement(toDelete);
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