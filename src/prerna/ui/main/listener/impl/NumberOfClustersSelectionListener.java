/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

import javax.swing.JComboBox;
import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.playsheets.MachineLearningModulePlaySheet;

/**
 * Controls the attribute to classify on. Tied to a JComboBox in the ClassifyClusterPlaySheet.
 * Greys out row in independent variable table when it has been selected
 */
public class NumberOfClustersSelectionListener extends AbstractListener {
	static final Logger logger = LogManager.getLogger(ClassificationSelectionListener.class.getName());
	
	//shows or hides the JTextField for inputting a number of clusters depending on if the JComboBox is set to manually select
	private MachineLearningModulePlaySheet playSheet;
	
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JComboBox<String> bx = (JComboBox<String>)e.getSource();
		String selection = bx.getSelectedItem() + "";
		//if manually select need to show text field, otherwise hide it
		if(selection.equals(playSheet.getManuallySelectNumClustersText()))
			playSheet.showSelectNumClustersTextField(true);
		else
			playSheet.showSelectNumClustersTextField(false);
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.playSheet = (MachineLearningModulePlaySheet) view;
	}
	

}
