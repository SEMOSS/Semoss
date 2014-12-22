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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JComboBox;
import javax.swing.JComponent;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.playsheets.ClassifyClusterPlaySheet;

/**
 * Controls the algorithm to use, whether clustering or classifying. Tied to the JComboBox in the ClassifyClusterPlaySheet.
 */
public class SelectAlgorithmListener extends AbstractListener {
	static final Logger logger = LogManager.getLogger(SelectAlgorithmListener.class.getName());
	
	//given two panels, the cluster panel and the classify panel and determines which one to show based on what is clicked.
	private ClassifyClusterPlaySheet playSheet;
	
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JComboBox<String> bx = (JComboBox<String>)e.getSource();
		String selection = bx.getSelectedItem() + "";
		if(selection.equals("Cluster") ) {
			playSheet.showClassify(false);
			playSheet.showOutlier(false);
			playSheet.showCluster(true);
		} else if(selection.equals("Classify")){
			playSheet.showCluster(false);
			playSheet.showOutlier(false);
			playSheet.showClassify(true);
		} else if(selection.equals("Outliers")) {
			playSheet.showCluster(false);
			playSheet.showClassify(false);
			playSheet.showOutlier(true);
		} else if(selection.equals("Similarity") || selection.equals("Predictability")) {
			playSheet.showCluster(false);
			playSheet.showClassify(false);
			playSheet.showOutlier(false);
		}
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.playSheet = (ClassifyClusterPlaySheet)view;
	}

}
