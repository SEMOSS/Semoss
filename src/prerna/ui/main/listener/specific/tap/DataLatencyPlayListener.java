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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyVetoException;

import javax.swing.JTextField;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.components.specific.tap.DataLatencyPlayPopup;

/**
 * Opens the Data Latency Scenario user interface when JMenuItem "Data Latency Scenario" is selected by the user
 */
public class DataLatencyPlayListener implements ActionListener{

	GraphPlaySheet ps = null;
	SEMOSSVertex [] pickedVertex = null;
	static final Logger logger = LogManager.getLogger(DataLatencyPlayListener.class.getName());
	
	/**
	 * Constructor for DataLatencyPlayListener.
	 * @param p 		GraphPlaySheet
	 * @param pickedV 	DBCMVertex[]
	 */
	public DataLatencyPlayListener(GraphPlaySheet p, SEMOSSVertex[] pickedV){
		ps = p;
		pickedVertex = pickedV;
	}

	/**
	 * Opens the Data Latency Scenario user interface when JMenuItem "Data Latency Scenario" is selected by the user
	 * Calls method createPopup to create the user interface
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		logger.info("Beginning Data Latency Analysis");
		//add the data latency slider popup to the desktop pane
		
		if(ps.dataLatencyPopUp!=null && ps.dataLatencyPopUp.isVisible()){
			//this means that the data latency analysis is currently being performed
			//close data latency stuff and run data latency play stuff
			ps.dataLatencyPopUp.doDefaultCloseAction();
			createPopup();
		}
		else if(ps.dataLatencyPlayPopUp != null && ps.dataLatencyPlayPopUp.isVisible()){
			//this means that the data latency play is currently being run
			//reset to the beginning and let her rip
			ps.dataLatencyPlayPopUp.setHoursValue(0.0);
			ps.dataLatencyPlayPopUp.toFront();
			try {
				ps.dataLatencyPlayPopUp.setSelected(true);
			} catch (PropertyVetoException e1) {
				e1.printStackTrace();
			}
		}
		else {
			createPopup();
		}
	}
	
	/**
	 * Calls DataLatencyPlayPopup to create the user interface
	 */
	public void createPopup(){
		final DataLatencyPlayPopup dataLatePopup = new DataLatencyPlayPopup(0.0, ps, pickedVertex);
		dataLatePopup.create();
		dataLatePopup.display();
		dataLatePopup.setHoursValue(0.0);
		dataLatePopup.toFront();
		try {
			dataLatePopup.setSelected(true);
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
	}
}
