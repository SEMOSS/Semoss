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
package prerna.ui.main.listener.specific.tap;

import java.awt.Component;

import javax.swing.JSlider;
import javax.swing.JTextField;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.DataLatencyPerformer;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;


/**
 */
public class DataLatencySliderListener implements ChangeListener{

	GraphPlaySheet ps = null;
	SEMOSSVertex [] pickedVertex = null;
	static final Logger logger = LogManager.getLogger(DataLatencySliderListener.class.getName());
	DataLatencyPerformer latePerf;
	JTextField hoursTextField;
	JTextField daysTextField;
	JTextField weeksTextField;
	
	/**
	 * Constructor for DataLatencySliderListener.
	 * @param gps GraphPlaySheet
	 * @param vertVect DBCMVertex[]
	 */
	public DataLatencySliderListener(GraphPlaySheet gps, SEMOSSVertex[] vertVect){
		ps = gps;
		pickedVertex = vertVect;
		latePerf = new DataLatencyPerformer(ps, pickedVertex);
		
		setTextFields();
	}

	/**
	 * Method stateChanged.
	 * @param arg0 ChangeEvent
	 */
	@Override
	public void stateChanged(ChangeEvent arg0) {
		
		JSlider slider = (JSlider) arg0.getSource();
		
		int sliderValue = slider.getValue();
		logger.info("Value set to " + sliderValue);
		
		if(!latePerf.pickedVertex.equals(pickedVertex)){
			latePerf.setPickedVertex(pickedVertex);
		}
		
		latePerf.setValue(sliderValue);
		
		latePerf.execute();
		
		setTextValues(sliderValue);
		
	}
	
	/**
	 * Method setPickedVertex.
	 * @param verts DBCMVertex[]
	 */
	public void setPickedVertex(SEMOSSVertex[] verts){
		pickedVertex = verts;
	}
	
	/**
	 * Method setTextValues.
	 * @param sliderValue double
	 */
	private void setTextValues(double sliderValue){
		double totalHours = sliderValue;
		int weeks = (int) Math.floor(totalHours/168);
		int days = (int) Math.floor((totalHours-(weeks*168))/24);
		int hours = (int) (totalHours-(weeks*168)-(days*24));
		weeksTextField.setText(Integer.toString(weeks));
		daysTextField.setText(Integer.toString(days));
		hoursTextField.setText(Integer.toString(hours));
	}
	
	/**
	 * Method setTextFields.
	 */
	private void setTextFields(){
		Component[] comps = ps.dataLatencyPopUp.getContentPane().getComponents();
		for (int i = 0; i<comps.length; i++){
			String compName = comps[i].getName();
			if(compName!=null){
				if(compName.equals(Constants.DATA_LATENCY_WEEKS_TEXT))
					weeksTextField = (JTextField) comps[i];
				else if (compName.equals(Constants.DATA_LATENCY_DAYS_TEXT))
					daysTextField = (JTextField) comps[i];
				else if (compName.equals(Constants.DATA_LATENCY_HOURS_TEXT))
					hoursTextField = (JTextField) comps[i];
			}
		}
	}


}
