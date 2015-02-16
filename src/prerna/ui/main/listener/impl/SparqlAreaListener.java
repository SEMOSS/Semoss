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
import java.util.Hashtable;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JTextArea;

import prerna.ui.components.ParamComboBox;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Controls reading of the input from the SPARQL area.
 */
public class SparqlAreaListener extends AbstractListener {
	
	JTextArea sparql = null;
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param actionevent ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		// get to the parent view
		// get the param panel
		// get all the parameters
		// convert it into hashtable
		JPanel panel = (JPanel)DIHelper.getInstance().getLocalProp(Constants.PARAM_PANEL_FIELD);
		// get the currently visible panel
		Component [] comps = panel.getComponents();
		JComponent curPanel = null;
		for(int compIndex = 0;compIndex < comps.length && curPanel == null;compIndex++)
			if(comps[compIndex].isVisible())
				curPanel = (JComponent)comps[compIndex];
		
		// get all the param field
		Component [] fields = curPanel.getComponents();
		Hashtable paramHash = new Hashtable();
		for(int compIndex = 0;compIndex < fields.length;compIndex++)
		{
			if(fields[compIndex] instanceof ParamComboBox)
			{	
				String fieldName = ((ParamComboBox)fields[compIndex]).getParamName();
				String fieldValue = ((ParamComboBox)fields[compIndex]).getSelectedItem() + "";
				paramHash.put(fieldName, fieldValue);
			}	
		}
		// now get the text area
		this.sparql.setText(prerna.util.Utility.fillParam(this.sparql.getText(), paramHash));		
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.sparql = (JTextArea)view;
	}

}
