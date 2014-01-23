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
