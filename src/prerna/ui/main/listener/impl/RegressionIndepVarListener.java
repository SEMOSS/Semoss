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
import java.util.List;

import javax.swing.DefaultListModel;
import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import prerna.ui.components.api.IChakraListener;

/**
 * Controls the independent variable inputs.
 */
public class RegressionIndepVarListener implements IChakraListener {
	
	/**
	 * Method actionPerformed.
	 * Pulls the selected items from the possible input list and puts it in the selected regressors list.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		Component source = (Component) e.getSource();
		JPanel regPanel = (JPanel) source.getParent().getParent();
		
		JScrollPane possibleInputScrollPane = (JScrollPane)(regPanel.getComponent(3));
		JList possibleInputList = (JList)((possibleInputScrollPane.getViewport()).getView());
		List<String> indepVarToAdd = (possibleInputList.getSelectedValuesList());
		
		JScrollPane indepVarScrollPane = (JScrollPane)(regPanel.getComponent(9));
		JList indepVarList = (JList)((indepVarScrollPane.getViewport()).getView());
		DefaultListModel indepVarModelList = (DefaultListModel)indepVarList.getModel();
		for(String toAdd : indepVarToAdd)
		{
			if(!indepVarModelList.contains(toAdd))
				indepVarModelList.addElement(toAdd);
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