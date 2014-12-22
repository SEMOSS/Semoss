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

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.util.List;

import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;

import prerna.ui.components.api.IChakraListener;

/**
 * Controls the dependent variable inputs.
 */
public class RegressionDepVarListener implements IChakraListener {
	
	/**
	 * Method actionPerformed.
	 * Pulls the selected item from the possible input list and puts it in the selected dependent variable list.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		Component source = (Component) e.getSource();
		JPanel regPanel = (JPanel) source.getParent();
		
		JScrollPane scrollPane = (JScrollPane)(regPanel.getComponent(3));
		JList possibleInputList = (JList)((scrollPane.getViewport()).getView());
		List selected = possibleInputList.getSelectedValuesList();
		if(selected.size()>0)
		{
			String depVar = (String)(selected.get(0));
		
			JTextField depVarTextField = (JTextField) (regPanel.getComponent(7));
			depVarTextField.setText(depVar);
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
