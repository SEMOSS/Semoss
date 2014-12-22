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
import java.util.ArrayList;

import javax.swing.DefaultListModel;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;

import prerna.ui.components.ParamComboBox;
import prerna.ui.components.RegCalculationPerformer;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.playsheets.RegressionAnalysisPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Controls the regression analysis button to perform the regression.
 */
public class RegressionAnalysisButtonListener implements IChakraListener {
	
	/**
	 * 
	 * Method actionPerformed.
	 * Pulls the selected dependent variable and regressors and then creates a RegCacluationPerformer
	 * to run the regression calculation.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		Component source = (Component) e.getSource();
		JPanel regPanel = (JPanel) source.getParent();
		RegressionAnalysisPlaySheet regPlaySheet = (RegressionAnalysisPlaySheet)(regPanel.getParent().getParent().getParent().getParent().getParent());

		ParamComboBox nodeTypeCombo = (ParamComboBox)(regPanel.getComponent(1));
		String selectedNodeType = (String) nodeTypeCombo.getSelectedItem();
		String nodeUri = nodeTypeCombo.getURI(selectedNodeType);
		
		JTextField depVarTextField = (JTextField)(regPanel.getComponent(7));
		String depVarText = depVarTextField.getText();
		
		JScrollPane indepVarScrollPane = (JScrollPane)(regPanel.getComponent(9));
		JList indepVarList = (JList)((indepVarScrollPane.getViewport()).getView());
		DefaultListModel indepVarModelList = (DefaultListModel)indepVarList.getModel();

		if(depVarText.length()==0 || indepVarModelList.size()==0)
		{
			displayCheckBoxError();
			return;
		}
		
		ArrayList<String> indepVars = new ArrayList<String>();
		for(int i=0;i<indepVarModelList.size();i++)
		{
			indepVars.add((String)indepVarModelList.getElementAt(i));
		}
		
		RegCalculationPerformer regCalc = new RegCalculationPerformer();
		regCalc.setPlaySheet(regPlaySheet);
		regCalc.setNodeUri(nodeUri);
		regCalc.setDependentVar(depVarText);
		regCalc.setIndependentVar(indepVars);
		regCalc.regCalculate();
		
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
	}
	
	/**
	 * Method displayCheckBoxError.
	 * Shows an error message for when dependent variable and/or regressors are not selected.
	 */
	public void displayCheckBoxError(){
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		JOptionPane.showMessageDialog(playPane, "Please select at least one dependent and independent variable.", "Error", JOptionPane.ERROR_MESSAGE);
		
	}

}