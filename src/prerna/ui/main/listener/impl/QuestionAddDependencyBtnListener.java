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
import java.util.Vector;

import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JTextPane;
import javax.swing.ListModel;

import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class QuestionAddDependencyBtnListener implements IChakraListener {

	@Override
	public void actionPerformed(ActionEvent e) {
		JList<String> parameterDependenciesList = (JList<String>) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPENDENCIES_JLIST);
		JTextPane parameterDependTextPane = (JTextPane) DIHelper.getInstance().getLocalProp(Constants.PARAMETER_DEPEND_TEXT_PANE);
		
		Vector<String> listData = new Vector<String>();
		ListModel<String> model = parameterDependenciesList.getModel();
		
		System.err.println(model.getSize());
		String newDependency = parameterDependTextPane.getText().trim();

		//special spacings are not recognized by the JList so repalcing with "_-_"
		newDependency = newDependency.replace("\t", "_-_").replace("\r", "_-_").replace("\n", "_-_").replace("_DEPEND ", "_DEPEND_-_");
		
		if(newDependency.contains("_DEPEND") && !newDependency.contains("Example:")){
			for(int i=0; i < model.getSize(); i++){
				listData.addElement(model.getElementAt(i));
			}
			
			listData.addElement(newDependency);
			
			parameterDependenciesList.setListData(listData);
			parameterDependTextPane.setText("");
			parameterDependTextPane.requestFocusInWindow();
		}
		else{
			JOptionPane
			.showMessageDialog(null,
					"Dependency format is incorrect. It must be in the format of [ParameterName]_DEPEND [ParameterName]");
		}
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub
		
	}

}
