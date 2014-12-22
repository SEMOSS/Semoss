
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

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JTextField;

import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.CommonSubgraphFunctions;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * CommonSubgraphButtonListener calls the common subgraph algorithm when commonSubgraphButton is clicked.
 * Determines the largest common subgraph between the metamodels of the two databases selected when the nodes are above the minimum threshold.
 * @author ksmart
 *
 */
public class CommonSubgraphButtonListener implements IChakraListener{
	
	public void actionPerformed(ActionEvent arg0) {
		JTextField thresholdTextField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.COMMON_SUBGRAPH_THRESHOLD_TEXT_BOX);
		String thresholdTextValue = thresholdTextField.getText();
		Double thresholdValue = 0.0;
		try{
			thresholdValue = Double.parseDouble(thresholdTextValue);
		}catch(RuntimeException e){
			Utility.showError("Threshold must be a number");
			return;
		}
		
		JComboBox<String> commonSubgraphComboBox0 = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.COMMON_SUBGRAPH_COMBO_BOX_0);
		JComboBox<String> commonSubgraphComboBox1 = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.COMMON_SUBGRAPH_COMBO_BOX_1);
		String engine0Name = (String)commonSubgraphComboBox0.getSelectedItem();
		String engine1Name = (String)commonSubgraphComboBox1.getSelectedItem();
		
		if(engine0Name==null||engine1Name==null||engine0Name.equals(engine1Name)) {
			Utility.showError("Please select 2 different databases from the database lists in order to run the common subgraph algorithm");
			return;
		}
		
		CommonSubgraphFunctions csf = new CommonSubgraphFunctions();
		csf.CSIA(thresholdValue,engine0Name,engine1Name);
	}
	
	/**
	 * Override method from IChakraListener
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {
	}

}
