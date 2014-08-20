
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
