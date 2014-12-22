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

import javax.swing.JComponent;
import javax.swing.JRadioButton;

import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Used to toggle between selecting apply overhead in base transition costs for TAP_Cost db
 * Radio button for applying overhead is rdbtnApplyTapOverhead in MHS_Tab
 * Radio button for not applying overhead is rdbtnDoNotApplyOverhead in MHS_Tab
 */
public class TransOverheadRadioButtonListener implements IChakraListener {
	
	/**
	 * Toggle between the radio buttons for apply overhead or no overhead so that when one is selected, the other is not selected
	 * @param actionevent ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		JRadioButton overhead = (JRadioButton)DIHelper.getInstance().getLocalProp(Constants.TRANSITION_APPLY_OVERHEAD_RADIO);
		JRadioButton noOverhead = (JRadioButton)DIHelper.getInstance().getLocalProp(Constants.TRANSITION_NOT_APPLY_OVERHEAD_RADIO);
		
		if (((JRadioButton)actionevent.getSource()).getName().equals(overhead.getName()) && noOverhead.isSelected()) {
			noOverhead.setSelected(!overhead.isSelected());
		}
		else if (((JRadioButton)actionevent.getSource()).getName().equals(noOverhead.getName()) && overhead.isSelected()) {
			overhead.setSelected(!noOverhead.isSelected());
		}
		else if (((JRadioButton)actionevent.getSource()).getName().equals(overhead.getName()) && !noOverhead.isSelected()) {
			overhead.setSelected(true);
		}
		else if (((JRadioButton)actionevent.getSource()).getName().equals(noOverhead.getName()) && !overhead.isSelected()) {
			noOverhead.setSelected(true);
		}
	}

	/**
	 * Override method from IChakraListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {
		
	}

}
