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
		
		if (actionevent.getSource().equals(overhead)&& noOverhead.isSelected())
		{
			noOverhead.setSelected(!overhead.isSelected());
		}
		else if (actionevent.getSource().equals(noOverhead)&& overhead.isSelected())
		{
			overhead.setSelected(!noOverhead.isSelected());
		}
		else if (actionevent.getSource().equals(overhead)&& !noOverhead.isSelected())
		{
			overhead.setSelected(true);
		}
		else if (actionevent.getSource().equals(noOverhead)&& !overhead.isSelected())
		{
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
