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
import java.util.Enumeration;

import javax.swing.JComponent;
import javax.swing.JRadioButton;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.specific.tap.SelectRadioButtonPanel;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;

/**
 * Determines which functional areas the user wants to incorporate in RFP report
 * Used to determine if user wants to include HSD, HSS, or FHP functional areas in RFP report
 * Will populate sourceSelectPanel with all capabilities included in functional areas
 */
public class DHMSMAccessTypeSelectorListener extends AbstractListener {
	JRadioButton realAccessButton;
	JRadioButton nearAccessButton;
	JRadioButton archiveAccessButton;
	JRadioButton ignoreAccessButton;
	IEngine engine;

	/**
	 * Determines if the user has selected HSD, HSS, FHP check box's in MHS TAP to include functional areas to include in RFP report
	 * Will populate sourceSelectPanel to show all capabilities for the functional area's selected
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		realAccessButton = (JRadioButton) DIHelper.getInstance().getLocalProp(ConstantsTAP.DHMSM_ACCESS_REAL_BUTTON);
		nearAccessButton = (JRadioButton) DIHelper.getInstance().getLocalProp(ConstantsTAP.DHMSM_ACCESS_NEAR_BUTTON);
		archiveAccessButton = (JRadioButton) DIHelper.getInstance().getLocalProp(ConstantsTAP.DHMSM_ACCESS_ARCHIVE_BUTTON);
		ignoreAccessButton = (JRadioButton) DIHelper.getInstance().getLocalProp(ConstantsTAP.DHMSM_ACCESS_IGNORE_BUTTON);

		SelectRadioButtonPanel selectRadioPanel = (SelectRadioButtonPanel) DIHelper.getInstance().getLocalProp(Constants.SELECT_RADIO_PANEL);
		if (realAccessButton.isSelected())
		{
			Enumeration<String> enumKey = selectRadioPanel.radioRealBoxHash.keys();
			while(enumKey.hasMoreElements()) {
			    String key = enumKey.nextElement();
				JRadioButton radButton = (JRadioButton) selectRadioPanel.radioRealBoxHash.get(key);
				radButton.setSelected(true);
			}
		}
		else if (nearAccessButton.isSelected())
		{
			Enumeration<String> enumKey = selectRadioPanel.radioNearBoxHash.keys();
			while(enumKey.hasMoreElements()) {
			    String key = enumKey.nextElement();
				JRadioButton radButton = (JRadioButton) selectRadioPanel.radioNearBoxHash.get(key);
				radButton.setSelected(true);
			}
		}
		else if (archiveAccessButton.isSelected())
		{
			Enumeration<String> enumKey = selectRadioPanel.radioArchiveBoxHash.keys();
			while(enumKey.hasMoreElements()) {
			    String key = enumKey.nextElement();
				JRadioButton radButton = (JRadioButton) selectRadioPanel.radioArchiveBoxHash.get(key);
				radButton.setSelected(true);
			}
		}
		else if (ignoreAccessButton.isSelected())
		{
			Enumeration<String> enumKey = selectRadioPanel.radioIgnoreBoxHash.keys();
			while(enumKey.hasMoreElements()) {
			    String key = enumKey.nextElement();
				JRadioButton radButton = (JRadioButton) selectRadioPanel.radioIgnoreBoxHash.get(key);
				radButton.setSelected(true);
			}
		}

	}

	/**
	 * Override method from AbstractListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
}
