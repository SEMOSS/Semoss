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
	JRadioButton integratedAccessButton;
	JRadioButton hybridAccessButton;
	JRadioButton manualAccessButton;
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
		integratedAccessButton = (JRadioButton) DIHelper.getInstance().getLocalProp(ConstantsTAP.DHMSM_ACCESS_INTEGRATED_BUTTON);
		hybridAccessButton = (JRadioButton) DIHelper.getInstance().getLocalProp(ConstantsTAP.DHMSM_ACCESS_HYBRID_BUTTON);
		manualAccessButton = (JRadioButton) DIHelper.getInstance().getLocalProp(ConstantsTAP.DHMSM_ACCESS_MANUAL_BUTTON);

		realAccessButton = (JRadioButton) DIHelper.getInstance().getLocalProp(ConstantsTAP.DHMSM_ACCESS_REAL_BUTTON);
		nearAccessButton = (JRadioButton) DIHelper.getInstance().getLocalProp(ConstantsTAP.DHMSM_ACCESS_NEAR_BUTTON);
		archiveAccessButton = (JRadioButton) DIHelper.getInstance().getLocalProp(ConstantsTAP.DHMSM_ACCESS_ARCHIVE_BUTTON);
		ignoreAccessButton = (JRadioButton) DIHelper.getInstance().getLocalProp(ConstantsTAP.DHMSM_ACCESS_IGNORE_BUTTON);

		SelectRadioButtonPanel selectRadioPanel = (SelectRadioButtonPanel) DIHelper.getInstance().getLocalProp(Constants.SELECT_RADIO_PANEL);
		if (integratedAccessButton.isSelected())
		{
			Enumeration<String> enumKey = selectRadioPanel.radioIntegratedBoxHash.keys();
			while(enumKey.hasMoreElements()) {
			    String key = enumKey.nextElement();
				JRadioButton radButton = (JRadioButton) selectRadioPanel.radioIntegratedBoxHash.get(key);
				radButton.setSelected(true);
			}
		}
		else if (hybridAccessButton.isSelected())
		{
			Enumeration<String> enumKey = selectRadioPanel.radioHybridBoxHash.keys();
			while(enumKey.hasMoreElements()) {
			    String key = enumKey.nextElement();
				JRadioButton radButton = (JRadioButton) selectRadioPanel.radioHybridBoxHash.get(key);
				radButton.setSelected(true);
			}
		}
		else if (manualAccessButton.isSelected())
		{
			Enumeration<String> enumKey = selectRadioPanel.radioManualBoxHash.keys();
			while(enumKey.hasMoreElements()) {
			    String key = enumKey.nextElement();
				JRadioButton radButton = (JRadioButton) selectRadioPanel.radioManualBoxHash.get(key);
				radButton.setSelected(true);
			}
		}
		
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
