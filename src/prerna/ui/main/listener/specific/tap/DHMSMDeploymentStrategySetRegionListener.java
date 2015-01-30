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
import java.awt.event.ActionListener;

import javax.swing.JToggleButton;

import prerna.ui.components.specific.tap.DHMSMDeploymentStrategyPlaySheet;
import prerna.ui.components.specific.tap.SerOptPlaySheet;
import prerna.ui.components.specific.tap.SysOptPlaySheet;

/**
 * TODO
 * Listener to show advanced parameters for TAP optimization
 * Used for when showParamBtn, showSystemSelectBtn, or showSystemCapSelectBtn
 * is pressed on the TAP service optimization or TAP system optimization playsheet
 */
public class DHMSMDeploymentStrategySetRegionListener implements ActionListener {
	
	DHMSMDeploymentStrategyPlaySheet ps;
	
	/**
	 * TODO
	 * Action for when showParamBtn, showSystemSelectBtn, or showSystemCapSelectBtn is pressed
	 * on the TAP service or TAP system optimization playsheet
	 * When pressed, makes advParamPanel become visible or system/capability panel visible
	 * @param e ActionEvent
	 */
	public void actionPerformed(ActionEvent e) {

		if(ps.getSelectRegionTimesButton().isSelected()) {
			ps.showSelectRegionTimesPanel(true);
		}
		else {
			ps.showSelectRegionTimesPanel(false);
		}
	}

	/**
	 * TODO
	 * Determines the playsheet used for TAP service or TAP system optimization
	 * @param ps 	SerOptPlaySheet the playsheet used for TAP service or TAP system optimization
	 */
	public void setPlaySheet (DHMSMDeploymentStrategyPlaySheet ps)
	{
		this.ps = ps;
	}
}