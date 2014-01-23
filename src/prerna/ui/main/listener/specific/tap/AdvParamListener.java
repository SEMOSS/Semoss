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
import java.awt.event.ActionListener;

import prerna.ui.components.specific.tap.SerOptPlaySheet;

/**
 * Listener to show advanced parameters for TAP service optimization
 * Used for when showParamBtn is pressed on the TAP service optimization playsheet
 */
public class AdvParamListener implements ActionListener {
	
	SerOptPlaySheet ps;
	
	/**
	 * Action for when showParamBtn is pressed on the TAP service optimization playsheet
	 * When pressed, makes advParamPanel become visible
	 * @param e ActionEvent
	 */
	public void actionPerformed(ActionEvent e) {
		if (ps.showParamBtn.isSelected())
		{
			ps.advParamPanel.setVisible(true);
		}
		else
		{
			ps.advParamPanel.setVisible(false);
		}
	}
	
	/**
	 * Determines the playsheet used for TAP service optimization
	 * @param ps 	SerOptPlaySheet the playsheet used for TAP service optimization
	 */
	public void setPlaySheet (SerOptPlaySheet ps)
	{
		this.ps = ps;
	}
	
}
