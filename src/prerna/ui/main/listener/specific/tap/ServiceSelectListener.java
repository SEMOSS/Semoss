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

import javax.swing.JCheckBox;

//TODO: this class is the same as SourceSelectListener

/**
 * Listener for selectAllCheck in ServiesSelectPanel
 */
public class ServiceSelectListener implements ActionListener {
	
	JCheckBox selectAllCheck;
	
	/**
	 * Once a user specifies that any service should not be included in the Transition Cost Report, the select all checkbox is no longer selected 
	 * @param e ActionEvent
	 */
	public void actionPerformed(ActionEvent e) {
		JCheckBox curCheckBox = (JCheckBox) e.getSource();
		if (curCheckBox.isSelected()==false){
			selectAllCheck.setSelected(false);
		}
	}
	
	/**
	 * Sets which checkbox is used to select all services for a select database to include in Transition Cost Report
	 * @param selectAllCheck 	JCheckBox to select all services
	 */
	public void setSelectAllCheck(JCheckBox selectAllCheck)
	{
		this.selectAllCheck= selectAllCheck;
	}
}