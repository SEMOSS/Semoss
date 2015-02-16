/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JCheckBox;

//TODO: listener is the same as ServiceSelectListener

/**
 * Listener for selectAllCheck checkbox within sourceSelectPanel
 */
public class SourceSelectListener implements ActionListener {
	
	JCheckBox selectAllCheck;
	
	/**
	 * Once a user specifies that any capability should not be included in the RFP report, the select all checkbox is no longer selected 
	 * @param e ActionEvent
	 */
	public void actionPerformed(ActionEvent e) {
		JCheckBox curCheckBox = (JCheckBox) e.getSource();
		if (curCheckBox.isSelected()==false){
			selectAllCheck.setSelected(false);
		}
	}
	
	/**
	 * Sets which checkbox is used to select all capabilities within sourceSelectPanel to include in RFP report
	 * @param selectAllCheck 	JCheckBox to select all capabilities within sourceSelectPanel
	 */
	public void setSelectAllCheck(JCheckBox selectAllCheck)
	{
		this.selectAllCheck= selectAllCheck;
	}
	
}
