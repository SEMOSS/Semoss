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
import java.util.Enumeration;
import java.util.Hashtable;

import javax.swing.JCheckBox;

//TODO: this is the same as ServiceSelectAllListener

/**
 * Selects all the different capabilities to be included in RFP report
 */
public class SourceSelectAllListener implements ActionListener {
	
	Hashtable checkHash;
	
	/**
	 * Default is to have all the capabilities selected in sourceSelectPanel to be included in the RFP report
	 * Allows the user to toggle between selecting all the capabilities or no capabilities 
	 * @param e ActionEvent
	 */
	public void actionPerformed(ActionEvent e) {
		int allSelectedIndicator = 0;
		Enumeration<String> enumKey = checkHash.keys();
		while(enumKey.hasMoreElements()) {
		    String key = enumKey.nextElement();
			JCheckBox checkBox = (JCheckBox) checkHash.get(key);
			if (!checkBox.isSelected())
			{
				checkBox.setSelected(true);
				allSelectedIndicator++;
			}
		}
		
		if (allSelectedIndicator == 0)
		{
			enumKey = checkHash.keys();
			while(enumKey.hasMoreElements()) {
			    String key = enumKey.nextElement();
				JCheckBox checkBox = (JCheckBox) checkHash.get(key);
				checkBox.setSelected(false);
			}
		}
		else
		{
			JCheckBox checkBox = (JCheckBox) e.getSource();
			checkBox.setSelected(true);
		}
		
	}
	
	/**
	 * Determine the Hashtable checkHash which contains the list of all checkboxes in the panel to be used within the class
	 * @param checkHash 	Hashtable to be used within the class
	 */
	public void setCheckHash(Hashtable checkHash)
	{
		this.checkHash= checkHash;
	}
	
}
