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
