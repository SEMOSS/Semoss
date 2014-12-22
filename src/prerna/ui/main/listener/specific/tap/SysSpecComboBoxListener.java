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

import javax.swing.JComboBox;
import javax.swing.JComponent;

/**
 */
public class SysSpecComboBoxListener implements ActionListener{

	JComponent item;
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JComboBox comboBox=(JComboBox) e.getSource();
		if(comboBox.getSelectedItem().toString().equals("System Specific"))
		{
			item.setVisible(true);
		}
		else
		{
			item.setVisible(false);
		}
		
	}
	
	/**
	 * Method setShowItem.
	 * @param component JComponent
	 */
	public void setShowItem (JComponent component)
	{
		this.item = component;
	}

}
