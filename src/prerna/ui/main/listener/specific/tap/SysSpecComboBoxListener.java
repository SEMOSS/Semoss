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
