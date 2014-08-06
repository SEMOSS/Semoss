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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.ColorMenuItem;

/**
 * Controls the color menu.
 */
public class ColorMenuListener implements ActionListener {

	public static ColorMenuListener instance = null;
	static final Logger logger = LogManager.getLogger(ColorMenuListener.class.getName());
	
	/**
	 * Constructor for ColorMenuListener.
	 */
	protected ColorMenuListener()
	{
		
	}
	
	/**
	 * Method getInstance. Retrieves an instance of the color menu.	
	 * @return ColorMenuListener */
	public static ColorMenuListener getInstance()
	{
		if(instance == null)
			instance = new ColorMenuListener();
		return instance;
	}
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		ColorMenuItem item = (ColorMenuItem)e.getSource();
		item.paintColor();
	}
}
