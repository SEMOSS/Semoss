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

import prerna.ui.components.NeighborMenuItem;

/**
 * Controls the painting of the neighbor menu items.
 */
public class NeighborMenuListener implements ActionListener {

	public static NeighborMenuListener instance = null;
	static final Logger logger = LogManager.getLogger(NeighborMenuListener.class.getName());
	
	/**
	 * Constructor for NeighborMenuListener.
	 */
	protected NeighborMenuListener()
	{
		
	}
	
	/**
	 * Method getInstance.  Gets the instance of the neighbor menu listener.	
	 * @return NeighborMenuListener */
	public static NeighborMenuListener getInstance()
	{
		if(instance == null)
			instance = new NeighborMenuListener();
		return instance;
	}
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		// get the engine
		// execute the neighbor hood 
		// paint it
		NeighborMenuItem item = (NeighborMenuItem)e.getSource();
		item.paintNeighborhood();
	}
}
