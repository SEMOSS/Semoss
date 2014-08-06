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

import prerna.ui.components.LayoutMenuItem;

/**
 * Controls actions selected from the layout menu/
 */
public class LayoutMenuListener implements ActionListener {

	public static LayoutMenuListener instance = null;
	static final Logger logger = LogManager.getLogger(LayoutMenuListener.class.getName());
	
	/**
	 * Constructor for LayoutMenuListener.
	 */
	protected LayoutMenuListener()
	{		
	}
	
	/**
	 * Method getInstance.  Gets the instance of the layout menu listener.
	 * @return LayoutMenuListener */
	public static LayoutMenuListener getInstance()
	{
		if(instance == null)
			instance = new LayoutMenuListener();
		return instance;
	}
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		// get the engine
		// execute the neighbor hood 
		// paint it
		// get the query from the 
		LayoutMenuItem item = (LayoutMenuItem)e.getSource();
		//try{
			item.paintLayout();
		//}catch(Exception ex){
		//	logger.warn(ex);
		//}
	}
}
