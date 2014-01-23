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
package prerna.ui.components.api;

import java.awt.event.ActionListener;

import javax.swing.JComponent;

/**
 * This is the interface used to standardize all of the listeners used on the main PlayPane.  When the PlayPane is created on 
 * startup, it initializes all of listeners that are tied to public UI components as specified in the 
 * Map.Properties file.  Each listener, when initialized, uses each of these functions with the bindings specified in the 
 * Map.Properties file so as to give the listener a customized startup.
 * 
 * @author karverma
 * @version $Revision: 1.0 $
 */
public interface IChakraListener extends ActionListener {

	// view component
	/**
	 * Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	public void setView(JComponent view);
	

}
