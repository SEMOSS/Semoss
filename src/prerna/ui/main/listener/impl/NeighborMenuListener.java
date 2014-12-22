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
