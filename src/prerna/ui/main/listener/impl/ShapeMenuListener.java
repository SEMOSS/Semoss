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

import prerna.ui.components.ShapeMenuItem;

/**
 * Controls painting of shapes on the graph.
 */
public class ShapeMenuListener implements ActionListener {

	public static ShapeMenuListener instance = null;
	static final Logger logger = LogManager.getLogger(ShapeMenuListener.class.getName());
	
	/**
	 * Constructor for ShapeMenuListener.
	 */
	protected ShapeMenuListener()
	{
		
	}
	
	/**
	 * Method getInstance. Gets the instance of the shape menu listener.	
	 * @return ShapeMenuListener */
	public static ShapeMenuListener getInstance()
	{
		if(instance == null)
			instance = new ShapeMenuListener();
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
		ShapeMenuItem item = (ShapeMenuItem)e.getSource();
		item.paintShape();
	}
}
