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
