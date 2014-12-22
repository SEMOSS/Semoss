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
