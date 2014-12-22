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

import java.awt.Desktop;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.IOException;

import javax.swing.JButton;
import javax.swing.JComponent;

import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Controls the buttons to start the training modules from the Help tab.
 */
public class StartTrainingListener extends AbstractListener {
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param actionevent ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		JButton htmlButton= (JButton)DIHelper.getInstance().getLocalProp(Constants.HTML_TRAINING_BUTTON);
		JButton pptButton= (JButton)DIHelper.getInstance().getLocalProp(Constants.PPT_TRAINING_BUTTON);
		File file = null;
		if (actionevent.getSource() == htmlButton)
		{
			file = new File(workingDir+"/training/html/Level1-Training/index.html");
		}
		else if (actionevent.getSource() == pptButton)
		{
			file = new File(workingDir+"/training/powerpoint/MHSGraphTool-L1-20130301.ppsx");
		}
		
		Desktop desktop = null;
		 if (Desktop.isDesktopSupported()) {
		        desktop = Desktop.getDesktop();
		        try {
					desktop.open(file);
				} catch (IOException e) {
					e.printStackTrace();
				}
		 }
		 
		
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {

	}
}
