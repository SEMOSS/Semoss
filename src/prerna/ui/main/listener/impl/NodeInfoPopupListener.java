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

import javax.swing.JDesktopPane;

import prerna.om.SEMOSSVertex;
import prerna.ui.components.NodeInfoPopup;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Controls showing the node info pop up.
 */
public class NodeInfoPopupListener implements ActionListener{
	GraphPlaySheet ps = null;
	SEMOSSVertex[] selectedNodes = null;
	
	/**
	 * Constructor for NodeInfoPopupListener.
	 * @param p GraphPlaySheet
	 * @param pickedV DBCMVertex[]
	 */
	public NodeInfoPopupListener(GraphPlaySheet p, SEMOSSVertex[] pickedV){
		ps = p;
		selectedNodes = pickedV;
	}

	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		NodeInfoPopup pop = new NodeInfoPopup(ps, selectedNodes);
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		pop.setJDesktopPane(pane);
		pop.runTable();
	}

}
