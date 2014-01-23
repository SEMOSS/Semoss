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
