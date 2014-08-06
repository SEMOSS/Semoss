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

import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;

/**
 * Controls the un-hiding of the vertex pop up menu.
 */
public class UnHideVertexPopupMenuListener implements ActionListener {

	GraphPlaySheet ps = null;
	SEMOSSVertex [] vertices = null;
	
	static final Logger logger = LogManager.getLogger(UnHideVertexPopupMenuListener.class.getName());
	
	/**
	 * Method setPlaysheet.  Sets the play sheet that the listener will access.
	 * @param ps GraphPlaySheet
	 */
	public void setPlaysheet(GraphPlaySheet ps)
	{
		this.ps = ps;
	}
	
	/**
	 * Method setDBCMVertex.  Sets the DBCMVertex that the listener will access.
	 * @param vertices DBCMVertex[]
	 */
	public void setDBCMVertex(SEMOSSVertex [] vertices)
	{
		logger.debug("Set the vertices " + vertices.length);
		this.vertices = vertices;
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
		ps.getFilterData().unfilterAll();
		ps.refineView();
		//ps.createView();
	}
}
