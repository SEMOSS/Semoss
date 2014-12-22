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

import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;

/**
 * Controls hiding the pop up menu for nodes on the graph play sheet.
 */
public class HideVertexPopupMenuListener implements ActionListener {

	GraphPlaySheet ps = null;
	SEMOSSVertex [] vertices = null;
	
	static final Logger logger = LogManager.getLogger(HideVertexPopupMenuListener.class.getName());
	
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
		
		for(int vertIndex = 0;vertIndex < vertices.length;vertIndex++)
		{
			// take the vertex and add it to the sheet
			ps.getFilterData().addNodeToFilter(vertices[vertIndex]);
		}
		ps.refineView();
		//ps.createView();
	}
}
