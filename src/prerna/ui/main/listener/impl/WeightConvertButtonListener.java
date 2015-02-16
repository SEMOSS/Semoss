/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.GraphToTreeConverter;
import prerna.ui.components.WeightDropDownButton;
import prerna.ui.components.playsheets.GraphPlaySheet;

/**
 * 
 */
public class WeightConvertButtonListener implements ActionListener{

	GraphPlaySheet playSheet;
	GraphToTreeConverter converter;

	/**
	 * Constructor for WeightConvertButtonListener.
	 */
	public WeightConvertButtonListener(){
		converter = new GraphToTreeConverter();
	}
	
	/**
	 * Method setPlaySheet.  Sets the play sheet that the listener will access.
	 * @param ps
	 */
	public void setPlaySheet(GraphPlaySheet ps){
		this.playSheet = ps;
	}

	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		WeightDropDownButton btn = (WeightDropDownButton) e.getSource();
		// propArray = new String [1];
		//btn.setupButton(propArray);

		//get node weights
		Collection<SEMOSSVertex> nodeArray = playSheet.forest.getVertices();
		ArrayList nodePropList = new ArrayList();
		for (SEMOSSVertex node: nodeArray)
		{
			Enumeration enumKey = node.propHash.keys();
			while (enumKey.hasMoreElements())
			{
				String key = (String) enumKey.nextElement();
				Object value = (Object) node.propHash.get(key);
				if(!nodePropList.contains(key) && value instanceof Double)
				{
					nodePropList.add(key);
				}
			}
		}

		Collection<SEMOSSEdge> edgeArray = playSheet.forest.getEdges();
		ArrayList edgePropList = new ArrayList();
		for (SEMOSSEdge edge: edgeArray)
		{
			Enumeration enumKey = edge.propHash.keys();
			while (enumKey.hasMoreElements())
			{
				String key = (String) enumKey.nextElement();
				Object value = (Object) edge.propHash.get(key);
				if(!nodePropList.contains(key) && value instanceof Double)
				{
					edgePropList.add(key);
				}
			}
		}

		//setup playSheet and the property lists
		btn.setPlaySheet(playSheet);
		btn.setupLists(nodePropList,edgePropList);
		


	}


}
