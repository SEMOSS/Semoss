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
import java.util.Arrays;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.DistanceDownstreamProcessor;
import prerna.om.GraphDataModel;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;

/**
 * When the distance downstream algorithm is selected, calls the algorithm processor to run the algorithm.
 */
public class DistanceDownstreamListener implements ActionListener {

	GraphPlaySheet ps = null;
	SEMOSSVertex [] pickedVertex = null;
	Hashtable nodeTable = new Hashtable();
	Hashtable edgeTable = new Hashtable();
	static final Logger logger = LogManager.getLogger(DistanceDownstreamListener.class.getName());
	
	/**
	 * Constructor for DistanceDownstreamListener.
	 * @param p GraphPlaySheet
	 * @param pickedV DBCMVertex[]
	 */
	public DistanceDownstreamListener(GraphPlaySheet p, SEMOSSVertex[] pickedV){
		ps = p;
		pickedVertex = pickedV;
	}
		
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		//get the forest
		GraphDataModel gdm = ps.gdm;
		
		DistanceDownstreamProcessor pro = new DistanceDownstreamProcessor();
		pro.setGraphDataModel(gdm);
		ArrayList<SEMOSSVertex> verts = new ArrayList<SEMOSSVertex>(Arrays.asList(pickedVertex));
		pro.setSelectedNodes(verts);
		pro.setPlaySheet(ps);
	
		pro.execute();
		pro.setGridFilterData();
		pro.createTab();
			
	}
}
