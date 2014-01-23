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
import java.util.Hashtable;

import org.apache.log4j.Logger;

import prerna.algorithm.impl.DistanceDownstreamProcessor;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;
import edu.uci.ics.jung.graph.DelegateForest;

/**
 * When the distance downstream algorithm is selected, calls the algorithm processor to run the algorithm.
 */
public class DistanceDownstreamListener implements ActionListener {

	GraphPlaySheet ps = null;
	SEMOSSVertex [] pickedVertex = null;
	Hashtable nodeTable = new Hashtable();
	Hashtable edgeTable = new Hashtable();
	Logger logger = Logger.getLogger(getClass());
	
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
		DelegateForest forest = ps.forest;
		
		DistanceDownstreamProcessor pro = new DistanceDownstreamProcessor();
		pro.setForest(forest);
		pro.setSelectedNodes(pickedVertex);
		pro.setPlaySheet(ps);
	
		pro.execute();
		pro.setGridFilterData();
		pro.createTab();
			
	}
}
