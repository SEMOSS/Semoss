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

import prerna.algorithm.impl.IslandIdentifierProcessor;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;
import edu.uci.ics.jung.graph.DelegateForest;

/**
 * Controls running the island identifier algorithm.
 */
public class IslandIdentifierListener implements ActionListener{

	GraphPlaySheet ps = null;
	SEMOSSVertex [] pickedVertex = null;
	static final Logger logger = LogManager.getLogger(IslandIdentifierListener.class.getName());
	
	/**
	 * Constructor for IslandIdentifierListener.
	 * @param p GraphPlaySheet
	 * @param pickedV DBCMVertex[]
	 */
	public IslandIdentifierListener(GraphPlaySheet p, SEMOSSVertex[] pickedV){
		ps = p;
		pickedVertex = pickedV;
	}
		
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		DelegateForest forest = ps.forest;
		
		IslandIdentifierProcessor pro = new IslandIdentifierProcessor();
		pro.setForest(forest);
		pro.setSelectedNodes(pickedVertex);
		pro.setPlaySheet(ps);
	
		pro.execute();
		//pro.setGridFilterData();
		//pro.createTab();
			
	}

}
