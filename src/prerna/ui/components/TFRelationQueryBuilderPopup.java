/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.ui.components;

import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JMenu;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.QueryStruct;
import prerna.engine.api.IDatabaseEngine;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.JoinTransformation;
import prerna.ui.main.listener.impl.NeighborMenuListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 */
public class TFRelationQueryBuilderPopup extends JMenu implements MouseListener{

	// need a way to cache this for later
	// sets the visualization viewer
	IPlaySheet ps = null;
	// sets the picked node list
	SEMOSSVertex [] pickedVertex = null;
	static final Logger logger = LogManager.getLogger(TFRelationQueryBuilderPopup.class.getName());

	boolean populated = false;
	// core class for neighbor hoods etc.
	/**
	 * Constructor for TFRelationPopup.
	 * @param name String
	 * @param ps IPlaySheet
	 * @param pickedVertex DBCMVertex[]
	 */
	public TFRelationQueryBuilderPopup(String name, IPlaySheet ps, SEMOSSVertex [] pickedVertex)
	{
		super(name);
		// need to get this to read from popup menu
		this.ps = ps;
		this.pickedVertex = pickedVertex;
		addMouseListener(this);
	}

	/**
	 * Executes query and adds appropriate relations.
	 * @param prefix	Prefix used to create the type query.
	 */
	public void addRelations(String prefix)
	{
		// get the selected repository
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object [] repos = (Object [])list.getSelectedValues();
		// I am only going to get one repository
		// hopefully they have selected one :)
		String repo = repos[0] +"";
		IDatabaseEngine engine = (IDatabaseEngine)DIHelper.getInstance().getLocalProp(repo);
		
		for(int pi = 0;pi < pickedVertex.length;pi++)
		{		
			SEMOSSVertex thisVert = pickedVertex[pi];
			String typeUri = thisVert.getURI();
			String type = Utility.getClassName(typeUri);
			
			List<String> traverseFromList = new Vector<String>();
			traverseFromList.add(type);
			Map<String, Map<String, Object>> connectedConcepts = MasterDatabaseUtility.getConnectedConceptsRDBMS(traverseFromList, null);
			
			logger.debug("Found the engine for repository   " + repo);
			// for now we'll just show the values in this specific db
			if(connectedConcepts != null && !connectedConcepts.isEmpty()) {
				Map<String, Object> myDbConnections = connectedConcepts.get(repo);
				if(myDbConnections != null && !myDbConnections.isEmpty()) {
					// get the list of equivalent uris to a map containing keys upstream/downstream which point to the list of objects
					Set<String> equivUris = myDbConnections.keySet();
					// currently only going to show for our specific type
					if(equivUris.contains(type)) {
						Map<String, Set<String>> streamMap = (Map<String, Set<String>>) myDbConnections.get(type);
						if(streamMap.containsKey("upstream")) {
							addLabel("From:");
							Set<String> upstreamRels = streamMap.get("upstream");
							for(String upstreamRelType: upstreamRels){
								QueryStruct data = new QueryStruct();
								data.addSelector(type, null);
								data.addSelector(upstreamRelType, null);
								data.addRelation(type, upstreamRelType, "inner.join");
								DataMakerComponent dmc = new DataMakerComponent(engine, data);
								addJoinTransformation(dmc, type, type);
								NeighborQueryBuilderMenuItem nItem = new NeighborQueryBuilderMenuItem(upstreamRelType, dmc, engine);
								nItem.addActionListener(NeighborMenuListener.getInstance());
								add(nItem);
							}
						}
						if(streamMap.containsKey("downstream")) {
							addLabel("To:");
							Set<String> upstreamRels = streamMap.get("downstream");
							for(String upstreamRelType: upstreamRels){
								QueryStruct data = new QueryStruct();
								data.addSelector(type, null);
								data.addSelector(upstreamRelType, null);
								data.addRelation(upstreamRelType, type, "inner.join");
								DataMakerComponent dmc = new DataMakerComponent(engine, data);
								addJoinTransformation(dmc, type, type);
								NeighborQueryBuilderMenuItem nItem = new NeighborQueryBuilderMenuItem(upstreamRelType, dmc, engine);
								nItem.addActionListener(NeighborMenuListener.getInstance());
								add(nItem);
							}
						}
					}
				}
			}
		}
		populated = true;
		repaint();
	}
	
	private void addJoinTransformation(DataMakerComponent dmc, String type, String equivUri){
		// 2. b. Add join transformation since we know a tree already exists and we will have to join to it
		JoinTransformation joinTrans = new JoinTransformation();
		Map<String, Object> selectedOptions = new HashMap<String, Object>();
		selectedOptions.put(JoinTransformation.COLUMN_ONE_KEY, Utility.getInstanceName(type));
		selectedOptions.put(JoinTransformation.COLUMN_TWO_KEY, Utility.getInstanceName(equivUri));
		selectedOptions.put(JoinTransformation.JOIN_TYPE, "inner");
		joinTrans.setProperties(selectedOptions);
//		dmc.addPostTrans(joinTrans);
		dmc.addPreTrans(joinTrans);
	}

	/**
	 * Invoked when the mouse button has been clicked (pressed and released) on a component.
	 * @param arg0 MouseEvent
	 */
	@Override
	public void mouseClicked(MouseEvent arg0) {
	}

	/**
	 * Invoked when the mouse enters a component.
	 * Adds relations if it is not populated.
	 * @param arg0 MouseEvent
	 */
	@Override
	public void mouseEntered(MouseEvent arg0) {
		if(!populated)
		{
			addRelations("");
		}
	}

	/**
	 * Adds label.
	 * @param label 	Label, in string form.
	 */
	public void addLabel(String label){
		add(new JLabel(label));
	}

	/**
	 * Invoked when the mouse exits a component.
	 * @param arg0 MouseEvent
	 */
	@Override
	public void mouseExited(MouseEvent arg0) {

	}

	/**
	 * Invoked when a mouse button has been pressed on a component.
	 * @param arg0 MouseEvent
	 */
	@Override
	public void mousePressed(MouseEvent arg0) {

	}

	/**
	 * Invoked when a mouse button has been released on a component.
	 * @param arg0 MouseEvent
	 */
	@Override
	public void mouseReleased(MouseEvent arg0) {

	}	
}
