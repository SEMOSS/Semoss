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
package prerna.ui.components;

import java.awt.Component;

import javax.swing.JList;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.RemoteSparqlEngine;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.main.listener.impl.AdjacentPopupMenuListener;
import prerna.ui.main.listener.impl.ChartListener;
import prerna.ui.main.listener.impl.DistanceDownstreamListener;
import prerna.ui.main.listener.impl.GraphNodeRankListener;
import prerna.ui.main.listener.impl.GraphPlaySheetExportListener;
import prerna.ui.main.listener.impl.HideVertexPopupMenuListener;
import prerna.ui.main.listener.impl.IslandIdentifierListener;
import prerna.ui.main.listener.impl.LoopIdentifierListener;
import prerna.ui.main.listener.impl.MSTPopupMenuListener;
import prerna.ui.main.listener.impl.MousePickingPopupMenuListener;
import prerna.ui.main.listener.impl.MouseTransformPopupMenuListener;
import prerna.ui.main.listener.impl.NodeEditorListener;
import prerna.ui.main.listener.impl.NodeInfoPopupListener;
import prerna.ui.main.listener.impl.UnHideVertexPopupMenuListener;
import prerna.ui.main.listener.specific.tap.DataLatencyInitiationListener;
import prerna.ui.main.listener.specific.tap.DataLatencyPlayListener;
import prerna.ui.main.listener.specific.tap.HealthGridChartListener;
import prerna.ui.main.listener.specific.tap.SOATransitionListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class is used to create the popup menu visualization for the graph playsheet.
 */
public class GraphNodePopup extends JPopupMenu {
	
	// sets the visualization viewer
	GraphPlaySheet ps = null;
	// sets the picked node list
	SEMOSSVertex [] pickedVertex = null;
	static final Logger logger = LogManager.getLogger(GraphNodePopup.class.getName());
	Component comp = null;
	IEngine engine;
	int x;
	int y;
	
	// core class for neighbor hoods etc.
	/**
	 * Constructor for GraphNodePopup.
	 * @param ps 			Graph playsheet.
	 * @param pickedVertex 	Picked node.
	 * @param comp 			Component.
	 * @param x 			X coordinate for popup.
	 * @param y 			Y coordinate for popup.
	 */
	public GraphNodePopup(GraphPlaySheet ps, SEMOSSVertex [] pickedVertex, Component comp, int x, int y)
	{
		super();
		// need to get this to read from popup menu
		this.ps = ps;
		this.pickedVertex = pickedVertex;
		this.comp = comp;
		this.x = x;
		this.y = y;
		JMenuItem item = null;

		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object repo = list.getSelectedValue();
		this.engine = (IEngine)DIHelper.getInstance().getLocalProp(repo+"");
		
		/*NeighborhoodDataPainter dlistener = new NeighborhoodDataPainter();
		dlistener.setPlaysheet(ps);
		dlistener.setDBCMVertex(pickedVertex);
		logger.info("Picked Vertex set to " + pickedVertex);
		item = add("Append Data To System");
		item.setEnabled(pickedVertex.length > 0);
		item.addActionListener(dlistener);
		*/
		
		
		AdjacentPopupMenuListener aListener = new AdjacentPopupMenuListener();
		aListener.setPlaysheet(ps);
		aListener.setDBCMVertex(pickedVertex);
		
		JMenuItem highAdjBoth = new JMenuItem("Highlight Adjacent");
		highAdjBoth.setName("Upstream and Downstream");//used by the listener
		highAdjBoth.setEnabled(pickedVertex.length > 0);
		highAdjBoth.addActionListener(aListener);
		
		JMenu moreHighlight = new JMenu("More Highlight Options");

		JMenuItem highAdjDown = new JMenuItem("Highlight Downstream");
		highAdjDown.setName("Downstream");//used by the listener
		highAdjDown.setEnabled(pickedVertex.length > 0);
		highAdjDown.addActionListener(aListener);

		JMenuItem highAdjUp = new JMenuItem("Highlight Upstream");
		highAdjUp.setName("Upstream");//used by the listener
		highAdjUp.setEnabled(pickedVertex.length > 0);
		highAdjUp.addActionListener(aListener);

		JMenuItem highAdjAll = new JMenuItem("Highlight All Downstream");
		highAdjAll.setName("All");//used by the listener
		highAdjAll.setEnabled(pickedVertex.length > 0);
		highAdjAll.addActionListener(aListener);

		MSTPopupMenuListener mstListener = new MSTPopupMenuListener();
		mstListener.setPlaysheet(ps);
		JMenuItem MST = new JMenuItem("Highlight Minimum Spanning Tree");
		MST.addActionListener(mstListener);
		
		this.add(highAdjBoth);
		moreHighlight.add(highAdjUp);
		moreHighlight.add(highAdjDown);
		moreHighlight.add(highAdjAll);
		moreHighlight.add(MST);
		this.add(moreHighlight);

		addSeparator();
		
		MouseTransformPopupMenuListener mtl = new MouseTransformPopupMenuListener();
		mtl.setPlaysheet(ps);
		item = add("Move Graph");
		item.addActionListener(mtl);

		MousePickingPopupMenuListener mpl = new MousePickingPopupMenuListener();
		mpl.setPlaysheet(ps);
		item = add("Pick Graph");
		item.addActionListener(mpl);

		addSeparator();
		
		GraphPlaySheetExportListener exp = new GraphPlaySheetExportListener();
		exp.setPlaysheet(ps);
		item = add("Convert to Table");
		item.addActionListener(exp);
		
		NodeInfoPopupListener nip = new NodeInfoPopupListener(ps, pickedVertex);
		item = add("Show Selected Node Information");
		item.addActionListener(nip);
		/*
		GraphExportListener2 exp2 = new GraphExportListener2();
		item = add("Export Graph2");
		item.addActionListener(exp2);
		*/
		addSeparator();
		SOATransitionListener stl = new SOATransitionListener();
		stl.setPlaySheet(ps);
		boolean icdCheck = checkICD();
		item = add("SOA Transition All");
		item.setEnabled(icdCheck);
		
		item.addActionListener(stl);
		
		addSeparator();
		
		//JCheckBoxMenuItem relTypes = new JCheckBoxMenuItem("Relation Types");
		/*RelationPopup popup = new RelationPopup(ps,this.pickedVertex);
		item = add(popup);
		item.setEnabled(pickedVertex.length > 0);
	
		//JCheckBoxMenuItem relTypes = new JCheckBoxMenuItem("Relation Types");
		RelationInstancePopup popup2 = new RelationInstancePopup(ps,this.pickedVertex);
		item = add(popup2);
		item.setEnabled(pickedVertex.length > 0);

		SubjectPopup popup3 = new SubjectPopup(ps,this.pickedVertex);
		item = add(popup3);
		item.setEnabled(pickedVertex.length > 0);

		SubjectInstancePopup popup4 = new SubjectInstancePopup(ps,this.pickedVertex);
		item = add(popup4);
		item.setEnabled(pickedVertex.length > 0);

		RelationPredictPopup popup5 = new RelationPredictPopup("Predict Relation", ps,this.pickedVertex);
		item = add(popup5);
		item.setEnabled(pickedVertex.length == 2);
		*/
		
		String fromNode = "";
		if(this.pickedVertex.length > 0)
			fromNode = "All " + Utility.getClassName(pickedVertex[0].getURI()) + "(s) ";
		TFRelationPopup popup6 = new TFRelationPopup("Traverse Freely: " + fromNode, ps,this.pickedVertex);
		item = add(popup6);
		item.setEnabled(pickedVertex.length > 0);
		

		
		fromNode = "";
		if(this.pickedVertex.length > 0)
			fromNode = Utility.getInstanceName(pickedVertex[0].getURI());
		TFInstanceRelationPopup popup13 = new TFInstanceRelationPopup("Traverse Freely: " + fromNode, ps,this.pickedVertex);
		item = add(popup13);
		item.setEnabled(pickedVertex.length > 0);
		
//		fromNode = "";
//		if(this.pickedVertex.length > 0)
//			fromNode = Utility.getInstanceName(pickedVertex[0].getURI());
//		TFInstanceRelationInstancePopup tfrip = new TFInstanceRelationInstancePopup("Traverse Instance Freely: " + fromNode, ps,this.pickedVertex);
//		item = add(tfrip);
//		item.setEnabled(pickedVertex.length > 0);
		
		JMenu algoPop = new JMenu("Perform Algorithms");
		item = add(algoPop);
		item.setEnabled(true);
		
		GraphNodeRankListener gp = new GraphNodeRankListener();
		JMenuItem algoItemNodeRank = new JMenuItem("NodeRank Algorithm");
		algoItemNodeRank.addActionListener(gp);
		algoPop.add(algoItemNodeRank);


		
		DistanceDownstreamListener ddc = new DistanceDownstreamListener(ps, this.pickedVertex);
		JMenuItem algoItemDistanceDownstream  = new JMenuItem("Distance Downstream");
		algoItemDistanceDownstream.addActionListener(ddc);
		algoPop.add(algoItemDistanceDownstream);

		LoopIdentifierListener lil = new LoopIdentifierListener(ps, this.pickedVertex);
		JMenuItem algoItemLoopIdentifier  = new JMenuItem("Loop Identifier");
		algoItemLoopIdentifier.addActionListener(lil);
		algoPop.add(algoItemLoopIdentifier);

		IslandIdentifierListener iil = new IslandIdentifierListener(ps, this.pickedVertex);
		JMenuItem algoItemIslandIdentifier  = new JMenuItem("Island Identifier");
		algoItemIslandIdentifier.addActionListener(iil);
		algoPop.add(algoItemIslandIdentifier);

		DataLatencyInitiationListener dlil = new DataLatencyInitiationListener(ps, this.pickedVertex);
		JMenuItem algoItemDataLate = new JMenuItem("Data Latency Analysis");
		algoItemDataLate.addActionListener(dlil);
		algoPop.add(algoItemDataLate);

		DataLatencyPlayListener dlpl = new DataLatencyPlayListener(ps, this.pickedVertex);
		JMenuItem algoItemDataLatePlay = new JMenuItem("Data Latency Scenario");
		algoItemDataLatePlay.addActionListener(dlpl);
		algoPop.add(algoItemDataLatePlay);
		
		addSeparator();
		ColorPopup popup7 = new ColorPopup("Modify Color ", ps, pickedVertex);
		item = add(popup7);
		item.setEnabled(pickedVertex.length > 0);
		ShapePopup popup8 = new ShapePopup("Modify Shape ", ps, pickedVertex);
		item = add(popup8);
		item.setEnabled(pickedVertex.length > 0);
		LayoutPopup popup9 = new LayoutPopup("Modify Layout ", ps);
		item = add(popup9);
		// add the hider
		addSeparator();
		item = add("Hide Nodes");
		HideVertexPopupMenuListener hvl = new HideVertexPopupMenuListener();
		hvl.setDBCMVertex(pickedVertex);
		hvl.setPlaysheet(ps);
		item.addActionListener(hvl);
		item.setEnabled(pickedVertex.length > 0);
		item = add("Unhide Nodes");
		UnHideVertexPopupMenuListener uhvl = new UnHideVertexPopupMenuListener();
		uhvl.setPlaysheet(ps);
		item.addActionListener(uhvl);

		addSeparator();
		JMenu chartPop = new JMenu("Chart It!!");
		item = add(chartPop);
        item.setEnabled(true);
        
        ChartListener cl = new ChartListener();
        JMenuItem customChart = new JMenuItem("Create Custom Chart");
        customChart.addActionListener(cl);
        chartPop.add(customChart);
        
        HealthGridChartListener hg = new HealthGridChartListener();
        JMenuItem healthGrid = new JMenuItem("Create Health Grid");
        healthGrid.addActionListener(hg);
        chartPop.add(healthGrid);        

		item = add("Edit Node");
		item.setEnabled(pickedVertex.length > 0);
        if(pickedVertex.length > 0){
			NodeEditorListener ne = new NodeEditorListener();
			String dbType = dbCheck(pickedVertex.length);
			ne.setDBType(dbType);
			ne.setNode(this.pickedVertex[0]);
			ne.setEngine(engine);
			ne.setGps(ps);
	
			item.addActionListener(ne);
			
        }

		
	}
	
	/**
	 * Returns whether the particular database is local or remote.
	 * @param nodeLength 	Length of node.
	
	 * @return String 		Type of database. */
	public String dbCheck(Integer nodeLength)
	{
		String dbType = "local";
		
		if (engine instanceof RemoteSparqlEngine && nodeLength>0)
		{
			dbType = "remote";
		}
		
		
		return dbType;
	}
	
	/**
	 * Checks whether the node type represents an interface control document.
	
	 * @return boolean	True if the type of node represents an ICD. */
	public boolean checkICD()
	{
		boolean icdCheck = false;
		VertexFilterData vfd = ps.getFilterData();
		String[] nodeTypeArray = vfd.getNodeTypes();
		if(nodeTypeArray==null) return false;
		for (int i=0; i<nodeTypeArray.length;i++)
		{
			if (nodeTypeArray[i].equals("InterfaceControlDocument"))
			{
				icdCheck=true;
				return icdCheck;
			}
		}
		return icdCheck;
		
	}
	
}
