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
package prerna.ui.components.playsheets;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.beans.PropertyVetoException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JInternalFrame;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jgrapht.graph.SimpleGraph;

import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.DelegateForest;
import edu.uci.ics.jung.visualization.GraphZoomScrollPane;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.ModalGraphMouse;
import edu.uci.ics.jung.visualization.picking.PickedState;
import edu.uci.ics.jung.visualization.renderers.BasicRenderer;
import edu.uci.ics.jung.visualization.renderers.Renderer;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.om.InsightStore;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.ControlData;
import prerna.ui.components.ControlPanel;
import prerna.ui.components.LegendPanel2;
import prerna.ui.components.NewHoriScrollBarUI;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.components.PropertySpecData;
import prerna.ui.components.VertexColorShapeData;
import prerna.ui.components.VertexFilterData;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.specific.tap.DataLatencyPlayPopup;
import prerna.ui.main.listener.impl.GraphNodeListener;
import prerna.ui.main.listener.impl.GraphPlaySheetListener;
import prerna.ui.main.listener.impl.PickedStateListener;
import prerna.ui.main.listener.impl.PlaySheetColorShapeListener;
import prerna.ui.main.listener.impl.PlaySheetControlListener;
import prerna.ui.main.listener.impl.PlaySheetOWLListener;
import prerna.ui.transformer.ArrowDrawPaintTransformer;
import prerna.ui.transformer.ArrowFillPaintTransformer;
import prerna.ui.transformer.EdgeArrowStrokeTransformer;
import prerna.ui.transformer.EdgeLabelFontTransformer;
import prerna.ui.transformer.EdgeLabelTransformer;
import prerna.ui.transformer.EdgeStrokeTransformer;
import prerna.ui.transformer.EdgeTooltipTransformer;
import prerna.ui.transformer.VertexIconTransformer;
import prerna.ui.transformer.VertexLabelFontTransformer;
import prerna.ui.transformer.VertexLabelTransformer;
import prerna.ui.transformer.VertexPaintTransformer;
import prerna.ui.transformer.VertexShapeTransformer;
import prerna.ui.transformer.VertexStrokeTransformer;
import prerna.ui.transformer.VertexTooltipTransformer;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 */
public class GraphTinkerPlaySheet extends AbstractGraphPlaySheet {

	/*
	 * this will have references to the following a. Internal Frame that needs to be displayed b. The panel of
	 * parameters c. The composed SPARQL Query d. Perspective selected e. The question selected by the user f. Filter
	 * criterias including slider values
	 */
	private static final Logger logger = LogManager.getLogger(GraphTinkerPlaySheet.class.getName());
	protected ITableDataFrame dataFrame = null;

	/**
	 * Constructor for GraphPlaySheet.
	 */
	public GraphTinkerPlaySheet()
	{
		logger.info("Graph Tinker PlaySheet ");
	}
	
	/**
	 * Method createView.
	 */
	@Override
	public void createView() {
		if(this.dataFrame.isEmpty()) {
			String questionID = getQuestionID();
			InsightStore.getInstance().remove(questionID); //TODO: QUESTION_ID MUST MATCH INSIGHT_ID
			if(InsightStore.getInstance().isEmpty())
			{
				JButton btnShowPlaySheetsList = (JButton) DIHelper.getInstance().getLocalProp(Constants.SHOW_PLAYSHEETS_LIST);
				btnShowPlaySheetsList.setEnabled(false);
			}
			Utility.showError("Query returned no results.");
			return;		
		} else {
			updateProgressBar("20%...Creating Visualization", 80);
			this.setPreferredSize(new Dimension(1000,750));
			searchPanel=new ControlPanel(ENABLE_SEARCH);
			try {
				// get the graph query result and paint it
				// need to get all the vertex transformers here

				// create initial panel
				// addInitialPanel();
				// execute the query now
				addInitialPanel();

				addToMainPane(pane);
				showAll();
					
				updateProgressBar("60%...Processing RDF Statements	", 60);
				
				logger.debug("Executed the select");
				createForest();
				createLayout();
				processView();
				updateProgressBar("100%...Graph Generation Complete", 100);
				
			} catch (RuntimeException e) {
				e.printStackTrace();
				logger.fatal(e.getStackTrace());
			}
		}
		updateProgressBar("100%...Table Generation Complete", 100);
	}
	
	/**
	 * Method undoView.
	 */
//	public void undoView()
//	{
//		// get the latest and undo it
//		// Need to find a way to keep the base relationships
//		try {
//			if(gdm.modelCounter > 1)
//			{
//				updateProgressBar("30%...Getting Previous Model", 30);
//				gdm.undoData();
//				filterData = new VertexFilterData();
//				controlData = new ControlData();
//				gdm.predData = new PropertySpecData();
//				updateProgressBar("50%...Graph Undo in Progress", 50);
//				
//				refineView();
//				logger.info("model size: " + gdm.rc.size());
//			}
//			this.setSelected(false);
//			this.setSelected(true);
//			printConnectedNodes();
//			printSpanningTree();
//
//			genAllData();
//		} catch (RepositoryException e) {
//			e.printStackTrace();
//		} catch (PropertyVetoException e) {
//			e.printStackTrace();
//		}
//		updateProgressBar("100%...Graph Undo Complete", 100);
//	}

	
    /**
     * Method redoView.
     */
//    public void redoView() {
//        try {
//               if(gdm.rcStore.size() > gdm.modelCounter-1)
//               {
//                     updateProgressBar("30%...Getting Previous Model", 30);
//                     gdm.redoData();
//                     updateProgressBar("50%...Graph Redo in Progress", 50);
//                     refineView();
//                     
//               }
//               this.setSelected(false);
//               this.setSelected(true);
//               printConnectedNodes();
//               printSpanningTree();
//        }catch (PropertyVetoException e) {
//        	e.printStackTrace();
//        }
//        updateProgressBar("100%...Graph Redo Complete", 100);
//    }

//    public void overlayView(){
//		try
//		{
////			semossGraph.rc.commit();\
//			getForest();
//			createForest();
//			
//			//add to overall modelstore
//			
//			boolean successfulLayout = createLayout();
//			if(!successfulLayout){
//				Utility.showMessage("Current layout cannot handle the extend. Resetting to " + Constants.FR + " layout...");
//				layoutName = Constants.FR;
//				createLayout();
//			}
//			
//			processView();
//			setUndoRedoBtn();
//			updateProgressBar("100%...Graph Extension Complete", 100);
//		}catch(RuntimeException ex)
//		{
//			ex.printStackTrace();
//			logger.fatal(ex);
//		}
//    }
	
	/**
	 * Method removeView.
	 */
//	public void removeView()
//	{
//		gdm.removeView(gdm.getQuery(), gdm.getEngine());
//		//sc.addStatement(vf.createURI("<http://semoss.org/ontologies/Concept/Service/tom2>"),vf.createURI("<http://semoss.org/ontologies/Relation/Exposes>"),vf.createURI("<http://semoss.org/ontologies/Concept/BusinessLogicUnit/tom1>"));
//		logger.debug("\nSPARQL: " + gdm.getQuery());
//		//tq.setIncludeInferred(true /* includeInferred */);
//		//tq.evaluate();
//
//		gdm.fillStoresFromModel(gdm.getEngine());
//		updateProgressBar("80%...Creating Visualization", 80);
//
//		refineView();
//		logger.debug("Removing Forest Complete >>>>>> ");
//		updateProgressBar("100%...Graph Remove Complete", 100);
//	}
	

	
	/**
	 * Method refineView.
	 */
//	public void refineView()
//	{
//		try {
//			getForest();
//			gdm.fillStoresFromModel(gdm.getEngine());
//			createForest();
//			logger.info("Refining Forest Complete >>>>>");
//			
//			// create the specified layout
//			createLayout();
//			// identify the layout specified for this perspective
//			// now create the visualization viewer and we are done
//			createVisualizer();
//			
//			// add the panel
//			addPanel();
//			// addpane
//			// addpane
//			legendPanel.drawLegend();
//			//showAll();
//			//progressBarUpdate("100%...Graph Refine Complete", 100);
//			setUndoRedoBtn();
//		} catch (RuntimeException e) {
//			// TODO: Specify exception
//			e.printStackTrace();
//		}
//	}
	
	/**
	 * Method createForest.
	 */
	public void createForest()
	{
		logger.debug("creating the in memory jena model");

		getForest();
		Hashtable<String, String> filteredNodes = filterData.filterNodes;
		logger.warn("Filtered Nodes " + filteredNodes);
		
		//use edge store to add all edges to forest
		logger.info("Adding edges from edgeStore to forest");
		
		Map data = ((TinkerFrame)this.dataFrame).getGraphOutput();
		HashMap<String, SEMOSSVertex> vertStore = (HashMap<String, SEMOSSVertex>) data.get("nodes");
		Collection<SEMOSSEdge> edgeStore = (Collection<SEMOSSEdge>) data.get("edges");
		Iterator<SEMOSSEdge> edgeIt = edgeStore.iterator();
		while(edgeIt.hasNext()){
			
			SEMOSSEdge edge = edgeIt.next();
			SEMOSSVertex outVert = edge.outVertex;
			SEMOSSVertex inVert = edge.inVertex;
			
			
			//System.out.println("{ u: \""  + outVert.getProperty(Constants.VERTEX_NAME) + "\", v: \"" + inVert.getProperty(Constants.VERTEX_NAME)+ "\", value: { label: \"\" } }," );
			
			
				if ((filteredNodes == null) || (filteredNodes != null && !filteredNodes.containsKey(inVert.getURI())
						&& !filteredNodes.containsKey(outVert.getURI()) && !filterData.edgeFilterNodes.containsKey(edge.getURI()))){
				//add to forest
				forest.addEdge(edge, outVert, inVert);
				processControlData(edge);
				
				//add to filter data
				filterData.addEdge(edge);
				
				//add to simple graph
				graph.addVertex(outVert);
				graph.addVertex(inVert);
				if(outVert != inVert) // loops not allowed in simple graph... can we get rid of this simple grpah entirely?
					graph.addEdge(outVert, inVert, edge);
			}
		}
		logger.info("Done with edges... checking for isolated nodes");
		//now for vertices--process control data and add what is necessary to the graph
		//use vert store to check for any isolated nodes and add to forest
		Collection<SEMOSSVertex> verts = vertStore.values();
		for(SEMOSSVertex vert : verts)
		{
			if((filteredNodes == null) || (filteredNodes != null && !filteredNodes.containsKey(vert.getURI()))){
				processControlData(vert);
				filterData.addVertex(vert);
				if(!forest.containsVertex(vert)){
					forest.addVertex(vert);
					graph.addVertex(vert);
				}
			}
		}
		logger.info("Done with forest creation");

		genAllData();
		
		logger.info("Creating Forest Complete >>>>>> ");										
	}
	
//	/**
//	 * Method setUndoRedoBtn.
//	 */
//	private void setUndoRedoBtn()
//	{
//		if(gdm.modelCounter>1)
//		{
//			searchPanel.undoBtn.setEnabled(true);
//		}
//		else
//		{
//			searchPanel.undoBtn.setEnabled(false);
//		}
//		if(gdm.rcStore.size()>=gdm.modelCounter)
//		{
//			searchPanel.redoBtn.setEnabled(true);
//		}
//		else
//		{
//			searchPanel.redoBtn.setEnabled(false);
//		}
//	}
	
	@Override
	public void setDataMaker(IDataMaker data) {
		if(data instanceof TinkerFrame){
			this.dataFrame = (TinkerFrame) data;
		}
		else {
			logger.error("Trying to set illegal data type in GraphTinkerPlaySheet");
			logger.error("Failed setting " + data.getClass() + " when only TinkerFrame are allowed");
		}
	}

	@Override
	public IDataMaker getDataMaker() {
		return this.dataFrame;
	}

	@Override
	public boolean getSudowl() {
		return false;
	}

    public void overlayView(){
		try
		{
//			semossGraph.rc.commit();\
			getForest();
			createForest();
			
			//add to overall modelstore
			
			boolean successfulLayout = createLayout();
			if(!successfulLayout){
				Utility.showMessage("Current layout cannot handle the extend. Resetting to " + Constants.FR + " layout...");
				layoutName = Constants.FR;
				createLayout();
			}
			
			processView();
			setUndoRedoBtn();
			updateProgressBar("100%...Graph Extension Complete", 100);
		}catch(RuntimeException ex)
		{
			ex.printStackTrace();
			logger.fatal(ex);
		}
    }
	
	/**
	 * Method setUndoRedoBtn.
	 */
	private void setUndoRedoBtn()
	{
//		if(gdm.modelCounter>1)
//		{
//			searchPanel.undoBtn.setEnabled(true);
//		}
//		else
//		{
//			searchPanel.undoBtn.setEnabled(false);
//		}
//		if(gdm.rcStore.size()>=gdm.modelCounter)
//		{
//			searchPanel.redoBtn.setEnabled(true);
//		}
//		else
//		{
//			searchPanel.redoBtn.setEnabled(false);
//		}
	}

	@Override
	public void removeView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void redoView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void undoView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Collection<SEMOSSVertex> getVerts() {
		if(this.forest!=null){
			return this.forest.getVertices();
		}
		else {
			return ((Map)((TinkerFrame)this.dataFrame).getGraphOutput().get("nodes")).values();
		}
	}
}
