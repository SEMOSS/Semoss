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

import java.awt.Dimension;
import java.beans.PropertyVetoException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.swing.JButton;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.repository.RepositoryConnection;

import prerna.ds.TinkerFrame;
import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.ControlData;
import prerna.ui.components.ControlPanel;
import prerna.ui.components.PropertySpecData;
import prerna.ui.components.VertexFilterData;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.helpers.NetworkGraphHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class handles all thick client graph operations that need to interact with a tinker frame
 */
public class GraphTinkerPlaySheetHelper extends NetworkGraphHelper {

	private static final Logger logger = LogManager.getLogger(GraphTinkerPlaySheetHelper.class.getName());
	protected GraphPlaySheet gps = null;

	/**
	 * Constructor for GraphPlaySheet.
	 */
	public GraphTinkerPlaySheetHelper(GraphPlaySheet gps) {
		this.gps = gps;
		logger.info("Graph Tinker PlaySheet ");
	}

	/**
	 * Method createView.
	 */
	@Override
	public void createView() {
		if (((TinkerFrame) this.gps.dataFrame).isEmpty()) {
			String questionID = this.gps.getQuestionID();
			InsightStore.getInstance().remove(questionID); // TODO: QUESTION_ID
															// MUST MATCH
															// INSIGHT_ID
			if (InsightStore.getInstance().isEmpty()) {
				JButton btnShowPlaySheetsList = (JButton) DIHelper.getInstance()
						.getLocalProp(Constants.SHOW_PLAYSHEETS_LIST);
				btnShowPlaySheetsList.setEnabled(false);
			}
			Utility.showError("Query returned no results.");
			return;
		} else {
			this.gps.updateProgressBar("20%...Creating Visualization", 80);
			this.gps.setPreferredSize(new Dimension(1000, 750));
			this.gps.searchPanel = new ControlPanel(this.gps.ENABLE_SEARCH);
			try {
				// get the graph query result and paint it
				// need to get all the vertex transformers here

				// create initial panel
				// addInitialPanel();
				// execute the query now
				this.gps.addInitialPanel();

				this.gps.addToMainPane(this.gps.pane);
				this.gps.showAll();

				this.gps.updateProgressBar("60%...Processing RDF Statements	", 60);

				logger.debug("Executed the select");
				createForest();
				this.gps.createLayout();
				this.gps.processView();
				this.gps.updateProgressBar("100%...Graph Generation Complete", 100);

			} catch (RuntimeException e) {
				logger.error(Constants.STACKTRACE, e);
				logger.fatal(Constants.STACKTRACE, e);
			}
		}
		this.gps.updateProgressBar("100%...Table Generation Complete", 100);
	}

	/**
	 * Method undoView.
	 */
	public void undoView() {
		// get the latest and undo it
		// Need to find a way to keep the base relationships
		try {
			// if(gdm.modelCounter > 1)
			// {
			this.gps.updateProgressBar("30%...Getting Previous Model", 30);
			// gdm.undoData();
			this.gps.filterData = new VertexFilterData();
			this.gps.controlData = new ControlData();
			this.gps.predData = new PropertySpecData();
			this.gps.updateProgressBar("50%...Graph Undo in Progress", 50);

			refineView();
			// }
			this.gps.setSelected(false);
			this.gps.setSelected(true);

			this.gps.genAllData();
		} catch (PropertyVetoException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		this.gps.updateProgressBar("100%...Graph Undo Complete", 100);
	}

	/**
	 * Method redoView.
	 */
	// public void redoView() {
	// try {
	// if(gdm.rcStore.size() > gdm.modelCounter-1)
	// {
	// updateProgressBar("30%...Getting Previous Model", 30);
	// gdm.redoData();
	// updateProgressBar("50%...Graph Redo in Progress", 50);
	// refineView();
	//
	// }
	// this.setSelected(false);
	// this.setSelected(true);
	// printConnectedNodes();
	// printSpanningTree();
	// }catch (PropertyVetoException e) {
	// logger.error(Constants.STACKTRACE, e);
	// }
	// updateProgressBar("100%...Graph Redo Complete", 100);
	// }

	// public void overlayView(){
	// try
	// {
	//// semossGraph.rc.commit();\
	// getForest();
	// createForest();
	//
	// //add to overall modelstore
	//
	// boolean successfulLayout = createLayout();
	// if(!successfulLayout){
	// Utility.showMessage("Current layout cannot handle the extend. Resetting
	// to " + Constants.FR + " layout...");
	// layoutName = Constants.FR;
	// createLayout();
	// }
	//
	// processView();
	// setUndoRedoBtn();
	// updateProgressBar("100%...Graph Extension Complete", 100);
	// }catch(RuntimeException ex)
	// {
	// classLogger.error(Constants.STACKTRACE, ex);
	// logger.fatal(ex);
	// }
	// }

	/**
	 * Method removeView.
	 */
	// public void removeView()
	// {
	// gdm.removeView(gdm.getQuery(), gdm.getEngine());
	// //sc.addStatement(vf.createURI("<http://semoss.org/ontologies/Concept/Service/tom2>"),vf.createURI("<http://semoss.org/ontologies/Relation/Exposes>"),vf.createURI("<http://semoss.org/ontologies/Concept/BusinessLogicUnit/tom1>"));
	// logger.debug("\nSPARQL: " + gdm.getQuery());
	// //tq.setIncludeInferred(true /* includeInferred */);
	// //tq.evaluate();
	//
	// gdm.fillStoresFromModel(gdm.getEngine());
	// updateProgressBar("80%...Creating Visualization", 80);
	//
	// refineView();
	// logger.debug("Removing Forest Complete >>>>>> ");
	// updateProgressBar("100%...Graph Remove Complete", 100);
	// }

	/**
	 * Method refineView.
	 */
	public void refineView() {
		try {
			createForest();
			logger.info("Refining Forest Complete >>>>>");

			// create the specified layout
			this.gps.createLayout();
			// identify the layout specified for this perspective
			// now create the visualization viewer and we are done
			this.gps.createVisualizer();

			// add the panel
			this.gps.addPanel();
			// addpane
			// addpane
			this.gps.legendPanel.drawLegend();
			// showAll();
			// progressBarUpdate("100%...Graph Refine Complete", 100);
			setUndoRedoBtn();
		} catch (RuntimeException e) {
			// TODO: Specify exception
			logger.error(Constants.STACKTRACE, e);
		}
	}

	/**
	 * Method createForest.
	 */
	public void createForest() {
		logger.debug("creating the in memory jena model");

		this.gps.getForest();
		Hashtable<String, String> filteredNodes = this.gps.filterData.filterNodes;
		logger.warn("Filtered Nodes " + filteredNodes);

		// use edge store to add all edges to forest
		logger.info("Adding edges from edgeStore to forest");

		Map data = ((TinkerFrame) this.gps.dataFrame).getGraphOutput();
		HashMap<String, SEMOSSVertex> vertStore = (HashMap<String, SEMOSSVertex>) data.get("nodes");
		Collection<SEMOSSEdge> edgeStore = (Collection<SEMOSSEdge>) data.get("edges");
		Iterator<SEMOSSEdge> edgeIt = edgeStore.iterator();
		while (edgeIt.hasNext()) {

			SEMOSSEdge edge = edgeIt.next();
			SEMOSSVertex outVert = edge.outVertex;
			SEMOSSVertex inVert = edge.inVertex;

			// System.out.println("{ u: \"" +
			// outVert.getProperty(Constants.VERTEX_NAME) + "\", v: \"" +
			// inVert.getProperty(Constants.VERTEX_NAME)+ "\", value: { label:
			// \"\" } }," );

			if ((filteredNodes == null) || (filteredNodes != null && !filteredNodes.containsKey(inVert.getURI())
					&& !filteredNodes.containsKey(outVert.getURI())
					&& !this.gps.filterData.edgeFilterNodes.containsKey(edge.getURI()))) {
				// add to forest
				this.gps.forest.addEdge(edge, outVert, inVert);
				this.gps.processControlData(edge);

				// add to filter data
				this.gps.filterData.addEdge(edge);

				// add to simple graph
				this.gps.graph.addVertex(outVert);
				this.gps.graph.addVertex(inVert);
				if (outVert != inVert) // loops not allowed in simple graph...
										// can we get rid of this simple grpah
										// entirely?
					this.gps.graph.addEdge(outVert, inVert, edge);
			}
		}
		logger.info("Done with edges... checking for isolated nodes");
		// now for vertices--process control data and add what is necessary to
		// the graph
		// use vert store to check for any isolated nodes and add to forest
		Collection<SEMOSSVertex> verts = vertStore.values();
		for (SEMOSSVertex vert : verts) {
			if ((filteredNodes == null) || (filteredNodes != null && !filteredNodes.containsKey(vert.getURI()))) {
				this.gps.processControlData(vert);
				this.gps.filterData.addVertex(vert);
				if (!this.gps.forest.containsVertex(vert)) {
					this.gps.forest.addVertex(vert);
					this.gps.graph.addVertex(vert);
				}
			}
		}
		logger.info("Done with forest creation");

		this.gps.genAllData();

		logger.info("Creating Forest Complete >>>>>> ");
	}

	public boolean getSudowl() {
		return false;
	}

	public void overlayView() {
		try {
			// semossGraph.rc.commit();\
			this.gps.getForest();
			createForest();

			// add to overall modelstore

			boolean successfulLayout = this.gps.createLayout();
			if (!successfulLayout) {
				Utility.showMessage(
						"Current layout cannot handle the extend. Resetting to " + Constants.FR + " layout...");
				this.gps.layoutName = Constants.FR;
				this.gps.createLayout();
			}

			this.gps.processView();
			setUndoRedoBtn();
			this.gps.updateProgressBar("100%...Graph Extension Complete", 100);
		} catch (RuntimeException ex) {
			logger.error(Constants.STACKTRACE, ex);
			logger.fatal(ex);
		}
	}

	/**
	 * Method setUndoRedoBtn.
	 */
	public void setUndoRedoBtn() {
		OldInsight in = (OldInsight) InsightStore.getInstance().get(this.gps.questionNum);
		List<DataMakerComponent> dmcList = in.getDataMakerComponents();
		if (dmcList.size() > 1) {
			this.gps.searchPanel.undoBtn.setEnabled(true);
		} else {
			this.gps.searchPanel.undoBtn.setEnabled(false);
		}
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
	public Collection<SEMOSSVertex> getVerts() {
		if (this.gps.forest != null) {
			return this.gps.forest.getVertices();
		} else {
			return ((Map) ((TinkerFrame) this.gps.dataFrame).getGraphOutput().get("nodes")).values();
		}
	}

	@Override
	public Collection<SEMOSSEdge> getEdges() {
		if (this.gps.forest != null) {
			return this.gps.forest.getEdges();
		} else {
			return ((Map) ((TinkerFrame) this.gps.dataFrame).getGraphOutput().get("edges")).values();
		}
	}

	@Override
	public void exportDB() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setRC(RepositoryConnection rc) {
		// TODO Auto-generated method stub

	}

	@Override
	public RepositoryConnection getRC() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeExistingConcepts(Vector<String> subVector) {
		// TODO Auto-generated method stub

	}

	@Override
	public String addNewConcepts(String subjects, String baseObject, String predicate) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearStores() {
		// TODO Auto-generated method stub

	}

	@Override
	public Hashtable<String, SEMOSSEdge> getEdgeStore() {
		Collection<SEMOSSEdge> edges = getEdges();
		Iterator<SEMOSSEdge> it = edges.iterator();
		Hashtable<String, SEMOSSEdge> ret = new Hashtable<String, SEMOSSEdge>();
		while(it.hasNext()){
			SEMOSSEdge edge = it.next();
			ret.put(edge.getURI(), edge);
		}
		return ret;
	}

	@Override
	public Hashtable<String, SEMOSSVertex> getVertStore() {
		Collection<SEMOSSVertex> verts = getVerts();
		Iterator<SEMOSSVertex> it = verts.iterator();
		Hashtable<String, SEMOSSVertex> ret = new Hashtable<String, SEMOSSVertex>();
		while(it.hasNext()){
			SEMOSSVertex vert = it.next();
			ret.put(vert.getURI(), vert);
		}
		return ret;
	}

}
