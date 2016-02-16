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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JInternalFrame;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jgrapht.Graph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.alg.KruskalMinimumSpanningTree;
import org.jgrapht.graph.SimpleGraph;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.DelegateForest;
import edu.uci.ics.jung.visualization.GraphZoomScrollPane;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.ModalGraphMouse;
import edu.uci.ics.jung.visualization.picking.PickedState;
import edu.uci.ics.jung.visualization.renderers.BasicRenderer;
import edu.uci.ics.jung.visualization.renderers.Renderer;
import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IConstructWrapper;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.InMemorySesameEngine;
import prerna.om.GraphDataModel;
import prerna.om.InsightStore;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.wrappers.ConstructStatement;
import prerna.rdf.engine.wrappers.WrapperManager;
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
public class GraphPlaySheet extends AbstractGraphPlaySheet {

	/*
	 * this will have references to the following a. Internal Frame that needs to be displayed b. The panel of
	 * parameters c. The composed SPARQL Query d. Perspective selected e. The question selected by the user f. Filter
	 * criterias including slider values
	 */
	private static final Logger logger = LogManager.getLogger(GraphPlaySheet.class.getName());
	public GraphDataModel gdm;

	/**
	 * Constructor for GraphPlaySheet.
	 */
	public GraphPlaySheet()
	{
		logger.info("Graph PlaySheet ");
	}
	
	/**
	 * Method setAppend.
	 * @param append boolean
	 */
	public void setAppend(boolean append) {
		logger.debug("Append set to " + append);
		gdm.setOverlay(append);
	}
	
	public boolean getSudowl(){
		return gdm.getSudowl();
	}
	
	/**
	 * Method createView.
	 */
	public void createView() {
		if(gdm.rc==null){
			String questionID = getQuestionID();
			// fill the nodetype list so that they can choose from
			// remove from store
			// this will also clear out active sheet
			InsightStore.getInstance().remove(questionID); //TODO: QUESTION_ID MUST MATCH INSIGHT_ID
//			QuestionPlaySheetStore.getInstance().remove(questionID);
//			if(QuestionPlaySheetStore.getInstance().isEmpty())
			if(InsightStore.getInstance().isEmpty())
			{
				JButton btnShowPlaySheetsList = (JButton) DIHelper.getInstance().getLocalProp(
						Constants.SHOW_PLAYSHEETS_LIST);
				btnShowPlaySheetsList.setEnabled(false);
			}
			Utility.showError("Query returned no results.");
			return;
		}
		super.createView();
		
		this.setPreferredSize(new Dimension(1000,750));

		searchPanel=new ControlPanel(gdm.getSearch());

		try {
			// get the graph query result and paint it
			// need to get all the vertex transformers here

			// create initial panel
			// addInitialPanel();
			// execute the query now
			setAppend(false);
			
			//writeStatus(" Starting create view");
			getForest();
			
			
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
	
	/**
	 * Method undoView.
	 */
	public void undoView()
	{
		// get the latest and undo it
		// Need to find a way to keep the base relationships
		try {
			if(gdm.modelCounter > 1)
			{
				updateProgressBar("30%...Getting Previous Model", 30);
				gdm.undoData();
				filterData = new VertexFilterData();
				controlData = new ControlData();
				gdm.predData = new PropertySpecData();
				updateProgressBar("50%...Graph Undo in Progress", 50);
				
				refineView();
				logger.info("model size: " + gdm.rc.size());
			}
			this.setSelected(false);
			this.setSelected(true);
//			printConnectedNodes();
//			printSpanningTree();

			genAllData();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
		updateProgressBar("100%...Graph Undo Complete", 100);
	}

	
    /**
     * Method redoView.
     */
    public void redoView() {
        try {
               if(gdm.rcStore.size() > gdm.modelCounter-1)
               {
                     updateProgressBar("30%...Getting Previous Model", 30);
                     gdm.redoData();
                     updateProgressBar("50%...Graph Redo in Progress", 50);
                     refineView();
                     
               }
               this.setSelected(false);
               this.setSelected(true);
//               printConnectedNodes();
//               printSpanningTree();
        }catch (PropertyVetoException e) {
        	e.printStackTrace();
        }
        updateProgressBar("100%...Graph Redo Complete", 100);
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
	 * Method removeView.
	 */
	public void removeView()
	{
		gdm.removeView(gdm.getQuery(), gdm.getEngine());
		//sc.addStatement(vf.createURI("<http://semoss.org/ontologies/Concept/Service/tom2>"),vf.createURI("<http://semoss.org/ontologies/Relation/Exposes>"),vf.createURI("<http://semoss.org/ontologies/Concept/BusinessLogicUnit/tom1>"));
		logger.debug("\nSPARQL: " + gdm.getQuery());
		//tq.setIncludeInferred(true /* includeInferred */);
		//tq.evaluate();

		gdm.fillStoresFromModel(gdm.getEngine());
		updateProgressBar("80%...Creating Visualization", 80);

		refineView();
		logger.debug("Removing Forest Complete >>>>>> ");
		updateProgressBar("100%...Graph Remove Complete", 100);
	}
	

	
	/**
	 * Method refineView.
	 */
	public void refineView()
	{
		try {
			getForest();
			gdm.fillStoresFromModel(gdm.getEngine());
			createForest();
			logger.info("Refining Forest Complete >>>>>");
			
			// create the specified layout
			createLayout();
			// identify the layout specified for this perspective
			// now create the visualization viewer and we are done
			createVisualizer();
			
			// add the panel
			addPanel();
			// addpane
			// addpane
			legendPanel.drawLegend();
			//showAll();
			//progressBarUpdate("100%...Graph Refine Complete", 100);
			setUndoRedoBtn();
		} catch (RuntimeException e) {
			// TODO: Specify exception
			e.printStackTrace();
		}
	}
	
	/**
	 * Method createForest.
	 */
	public void createForest()
	{
		// need to take the base information from the base query and insert it into the jena model
		// this is based on EXTERNAL ontology
		// then take the ontology and insert it into the jena model
		// (may be eventually we can run this through a reasoner too)
		// Now insert our base model into the same ontology
		// Now query the model for 
		// Relations - Paint the basic graph
		// Now find a way to get all the predicate properties from them
		// Hopefully the property is done using subproperty of
		// predicates - Pick all the predicates but for the properties
		// paint them
		// properties
		// and then paint it appropriately
		logger.debug("creating the in memory jena model");


		Hashtable<String, String> filteredNodes = filterData.filterNodes;
		logger.warn("Filtered Nodes " + filteredNodes);
		
		//use edge store to add all edges to forest
		logger.info("Adding edges from edgeStore to forest");

		Hashtable<String, SEMOSSVertex> vertStore = gdm.getVertStore();
		Hashtable<String, SEMOSSEdge> edgeStore = gdm.getEdgeStore();
		Iterator<String> edgeIt = edgeStore.keySet().iterator();
		while(edgeIt.hasNext()){
			
			String edgeURI = edgeIt.next();
			SEMOSSEdge edge = edgeStore.get(edgeURI);
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
				
				//add to pred data
				gdm.predData.addPredicateAvailable(edge.getURI());
				gdm.predData.addConceptAvailable(inVert.getURI());
				gdm.predData.addConceptAvailable(outVert.getURI());
				
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
		
		// first execute all the predicate selectors
//		// Backdoor entry
//		Thread thread = new Thread(){
//			public void run()
//			{
//				printAllRelationship();				
//			}
//		};
//		thread.start();
//		modelCounter++;
//shouldn't this be in create data?
		logger.info("Creating Forest Complete >>>>>> ");										
	}
	
	/**
	 * Method exportDB.
	 */
	public void exportDB() 
	{
		try {
			gdm.baseRelEngine.exportDB();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
//    /**
//     * Method printAllRelationship.
//     */
//    public void printAllRelationship()
//    {
//          String conceptHierarchyForSubject = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE " +
//          "{" +
//          "{?Subject ?Predicate ?Object}" + 
//          "}";
//          logger.debug(conceptHierarchyForSubject);
//          
//          IEngine jenaEngine = new InMemorySesameEngine();
//          ((InMemorySesameEngine)jenaEngine).setRepositoryConnection(gdm.rc);
//          
//          if(gdm.getQuery() == null) {
//        	  logger.debug("Query not set for current GraphPlaySheet");
//        	  return;
//          }
//          
//          //SesameJenaConstructWrapper sjsc;
//          IConstructWrapper sjsc = null;
//          
//          /*if(query.toUpperCase().contains("CONSTRUCT"))
//                sjsc = 	WrapperManager.getInstance().getCWrapper(jenaEngine, propertyQuery);
//          else
//                sjsc = new SesameJenaSelectCheater();
//			*/
//          
//          // = new SesameJenaSelectCheater();
//          //sjsc.setEngine(jenaEngine);
//          logger.warn("<<<<");
//          String end = "";
//          
//                while(!end.equalsIgnoreCase("end"))
//                {
//                      try {
//	                      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
//	                      logger.debug("Enter Query");
//	                      String query2 = reader.readLine();   
//	                      if(query2!=null){
//		                      end = query2;
//		                      logger.debug("Query is " + query2);
//		                      if(query2.toUpperCase().contains("CONSTRUCT"))
//		                            sjsc = WrapperManager.getInstance().getCWrapper(jenaEngine, query2);
//		                      else
//		                            sjsc = 	WrapperManager.getInstance().getChWrapper(jenaEngine, query2);
//
//		
//		                      // = new SesameJenaSelectCheater();
//		                      /*
//		                      sjsc.setEngine(jenaEngine);
//		                      sjsc.setQuery(query);//conceptHierarchyForSubject);
//		                      sjsc.setQuery(query2);
//		                      sjsc.execute();*/
//		                      while(sjsc.hasNext())
//		                      {
//		                            // read the subject predicate object
//		                            // add it to the in memory jena model
//		                            // get the properties
//		                            // add it to the in memory jena model
//		                            IConstructStatement st = sjsc.next();
//		                            logger.warn(st.getSubject() + "<<>>" + st.getPredicate() + "<<>>" + st.getObject());
//		                            //addToJenaModel(st);
//		                      }
//	                      }
//                      } catch (RuntimeException e) {
//                            // TODO: Specify exception
//                            e.printStackTrace();
//                      } catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//	                      
//                }
//
//          
//    }

	
	/**
	 * Method setUndoRedoBtn.
	 */
	private void setUndoRedoBtn()
	{
		if(gdm.modelCounter>1)
		{
			searchPanel.undoBtn.setEnabled(true);
		}
		else
		{
			searchPanel.undoBtn.setEnabled(false);
		}
		if(gdm.rcStore.size()>=gdm.modelCounter)
		{
			searchPanel.redoBtn.setEnabled(true);
		}
		else
		{
			searchPanel.redoBtn.setEnabled(false);
		}
	}
	
//	/**
//	 * Method printConnectedNodes.
//	 */
//	protected void printConnectedNodes()
//	{
//		logger.info("In print connected Nodes routine " );
//		ConnectivityInspector ins = new ConnectivityInspector(graph);
//		logger.info("Number of vertices " + graph.vertexSet().size() + "<>" + graph.edgeSet().size());
//		logger.info(" Graph Connected ? " + ins.isGraphConnected());
//		//writeStatus("Graph Connected ? " + ins.isGraphConnected());
//		logger.info("Number of connected sets are " + ins.connectedSets().size());
//		Iterator <Set<SEMOSSVertex>> csIterator = ins.connectedSets().iterator();
//		while(csIterator.hasNext())
//		{
//			Set <SEMOSSVertex> vertSet = csIterator.next();
//			Iterator <SEMOSSVertex> si = vertSet.iterator();
//			while(si.hasNext())
//			{
//				SEMOSSVertex vert = si.next();
//				//logger.info("Set " + count + ">>>> " + vert.getProperty(Constants.VERTEX_NAME));
//			}
//		}	
//	}	
	
//	/**
//	 * Method printSpanningTree.
//	 */
//	protected void printSpanningTree()
//	{
//		logger.info("In Spanning Tree " );
//		KruskalMinimumSpanningTree<SEMOSSVertex, SEMOSSEdge> ins = new KruskalMinimumSpanningTree<SEMOSSVertex, SEMOSSEdge>(graph);
//		
//		logger.info("Number of vertices " + graph.vertexSet().size());
//		logger.info(" Edges  " + ins.getEdgeSet().size());
//		Iterator <SEMOSSEdge> csIterator = ins.getEdgeSet().iterator();
//		int count = 0;
//		while(csIterator.hasNext())
//		{
//				SEMOSSEdge vert = csIterator.next();
//				logger.info("Set " + count + ">>>> " + vert.getProperty(Constants.EDGE_NAME));
//		}
////		count++;
//	}	
	
	/**
	 * Method setRC.
	 * @param rc RepositoryConnection
	 */
	public void setRC(RepositoryConnection rc)
	{
		this.gdm.rc=rc;
	}
	
	/**
	 * Method getRC.
	 * @param rc RepositoryConnection
	 */
	public RepositoryConnection getRC()
	{
		return gdm.rc;
	}
	
	// removes existing concepts 
	/**
	 * Method removeExistingConcepts.
	 * @param subVector Vector<String>
	 */
	public void removeExistingConcepts(Vector <String> subVector)
	{

		for(int remIndex = 0;remIndex < subVector.size();remIndex++)
		{
			try {
				String remQuery = subVector.elementAt(remIndex);
				logger.warn("Removing query " + remQuery);
				
				Update update = gdm.rc.prepareUpdate(QueryLanguage.SPARQL, remQuery);
				update.execute();
				this.gdm.baseRelEngine.insertData(remQuery);
			
			} catch (RuntimeException e) {
				// TODO: Specify exception
				e.printStackTrace();
			} catch (UpdateExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (RepositoryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MalformedQueryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	// adds existing concepts 
	/**
	 * Method addNewConcepts.
	 * @param subjects String
	 * @param baseObject String
	 * @param predicate String
	 * @return String
	 */
	public String addNewConcepts(String subjects, String baseObject, String predicate)
	{
		
		StringTokenizer tokenz = new StringTokenizer(subjects, ";");
		
		String listOfChilds = null;
		
		while(tokenz.hasMoreTokens())
		{
			String adder = tokenz.nextToken();
			
			String parent = adder.substring(0,adder.indexOf("@@"));
			String child = adder.substring(adder.indexOf("@@") + 2);
			
			if(listOfChilds == null)
				listOfChilds = child;
			else
			listOfChilds = listOfChilds + ";" + child;
			
			ConstructStatement st = new ConstructStatement();
			st.setSubject(child);
			st.setPredicate(predicate);
			st.setObject(baseObject);
			gdm.addToSesame(st,true, true);
			
			logger.info(" Query....  " + parent + "<>" + child);	
		}
		return listOfChilds;
	}

	public void clearStores(){
		gdm.getVertStore().clear();
		gdm.getEdgeStore().clear();
	}

	@Override
	public void setDataMaker(IDataMaker data) {
		if(data instanceof GraphDataModel){
			this.gdm = (GraphDataModel) data;
		}
		else {
			logger.error("Trying to set illegal data type in TablePlaySheet");
			logger.error("Failed setting " + data.getClass() + " when only ITableDataFrames are allowed");
		}
	}
	
	@Override
	public GraphDataModel getDataMaker() {
		return this.gdm;
	}

	@Override
	public IDataMaker getDefaultDataMaker() {
		return new GraphDataModel();
	}

}
