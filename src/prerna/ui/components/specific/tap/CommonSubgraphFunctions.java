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
package prerna.ui.components.specific.tap;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JDesktopPane;
import javax.swing.JList;
import javax.swing.JPanel;

import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

import prerna.algorithm.impl.IntakePortal;
import prerna.om.GraphDataModel;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.ui.components.ExecuteQueryProcessor;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

/**
 * The CommonSubgraphFunctions class creates two graphs and finds a common subgraph between them using the CSIA algorithm.
 */
public class CommonSubgraphFunctions {

	protected Logger logger = Logger.getLogger(getClass());
	
	//V and W that show all the vertices in the orginial graphs that are still to be mapped
	ArrayList<SEMOSSVertex> vertexList0;
	ArrayList<SEMOSSVertex> vertexList1;
		
	//X and Y that show all the verticies that have been mapped from V and W (vertexList0 and vertexList1)
	ArrayList<SEMOSSVertex> mappedVertexList0;
	ArrayList<SEMOSSVertex> mappedVertexList1;

	//X and Y that show all the verticies that have been mapped from V and W (vertexList0 and vertexList1)
	ArrayList<SEMOSSVertex> mappedVertexListFinal0;
	ArrayList<SEMOSSVertex> mappedVertexListFinal1;
	
	int nmax;
//	int n0;
//	int numOfFirstTriesCounter;
//	int numOfFirstTriesMax;
	//number of possible mappings to check.
	int numOfPossibleConnections;
	
//	boolean directed;
		
	/**
	 * Method CSIA.
	 * @param options Hashtable<String,Integer>
	 * @return Hashtable<String,Hashtable<String,Hashtable<String,Object>>> 
	 */
	
	public void CSIA(double threshold)
	{
//		String query0 = "CONSTRUCT {?Data1 ?provide ?System1. ?System2 ?passes ?System3. ?passes ?contains ?prop. ?passes ?subprop ?relation. ?provide ?contains2 ?crm.} WHERE { BIND( <http://health.mil/ontologies/Concept/DataObject/Account> AS ?Data1). { {?System1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?System1 ?provide ?Data1 ;} BIND(<http://semoss.org/ontologies/Relation/Contains/CRM> AS ?contains2). {?provide ?contains2 ?crm ;} } UNION { BIND(URI(CONCAT('http://health.mil/ontologies/Relation/', SUBSTR(STR(?System2), 45), ':', SUBSTR(STR(?System3), 45))) AS ?passes).{?System2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?contains <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> ;} {?icd1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>;} {?upstream1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?downstream1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?System2 ?upstream1 ?icd1 ;}{?icd1 ?downstream1 ?System3;}{?icd1 ?carries ?Data1;} {?carries ?contains ?prop ;} } BIND(<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> AS ?subprop) BIND(<http://semoss.org/ontologies/Relation> AS ?relation)} BINDINGS ?crm {('C') ('M')}";
//		String query1 = "CONSTRUCT {?Data1 ?provide ?System1. ?System2 ?passes ?System3. ?passes ?contains ?prop. ?passes ?subprop ?relation. ?provide ?contains2 ?crm.} WHERE { BIND( <http://health.mil/ontologies/Concept/DataObject/Appointment> AS ?Data1). { {?System1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?System1 ?provide ?Data1 ;} BIND(<http://semoss.org/ontologies/Relation/Contains/CRM> AS ?contains2). {?provide ?contains2 ?crm ;} } UNION { BIND(URI(CONCAT('http://health.mil/ontologies/Relation/', SUBSTR(STR(?System2), 45), ':', SUBSTR(STR(?System3), 45))) AS ?passes).{?System2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?contains <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> ;} {?icd1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>;} {?upstream1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?downstream1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;} {?System2 ?upstream1 ?icd1 ;}{?icd1 ?downstream1 ?System3;}{?icd1 ?carries ?Data1;} {?carries ?contains ?prop ;} } BIND(<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> AS ?subprop) BIND(<http://semoss.org/ontologies/Relation> AS ?relation)} BINDINGS ?crm {('C') ('M')}";
//		//String query0 = "CONSTRUCT {?System1 ?Upstream ?ICD. ?ICD ?Downstream ?System2. ?ICD ?carries ?Data1. ?ICD ?contains2 ?prop2. ?System3 ?Upstream2 ?ICD2. ?ICD2 ?contains1 ?prop. ?ICD2 ?Downstream2 ?System1.?ICD2 ?carries2 ?Data2.?System1 ?Provide ?BLU} WHERE { {?System1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}  BIND(<http://health.mil/ontologies/Concept/System/DMLSS> AS ?System1){{?System2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?Data1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;}{?System1 ?Upstream ?ICD ;}{?ICD ?Downstream ?System2 ;} {?ICD ?carries ?Data1;}{?carries ?contains2 ?prop2} {?contains2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }} UNION {{?Upstream2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?Downstream2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}  {?ICD2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Data2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?carries2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?System3 ?Upstream2 ?ICD2 ;}{?ICD2 ?Downstream2 ?System1 ;} {?ICD2 ?carries2 ?Data2;} {?carries2 ?contains1 ?prop} {?contains1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }} UNION {{?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System1 ?Provide ?BLU}}}";
//		//String query1 = "CONSTRUCT {?System1 ?Upstream ?ICD. ?ICD ?Downstream ?System2. ?ICD ?carries ?Data1. ?ICD ?contains2 ?prop2. ?System3 ?Upstream2 ?ICD2. ?ICD2 ?contains1 ?prop. ?ICD2 ?Downstream2 ?System1.?ICD2 ?carries2 ?Data2.?System1 ?Provide ?BLU} WHERE { {?System1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}  BIND(<http://health.mil/ontologies/Concept/System/DMLSS(JMAR)> AS ?System1){{?System2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?Data1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;}{?System1 ?Upstream ?ICD ;}{?ICD ?Downstream ?System2 ;} {?ICD ?carries ?Data1;}{?carries ?contains2 ?prop2} {?contains2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }} UNION {{?Upstream2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?Downstream2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>;}  {?ICD2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Data2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?carries2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?System3 ?Upstream2 ?ICD2 ;}{?ICD2 ?Downstream2 ?System1 ;} {?ICD2 ?carries2 ?Data2;} {?carries2 ?contains1 ?prop} {?contains1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }} UNION {{?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System1 ?Provide ?BLU}}}";
//
//		vertexList0 = new ArrayList<SEMOSSVertex>();
//		vertexList1 = new ArrayList<SEMOSSVertex>();
		
//		logger.info("Generating graph from query 0: "+query0);
//		vertexList0 = createGraphs(query0);
//		logger.info("Generating graph from query 1: "+query1);
//		vertexList1 = createGraphs(query1);
//		
//		logger.info("Generating graph 0 ");
//		vertexList0 = createTestGraphs1();
//		logger.info("Generating graph 1");
//		vertexList1 = createTestGraphs2();
//		
		JComboBox commonSubgraphComboBox0 = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.COMMON_SUBGRAPH_COMBO_BOX_0);
		JComboBox commonSubgraphComboBox1 = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.COMMON_SUBGRAPH_COMBO_BOX_1);
		String engine0Name = (String)commonSubgraphComboBox0.getSelectedItem();
		String engine1Name = (String)commonSubgraphComboBox1.getSelectedItem();
		
		if(engine0Name==null||engine1Name==null||engine0Name.equals(engine1Name)) {
			logger.info("Please select 2 different databases from the database lists in order to run the common subgraph algorithm");
			return;
		}
		
		IEngine engine0 = (IEngine) DIHelper.getInstance().getLocalProp(engine0Name + "");
		IEngine engine1 = (IEngine) DIHelper.getInstance().getLocalProp(engine1Name + "");
		
		logger.info("Generating graph from metamodel 0: "+engine0.getEngineName());
		GraphPlaySheet playSheet0 = createMetamodel(engine0);
		vertexList0 = createVertexList(playSheet0);
		logger.info("Generating graph from metamodel 1: "+engine1.getEngineName());
		GraphPlaySheet playSheet1 = createMetamodel(engine1);
		vertexList1 = createVertexList(playSheet1);
		
		logger.info("Printing verticies in graph 0: ");
		printVertexList(vertexList0);
		logger.info("Printing verticies in graph 1: ");
		printVertexList(vertexList1);
		
		double[][] similarityMapping = createSimilarityMapping(vertexList0,vertexList1);
//		double threshold = 2.0;
		Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> mappingOptions = initialize(threshold, similarityMapping);
//		printMappingOptions(mappingOptions);
		mappedVertexList0 = new ArrayList<SEMOSSVertex>();
		mappedVertexList1 = new ArrayList<SEMOSSVertex>();

		mappedVertexListFinal0 = new ArrayList<SEMOSSVertex>();
		mappedVertexListFinal1 = new ArrayList<SEMOSSVertex>();
		nmax = 0;
//		n0 = 10;
//		numOfFirstTriesCounter=0;
//		numOfFirstTriesMax=5;
		numOfPossibleConnections=10;//1;
//		directed=true;
		
		backtrack(mappingOptions);
		printMap(mappedVertexListFinal0,mappedVertexListFinal1);
		
		//setJDesktopPane
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		playSheet0.setJDesktopPane(pane);
		playSheet0.createView();
		playSheet1.setJDesktopPane(pane);
		playSheet1.createView();
		
		GraphPlaySheet mergedPlaySheet = createGraph(playSheet0,playSheet1);
		mergedPlaySheet = createGrid(mergedPlaySheet,engine0.getEngineName(),engine1.getEngineName());
	}
	
	public GraphPlaySheet createGraph(GraphPlaySheet playSheet0, GraphPlaySheet playSheet1){
		QuestionPlaySheetStore.getInstance().idCount++;
		String insightID = QuestionPlaySheetStore.getInstance().getIDCount()+". "+ "Common Subgraph for "+playSheet0.engine.getEngineName()+" "+playSheet1.engine.getEngineName();
		DIHelper.getInstance().setLocalProperty(Constants.UNDO_BOOLEAN, false);
		GraphPlaySheet newPlaySheet = new GraphPlaySheet();
		
		newPlaySheet.setTitle("Common Subgraph: "+playSheet0.engine.getEngineName()+" "+playSheet1.engine.getEngineName());
		newPlaySheet.setQuestionID(insightID);
		
		//create new graph data
		GraphDataModel gdm = playSheet0.getGraphData();
		
		Hashtable<String, SEMOSSEdge> edgeStore = gdm.getEdgeStore();
		Hashtable<String, SEMOSSVertex> vertStore = gdm.getVertStore();
		ArrayList<String> edges = new ArrayList<String>(edgeStore.keySet());
		for(String edgeName : edges)
		{
			SEMOSSEdge edge = edgeStore.get(edgeName);
			String inVertexURI = edge.inVertex.getURI();
			String outVertexURI = edge.outVertex.getURI();
			if(!isVertexURIInList(inVertexURI,mappedVertexListFinal0)||!isVertexURIInList(outVertexURI,mappedVertexListFinal0)) {
				vertStore.get(inVertexURI).edgeHash.remove(edgeName);
				vertStore.get(outVertexURI).edgeHash.remove(edgeName);
				edgeStore.remove(edgeName);
			}
		}
		ArrayList<String> verts = new ArrayList<String>(vertStore.keySet());
		for(String vertName : verts)
		{
			if(!isVertexURIInList(vertName,mappedVertexListFinal0)) {
				vertStore.remove(vertName);
			}
		}
		
		newPlaySheet.setGraphData(gdm);
		JDesktopPane pane = (JDesktopPane)DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		QuestionPlaySheetStore.getInstance().put(insightID, newPlaySheet);
		newPlaySheet.setJDesktopPane(pane);
		newPlaySheet.createView();
		
		return newPlaySheet;		
	}
	
	private GraphPlaySheet createGrid(GraphPlaySheet playSheet, String engine0Name, String engine1Name){
		ArrayList<Object []> list = new ArrayList<Object []>();
		String[] colNames = new String[2];
		colNames[0]=engine0Name+" Node";
		colNames[1]="Mapped to "+engine1Name+" Node";
		for(int i=0;i<mappedVertexListFinal0.size();i++) {
			Object[] row = new Object[2];
			row[0] = mappedVertexListFinal0.get(i).getURI();
			row[1] = mappedVertexListFinal1.get(i).getURI();
			list.add(row);
		}
		
		JPanel gridMappedPanel = new JPanel();
		GridBagLayout gbl_gridMappedPanel = new GridBagLayout();
		gbl_gridMappedPanel.columnWidths = new int[]{0, 0};
		gbl_gridMappedPanel.rowHeights = new int[]{0, 0};
		gbl_gridMappedPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_gridMappedPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		gridMappedPanel.setLayout(gbl_gridMappedPanel);
		String tabTitle = "Mapped Nodes";
		playSheet.jTab.add(tabTitle, gridMappedPanel);
		playSheet.jTab.setSelectedComponent(gridMappedPanel);
		displayListOnTab(colNames, list, gridMappedPanel);
	
		return playSheet;
	}
	
	public void displayListOnTab(String[] colNames,ArrayList <Object []> list,JPanel panel) {
		GridScrollPane pane = new GridScrollPane(colNames, list);
		
		panel.removeAll();
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gridBagLayout.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		panel.setLayout(gridBagLayout);
		GridBagConstraints gbc_panel_1_1 = new GridBagConstraints();
		gbc_panel_1_1.insets = new Insets(0, 0, 5, 5);
		gbc_panel_1_1.fill = GridBagConstraints.BOTH;
		gbc_panel_1_1.gridx = 0;
		gbc_panel_1_1.gridy = 0;
		panel.add(pane, gbc_panel_1_1);
		panel.repaint();
	}
	
	
	private boolean isVertexURIInList(String vertURI,ArrayList<SEMOSSVertex> vertList) {
		for(int i=0;i<vertList.size();i++) {
			SEMOSSVertex comparisonVert = vertList.get(i);
			if(vertURI.equals(comparisonVert.getURI()))
				return true;
		}
		return false;
	}
	public void printVertexList(ArrayList<SEMOSSVertex> vertList)
	{
		for(SEMOSSVertex vert : vertList)
		{
			logger.info("Printing Graph ... vertex: "+vert.getURI());
			Iterator<SEMOSSEdge> outEdgeIt = vert.getOutEdges().iterator();
			while(outEdgeIt.hasNext())
			{
				SEMOSSEdge edge= (SEMOSSEdge) outEdgeIt.next();
				logger.info("Printing Graph ... vertex: "+vert.getURI()+" out edge is "+edge.getURI());
				logger.info("Printing Graph ... vertex: "+vert.getURI()+" other vertex is "+edge.inVertex.getURI());
			}
			Iterator<SEMOSSEdge> inEdgeIt = vert.getInEdges().iterator();
			while(inEdgeIt.hasNext())
			{
				SEMOSSEdge edge= (SEMOSSEdge) inEdgeIt.next();
				logger.info("Printing Graph ... vertex: "+vert.getURI()+" in edge is "+edge.getURI());
				logger.info("Printing Graph ... vertex: "+vert.getURI()+" other vertex is "+edge.outVertex.getURI());
			}
		}
	}
	
	public void printMap(ArrayList<SEMOSSVertex> vertList0,ArrayList<SEMOSSVertex> vertList1)
	{
		if(vertList0.size()==0)
			logger.info("Printing Map ... no mapping exists");
		else
			logger.info("Printing Map ... mapped "+vertList0.size()+" vertices");
		for(int i=0;i<vertList0.size();i++)
		{
			logger.info("Printing Map ... vertex "+vertList0.get(i).getURI()+" to "+vertList1.get(i).getURI());
		}

	}
	
	public void printMappingOptions(Hashtable<SEMOSSVertex,ArrayList<SEMOSSVertex>> mapOptions)
	{
		logger.info("Printing Map Options ...");
		Iterator<SEMOSSVertex> optionIt = mapOptions.keySet().iterator();
		while(optionIt.hasNext())
		{
			SEMOSSVertex vert = (SEMOSSVertex)optionIt.next();
			System.out.println("Printing Map Options ... vertex is: "+vert.getURI());
			//logger.info("Printing Map Options ... vertex is: "+vert.getURI());
			ArrayList<SEMOSSVertex> mapOptionsForVert = mapOptions.get(vert);
			for(SEMOSSVertex mapVerts : mapOptionsForVert)
				System.out.print(mapVerts.getURI().substring(mapVerts.getURI().lastIndexOf("/"))+"    ");
				//logger.info("Printing Map Options ... vertex is: "+vert.getURI()+" option is: " + mapVerts.getURI());
				//logger.info("Printing Map Options ... vertex is: "+vert.getURI()+" option is: " + mapVerts.getURI());
			System.out.println();
			
		}
	}
	
	public GraphPlaySheet createMetamodel(IEngine engine){
		ExecuteQueryProcessor exQueryProcessor = new ExecuteQueryProcessor();
		//hard code playsheet attributes since no insight exists for this
		String sparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 1";
		String playSheetName = "prerna.ui.components.playsheets.GraphPlaySheet";
		String title = engine.getEngineName() +"-Metamodel";
		String id = engine.getEngineName() + "-Metamodel";
		AbstractEngine eng = ((AbstractEngine)engine).getBaseDataEngine();
		eng.setEngineName(id);
		eng.setBaseData((RDFFileSesameEngine) eng);
		Hashtable<String, String> filterHash = new Hashtable<String, String>();
		filterHash.put("http://semoss.org/ontologies/Relation", "http://semoss.org/ontologies/Relation");
		eng.setBaseHash(filterHash);
		
		exQueryProcessor.prepareQueryOutputPlaySheet(eng, sparql, playSheetName, title, id);

		GraphPlaySheet playSheet= (GraphPlaySheet) exQueryProcessor.getPlaySheet();
		playSheet.getGraphData().setSubclassCreate(true);//this makes the base queries use subclass instead of type--necessary for the metamodel query
		playSheet.createData();
		playSheet.runAnalytics();

		return playSheet;
	}

	public ArrayList<SEMOSSVertex> createVertexList(GraphPlaySheet playSheet){
		Hashtable<String, SEMOSSVertex> vertHash = new Hashtable<String,SEMOSSVertex>();//((GraphPlaySheet)playSheet).vertStore;
		vertHash = playSheet.getGraphData().getVertStore();
		
		ArrayList<SEMOSSVertex> vertexList = new ArrayList<SEMOSSVertex>();
		Iterator<String> vertIterator = vertHash.keySet().iterator();
		while(vertIterator.hasNext())
		{
			SEMOSSVertex vert = vertHash.get((String)vertIterator.next());
			vertexList.add(vert);
//			System.out.println((String)vert.getProperty(Constants.VERTEX_NAME));
		}
		return vertexList;		
	}
	
	public ArrayList<SEMOSSVertex> createGraphs(String query){

		ArrayList<SEMOSSVertex> vertexList = new ArrayList<SEMOSSVertex>();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
		
		String id = "Common Subgraph";
		String question = QuestionPlaySheetStore.getInstance().getIDCount() +". "+ id;
				
		GraphPlaySheet playSheet = new GraphPlaySheet();
					
		playSheet.setQuery(query);
		playSheet.setRDFEngine((IEngine) engine);
		playSheet.setQuestionID(question);
		
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		playSheet.setJDesktopPane(pane);

		// put it into the store
		QuestionPlaySheetStore.getInstance().put(question, playSheet);
		
		playSheet.createData();
		
		Hashtable<String, SEMOSSVertex> vertHash = new Hashtable<String,SEMOSSVertex>();//((GraphPlaySheet)playSheet).vertStore;
		vertHash = ((GraphPlaySheet)playSheet).getGraphData().getVertStore();

		Iterator<String> vertIterator = vertHash.keySet().iterator();
		while(vertIterator.hasNext())
		{
			SEMOSSVertex vert = vertHash.get((String)vertIterator.next());
			vertexList.add(vert);
		}
		return vertexList;
	}

	public ArrayList<SEMOSSVertex> createTestGraphs1()
	{
		ArrayList<SEMOSSVertex> vertexList = new ArrayList<SEMOSSVertex>();
		SEMOSSVertex vert1 = new SEMOSSVertex("<http://health.mil/ontologies/Concept/Node/number1>");
		SEMOSSVertex vert2 = new SEMOSSVertex("<http://health.mil/ontologies/Concept/Node/number2>");
		SEMOSSVertex vert3 = new SEMOSSVertex("<http://health.mil/ontologies/Concept/Node/number3>");
		SEMOSSVertex vert4 = new SEMOSSVertex("<http://health.mil/ontologies/Concept/Node/number4>");
		SEMOSSVertex vert5 = new SEMOSSVertex("<http://health.mil/ontologies/Concept/Node/number5>");
		SEMOSSVertex vert6 = new SEMOSSVertex("<http://health.mil/ontologies/Concept/Node/number6>");

//		SEMOSSVertex vert7 = new SEMOSSVertex("<http://health.mil/ontologies/Concept/Node/number7>");
		
		SEMOSSEdge edge12 = new SEMOSSEdge(vert1,vert2,"<http://semoss.org/ontologies/Relation/Numbers/number1:number2>");
		SEMOSSEdge edge13 = new SEMOSSEdge(vert1,vert3,"<http://semoss.org/ontologies/Relation/Numbers/number1:number3>");
		SEMOSSEdge edge34 = new SEMOSSEdge(vert3,vert4,"<http://semoss.org/ontologies/Relation/Numbers/number3:number4>");
		SEMOSSEdge edge42 = new SEMOSSEdge(vert4,vert2,"<http://semoss.org/ontologies/Relation/Numbers/number4:number2>");
		SEMOSSEdge edge53 = new SEMOSSEdge(vert5,vert3,"<http://semoss.org/ontologies/Relation/Numbers/number5:number3>");
		SEMOSSEdge edge54 = new SEMOSSEdge(vert5,vert4,"<http://semoss.org/ontologies/Relation/Numbers/number5:number4>");
		SEMOSSEdge edge56 = new SEMOSSEdge(vert5,vert6,"<http://semoss.org/ontologies/Relation/Numbers/number5:number6>");

//		SEMOSSEdge edge27 = new SEMOSSEdge(vert2,vert7,"<http://semoss.org/ontologies/Relation/Numbers/number2:number7>");

		vertexList.add(vert1);
		vertexList.add(vert2);
		vertexList.add(vert3);
		vertexList.add(vert4);
		vertexList.add(vert5);
		vertexList.add(vert6);
//		vertexList.add(vert7);
		return vertexList;
	}
	
	public ArrayList<SEMOSSVertex> createTestGraphs2()
	{
		ArrayList<SEMOSSVertex> vertexList = new ArrayList<SEMOSSVertex>();
		SEMOSSVertex vertA = new SEMOSSVertex("<http://health.mil/ontologies/Concept/Node/letterA>");
		SEMOSSVertex vertB = new SEMOSSVertex("<http://health.mil/ontologies/Concept/Node/letterB>");
		SEMOSSVertex vertC = new SEMOSSVertex("<http://health.mil/ontologies/Concept/Node/letterC>");
		SEMOSSVertex vertD = new SEMOSSVertex("<http://health.mil/ontologies/Concept/Node/letterD>");
		SEMOSSVertex vertE = new SEMOSSVertex("<http://health.mil/ontologies/Concept/Node/letterE>");
//		SEMOSSVertex vertF = new SEMOSSVertex("<http://health.mil/ontologies/Concept/Node/letterF>");
		
		SEMOSSEdge edgeAB = new SEMOSSEdge(vertA,vertB,"<http://semoss.org/ontologies/Relation/Letters/letterA:letterB>");
		SEMOSSEdge edgeAC = new SEMOSSEdge(vertA,vertC,"<http://semoss.org/ontologies/Relation/Letters/letterA:letterC>");
		SEMOSSEdge edgeBC = new SEMOSSEdge(vertB,vertC,"<http://semoss.org/ontologies/Relation/Letters/letterB:letterC>");
		SEMOSSEdge edgeCD = new SEMOSSEdge(vertC,vertD,"<http://semoss.org/ontologies/Relation/Letters/letterC:letterD>");

		SEMOSSEdge edgeDE = new SEMOSSEdge(vertD,vertE,"<http://semoss.org/ontologies/Relation/Letters/letterD:letterE>");
		SEMOSSEdge edgeEC = new SEMOSSEdge(vertE,vertC,"<http://semoss.org/ontologies/Relation/Letters/letterE:letterC>");
		
//		SEMOSSEdge edgeED = new SEMOSSEdge(vertE,vertD,"<http://semoss.org/ontologies/Relation/Letters/letterE:letterD>");
//		SEMOSSEdge edgeEB = new SEMOSSEdge(vertE,vertB,"<http://semoss.org/ontologies/Relation/Letters/letterE:letterB>");
		
//		SEMOSSEdge edgeDF = new SEMOSSEdge(vertD,vertF,"<http://semoss.org/ontologies/Relation/Letters/letterD:letterF>");
		
		vertexList.add(vertA);
		vertexList.add(vertB);
		vertexList.add(vertC);
		vertexList.add(vertD);
		vertexList.add(vertE);
//		vertexList.add(vertF);
		return vertexList;
	}
	
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getRawVar(varName);
	}

	public double[][] createSimilarityMapping(ArrayList<SEMOSSVertex> vertList0, ArrayList<SEMOSSVertex> vertList1) {
		double[][] similarityMapping = null;
		try {
			similarityMapping = IntakePortal.WordNetMappingFunction(createStringList(vertexList0), createStringList(vertexList1));
		} catch (InvalidFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return similarityMapping;
	}
	
	public ArrayList<String> createStringList(ArrayList<SEMOSSVertex> vertList) {
		ArrayList<String> stringList = new ArrayList<String>();
		for(SEMOSSVertex vert : vertList) {
			String uri = vert.getURI();
			stringList.add(uri.substring(uri.lastIndexOf("/")+1));
		}
		return stringList;
	}
	
	public Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> initialize(double threshold, double[][] similarityMapping)
	{
		Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> mappingOptions = new Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>>();
		for(int vert0Ind=0;vert0Ind<vertexList0.size();vert0Ind++)
		{
			SEMOSSVertex vert0 = vertexList0.get(vert0Ind);
			ArrayList<SEMOSSVertex> possibleMappings = new ArrayList<SEMOSSVertex>();
			for(int vert1Ind=0;vert1Ind<vertexList1.size();vert1Ind++)
			{
				SEMOSSVertex vert1 = vertexList1.get(vert1Ind);
				if(possibleVertexMap(threshold, similarityMapping,vert0Ind,vert1Ind))
					possibleMappings.add(vert1);
			}
			mappingOptions.put(vert0, possibleMappings);
		}
		return mappingOptions;
	}
	
	public Boolean possibleVertexMap(double threshold, double[][] similarityMapping, int vert0Ind, int vert1Ind)
	{
		if(similarityMapping[vert0Ind][vert1Ind] >= threshold)
			return true;
		else return false;
//		if(((String)vert0.getProperty(Constants.VERTEX_NAME)).equals((String)vert1.getProperty(Constants.VERTEX_NAME)))
//			return true;
//		else return false;
		
//		if(((String)vert0.getProperty(Constants.VERTEX_TYPE)).equals((String)vert1.getProperty(Constants.VERTEX_TYPE)))
//			return true;
//		else return false;
//		
//		int deg0 = vert0.getInEdges().size() + vert0.getOutEdges().size();
//		int deg1 = vert1.getInEdges().size() + vert1.getOutEdges().size();
//		if(deg0>=deg1)
//			return true;
//		else return false;
	}
	
	/**
	 * 	Checking if the matrix is extendable.
	 * If it is extendable, it will return the next Vertex to map.
	 * If it is not extendable, it will return null.
	 * @param mappingOptions
	 * @return
	 */
	public SEMOSSVertex extendable(Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> mappingOptions)
	{

//		int q = mappedVertexList0.size();
		int numPossibleMappedVerticies = mappedVertexList0.size();//s in the algorithm
		
		//check to make sure that we are not out of options
		for(SEMOSSVertex currVertex : vertexList0)
		{
			//make sure we have not already mapped this vertex and thus counted it above
			if(!mappedVertexList0.contains(currVertex))
			{
				//if there are still mappings for this vertex remaining, then increment total num of possible mapped verticies
				if(mappingOptions.get(currVertex).size()>0)
					numPossibleMappedVerticies++;
			}
		}
		//if the number of already mapped verticies and those remaining that could possibly still be mapped is greater than
		//the number originally asked for (n0), the lesser number of nodes of V and W (nmax), and there still exist other
		//possibilities in the list of mapping options, then the set may be extended.
		if(numPossibleMappedVerticies>=nmax && numPossibleMappedVerticies>mappedVertexList0.size())//s >= nmax and s>q
			//return chosenVertex;
			return pickVertex(mappingOptions);
		else
			return null;

	}

	public Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> refine(Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> mappingOptions)
	{
//		logger.info("refine");

		Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> refinedMappingOptions = new Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>>();
		//go through all verticies
		for(SEMOSSVertex currVertex : vertexList0)
		{
			//only look at verticies that haven't yet been mapped (not in X).
			if(!mappedVertexList0.contains(currVertex))
			{
				ArrayList<SEMOSSVertex> oldPossibleMappings = mappingOptions.get(currVertex);
				ArrayList<SEMOSSVertex> refinedPossibleMappings = new ArrayList<SEMOSSVertex>();
				for(SEMOSSVertex possibleMapVert : oldPossibleMappings)
				{
					//FIX: shouldn't be checking if just possible edge map between current vertex and most recently added, also need to check if there is something else that should prevent it.
					if (!mappedVertexList1.contains(possibleMapVert)&&possibleEdgeMap(currVertex,mappedVertexList0.get(mappedVertexList0.size()-1),possibleMapVert,mappedVertexList1.get(mappedVertexList1.size()-1)))
					{
						refinedPossibleMappings.add(possibleMapVert);
					}
				}
				refinedMappingOptions.put(currVertex,refinedPossibleMappings);
			}
		}
		return refinedMappingOptions;
	}

	public Boolean possibleEdgeMap(SEMOSSVertex vert0, SEMOSSVertex mappedVert0,SEMOSSVertex vert1,SEMOSSVertex mappedVert1)
	{
		//TODO: extend to island nodes
		String mappedVert0URI = mappedVert0.getURI();
		String mappedVert1URI = mappedVert1.getURI();
		Vector<SEMOSSEdge> inEdgesForVert0List = vert0.getInEdges();
		Vector<SEMOSSEdge> inEdgesForVert1List = vert1.getInEdges();
		boolean vert0Mapped = false;
		boolean vert1Mapped = false;
		for(SEMOSSEdge inEdgeForVert0 : inEdgesForVert0List)
		{
			if(inEdgeForVert0.outVertex.getURI().equals(mappedVert0URI))
				vert0Mapped = true;
		}
		for(SEMOSSEdge inEdgeForVert1 : inEdgesForVert1List)
		{
			if(inEdgeForVert1.outVertex.getURI().equals(mappedVert1URI))
				vert1Mapped = true;
		}
		
		if(vert0Mapped!=vert1Mapped)
			return false;
		
		Vector<SEMOSSEdge> outEdgesForVert0List = vert0.getOutEdges();
		Vector<SEMOSSEdge> outEdgesForVert1List = vert1.getOutEdges();
		vert0Mapped = false;
		vert1Mapped = false;
		for(SEMOSSEdge outEdgeForVert0 : outEdgesForVert0List)
		{
			if(outEdgeForVert0.inVertex.getURI().equals(mappedVert0.getURI()))
				vert0Mapped = true;
		}
		for(SEMOSSEdge outEdgeForVert1 : outEdgesForVert1List)
		{
			if(outEdgeForVert1.inVertex.getURI().equals(mappedVert1.getURI()))
				vert1Mapped = true;
		}
		
		if(vert0Mapped!=vert1Mapped)
			return false;

		return true;
	}
	
	public Integer findVertInMappedList(SEMOSSVertex vert, ArrayList<SEMOSSVertex> vertList)
	{
		String vertURI = vert.getURI();
		int index=0;
		while(index<vertList.size())
		{
			if(vertURI.equals(vertList.get(index).getURI()))
				return index;
			index++;
		}
		return -1;
	}
	
	
	public SEMOSSVertex pickVertex(Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> mappingOptions)
	{

		SEMOSSVertex chosenVertex = null;
		int minNumOptions = -1;
		if(mappedVertexList0.size()>0)
		{
			ArrayList<SEMOSSVertex> allConnectedVerticies = new ArrayList<SEMOSSVertex>();
			for(SEMOSSVertex currMappedVertex : mappedVertexList0)
			{
				Iterator<SEMOSSEdge> inEdgeIterator = currMappedVertex.getInEdges().iterator();
				while(inEdgeIterator.hasNext())
				{
					SEMOSSEdge inEdge = (SEMOSSEdge)inEdgeIterator.next();
					SEMOSSVertex possibleConnection = inEdge.outVertex;
					if(vertexList0.contains(possibleConnection)&&!mappedVertexList0.contains(possibleConnection)&&!allConnectedVerticies.contains(possibleConnection))
					{
						allConnectedVerticies.add(possibleConnection);
						int currNumOptions = mappingOptions.get(possibleConnection).size();
						if((currNumOptions>0 && (currNumOptions<minNumOptions||minNumOptions==-1)))
						{
							chosenVertex = possibleConnection;
							minNumOptions = currNumOptions;
						}
					}
				}
				Iterator<SEMOSSEdge> outEdgeIterator = currMappedVertex.getOutEdges().iterator();
				while(outEdgeIterator.hasNext())
				{
					SEMOSSEdge outEdge = (SEMOSSEdge)outEdgeIterator.next();
					SEMOSSVertex possibleConnection = outEdge.inVertex;
					
					if(vertexList0.contains(possibleConnection)&&!mappedVertexList0.contains(possibleConnection)&&!allConnectedVerticies.contains(possibleConnection))
					{
						allConnectedVerticies.add(possibleConnection);
						int currNumOptions = mappingOptions.get(possibleConnection).size();
						if((currNumOptions>0 && (currNumOptions<minNumOptions||minNumOptions==-1)))
						{
							chosenVertex = possibleConnection;
							minNumOptions = currNumOptions;
						}
					}
				}
			}
		}
		else
		{
			minNumOptions=0;
			for(SEMOSSVertex currVertex : vertexList0)
			{
				if(!mappedVertexList0.contains(currVertex))
				{
					int currNumOptions = mappingOptions.get(currVertex).size();
					//making sure there is a vertex picked and minnum set if this is the first vertext we are iterating over
					//minNumOptions will only be zero if this is the first vertex iterated over since there must be at least one vertex with a possible map to get to this point
					if(chosenVertex == null || minNumOptions == 0 || (currNumOptions>0 && currNumOptions<minNumOptions))
					{
						chosenVertex = currVertex;
						minNumOptions = currNumOptions;
					}
				}
			}	
		}
		return chosenVertex;
	}
	
	public ArrayList<SEMOSSVertex> getMappableVerticies(SEMOSSVertex vert, Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> mappingOptions)
	{
		ArrayList<SEMOSSVertex> optionsList =  mappingOptions.get(vert);
		int numInEdgesVert = vert.getInEdges().size();
		int numOutEdgesVert = vert.getOutEdges().size();
		ArrayList<Integer> optionsListDegrees = new ArrayList<Integer>();
		ArrayList<Integer> optionsListDegreesTieBreaker = new ArrayList<Integer>();
		ArrayList<SEMOSSVertex> retList = new ArrayList<SEMOSSVertex>();
		for(SEMOSSVertex optionVertex: optionsList)
		{
			optionsListDegrees.add(Math.min(numInEdgesVert, optionVertex.getInEdges().size())+Math.min(numOutEdgesVert, optionVertex.getOutEdges().size()));
			optionsListDegreesTieBreaker.add(optionVertex.getInEdges().size()-Math.min(numInEdgesVert, optionVertex.getInEdges().size())+optionVertex.getOutEdges().size()-Math.min(numOutEdgesVert, optionVertex.getOutEdges().size()));

		}
		int i=0;
		while(i<numOfPossibleConnections &&i<optionsList.size())
//		for(int i=0;i<numOfPossibleConnections;i++)
		{
			int maxDegree=0;
			int maxDegreeTiebreaker=0;
			int maxDegreeIndex=0;
			for(int optionNumber=0;optionNumber<optionsListDegrees.size();optionNumber++)
			{
				if(optionsListDegrees.get(optionNumber)>maxDegree||(optionsListDegrees.get(optionNumber)==maxDegree&&optionsListDegreesTieBreaker.get(optionNumber)>maxDegreeTiebreaker))
				{
					maxDegree=optionsListDegrees.get(optionNumber);
					maxDegreeTiebreaker=optionsListDegreesTieBreaker.get(optionNumber);
					maxDegreeIndex=optionNumber;
				}
			}
			retList.add(optionsList.get(maxDegreeIndex));
			optionsList.remove(maxDegreeIndex);
			optionsListDegrees.remove(maxDegreeIndex);
			optionsListDegreesTieBreaker.remove(maxDegreeIndex);
			i++;
		}
		return retList;
	}
	
	public void backtrack(Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> mappingOptions)
	{
//		logger.info("backtrack "+numTimes+"number of nodes in subgraph"+mappedVertexListFinal0.size());

		//next vertex to map. null if no longer extendable.
		SEMOSSVertex vert0 = extendable(mappingOptions);
		if(vert0!=null)
		{					
//				logger.info("Backtrack ... picked vertex is "+vert0.getURI());
				ArrayList<SEMOSSVertex> possibleMappedVertexList = getMappableVerticies(vert0,mappingOptions);
//				for(SEMOSSVertex printing : possibleMappedVertexList)
//				{
//					logger.info("Backtrack ... picked vertex is "+vert0.getURI() + " possible mapping is "+printing.getURI());
//				}
				int index = 0;
				while(index<possibleMappedVertexList.size()&&index<numOfPossibleConnections)
				{
					SEMOSSVertex possibleMappedVertex = possibleMappedVertexList.get(index);
//				for(SEMOSSVertex possibleMappedVertex : possibleMappedVertexList)
//				{
//					logger.info("Backtrack ... picked vertex is "+vert0.getURI() + " mapping with vertex " +possibleMappedVertex.getURI());

					mappedVertexList0.add(vert0);
					mappedVertexList1.add(possibleMappedVertex);
//					logger.info("Mapped vertex lists with added vertices are ... ");
//					printMap(mappedVertexList0, mappedVertexList1);
					Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> refinedMappingOptions = refine(mappingOptions);
	//				refinedMappingOptions = refineLeaves(refinedMappingOptions,vert0,possibleMappedVertex);
//					logger.info("Refined Mapping Options");
//					printMappingOptions(refinedMappingOptions);
//					logger.info("Starting a new iteration of backtrack from for loop for vertex "+possibleMappedVertex.getURI());
//					if(nmax<n0)
						backtrack(refinedMappingOptions);
//					logger.info("Returning from previous backtrack to for loop for vertex "+possibleMappedVertex.getURI());
					mappedVertexList0.remove(vert0);
					mappedVertexList1.remove(possibleMappedVertex);
//					logger.info("Mapped vertex lists with removed verticies are ... ");
	//				printMap(mappedVertexList0,mappedVertexList1);
					index++;
				}
				
				vertexList0.remove(vert0);
//				logger.info("Starting a new iteration of backtrack after removing vert0 "+vert0.getURI());
//				if(nmax<n0)//&&numOfFirstTriesCounter<numOfFirstTriesMax)
					backtrack(mappingOptions);
//				logger.info("Returning from previous backtrack and adding vert0 "+vert0.getURI());
		//		vertexList0.add(vert0);
		}
		else
		{
			logger.info("Backtrack ... Extendable is false");
			if(mappedVertexList0.size()>nmax)
			{
				printMap(mappedVertexList0,mappedVertexList1);
				nmax=mappedVertexList0.size();
				mappedVertexListFinal0=deepCopy(mappedVertexList0);
				mappedVertexListFinal1=deepCopy(mappedVertexList1);
			}
		}
	}
	
	public ArrayList<SEMOSSVertex> deepCopy (ArrayList<SEMOSSVertex> toCopy)
	{
		ArrayList<SEMOSSVertex> retList = new ArrayList<SEMOSSVertex>();
		for(SEMOSSVertex vert : toCopy)
		{
			retList.add(vert);
		}
		return retList;
	}

	
}
