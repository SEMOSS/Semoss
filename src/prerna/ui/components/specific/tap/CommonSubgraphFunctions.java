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
package prerna.ui.components.specific.tap;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.JDesktopPane;
import javax.swing.JPanel;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

import prerna.algorithm.nlp.IntakePortal;
import prerna.om.GraphDataModel;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.ui.components.ExecuteQueryProcessor;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

/**
 * The CommonSubgraphFunctions class takes two metamodels and finds a common subgraph between them using the CSIA algorithm.
 */
public class CommonSubgraphFunctions {

	private static final Logger logger = LogManager.getLogger(CommonSubgraphFunctions.class.getName());
	
	//V and W that show all the vertices in the original graphs.
	ArrayList<SEMOSSVertex> vertexList0;
	ArrayList<SEMOSSVertex> vertexList1;
		
	//X and Y that show all the vertices that have been mapped from V and W (vertexList0 and vertexList1)
	ArrayList<SEMOSSVertex> mappedVertexList0;
	ArrayList<SEMOSSVertex> mappedVertexList1;

	//X and Y that show all the vertices that have been mapped from V and W (vertexList0 and vertexList1) and are to be saved as final
	ArrayList<SEMOSSVertex> mappedVertexListFinal0;
	ArrayList<SEMOSSVertex> mappedVertexListFinal1;
	
	//the highest number of vertices currently mapped
	int nmax;
	//number of mapping options to check. Keeps the code from going forever with highly webbed databases.
	int numOfPossibleConnections;
		
	/**
	 * Determines the common subgraph between the two selected databases.
	 * Displays the original two metamodels as graphs and the common subgraph between them as another graph.
	 * @param threshold The minimum similarity the two types must have to be considered a possible match.
	 */
	public void CSIA(double threshold,String engine0Name,String engine1Name)
	{			
		IEngine engine0 = (IEngine) DIHelper.getInstance().getLocalProp(engine0Name + "");
		IEngine engine1 = (IEngine) DIHelper.getInstance().getLocalProp(engine1Name + "");
		
		logger.info("Generating graph from metamodel 0: "+engine0.getEngineName());
		GraphPlaySheet playSheet0 = createMetamodel(engine0);
		vertexList0 = createVertexList(playSheet0);
		logger.info("Generating graph from metamodel 1: "+engine1.getEngineName());
		GraphPlaySheet playSheet1 = createMetamodel(engine1);
		vertexList1 = createVertexList(playSheet1);
		
		logger.debug("Printing verticies in graph 0: ");
		printVertexList(vertexList0);
		logger.debug("Printing verticies in graph 1: ");
		printVertexList(vertexList1);
		
		double[][] similarityMapping = createSimilarityMapping(vertexList0,vertexList1);
		Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> mappingOptions = initialize(threshold, similarityMapping);
		printMappingOptions(mappingOptions);
		mappedVertexList0 = new ArrayList<SEMOSSVertex>();
		mappedVertexList1 = new ArrayList<SEMOSSVertex>();

		mappedVertexListFinal0 = new ArrayList<SEMOSSVertex>();
		mappedVertexListFinal1 = new ArrayList<SEMOSSVertex>();
		nmax = 0;
		numOfPossibleConnections=10;
		
		backtrack(mappingOptions);
		printMap(mappedVertexListFinal0,mappedVertexListFinal1);
		
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		playSheet0.setJDesktopPane(pane);
		playSheet0.createView();
		playSheet1.setJDesktopPane(pane);
		playSheet1.createView();
		
		GraphPlaySheet subgraphPlaySheet = createSubgraph(playSheet0,playSheet1);
		subgraphPlaySheet = createSubgraphGrid(subgraphPlaySheet,engine0.getEngineName(),engine1.getEngineName());
	}
	
	/**
	 * Creates and displays the subgraph as a GraphPlaySheet
	 * @param playSheet0 First GraphPlaySheet displaying metamodel from first database selected
	 * @param playSheet1 Second GraphPlaySheet displaying metamodel from second database selected
	 * @return GraphPlaySheet displaying metamodel that is the largest common subgraph of the two databases.
	 */
	private GraphPlaySheet createSubgraph(GraphPlaySheet playSheet0, GraphPlaySheet playSheet1){
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
	
	/**
	 * Creates and displays a grid as a second tab on the GraphPlaySheet
	 * @param playSheet GraphPlaySheet displaying metamodel that is the largest common subgraph of the two databases.
	 * @param engine0Name	First Database's name
	 * @param engine1Name	Second Database's name
	 * @return GraphPlaySheet displaying metamodel that is the largest common subgraph of the two databases with the GridPlaySheet tab detailing with mapping
	 */
	private GraphPlaySheet createSubgraphGrid(GraphPlaySheet playSheet, String engine0Name, String engine1Name){
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
	
	/**
	 * Creates and displays a grid from the list of elements on the panel provided.
	 * @param colNames	Names of the columns
	 * @param list	List of elements to put in each row
	 * @param panel	Panel to add the grid to
	 */
	private void displayListOnTab(String[] colNames,ArrayList <Object []> list,JPanel panel) {
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
	
	/**
	 * Checks if a list contains the given vertex. Returns true if so, false if not.
	 * @param vertURI URI of the vertex to look for
	 * @param vertList	ArrayList<SEMOSSVertex> to look for vertex in
	 * @return true if list contains a vertex with the given URI. False if not.
	 */
	private boolean isVertexURIInList(String vertURI,ArrayList<SEMOSSVertex> vertList) {
		for(int i=0;i<vertList.size();i++) {
			SEMOSSVertex comparisonVert = vertList.get(i);
			if(vertURI.equals(comparisonVert.getURI()))
				return true;
		}
		return false;
	}
	
	/**
	 * Outputs the verticies in a list for debugging purposes.
	 * Also prints all the edges for each vertex.
	 * @param vertList ArrayList<SEMOSSVertex> of verticies to print out
	 */
	private void printVertexList(ArrayList<SEMOSSVertex> vertList)
	{
		for(SEMOSSVertex vert : vertList)
		{
			logger.debug("Printing Graph ... vertex: "+vert.getURI());
			Iterator<SEMOSSEdge> outEdgeIt = vert.getOutEdges().iterator();
			while(outEdgeIt.hasNext())
			{
				SEMOSSEdge edge= (SEMOSSEdge) outEdgeIt.next();
				logger.debug("Printing Graph ... vertex: "+vert.getURI()+" out edge is "+edge.getURI());
				logger.debug("Printing Graph ... vertex: "+vert.getURI()+" other vertex is "+edge.inVertex.getURI());
			}
			Iterator<SEMOSSEdge> inEdgeIt = vert.getInEdges().iterator();
			while(inEdgeIt.hasNext())
			{
				SEMOSSEdge edge= (SEMOSSEdge) inEdgeIt.next();
				logger.debug("Printing Graph ... vertex: "+vert.getURI()+" in edge is "+edge.getURI());
				logger.debug("Printing Graph ... vertex: "+vert.getURI()+" other vertex is "+edge.outVertex.getURI());
			}
		}
	}
	
	/**
	 * Prints the mapping that has been determined for debugging purposes.
	 * @param vertList0 ArrayList<SEMOSSVertex> of verticies from the first database that were mapped
	 * @param vertList1 ArrayList<SEMOSSVertex> of verticies from the second database that were mapped to
	 */
	private void printMap(ArrayList<SEMOSSVertex> vertList0,ArrayList<SEMOSSVertex> vertList1)
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
	
	/**
	 * Prints the possible mapping options from database 2 that are possible for each node in database 1.
	 * @param mapOptions Hashtable<SEMOSSVertex,ArrayList<SEMOSSVertex>> storing the SEMOSSVertex from database 1 and all its possible mappings in database2.
	 */
	private void printMappingOptions(Hashtable<SEMOSSVertex,ArrayList<SEMOSSVertex>> mapOptions)
	{
		logger.debug("Printing Map Options ...");
		Iterator<SEMOSSVertex> optionIt = mapOptions.keySet().iterator();
		while(optionIt.hasNext())
		{
			SEMOSSVertex vert = (SEMOSSVertex)optionIt.next();
			logger.debug("Printing Map Options ... vertex is: "+vert.getURI());
			ArrayList<SEMOSSVertex> mapOptionsForVert = mapOptions.get(vert);
			for(SEMOSSVertex mapVerts : mapOptionsForVert) {
				logger.debug(mapVerts.getURI().substring(mapVerts.getURI().lastIndexOf("/"))+"    ");
				//logger.debug("Printing Map Options ... vertex is: "+vert.getURI()+" option is: " + mapVerts.getURI());
				//logger.debug("Printing Map Options ... vertex is: "+vert.getURI()+" option is: " + mapVerts.getURI());
			}
		}
	}
	
	/**
	 * Creates the GraphPlaySheet for a database that shows the metamodel.
	 * @param engine IEngine to create the metamodel from
	 * @return GraphPlaySheet that displays the metamodel
	 */
	private GraphPlaySheet createMetamodel(IEngine engine){
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

	/**
	 * Creates a list of the nodes in the GraphPlaySheet
	 * @param playSheet GraphPlaySheet to pull the nodes from
	 * @return ArrayList<SEMOSSVertex> that contains all the verticies on the GraphPlaySheet
	 */
	private ArrayList<SEMOSSVertex> createVertexList(GraphPlaySheet playSheet){
		Hashtable<String, SEMOSSVertex> vertHash = new Hashtable<String,SEMOSSVertex>();
		vertHash = playSheet.getGraphData().getVertStore();
		
		ArrayList<SEMOSSVertex> vertexList = new ArrayList<SEMOSSVertex>();
		Iterator<String> vertIterator = vertHash.keySet().iterator();
		while(vertIterator.hasNext())
		{
			SEMOSSVertex vert = vertHash.get((String)vertIterator.next());
			vertexList.add(vert);
		}
		return vertexList;		
	}
	
	/**
	 * Determines the similarity score between each element in the first list and every element in the second list.
	 * Returns a matrix with the similarity of each combination.
	 * @param vertList0 List of vertices from the first database to compare
	 * @param vertList1 List of vertices from the second database to compare
	 * @return	double[][] with the similarity score of each.
	 */
	private double[][] createSimilarityMapping(ArrayList<SEMOSSVertex> vertList0, ArrayList<SEMOSSVertex> vertList1) {
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
	
	/**
	 * Turns a SEMOSSVertex ArrayList into an ArrayList of URIs.
	 * @param vertList ArrayList<SEMOSSVertex> to convert
	 * @return ArrayList<String> list of SEMOSSVertices URIs
	 */
	private ArrayList<String> createStringList(ArrayList<SEMOSSVertex> vertList) {
		ArrayList<String> stringList = new ArrayList<String>();
		for(SEMOSSVertex vert : vertList) {
			String uri = vert.getURI();
			stringList.add(uri.substring(uri.lastIndexOf("/")+1));
		}
		return stringList;
	}
	
	/**
	 * Initializes the mappingOptions data store.
	 * This holds all the possible mappings in database 2 for each node in database1.
	 * mappings are possible if the similarity between the two nodes is greater than the required threshold.
	 * @param threshold Double representing the minimum similarity value to be a mapping 
	 * @param similarityMapping	double[][] that stores the similarity between every combination of nodes
	 * @return Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> where every key is a vertex in the first database and the value is its possible mappings.
	 */
	private Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> initialize(double threshold, double[][] similarityMapping)
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
	
	/**
	 * Determines if two vertices can be mapped to each other based on their similarity and the threshold
	 * We have looked at other ways of determining a possible mapping and are left as commented code.
	 * @param threshold Double representing the minimum similarity value to be a mapping 
	 * @param similarityMapping	double[][] that stores the similarity between every combination of nodes
	 * @param vert0Ind Vertex from database 1 to check
	 * @param vert1Ind Vertex from database 2 to check
	 * @return true if possible mapping, false if not.
	 */
	private Boolean possibleVertexMap(double threshold, double[][] similarityMapping, int vert0Ind, int vert1Ind)
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
	 * @param mappingOptions Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> where every key is a vertex in the first database and the value is its possible mappings.
	 * @return SEMOSSVertex from database 1 that should be the next vertex to map
	 */
	private SEMOSSVertex extendable(Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> mappingOptions)
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

	/**
	 * Refines the possible mapping options based on the newly mapped verticies.
	 * @param mappingOptions Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> where every key is a vertex in the first database and the value is its possible mappings.
	 * @return Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> where every key is a vertex in the first database and the value is its possible mappings.
	 */
	private Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> refine(Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> mappingOptions)
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

	/**
	 * Determines if an edge between two vertices in database 1 can map to an edge between two vertices in database 2.
	 * @param vert0 Vertex from database 1 that has not been mapped yet
	 * @param mappedVert0 Vertex from database 1 that has been mapped
	 * @param vert1 Vertex from database 2 that has not been mapped yet
	 * @param mappedVert1 Vertex from database 2 that has been mapped
	 * @return true if edges can map, false if not.
	 */
	private Boolean possibleEdgeMap(SEMOSSVertex vert0, SEMOSSVertex mappedVert0,SEMOSSVertex vert1,SEMOSSVertex mappedVert1)
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
	
	/**
	 * Pick the next vertex from database 1 to map.
	 * @param mappingOptions Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> where every key is a vertex in the first database and the value is its possible mappings.
	 * @return SEMOSSVertex representing the next vertex in database 1 to map.
	 */
	private SEMOSSVertex pickVertex(Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> mappingOptions)
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
	
	/**
	 * Selects all vertices in database 2 that may be mapped to a given vertex from database 1.
	 * @param vert SEMOSSVertex from database 1 that we are trying to find mappings for
	 * @param mappingOptions Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> where every key is a vertex in the first database and the value is its possible mappings.
	 * @return ArrayList<SEMOSSVertex> of possible vertices from database 2 that can be mapped to.
	 */
	private ArrayList<SEMOSSVertex> getMappableVerticies(SEMOSSVertex vert, Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> mappingOptions)
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
	
	/**
	 * Recursive method to construct the common subgraph for two given graphs.
	 * Constructs common subgraph one vertex at a time selecting available alternatives.
	 * Backtracks if contraction is impossible in an explored direction and continues in another.
	 * @param mappingOptions Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> where every key is a vertex in the first database and the value is its possible mappings.
	 */
	private void backtrack(Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> mappingOptions)
	{
		//next vertex to map. null if no longer extendable.
		SEMOSSVertex vert0 = extendable(mappingOptions);
		if(vert0!=null)
		{					
				logger.debug("Backtrack ... picked vertex is "+vert0.getURI());
				ArrayList<SEMOSSVertex> possibleMappedVertexList = getMappableVerticies(vert0,mappingOptions);
				for(SEMOSSVertex printing : possibleMappedVertexList)
					logger.debug("Backtrack ... picked vertex is "+vert0.getURI() + " possible mapping is "+printing.getURI());
				int index = 0;
				while(index<possibleMappedVertexList.size()&&index<numOfPossibleConnections)
				{
					SEMOSSVertex possibleMappedVertex = possibleMappedVertexList.get(index);
					logger.debug("Backtrack ... picked vertex is "+vert0.getURI() + " mapping with vertex " +possibleMappedVertex.getURI());

					mappedVertexList0.add(vert0);
					mappedVertexList1.add(possibleMappedVertex);
					logger.debug("Mapped vertex lists with added vertices are ... ");
//					printMap(mappedVertexList0, mappedVertexList1);
					Hashtable<SEMOSSVertex, ArrayList<SEMOSSVertex>> refinedMappingOptions = refine(mappingOptions);
					logger.debug("Refined Mapping Options");
//					printMappingOptions(refinedMappingOptions);
					logger.debug("Starting a new iteration of backtrack from for loop for vertex "+possibleMappedVertex.getURI());
					backtrack(refinedMappingOptions);
					logger.debug("Returning from previous backtrack to for loop for vertex "+possibleMappedVertex.getURI());
					mappedVertexList0.remove(vert0);
					mappedVertexList1.remove(possibleMappedVertex);
					logger.debug("Mapped vertex lists with removed verticies are ... ");
//					printMap(mappedVertexList0,mappedVertexList1);
					index++;
				}
				
				vertexList0.remove(vert0);
				logger.debug("Starting a new iteration of backtrack after removing vert0 "+vert0.getURI());
				backtrack(mappingOptions);
//				logger.info("Returning from previous backtrack and adding vert0 "+vert0.getURI());
//				vertexList0.add(vert0);
		}
		else
		{
			logger.debug("Backtrack ... Extendable is false");
			if(mappedVertexList0.size()>nmax)
			{
//				printMap(mappedVertexList0,mappedVertexList1);
				nmax=mappedVertexList0.size();
				mappedVertexListFinal0=deepCopy(mappedVertexList0);
				mappedVertexListFinal1=deepCopy(mappedVertexList1);
			}
		}
	}
	
	/**
	 * Creates a deep copy of an ArrayList<SEMOSSVertex>
	 * @param toCopy ArrayList<SEMOSSVertex> that should be copied
	 * @return ArrayList<SEMOSSVertex> that is a copy of the original array
	 */
	private ArrayList<SEMOSSVertex> deepCopy (ArrayList<SEMOSSVertex> toCopy)
	{
		ArrayList<SEMOSSVertex> retList = new ArrayList<SEMOSSVertex>();
		for(SEMOSSVertex vert : toCopy) {
			retList.add(vert);
		}
		return retList;
	}

	
}
