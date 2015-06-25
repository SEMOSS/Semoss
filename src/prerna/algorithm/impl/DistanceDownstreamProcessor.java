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
package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;

import javax.swing.JInternalFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import prerna.algorithm.api.IAlgorithm;
import prerna.om.GraphDataModel;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridTableModel;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;

/**
 * This class uses the information from DistanceDownstreamInserter in order to actually perform the distance downstream calculation.
 */
public class DistanceDownstreamProcessor implements IAlgorithm {

	protected GraphDataModel gdm = null;
	protected ArrayList<SEMOSSVertex> selectedVerts = new ArrayList<SEMOSSVertex>();
	GridFilterData gfd = new GridFilterData();
	protected GraphPlaySheet playSheet;
	public Hashtable masterHash = new Hashtable();
	public String distanceString = "Distance";
	public String pathString = "vertexPath";
	public String edgePathString = "edgePathString";
	public String leafString = "leafString";
	String selectedNodes="";
	protected ArrayList<SEMOSSVertex> nextNodes = new ArrayList<SEMOSSVertex>();
	
	
	/**
	 * Performs downstream processing.
	 */
	@Override
	public void execute() {
		ArrayList<SEMOSSVertex> currentNodes = setRoots();
		performDownstreamProcessing(currentNodes);
	}
	
	/**
	 * Starts with a hashtable of all the roots and moves downward without ever touching the same node twice to set the roots.
	
	 * @return ArrayList<DBCMVertex> 	List of roots. */
	protected ArrayList<SEMOSSVertex> setRoots(){
		//use current nodes as the next set of nodes that I will have to traverse downward from.  Starts with root nodes
		ArrayList<SEMOSSVertex> currentNodes = new ArrayList<SEMOSSVertex>();
		//as we go, put in masterHash with vertHash.  vertHash has distance and path with the key being the actual vertex
		if(selectedVerts.size()!=0){
			int count = 0;
			for(SEMOSSVertex selectedVert : selectedVerts) {
				if(count > 0) selectedNodes = selectedNodes +", ";
				selectedNodes = selectedNodes + selectedVert.getProperty(Constants.VERTEX_NAME);
				currentNodes.add(selectedVerts.indexOf(selectedVert), selectedVert);
				count++;
			}
		}
		else{
			selectedNodes = "All";
			currentNodes.addAll(gdm.getVertStore().values());
		}
		
		
		//start with the root nodes in the masterHash
		for(SEMOSSVertex vert: currentNodes) {
			Hashtable vertHash = new Hashtable();
			ArrayList<SEMOSSVertex> path = new ArrayList<SEMOSSVertex>();
			ArrayList<SEMOSSVertex> edgePath = new ArrayList<SEMOSSVertex>();
			path.add(vert);
			vertHash.put(distanceString, 0);
			vertHash.put(pathString, path);
			vertHash.put(edgePathString, edgePath);
			masterHash.put(vert, vertHash);
		}
		return currentNodes;
	}
	
	/**
	 * Performs the downstream processing by using the current nodes as the future set of nodes to traverse down from.
	 * @param currentNodes ArrayList<DBCMVertex>		List of current nodes.
	 */
	protected void performDownstreamProcessing(ArrayList<SEMOSSVertex> currentNodes){
		int nodeIndex = 0;
		int levelIndex = 1;
		while(!nextNodes.isEmpty() || levelIndex == 1){
			nextNodes.clear();

			while (!currentNodes.isEmpty()){
				nodeIndex = 0;
				SEMOSSVertex vert = currentNodes.remove(nodeIndex);

				Hashtable vertHash = (Hashtable) masterHash.get(vert);
				ArrayList<SEMOSSVertex> parentPath = (ArrayList<SEMOSSVertex>) vertHash.get(pathString);
				ArrayList<SEMOSSEdge> parentEdgePath = (ArrayList<SEMOSSEdge>) vertHash.get(edgePathString);
				
				ArrayList<SEMOSSVertex> subsetNextNodes = traverseDownward(vert, levelIndex, parentPath, parentEdgePath);
				
				nextNodes.addAll(subsetNextNodes);
				
//				System.err.println(vert.uri + "GOES TO : :::: :: :");
//				for(SEMOSSVertex printVert : nextNodes)
//					System.err.println(printVert.uri);
				
//				nodeIndex++;
			}
			currentNodes.addAll(nextNodes);
			
			levelIndex++;
		}
	}
	
	/**
	 * Traverses downward from the nodes.
	 * @param vert SEMOSSVertex						A single vertex.
	 * @param levelIndex int						Level index.
	 * @param parentPath ArrayList<SEMOSSVertex>		List of path distances.
	 * @param parentEdgePath ArrayList<SEMOSSVertex>	List of edge paths.
	
	 * @return ArrayList<DBCMVertex> 				Vert array. Used to calculate network value in DistanceDownstreamInserter. */
	public ArrayList<SEMOSSVertex> traverseDownward(SEMOSSVertex vert, int levelIndex, ArrayList<SEMOSSVertex> parentPath, ArrayList<SEMOSSEdge> parentEdgePath){
		ArrayList<SEMOSSVertex> vertArray = new ArrayList<SEMOSSVertex>();
		Collection<SEMOSSEdge> edgeArray = vert.getOutEdges();
		for (SEMOSSEdge edge: edgeArray){
			SEMOSSVertex inVert = edge.inVertex;
			if(!masterHash.containsKey(inVert)) {
				vertArray.add(inVert);//this is going to be the returned array, so this is all set
				
				//now I have to add this new vertex to masterHash.  This requires using the vertHash of the parent child to get path
				Hashtable vertHash = new Hashtable();
				ArrayList<SEMOSSVertex> newPath = new ArrayList<SEMOSSVertex>();
				ArrayList<SEMOSSEdge> newEdgePath = new ArrayList<SEMOSSEdge>();
				newPath.addAll(parentPath);
				newEdgePath.addAll(parentEdgePath);
				newPath.add(inVert);
				newEdgePath.add(edge);
				vertHash.put(distanceString, levelIndex);
				vertHash.put(pathString, newPath);
				vertHash.put(edgePathString, newEdgePath);
				masterHash.put(inVert, vertHash);
			}
		}
		
		//if the vertArray is null, I'm going to add a key saying that it is a leaf of the tree
		if(vertArray.size()==0){
			Hashtable parentHash = (Hashtable) masterHash.get(vert);
			parentHash.put(leafString, "Leaf");
		}
		
		return vertArray;
	}
	
	/**
	 * Sets the forest.
	 * @param f DelegateForest		Forest that is set.
	 */
	public void setGraphDataModel(GraphDataModel g){
		gdm = g;
	}
	
	/**
	 * Sets selected nodes.
	 * @param pickedVertices DBCMVertex[]		Array of picked vertices to be set.
	 */
	public void setSelectedNodes(ArrayList<SEMOSSVertex> pickedVertices) {
		selectedVerts.addAll(pickedVertices);
	}
	
	/**
	 * Iterates through the roots and adds them to the array list of selected vertices.
	 */
	public void setRootNodesAsSelected(){
		Collection roots = gdm.getVertStore().values();
		Iterator<SEMOSSVertex> rootsIt = roots.iterator();
		while (rootsIt.hasNext()){
			selectedVerts.add(rootsIt.next());
		}
	}

	/**
	 * Determines whether a selected node will be added.
	 * @param pickedVertex String		Object name as a string
	 * @param position int				If 0, have the chance to traverse from data object directly
	
	 * @return boolean					True if the picked vertex matches */
	public boolean addSelectedNode(String pickedVertex, int position){
		Collection<SEMOSSVertex> vertices = gdm.getVertStore().values();
		for(SEMOSSVertex vert : vertices){
			if(pickedVertex.equals(vert.uri)){
				selectedVerts.add(position, vert);
				System.out.println("SET VERT..................." + vert.uri + " to position " + position);
				return true;
			}
		}

		return false;
	}
	
	/**
	 * Sets playsheet as a graph play sheet.
	 * @param ps IPlaySheet		Playsheet that will be cast.
	 */
	@Override
	public void setPlaySheet(IPlaySheet ps){
		playSheet = (GraphPlaySheet) ps;
	}

	/**
	 * Creates new tab on GraphPlaySheet.
	 */
	public void createTab(){

		JTable table = new JTable();
		JInternalFrame nodeRankSheet = new JInternalFrame();
		GridTableModel model = new GridTableModel(gfd);
		table.setModel(model);
		table.setAutoCreateRowSorter(true);
		//table.getRowSorter().toggleSortOrder(2);
		//table.getRowSorter().toggleSortOrder(3);
		JScrollPane scrollPane = new JScrollPane(table);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane.setAutoscrolls(true);
		nodeRankSheet.setContentPane(scrollPane);
		
		//set tab on graphplaysheet
		playSheet.jTab.add("Hops From " + selectedNodes, nodeRankSheet);
		nodeRankSheet.setClosable(true);
		nodeRankSheet.setMaximizable(true);
		nodeRankSheet.setIconifiable(true);
		nodeRankSheet.setTitle("Hops Downstream From "+selectedNodes);
		playSheet.jTab.setSelectedComponent(nodeRankSheet);
	
		nodeRankSheet.pack();
		nodeRankSheet.setVisible(true);
	}

	/**
	 * Iterates through master hashtable to get vertex name, vertex type, hops, and root node.
	 * If the edge path has a weight, then add "multiplied weight" as a column name for the grid filter.
	 */
	public void setGridFilterData(){
		String[] colNames = new String[5];
		colNames[0] = "Vertex Name";
		colNames[1] = "Vertex Type";
		colNames[2] = "Hops";
		colNames[3] = "Root Node";

		//use masterHash to fill tableList and gfd
		ArrayList <Object []> tableList = new ArrayList();
		Iterator masterIt = masterHash.keySet().iterator();
		
		boolean weightCheck = false;
		while (masterIt.hasNext()){
			SEMOSSVertex vertex = (SEMOSSVertex) masterIt.next();
			Hashtable vertHash = (Hashtable) masterHash.get(vertex);
			
			int dist = (Integer) vertHash.get(distanceString);
			ArrayList path = (ArrayList) vertHash.get(pathString);
			String root = (String) ((SEMOSSVertex)path.get(0)).getProperty(Constants.VERTEX_NAME);

			String nodeName = (String) vertex.getProperty(Constants.VERTEX_NAME);
			String nodeType = (String) vertex.getProperty(Constants.VERTEX_TYPE);
			Double multWeight = getMultipliedWeight((ArrayList<SEMOSSEdge>) vertHash.get(edgePathString));
			Object[] rowArray = {nodeName, nodeType, dist, root, multWeight};
			tableList.add(rowArray);
			if (multWeight > 0) weightCheck = true;
		}

		if (weightCheck == true){
			colNames[4] = "Multiplied Weight";
		}
		else {
			colNames = new String[4];
			colNames[0] = "Vertex Name";
			colNames[1] = "Vertex Type";
			colNames[2] = "Hops";
			colNames[3] = "Root Node";
			//remove all weight columns
			tableList = removeColumn(tableList, 4);
		}
		gfd.setColumnNames(colNames);
		
		gfd.setDataList(tableList);
	}
	
	/**
	 * Removes column.
	 * @param tableList ArrayList			Existing list of column names.
	 * @param column int					Number of columns that currently exist.
	
	 * @return ArrayList 					List with new column names.*/
	private ArrayList removeColumn(ArrayList tableList, int column){
		ArrayList newTableList = new ArrayList();
		for(int i = 0; i<tableList.size(); i++){
			Object[] row = (Object[]) tableList.get(i);
			Object[] newRow = new Object[row.length-1];
			int count = 0;
			for(int j = 0; j<row.length;j++){
				if(j!=column){
					newRow[count] = row[j];
					count++;
				}
			}
			newTableList.add(newRow);
		}
		return newTableList;
	}
	
	/**
	 * Returns the multiplied weight of an edge.
	 * @param edgePath ArrayList<DBCMEdge>		List containing all the edges.
	
	 * @return Double 							Final edge weight. */
	private Double getMultipliedWeight(ArrayList<SEMOSSEdge> edgePath){
		int count = 0;
		double total = 1.0;
		Iterator<SEMOSSEdge> edgeIt = edgePath.iterator();
		while (edgeIt.hasNext()){
			SEMOSSEdge edge = edgeIt.next();
			if (edge.getProperty().containsKey("weight")){
				total = total* (Double) edge.getProperty("weight");
				count++;
			}
		}
		if(count>0) return total;
		return 0.0;
	}

	/**
	 * Gets variables.
	 * 
	//TODO: Return empty object instead of null
	 * @return String[] 			List of variable names stored in a string array. */
	@Override
	public String[] getVariables() {

		return null;
	}

	/**
	 * Gets algorithm name - in this case, "Distance Downstream."
	
	 * @return String			Name of algorithm. */
	@Override
	public String getAlgoName() {
		return "Distance Downstream";
	}
	
}
