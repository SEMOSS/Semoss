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
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.om.GraphDataModel;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.transformer.ArrowDrawPaintTransformer;
import prerna.ui.transformer.EdgeArrowStrokeTransformer;
import prerna.ui.transformer.EdgeStrokeTransformer;
import prerna.ui.transformer.VertexLabelFontTransformer;
import prerna.ui.transformer.VertexPaintTransformer;
import prerna.util.Constants;

/**
 * This class performs the calculations for data latency. 
 */
public class DataLatencyPerformer implements IAlgorithm{

	GraphPlaySheet ps = null;
	protected GraphDataModel gdm = new GraphDataModel();
	public SEMOSSVertex [] pickedVertex = null;
	static final Logger logger = LogManager.getLogger(DataLatencyPerformer.class.getName());
	double value;
	Vector<SEMOSSEdge> masterEdgeVector = new Vector();//keeps track of everything accounted for in the forest
	Vector<SEMOSSVertex> masterVertexVector = new Vector();
	Vector<SEMOSSVertex> currentPathVerts = new Vector<SEMOSSVertex>();//these are used for depth search first
	Vector<SEMOSSEdge> currentPathEdges = new Vector<SEMOSSEdge>();
	double currentPathLate;
	Hashtable validEdges = new Hashtable();
	Hashtable<String, String> validVerts = new Hashtable<String, String>();
	String selectedNodes = "";
	double naFrequencyFraction = 0;
	double notInterfaceFraction = 1;
	Hashtable<SEMOSSEdge, Double> finalEdgeScores = new Hashtable();
	Hashtable<SEMOSSVertex, Double> finalVertScores = new Hashtable();
	boolean finalScoresFilled = false;
	
	/**
	 * Given a specific playsheet and a vector, delegate the forest, get edges and vertices and add them to master vectors.
	 * 
	 * @param p GraphPlaySheet
	 * @param vect DBCMVertex[]
	 */
	public DataLatencyPerformer(GraphPlaySheet p, SEMOSSVertex[] vect){
		ps = p;
		pickedVertex = vect;
		gdm = ps.gdm;
		Collection<SEMOSSEdge> edges = gdm.getEdgeStore().values();
		Collection<SEMOSSVertex> v = gdm.getVertStore().values();
		masterEdgeVector.addAll(edges);
		masterVertexVector.addAll(v);
	}
	
	/**
	 * Sets initial value for performing data latency calculation.
	 * 
	 * @param val double
	 */
	public void setValue(double val){
		value = val;
	}
	
	/**
	 * Given an array of vertices, set the specific vertex for performing calculation.
	 * 
	 * @param v DBCMVertex[]
	 */
	public void setPickedVertex(SEMOSSVertex[] v){
		pickedVertex = v;
	}

	/**
	 * Sets the playsheet for calculation to be performed upon.
	 * 
	 * @param graphPlaySheet IPlaySheet
	 */
	@Override
	public void setPlaySheet(IPlaySheet graphPlaySheet) {
		ps = (GraphPlaySheet) graphPlaySheet;
		
	}

	/**
	 * Gets variables.
	 * 
	 * //TODO: Return empty object instead of null
	 * @return String[] */
	@Override
	public String[] getVariables() {
		return null;
	}

	/**
	 * Clears the hashtables containing strings and DBCM vertices.
	 * Create a new vector of vertices of forest roots and run the depth search to look for complete paths / loops.
	 */
	@Override
	public void execute() {
		executeWeb();
		setTransformers();
	}
	
	public void executeWeb() {
		validVerts.clear();
		validEdges.clear();
		Vector<SEMOSSVertex> forestRoots = getForestRoots();
		runDepthFirstSearch(forestRoots);
	}
	
	/**
	 * Puts selected roots into a vector and adds properties into the hashtable of valid vertices.
	 * If not specified, this method performs the above actions on all nodes.
	
	 * @return Vector<DBCMVertex> */
	private Vector<SEMOSSVertex> getForestRoots(){
		Vector<SEMOSSVertex> forestRoots = new Vector<SEMOSSVertex>();
		if(pickedVertex.length!=0){
			int count = 0;
			for(SEMOSSVertex selectedVert : pickedVertex) {
				forestRoots.add(selectedVert);
				validVerts.put((String) selectedVert.getProperty(Constants.URI), (String) selectedVert.getProperty(Constants.URI));
				finalVertScores.put(selectedVert, 0.0);
				if(count > 0) selectedNodes = selectedNodes +", ";
				selectedNodes = selectedNodes + selectedVert.getProperty(Constants.VERTEX_NAME);
				count++;
			}
		}
		else{
			selectedNodes = "All";
			Collection<SEMOSSVertex> forestRootsCollection = gdm.getVertStore().values();
			for(SEMOSSVertex v : forestRootsCollection) {
				forestRoots.add(v);
				validVerts.put((String) v.getProperty(Constants.URI), (String) v.getProperty(Constants.URI));
				finalVertScores.put(v, 0.0);
			}
		}
		return forestRoots;
	}
	
	/**
	 * Get all possible full length paths for every vertex in the master vertex vector.
	 * If a path returns back to the starting node, then put it inside the loop hashtable.
	 * 
	 * @param roots Vector<DBCMVertex>		List of roots.
	 */
	private void runDepthFirstSearch(Vector<SEMOSSVertex> roots){

		for(SEMOSSVertex vertex : roots){
			
			Hashtable <SEMOSSEdge, Double> usedLeafEdges = new Hashtable<SEMOSSEdge, Double>();//keeps track of all bottom edges previously visited and their score
			
			Vector<SEMOSSVertex> currentNodes = new Vector<SEMOSSVertex>();
			//use next nodes as the future set of nodes to traverse down from.
			Vector<SEMOSSVertex> nextNodes = new Vector<SEMOSSVertex>();
			
			int levelIndex = 0;
			while(!currentPathVerts.isEmpty() || levelIndex == 0){
				int pathIndex = 0;
				currentPathVerts.clear();
				currentNodes.add(vertex);
				currentPathEdges.clear();
				currentPathLate = 0;
				while(!nextNodes.isEmpty() || pathIndex == 0){
					nextNodes.clear();
					SEMOSSVertex nextNode = null;
					while (!currentNodes.isEmpty()){
						SEMOSSVertex vert = currentNodes.remove(0);
						 nextNode = traverseDepthDownward(vert, usedLeafEdges, vertex);
						if(nextNode!=null)
							nextNodes.add(nextNode);
						
						pathIndex++;
					}
					currentNodes.addAll(nextNodes);
					
					levelIndex++;
					//if the path has created a loop, it needs to be done.  Otherwise it will unfairly evaluate the rest of the paths downstream
					//if(currentPathVerts.indexOf(nextNode)!=currentPathVerts.lastIndexOf(nextNode)) nextNodes.clear();
				}
				// Complete path

				if(currentPathEdges.size()>0){
					SEMOSSEdge leafEdge = currentPathEdges.get(currentPathEdges.size()-1);
					addPathAsValid(currentPathEdges, currentPathVerts);
					usedLeafEdges.put(leafEdge, currentPathLate);
					//put in the final scores hash if it is a better score
					Double edgeScore = currentPathLate;
					if(finalEdgeScores.containsKey(leafEdge)){
						if(finalEdgeScores.get(leafEdge)<currentPathLate)
							edgeScore = finalEdgeScores.get(leafEdge);
					}
					finalEdgeScores.put(leafEdge, edgeScore);
				}
			}
			
		}
	}

	/**
	 * Returns the next vertex for data latency calculations to be performed upon.
	 * Uses the current vertex and keeps track of which edges are valid.
	 * Scores vertices based on most efficient way to get to that vertex.
	 * 
	 * @param vert DBCMVertex
	 * @param usedLeafEdges Hashtable<DBCMEdge,Double>
	 * @param rootVert DBCMVertex
	
	 * @return DBCMVertex */
	private SEMOSSVertex traverseDepthDownward(SEMOSSVertex vert, Hashtable<SEMOSSEdge,  Double> usedLeafEdges, SEMOSSVertex rootVert){
		SEMOSSVertex nextVert = null;
		Collection<SEMOSSEdge> edgeArray = getValidEdges(vert.getOutEdges());
		for (SEMOSSEdge edge: edgeArray){
			SEMOSSVertex inVert = edge.inVertex;
			String freqString = "";
			if(edge.getProperty("Frequency")!=null) {
				String frequency = edge.getProperty("Frequency") + "";
				freqString = frequency.replaceAll("\"", "");
			}
			else validEdges.put((String) edge.getProperty(Constants.URI), notInterfaceFraction);
			//if the edge is not available or doens't have a frequency, remove from master edges and make red
			if(!isAvailable(freqString)){
				//masterEdgeVector.remove(edge);
				validEdges.put((String) edge.getProperty(Constants.URI), naFrequencyFraction);
			}
			double freqDouble = translateString(freqString);
			double tempPathLate = currentPathLate + freqDouble;
			double leafEdgeScore = 0.0;
			if(usedLeafEdges.containsKey(edge)) leafEdgeScore = usedLeafEdges.get(edge);
			if(tempPathLate<= value && masterVertexVector.contains(inVert) && (!usedLeafEdges.containsKey(edge)||tempPathLate<leafEdgeScore) && !currentPathEdges.contains(edge)){
				nextVert = inVert;//this is going to be the returned vert, so this is all set
				if (currentPathVerts.contains(inVert)) {
					currentPathVerts.add(inVert);
					currentPathEdges.add(edge);
					return null;
				}
				currentPathVerts.add(inVert);
				currentPathEdges.add(edge);
				currentPathLate = tempPathLate;
				//add vertex to final scores if this is a better way to get to that vertex
				Double vertScore = currentPathLate;
				if(finalVertScores.containsKey(inVert)){
					if(finalVertScores.get(inVert)<currentPathLate)
						vertScore = finalVertScores.get(inVert);
				}
				finalVertScores.put(inVert, vertScore);
				return nextVert;
			}
		}
		return nextVert;
	}

	/**
	 * From the collection of DBCM edges, determine which edges are valid
	 * @param vector Collection<DBCMEdge>
	
	 * @return Vector<DBCMEdge> */
	private Vector<SEMOSSEdge> getValidEdges(Collection<SEMOSSEdge> vector){
		Vector<SEMOSSEdge> validEdges = new Vector<SEMOSSEdge>();
		if (vector==null) return validEdges;
		for(SEMOSSEdge edge : vector){
			if(masterEdgeVector.contains(edge))
				validEdges.add(edge);
		}
		return validEdges;
	}

	/**
	 * Goes through edges and vertices to put valid paths in the appropriate hash tables.
	 * Valid edges hashtable has properties and the edge score for the best paths.
	 * 
	 * @param edges Vector<DBCMEdge>
	 * @param verts Vector<DBCMVertex>
	 */
	private void addPathAsValid(Vector<SEMOSSEdge> edges, Vector<SEMOSSVertex> verts){
		for(SEMOSSVertex vertex: verts){
			validVerts.put((String) vertex.getProperty(Constants.URI), (String) vertex.getProperty(Constants.URI));
		}
		
		for(SEMOSSEdge edge : edges){
			double edgeScore = getEdgeScore(edge);
			validEdges.put((String) edge.getProperty(Constants.URI), edgeScore);
		}
	}
	
	/**
	 * Given an edge, return a double of the edge score based on the frequency of the edge.
	 * @param edge DBCMEdge
	
	 * @return double */
	private double getEdgeScore(SEMOSSEdge edge){
		double ret = 1.0;
		if(edge.getProperty("Frequency")==null)
			ret = notInterfaceFraction;
		else {
			String frequency = edge.getProperty("Frequency") + "";
			String freqString = frequency.replaceAll("\"", "");
			if(!isAvailable(freqString)){
				ret = naFrequencyFraction;
			}
		}
		
		return ret;
	}

	/**
	 * Get the name of the algorithm - in this case, Data Latency Performer.
	
	 * @return String */
	@Override
	public String getAlgoName() {
		return "Data Latency Performer";
	}
	
	/**
	 * If the string representing the frequency of data is anything other than TBD or N/A, then the calculation can be performed.
	 * @param freqString String		String representing how frequently data is released.
	
	 * @return boolean				True as long as frequency is not TBD or N/A for when data is added */
	private boolean isAvailable(String freqString){
		boolean available = true;
		if(freqString.equalsIgnoreCase("TBD")) available = false;
		if(freqString.equalsIgnoreCase("n/a")) available = false;
		return available;
	}
	
	/**
	 * Fills hashtables up to the point of the provided input value.
	 * Gets the forest roots from a vector of vertices and gets all possible full length paths.
	 * @param inputValue Double
	 */
	public void fillHashesWithValuesUpTo(Double inputValue){
		value = inputValue;

		Vector<SEMOSSVertex> forestRoots = getForestRoots();
		runDepthFirstSearch(forestRoots);
		
		finalScoresFilled = true;
	}

	/**
	 * Clears the valid vertices and edges hashtables; fills them with new values; sets the new transformers.
	 */
	public void executeFromHash() {
		validVerts.clear();
		validEdges.clear();
		fillValidComponentHashes();
		setTransformers();
	}
	
	/**
	 * Creates new vectors of valid edges and vertices.
	 * Iterates through the hashtables of vertices and vertex scores and edges and edge scores.
	 * Add the vertex or edge to the vectors as long as the score is not null.
	 * Given the valid edges and vertices, we can add the path as valid.
	 */
	public void fillValidComponentHashes(){
		Vector<SEMOSSEdge> validEdges = new Vector(); 
		Vector<SEMOSSVertex> validVerts = new Vector();
		Iterator vertIt = finalVertScores.keySet().iterator();
		while(vertIt.hasNext()){
			SEMOSSVertex vert = (SEMOSSVertex) vertIt.next();
			Double score = finalVertScores.get(vert);
			if (score!=null){
				if(score<=value) validVerts.add(vert);
			}
		}
		Iterator edgeIt = finalEdgeScores.keySet().iterator();
		while(edgeIt.hasNext()){
			SEMOSSEdge edge = (SEMOSSEdge) edgeIt.next();
			Double score = finalEdgeScores.get(edge);
			if(score!=null){//It will be null if it is TBD or if the max value wasn't big enough
				if(score<=value) validEdges.add(edge);
			}
		}
		addPathAsValid(validEdges, validVerts);
	}

	/**
	 * Given a string representing frequency of data sent through systems, quantify this value based on frequency.
	 * @param freqString String		String representing how frequently data is released.
	
	 * @return int					Number associated with the frequency string. */
	private int translateString(String freqString){
		int freqInt = 0;
		if(freqString.equalsIgnoreCase("TBD")) freqInt = 168;
		if(freqString.equalsIgnoreCase("n/a")) freqInt = 168;
		else if(freqString.equalsIgnoreCase("Real-time (user-initiated)")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Batch (monthly)")) freqInt = 720;
		else if(freqString.equalsIgnoreCase("Weekly")) freqInt = 168;
		else if(freqString.equalsIgnoreCase("Monthly")) freqInt = 720;
		else if(freqString.equalsIgnoreCase("Batch (daily)")) freqInt = 24;
		else if(freqString.equalsIgnoreCase("Batch(Daily)")) freqInt = 24;
		else if(freqString.equalsIgnoreCase("Real-time")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Transactional")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("On Demand")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Event Driven (seconds-minutes)")) freqInt = 60;
		else if(freqString.equalsIgnoreCase("TheaterFramework")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Event Driven (Seconds)")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Web services")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("TF")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Batch (12/day)")) freqInt = 2;
		else if(freqString.equalsIgnoreCase("SFTP")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Batch (twice monthly)")) freqInt = 360;
		else if(freqString.equalsIgnoreCase("Daily")) freqInt = 24;
		else if(freqString.equalsIgnoreCase("Hourly")) freqInt = 1;
		else if(freqString.equalsIgnoreCase("Near Real-time (transaction initiated)")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Batch (three times a week)")) freqInt = 56;
		else if(freqString.equalsIgnoreCase("Batch (weekly)")) freqInt = 168;
		else if(freqString.equalsIgnoreCase("Near Real-time")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Real Time")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Batch")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Batch (bi-monthly)")) freqInt = 1440;
		else if(freqString.equalsIgnoreCase("Batch (semiannually)")) freqInt = 4392;
		else if(freqString.equalsIgnoreCase("Event Driven (Minutes-hours)")) freqInt = 1;
		else if(freqString.equalsIgnoreCase("Annually")) freqInt = 8760;
		else if(freqString.equalsIgnoreCase("Batch(Monthly)")) freqInt = 720;
		else if(freqString.equalsIgnoreCase("Bi-Weekly")) freqInt = 336;
		else if(freqString.equalsIgnoreCase("Daily at end of day")) freqInt = 24;
		else if(freqString.equalsIgnoreCase("TCP")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("event-driven (Minutes-hours)")) freqInt = 1;
		else if(freqString.equalsIgnoreCase("Interactive")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Weekly Quarterly")) freqInt = 2184;
		else if(freqString.equalsIgnoreCase("Weekly Daily Weekly Weekly Weekly Weekly Daily Daily Daily")) freqInt = 168;
		else if(freqString.equalsIgnoreCase("Weekly Daily")) freqInt = 168;
		else if(freqString.equalsIgnoreCase("Periodic")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Batch (4/day)")) freqInt = 6;
		else if(freqString.equalsIgnoreCase("Batch(Daily/Monthly)")) freqInt = 720;
		else if(freqString.equalsIgnoreCase("Weekly; Interactive; Interactive")) freqInt = 168;
		else if(freqString.equalsIgnoreCase("interactive")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Batch (quarterly)")) freqInt = 2184;
		else if(freqString.equalsIgnoreCase("Every 8 hours (KML)/On demand (HTML)")) freqInt = 8;
		else if(freqString.equalsIgnoreCase("Monthly at beginning of month, or as user initiated")) freqInt = 720;
		else if(freqString.equalsIgnoreCase("On demad")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Monthly Bi-Monthly Weekly Weekly")) freqInt = 720;
		else if(freqString.equalsIgnoreCase("Quarterly")) freqInt = 2184;
		else if(freqString.equalsIgnoreCase("On-demand")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("user upload")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("1/hour (KML)/On demand (HTML)")) freqInt = 1;
		else if(freqString.equalsIgnoreCase("DVD")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Real-time ")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Weekly ")) freqInt = 168;
		else if(freqString.equalsIgnoreCase("Annual")) freqInt = 8760;
		else if(freqString.equalsIgnoreCase("Daily Interactive")) freqInt = 24;
		else if(freqString.equalsIgnoreCase("NFS, Oracle connection")) freqInt = 0;
		else if(freqString.equalsIgnoreCase("Batch(Weekly)")) freqInt = 168;
		else if(freqString.equalsIgnoreCase("Batch(Quarterly)")) freqInt = 2184;
		else if(freqString.equalsIgnoreCase("Batch (yearly)")) freqInt = 8760;
		else if(freqString.equalsIgnoreCase("Each user login instance")) freqInt = 0;
		return freqInt;
	}

	/**
	 * Sets the transformers based on valid edges and vertices for the playsheet.
	 */
	private void setTransformers(){
		EdgeStrokeTransformer tx = (EdgeStrokeTransformer)ps.getView().getRenderContext().getEdgeStrokeTransformer();
		tx.setEdges(validEdges);
		EdgeArrowStrokeTransformer stx = (EdgeArrowStrokeTransformer)ps.getView().getRenderContext().getEdgeArrowStrokeTransformer();
		stx.setEdges(validEdges);
		ArrowDrawPaintTransformer atx = (ArrowDrawPaintTransformer)ps.getView().getRenderContext().getArrowDrawPaintTransformer();
		atx.setEdges(validEdges);
		VertexPaintTransformer vtx = (VertexPaintTransformer)ps.getView().getRenderContext().getVertexFillPaintTransformer();
		vtx.setVertHash(validVerts);
		VertexLabelFontTransformer vlft = (VertexLabelFontTransformer)ps.getView().getRenderContext().getVertexFontTransformer();
		vlft.setVertHash(validVerts);
		// repaint it
		ps.getView().repaint();
	}

	public Hashtable getEdgeScores() {
		Hashtable scores = new Hashtable();
		for (SEMOSSEdge key : finalEdgeScores.keySet()) {
			if (!scores.containsKey(finalEdgeScores.get(key))) {
				ArrayList<SEMOSSEdge> uriList = new ArrayList();
				uriList.add(key);
				scores.put(finalEdgeScores.get(key), uriList);
			}
			else {
				ArrayList<SEMOSSEdge> uriList = (ArrayList<SEMOSSEdge>) scores.get(finalEdgeScores.get(key));
				uriList.add(key);
				scores.put(finalEdgeScores.get(key), uriList);
			}
		}
		return scores;
	}
}
