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
package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import prerna.algorithm.api.IAlgorithm;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.transformer.ArrowDrawPaintTransformer;
import prerna.ui.transformer.EdgeArrowStrokeTransformer;
import prerna.ui.transformer.EdgeStrokeTransformer;
import prerna.ui.transformer.VertexLabelFontTransformer;
import prerna.ui.transformer.VertexPaintTransformer;
import prerna.util.Constants;
import edu.uci.ics.jung.graph.DelegateForest;

/**
 * This class is used to identify loops within a network.
 */
public class LoopIdentifierProcessor implements IAlgorithm{

	DelegateForest forest = null;
	ArrayList<SEMOSSVertex> selectedVerts = new ArrayList<SEMOSSVertex>();
	GridFilterData gfd = new GridFilterData();
	GraphPlaySheet playSheet;
	Hashtable<String, SEMOSSEdge> nonLoopEdges = new Hashtable<String, SEMOSSEdge>();
	Hashtable<String, SEMOSSEdge> loopEdges = new Hashtable<String, SEMOSSEdge>();
	Hashtable<String, SEMOSSVertex> nonLoopVerts = new Hashtable<String, SEMOSSVertex>();
	Hashtable<String, String> loopVerts = new Hashtable<String, String>();
	String selectedNodes="";
	Vector<SEMOSSEdge> masterEdgeVector = new Vector();//keeps track of everything accounted for in the forest
	Vector<SEMOSSVertex> masterVertexVector = new Vector();
	Vector<SEMOSSVertex> currentPathVerts = new Vector<SEMOSSVertex>();//these are used for depth search first
	Vector<SEMOSSEdge> currentPathEdges = new Vector<SEMOSSEdge>();
	
	
	/**
	 * Executes the process of loop identification.
	 * If a node in the forest is not part of a loop, remove it from the forest.
	 * Run depth search in order to validate the remaining edges in the forest.
	 */
	public void execute(){
		//All I have to do is go through every node in the forest
		//if the node has in and out, it could be part of a loop
		//if a node has only in or only out edges, it is not part of a loop
		//therefore, remove the vertex and all edges associated with it from the forest
		//once there are no edges getting removed, its time to stop
		//Then I run depth search first to validate the edges left
		Collection<SEMOSSVertex> allVerts = forest.getVertices();
		Vector<SEMOSSVertex> currentVertices = new Vector<SEMOSSVertex>();
		Vector<SEMOSSVertex> nextVertices = new Vector<SEMOSSVertex>();
		currentVertices.addAll(allVerts);
		nextVertices.addAll(currentVertices);
		Vector<SEMOSSEdge> newlyRemovedEdges = new Vector<SEMOSSEdge>();
		int count = 0;
		while(count==0 || newlyRemovedEdges.size()!=0){
			newlyRemovedEdges.clear();
			for(SEMOSSVertex vertex : currentVertices){
				Vector<SEMOSSEdge> inEdges = getValidEdges(vertex.getInEdges());
				int inEdgeCount = inEdges.size();
				Vector<SEMOSSEdge> outEdges = getValidEdges(vertex.getOutEdges());
				int outEdgeCount = outEdges.size();
				//if inEdges is 0, put the vert and its edges in hashtables and remove everything associated with it from the forest
				if(inEdgeCount == 0){
					nonLoopVerts.put((String) vertex.getProperty(Constants.URI), vertex);
					putEdgesInHash(outEdges, nonLoopEdges);
					newlyRemovedEdges.addAll(removeEdgesFromMaster(outEdges));
					nextVertices.remove(vertex);
					masterVertexVector.remove(vertex);
				}
				else if (outEdgeCount == 0){
					nonLoopVerts.put((String) vertex.getProperty(Constants.URI), vertex);
					putEdgesInHash(inEdges, nonLoopEdges);
					newlyRemovedEdges.addAll(removeEdgesFromMaster(inEdges));
					nextVertices.remove(vertex);
					masterVertexVector.remove(vertex);
				}
				count++;
			}
			currentVertices.clear();
			currentVertices.addAll(nextVertices);
		}
		//phase 1 is now complete.  The only vertices and edges left must have in and out edges
		//However, there is still the possiblity of fake edges and nodes that exist only between two loops
		//Now I will perform depth search first on all remaining nodes to ensure that every edge is a loop
		runDepthSearchFirst();
		//Everything that is left in nextVertices and the forest now must be loopers
		//lets put them in their respective hashtables and set the transformers
		setTransformers();
		
	}
	
	/**
	 * Get all possible full length paths for every vertex in the master vertex vector.
	 * If a path returns back to the starting node, then put it inside the loop hashtable.
	 */
	private void runDepthSearchFirst(){
		//for every vertex remaining in master vertex vector, I will get all possible full length paths
		//If a path return back to the starting node, put it in the loop hash
		for(SEMOSSVertex vertex : masterVertexVector){
			Vector <SEMOSSVertex> usedLeafVerts = new Vector<SEMOSSVertex>();//keeps track of all bottom nodes previously visited
			usedLeafVerts.add(vertex);
			
			Vector<SEMOSSVertex> currentNodes = new Vector<SEMOSSVertex>();
			//use next nodes as the future set of nodes to traverse down from.
			Vector<SEMOSSVertex> nextNodes = new Vector<SEMOSSVertex>();
			
			//check if there is a loop with itself
			if(checkIfCompletesLoop(vertex, vertex)){
				addPathAsLoop(currentPathEdges, currentPathVerts);
			}
			
			int levelIndex = 0;
			while(!currentPathVerts.isEmpty() || levelIndex == 0){
				int pathIndex = 0;
				currentNodes.add(vertex);
				currentPathVerts.clear();
				currentPathEdges.clear();
				while(!nextNodes.isEmpty() || pathIndex == 0){
					nextNodes.clear();
					while (!currentNodes.isEmpty()){
						SEMOSSVertex vert = currentNodes.remove(0);
						
						SEMOSSVertex nextNode = traverseDepthDownward(vert, usedLeafVerts);
						if(nextNode!=null)
							nextNodes.add(nextNode);
						
						pathIndex++;
					}
					currentNodes.addAll(nextNodes);
					
					levelIndex++;
				}
				//Now I should have a complete path.  I need to check to see it it can make it back to the root node.
				//If it can make it back to the root node, it is a loop and should be added to the loop hashtables
				if(currentPathVerts.size()>0){
					SEMOSSVertex leafVert = currentPathVerts.get(currentPathVerts.size()-1);
					if(checkIfCompletesLoop(leafVert, vertex)){
						//add loop to loop hashtables
						addPathAsLoop(currentPathEdges, currentPathVerts);
					}
					usedLeafVerts.add(leafVert);
				}
			}
			
		}
	}
	
	/**
	 * Validate whether or not the loop is complete.
	 * @param leaf DBCMVertex		Child node.
	 * @param root DBCMVertex		Parent node.
	
	 * @return boolean 				Returns true if the loop is complete. */
	private boolean checkIfCompletesLoop(SEMOSSVertex leaf, SEMOSSVertex root){
		boolean retBool = false;
		if(leaf == null) return false;

		Collection<SEMOSSEdge> edgeArray = getValidEdges(forest.getOutEdges(leaf));
		for (SEMOSSEdge edge: edgeArray){
			SEMOSSVertex inVert = edge.inVertex;
			if(inVert.equals(root)) {
				currentPathEdges.add(edge);
				currentPathVerts.add(root);
				return true;
			}
		}
		return retBool;
	}
	
	/**
	 * Returns the next node for loop identification to be performed upon.
	 * Uses the current vertex and keeps track of which edges are valid.
	 * Scores vertices based on most efficient way to get to that vertex.
	 * @param vert DBCMVertex						Current node.
	 * @param usedLeafVerts Vector<DBCMVertex>
	
	 * @return DBCMVertex 							Next node for processing. */
	private SEMOSSVertex traverseDepthDownward(SEMOSSVertex vert, Vector<SEMOSSVertex> usedLeafVerts){
		SEMOSSVertex nextVert = null;
		Collection<SEMOSSEdge> edgeArray = getValidEdges(forest.getOutEdges(vert));
		for (SEMOSSEdge edge: edgeArray){
			SEMOSSVertex inVert = edge.inVertex;
			if(masterVertexVector.contains(inVert) && !usedLeafVerts.contains(inVert) && !currentPathVerts.contains(inVert)){
				nextVert = inVert;//this is going to be the returned vert, so this is all set
				currentPathVerts.add(inVert);
				currentPathEdges.add(edge);
				return nextVert;
			}
		}
		return nextVert;
	}
	/**
	 * From the collection of DBCM edges, determine which edges are valid.
	 * @param vector Collection<DBCMEdge>	Collection of edges.
	
	 * @return Vector<DBCMEdge>				List of valid edges. */
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
	 * Sets the transformers based on valid edges and vertices for the playsheet.
	 */
	private void setTransformers(){
			EdgeStrokeTransformer tx = (EdgeStrokeTransformer)playSheet.getView().getRenderContext().getEdgeStrokeTransformer();
			tx.setEdges(loopEdges);
			EdgeArrowStrokeTransformer stx = (EdgeArrowStrokeTransformer)playSheet.getView().getRenderContext().getEdgeArrowStrokeTransformer();
			stx.setEdges(loopEdges);
			ArrowDrawPaintTransformer atx = (ArrowDrawPaintTransformer)playSheet.getView().getRenderContext().getArrowDrawPaintTransformer();
			atx.setEdges(loopEdges);
			VertexPaintTransformer vtx = (VertexPaintTransformer)playSheet.getView().getRenderContext().getVertexFillPaintTransformer();
			vtx.setVertHash(loopVerts);
			VertexLabelFontTransformer vlft = (VertexLabelFontTransformer)playSheet.getView().getRenderContext().getVertexFontTransformer();
			vlft.setVertHash(loopVerts);
		// repaint it
		playSheet.getView().repaint();
	}
	
	/**
	 * Adds a given path as a loop in the network.
	 * @param edges Vector<DBCMEdge>		List of edges.
	 * @param verts Vector<DBCMVertex>		List of nodes.
	 */
	private void addPathAsLoop(Vector<SEMOSSEdge> edges, Vector<SEMOSSVertex> verts){
		for(SEMOSSVertex vertex: verts){
			loopVerts.put((String) vertex.getProperty(Constants.URI), (String) vertex.getProperty(Constants.URI));
		}
		
		for(SEMOSSEdge edge : edges){
			loopEdges.put((String) edge.getProperty(Constants.URI), edge);
		}
	}
	
	/**
	 * Removes edges from the master list of edges.
	 * @param edges Vector<DBCMEdge>		Original list of edges.
	
	 * @return Vector<DBCMEdge>				Updated list of edges. */
	private Vector<SEMOSSEdge> removeEdgesFromMaster(Vector <SEMOSSEdge> edges){
		Vector<SEMOSSEdge> removedEdges = new Vector<SEMOSSEdge>();
		for(int edgeIndex = 0;edgeIndex < edges.size();edgeIndex++)
		{
			SEMOSSEdge edge = edges.elementAt(edgeIndex);
			if(masterEdgeVector.contains(edge)){
				removedEdges.add(edge);
				masterEdgeVector.remove(edge);
			}
		}
		return removedEdges;
	}
	
	/**
	 * Put edges into hashtable. Iterates through collection of edges and puts the property of the edges into hashtable.
	 * @param edges Collection<DBCMEdge>		Collection of edges.
	 * @param hash Hashtable<String,DBCMEdge>	Hashtable of edges.
	
	 * @return Hashtable<String,DBCMEdge>		Final hashtable of properties and edges. */
	private Hashtable <String, SEMOSSEdge> putEdgesInHash(Collection <SEMOSSEdge> edges, Hashtable <String, SEMOSSEdge> hash)
	{
		Iterator edgeIt = edges.iterator();
		while(edgeIt.hasNext())
		{
			SEMOSSEdge edge = (SEMOSSEdge) edgeIt.next();
			hash.put((String)edge.getProperty(Constants.URI), edge);
		}
		return hash;
	}
	
	/**
	 * Sets the forest.
	 * @param f DelegateForest	Forest to be set.
	 */
	public void setForest(DelegateForest f){
		forest = f;
		Collection<SEMOSSEdge> edges = f.getEdges();
		Collection<SEMOSSVertex> v = f.getVertices();
		masterEdgeVector.addAll(edges);
		masterVertexVector.addAll(v);
	}
	
	/**
	 * Sets selected nodes.
	 * @param pickedVertices DBCMVertex[]		List of picked vertices to be set.
	 */
	public void setSelectedNodes(SEMOSSVertex[] pickedVertices){
		for (int idx = 0; idx< pickedVertices.length ; idx++){
			selectedVerts.add(pickedVertices[idx]);
		}
	}

	/**
	 * Sets playsheet as a graph playsheet.
	 * @param ps IPlaySheet		Playsheet to be cast.
	 */
	public void setPlaySheet(IPlaySheet ps){
		playSheet = (GraphPlaySheet) ps;
	}

	/**
	 * Gets variable names.
	
	 * //TODO: Return empty object instead of null
	 * @return String[] 	List of variable names in a string array. */
	@Override
	public String[] getVariables() {
		return null;
	}

	/**
	 * Gets algorithm name - in this case, "Loop Identifier."
	
	 * @return String		Name of algorithm. */
	@Override
	public String getAlgoName() {
		return "Loop Identifier";
	}

}
