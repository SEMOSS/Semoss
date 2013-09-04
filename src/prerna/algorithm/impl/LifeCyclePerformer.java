package prerna.algorithm.impl;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.om.DBCMEdge;
import prerna.om.DBCMVertex;
import prerna.ui.components.GraphPlaySheet;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.transformer.ArrowDrawPaintTransformer;
import prerna.ui.transformer.EdgeArrowStrokeTransformer;
import prerna.ui.transformer.EdgeStrokeTransformer;
import prerna.ui.transformer.SearchEdgeStrokeTransformer;
import prerna.ui.transformer.SearchVertexLabelFontTransformer;
import prerna.ui.transformer.SearchVertexPaintTransformer;
import prerna.ui.transformer.VertexLabelFontTransformer;
import prerna.ui.transformer.VertexPaintTransformer;
import prerna.util.Constants;
import edu.uci.ics.jung.graph.DelegateForest;

public class LifeCyclePerformer implements IAlgorithm{

	GraphPlaySheet ps = null;
	DelegateForest forest;
	public DBCMVertex [] pickedVertex = null;
	Logger logger = Logger.getLogger(getClass());
	double value;
	Vector<DBCMEdge> masterEdgeVector = new Vector();//keeps track of everything accounted for in the forest
	Vector<DBCMVertex> masterVertexVector = new Vector();
	Vector<DBCMVertex> currentPathVerts = new Vector<DBCMVertex>();//these are used for depth search first
	Vector<DBCMEdge> currentPathEdges = new Vector<DBCMEdge>();
	double currentPathLate;
	Hashtable validEdges = new Hashtable();
	Hashtable<String, DBCMVertex> validVerts = new Hashtable<String, DBCMVertex>();
	String selectedNodes = "";
	double naFrequencyFraction = 0;
	double notInterfaceFraction = 1;
	Hashtable<DBCMEdge, Double> finalEdgeScores = new Hashtable();
	Hashtable<DBCMVertex, Double> finalVertScores = new Hashtable();
	boolean finalScoresFilled = false;
	
	public LifeCyclePerformer(GraphPlaySheet p, DBCMVertex[] vect){
		ps = p;
		pickedVertex = vect;
		forest = ps.forest;
		Collection<DBCMEdge> edges = forest.getEdges();
		Collection<DBCMVertex> v = forest.getVertices();
		masterEdgeVector.addAll(edges);
		masterVertexVector.addAll(v);
	}
	
	public void setValue(double val){
		value = val;
	}
	
	public void setPickedVertex(DBCMVertex[] v){
		pickedVertex = v;
	}

	@Override
	public void setPlaySheet(IPlaySheet graphPlaySheet) {
		ps = (GraphPlaySheet) graphPlaySheet;
		
	}

	@Override
	public String[] getVariables() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void execute() {
		validVerts.clear();
		validEdges.clear();
		Vector<DBCMVertex> forestRoots = getForestRoots();
		runDepthSearchFirst(forestRoots);
		setTransformers();
	}
	
	private Vector<DBCMVertex> getForestRoots(){
		Vector<DBCMVertex> forestRoots = new Vector<DBCMVertex>();
		if(pickedVertex.length!=0){
			int count = 0;
			for(DBCMVertex selectedVert : pickedVertex) {
				forestRoots.add(selectedVert);
				validVerts.put((String) selectedVert.getProperty(Constants.URI), selectedVert);
				finalVertScores.put(selectedVert, 0.0);
				if(count > 0) selectedNodes = selectedNodes +", ";
				selectedNodes = selectedNodes + selectedVert.getProperty(Constants.VERTEX_NAME);
				count++;
			}
		}
		else{
			selectedNodes = "All";
			Collection<DBCMVertex> forestRootsCollection = forest.getRoots();
			for(DBCMVertex v : forestRootsCollection) {
				forestRoots.add(v);
				validVerts.put((String) v.getProperty(Constants.URI), v);
				finalVertScores.put(v, 0.0);
			}
		}
		return forestRoots;
	}
	
	private void runDepthSearchFirst(Vector<DBCMVertex> roots){
		//for every vertex remaining in master vertex vector, I will get all possible full length paths
		//If a path return back to the starting node, put it in the loop hash
		for(DBCMVertex vertex : roots){
			
			Hashtable <DBCMEdge, Double> usedLeafEdges = new Hashtable<DBCMEdge, Double>();//keeps track of all bottom edges previously visited and their score
			
			Vector<DBCMVertex> currentNodes = new Vector<DBCMVertex>();
			//use next nodes as the future set of nodes to traverse down from.
			Vector<DBCMVertex> nextNodes = new Vector<DBCMVertex>();
			
			int levelIndex = 0;
			while(!currentPathVerts.isEmpty() || levelIndex == 0){
				int pathIndex = 0;
				currentPathVerts.clear();
				currentNodes.add(vertex);
				currentPathEdges.clear();
				currentPathLate = 0;
				while(!nextNodes.isEmpty() || pathIndex == 0){
					nextNodes.clear();
					DBCMVertex nextNode = null;
					while (!currentNodes.isEmpty()){
						DBCMVertex vert = currentNodes.remove(0);
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
				//Now I should have a complete path.  I need to check to see it it can make it back to the root node.
				//If it can make it back to the root node, it is a loop and should be added to the loop hashtables
				if(currentPathEdges.size()>0){
					DBCMEdge leafEdge = currentPathEdges.get(currentPathEdges.size()-1);
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

	private DBCMVertex traverseDepthDownward(DBCMVertex vert, Hashtable<DBCMEdge,  Double> usedLeafEdges, DBCMVertex rootVert){
		DBCMVertex nextVert = null;
		Collection<DBCMEdge> edgeArray = getValidEdges(forest.getOutEdges(vert));
		for (DBCMEdge edge: edgeArray){
			DBCMVertex inVert = edge.inVertex;
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

	private Vector<DBCMEdge> getValidEdges(Collection<DBCMEdge> vector){
		Vector<DBCMEdge> validEdges = new Vector<DBCMEdge>();
		if (vector==null) return validEdges;
		for(DBCMEdge edge : vector){
			if(masterEdgeVector.contains(edge))
				validEdges.add(edge);
		}
		return validEdges;
	}

	private void addPathAsValid(Vector<DBCMEdge> edges, Vector<DBCMVertex> verts){
		for(DBCMVertex vertex: verts){
			validVerts.put((String) vertex.getProperty(Constants.URI), vertex);
		}
		
		for(DBCMEdge edge : edges){
			double edgeScore = getEdgeScore(edge);
			validEdges.put((String) edge.getProperty(Constants.URI), edgeScore);
		}
	}
	
	private double getEdgeScore(DBCMEdge edge){
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

	@Override
	public String getAlgoName() {
		// TODO Auto-generated method stub
		return "Data Latency Performer";
	}
	
	private boolean isAvailable(String freqString){
		boolean available = true;
		if(freqString.equalsIgnoreCase("TBD")) available = false;
		if(freqString.equalsIgnoreCase("n/a")) available = false;
		return available;
	}
	
	public void fillHashesWithValuesUpTo(Double inputValue){
		value = inputValue;

		Vector<DBCMVertex> forestRoots = getForestRoots();
		runDepthSearchFirst(forestRoots);
		
		finalScoresFilled = true;
	}

	public void executeFromHash() {
		validVerts.clear();
		validEdges.clear();
		fillValidComponentHashes();
		setTransformers();
	}
	
	public void fillValidComponentHashes(){
		Vector<DBCMEdge> validEdges = new Vector(); 
		Vector<DBCMVertex> validVerts = new Vector();
		Iterator vertIt = finalVertScores.keySet().iterator();
		while(vertIt.hasNext()){
			DBCMVertex vert = (DBCMVertex) vertIt.next();
			Double score = finalVertScores.get(vert);
			if (score!=null){
				if(score<=value) validVerts.add(vert);
			}
		}
		Iterator edgeIt = finalEdgeScores.keySet().iterator();
		while(edgeIt.hasNext()){
			DBCMEdge edge = (DBCMEdge) edgeIt.next();
			Double score = finalEdgeScores.get(edge);
			if(score!=null){//It will be null if it is TBD or if the max value wasn't big enough
				if(score<=value) validEdges.add(edge);
			}
		}
		addPathAsValid(validEdges, validVerts);
	}

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

	private void setTransformers(){
		if(ps.searchPanel.btnHighlight.isSelected()){
			SearchEdgeStrokeTransformer tx = (SearchEdgeStrokeTransformer)ps.getView().getRenderContext().getEdgeStrokeTransformer();
			tx.setEdges(validEdges);
			SearchVertexPaintTransformer vtx = (SearchVertexPaintTransformer)ps.getView().getRenderContext().getVertexFillPaintTransformer();
			vtx.setVertHash(validVerts);
			SearchVertexLabelFontTransformer vlft = (SearchVertexLabelFontTransformer)ps.getView().getRenderContext().getVertexFontTransformer();
			vlft.setVertHash(validVerts);
		}
		else{
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
		}
		// repaint it
		ps.getView().repaint();
	}
}
