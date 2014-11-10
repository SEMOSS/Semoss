package prerna.ui.components.playsheets;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.repository.RepositoryConnection;

import edu.uci.ics.jung.graph.DelegateForest;
import prerna.algorithm.impl.CentralityCalculator;
import prerna.algorithm.impl.PageRankCalculator;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.ui.components.ExecuteQueryProcessor;
import prerna.util.Constants;

@SuppressWarnings("serial")
public class MetamodelCentralityGridPlaySheet extends GridPlaySheet {

	private static final Logger logger = LogManager.getLogger(MetamodelCentralityGridPlaySheet.class.getName());
		
	@Override
	public void createData() {
		GraphPlaySheet graphPS = createMetamodel(((AbstractEngine)engine).getBaseDataEngine().getRC());

		Hashtable<String, SEMOSSVertex> vertStore  = graphPS.getGraphData().getVertStore();
		Hashtable<String,Set<String>> unDirectedEdges = processEdges(vertStore, false);

		names = new String[]{"Type","Undirected Closeness Centrality","Undirected Betweeness Centrality","Undirected Eccentricity Centrality","Undirected Page Rank"};

		list = new ArrayList<Object[]>();
		
		CentralityCalculator cCalc = new CentralityCalculator();
		Hashtable<String, Double> unDirCloseness = cCalc.calculateCloseness(unDirectedEdges);
		Hashtable<String, Double> unDirBetweenness = cCalc.calculateBetweenness(unDirectedEdges);
		Hashtable<String, Double> unDirEccentricity = cCalc.calculateEccentricity(unDirectedEdges);
		
		DelegateForest<SEMOSSVertex,SEMOSSEdge> forest = makeForestUndirected(graphPS.getGraphData().getEdgeStore(), graphPS.forest);
		PageRankCalculator pCalc = new PageRankCalculator();
		Hashtable<SEMOSSVertex, Double> ranks = pCalc.calculatePageRank(forest);
		
		Hashtable<SEMOSSVertex, Double> ranksTimesNodes = pCalc.calculatePageRank(forest);
		for(SEMOSSVertex vert : ranks.keySet()) {
			ranksTimesNodes.put(vert, ranks.get(vert)*ranks.keySet().size());
		}
		
		for(String node : vertStore.keySet()) {
			SEMOSSVertex vert = vertStore.get(node);
			String type = (String)vert.propHash.get(Constants.VERTEX_NAME);
			Object[] row = new Object[5];
			row[0] = type;
			row[1] = unDirCloseness.get(type);
			row[2] = unDirBetweenness.get(type);
			row[3] = unDirEccentricity.get(type);
			row[4] = ranks.get(vert);
			list.add(row);
		}
	}
	/**
	 * Creates the GraphPlaySheet for a database that shows the metamodel.
	 * @param engine IEngine to create the metamodel from
	 * @return GraphPlaySheet that displays the metamodel
	 */
	private GraphPlaySheet createMetamodel(RepositoryConnection rc){
		ExecuteQueryProcessor exQueryProcessor = new ExecuteQueryProcessor();
		//hard code playsheet attributes since no insight exists for this
		//String sparql = "SELECT ?s ?p ?o WHERE {?s ?p ?o} LIMIT 1";
		String playSheetName = "prerna.ui.components.playsheets.GraphPlaySheet";
		RDFFileSesameEngine sesameEngine = new RDFFileSesameEngine();
		sesameEngine.setRC(rc);
		sesameEngine.setEngineName("Metamodel Engine");
		
		sesameEngine.setBaseData(sesameEngine);
		Hashtable<String, String> filterHash = new Hashtable<String, String>();
		filterHash.put("http://semoss.org/ontologies/Relation", "http://semoss.org/ontologies/Relation");
		sesameEngine.setBaseHash(filterHash);
		
		exQueryProcessor.prepareQueryOutputPlaySheet(sesameEngine, query, playSheetName, "", "");

		GraphPlaySheet playSheet= (GraphPlaySheet) exQueryProcessor.getPlaySheet();
		playSheet.getGraphData().setSubclassCreate(true);//this makes the base queries use subclass instead of type--necessary for the metamodel query
		playSheet.createData();
		playSheet.runAnalytics();
		playSheet.getForest();
		playSheet.createForest();
		return playSheet;
	}
	private Hashtable<String,Set<String>> processEdges(Hashtable<String, SEMOSSVertex> vertStore, boolean directed) {
		Hashtable<String,Set<String>> edges = new Hashtable<String,Set<String>>();
		for(String key : vertStore.keySet()) {
			SEMOSSVertex vertex= vertStore.get(key);
			String type = (String)vertex.propHash.get(Constants.VERTEX_NAME);
			
			Set<String> neighbors = new HashSet<String>();
			if(!directed) {
				Vector<SEMOSSEdge> inEdges = vertex.getInEdges();
				for(SEMOSSEdge edge : inEdges) {
					neighbors.add((String)(edge.outVertex.propHash.get(Constants.VERTEX_NAME)));
				}
			}
			Vector<SEMOSSEdge> outEdges = vertex.getOutEdges();
			for(SEMOSSEdge edge : outEdges) {
				neighbors.add((String)(edge.inVertex.propHash.get(Constants.VERTEX_NAME)));
			}
			edges.put(type, neighbors);
		}
		return edges;
	}
	
	private DelegateForest<SEMOSSVertex,SEMOSSEdge> makeForestUndirected(Hashtable<String, SEMOSSEdge> edgeStore, DelegateForest<SEMOSSVertex,SEMOSSEdge> forest) {
		for(String edgeKey : edgeStore.keySet()) {
			SEMOSSEdge oldEdge = edgeStore.get(edgeKey);
			SEMOSSEdge newEdge = new SEMOSSEdge(oldEdge.inVertex,oldEdge.outVertex,oldEdge.inVertex.getURI()+":"+oldEdge.outVertex.propHash.get(Constants.VERTEX_NAME));//pull from the old edge
			forest.addEdge(newEdge, oldEdge.inVertex,oldEdge.outVertex);			
		}
		return forest;
	}
}
