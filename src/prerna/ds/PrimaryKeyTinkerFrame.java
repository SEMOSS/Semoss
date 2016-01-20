package prerna.ds;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;

public class PrimaryKeyTinkerFrame extends TinkerFrame implements ITableDataFrame {

	private static final Logger LOGGER = LogManager.getLogger(PrimaryKeyTinkerFrame.class.getName());
	
	//variables for primary keys
	final protected String PRIM_KEY = "PRIM_KEY";
	protected Long nextPrimKey = new Long(0);


	public PrimaryKeyTinkerFrame(String[] array) {
		// TODO Auto-generated constructor stub
		super(array);
	}


	/*********************************  END CONSTRUCTORS  ********************************/
	
	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		processPreTransformations(component, component.getPreTrans());
		
		IEngine engine = component.getEngine();
		// automatically created the query if stored as metamodel
		// fills the query with selected params if required
		// params set in insightcreatrunner
		String query = component.fillQuery();
		
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		String[] displayNames = wrapper.getDisplayVariables(); // pulled this outside of the if/else block on purpose. 

		
		// set this edge hash from component
		
		boolean hasMetaModel = component.getBuilderData() != null;
		g.variables().set(Constants.HEADER_NAMES, displayNames); // I dont know if i even need this moving forward.. but for now I will assume it is
		
		
		redoLevels(displayNames);
		
		if(hasMetaModel) {
			this.mergeEdgeHash(component.getBuilderData().getReturnConnectionsHash());
			while(wrapper.hasNext()){
				this.addRelationship(wrapper.next());
			}
		} else {
			this.createPrimKeyEdgeHash();
			while(wrapper.hasNext()){
				this.addRow(wrapper.next());
			}
		}

		processPostTransformations(component, component.getPostTrans(), this);
		
		processActions(component, component.getActions());

	}


	/******************************  END DATA MAKER METHODS ******************************/
	
	
	
	/******************************  AGGREGATION METHODS *********************************/

	@Override
	public void addRow(Object[] rowCleanData, Object[] rowRawData) {
		
		getHeaders(); // why take chances.. 
		if(rowCleanData.length != headerNames.length && rowRawData.length != headerNames.length) {
			throw new IllegalArgumentException("Input row must have same dimensions as levels in dataframe."); // when the HELL would this ever happen ?
		}
		
		Vertex primVertex = upsertVertex(this.PRIM_KEY, nextPrimKey.toString(), nextPrimKey.toString());
		
		for(int index = 0; index < headerNames.length; index++) {
			Vertex toVertex = upsertVertex(headerNames[index], rowCleanData[index], rowRawData[index]); // need to discuss if we need specialized vertices too		
			this.upsertEdge(primVertex, toVertex, this.PRIM_KEY);
		}
		
		this.nextPrimKey++;
	}
	
	public void addRelationship(ISelectStatement rowData) {
		Map<String, Object> rowCleanData = rowData.getPropHash();
		Map<String, Object> rowRawData = rowData.getRPropHash();
		
		boolean hasRel = false;
		for(String startNode : rowCleanData.keySet()) {
			
			Set<String> set = this.edgeHash.get(startNode);
			if(set != null) {
				
				for(String endNode : set) {
					
					if(rowCleanData.keySet().contains(endNode)) {
						hasRel = true;
						
						//get from vertex
						Object startNodeValue = getParsedValue(rowCleanData.get(startNode));
						String rawStartNodeValue = rowRawData.get(startNode).toString();
						Vertex fromVertex = upsertVertex(startNode, startNodeValue, rawStartNodeValue);
						
						
						//get to vertex	
						Object endNodeValue = getParsedValue(rowCleanData.get(endNode));
						String rawEndNodeValue = rowRawData.get(endNode).toString();
						Vertex toVertex = upsertVertex(endNode, endNodeValue, rawEndNodeValue);
						
						upsertEdge(fromVertex, toVertex);
						
						//establish edges from primary key
//						Iterator<Edge> edgeIterator = fromVertex.edges(Direction.IN, PRIM_KEY);
//						while(edgeIterator.hasNext()) {
//							Edge nextEdge = edgeIterator.next();
//							Vertex primFromVertex = nextEdge.outVertex();
//							
//							//need to figure out which node to attach to
//							upsertEdge(primFromVertex, toVertex, PRIM_KEY);
//						}
					}
				}
			}
		}
		
		// this is to replace the addRow method which needs to be called on the first iteration
		// since edges do not exist yet
		if(!hasRel) {
			String singleColName = rowCleanData.keySet().iterator().next();
			Object startNodeValue = getParsedValue(rowCleanData.get(singleColName));
			String rawStartNodeValue = rowRawData.get(singleColName).toString();
			upsertVertex(singleColName, startNodeValue, rawStartNodeValue);
		}
	}
	
	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Object> rowRawData) {
		//do any of the keys exist within the table?
			//if yes add the other keys that do not exist on to the same prim keys
			//if no business as usual
	}
	
	protected void createPrimKeyEdgeHash() {
		Set<String> primKeyEdges = new HashSet<>();
		for(String header : headerNames) {
			primKeyEdges.add(header);
		}
		this.edgeHash.put(PRIM_KEY, primKeyEdges);
	}
	
	@Override
	/**
	 * this.edgeHash is always in the form: 
	 * 		{PRIM_KEY -> <col1, col2, col3, ...>}
	 * 
	 * Parameter newEdgeHash is in the form: 
	 * 		{Key -> <newCol1, newCol2, ... >}
	 * 
	 * This method will produce a resulting edgeHash saved in this.edgeHash in the form:
	 * 		{PRIM_KEY -> <col1, col2, col3, Key, newCol1, newCol2, ...>}
	 * 		--Note that duplicates will not stored within this.edgeHash
	 */
	protected void mergeEdgeHash(Map <String, Set<String>> newEdgeHash) {
		
		Set<String> primKeyEdges = this.edgeHash.get(PRIM_KEY);
		List<String> newHeaders = new ArrayList<String>();
		
		for(String key : edgeHash.keySet()) {
			primKeyEdges.addAll(edgeHash.get(key));
			primKeyEdges.add(key);
			
			if(ArrayUtilityMethods.arrayContainsValue(this.headerNames, key)) {
				newHeaders.add(key);
			}
			
			for(String s : primKeyEdges) {
				if(ArrayUtilityMethods.arrayContainsValue(this.headerNames, s)) {
					newHeaders.add(s);
				}
			}
		}
		
		this.redoLevels(newHeaders.toArray(new String[newHeaders.size()]));
	}
	
	
	protected Edge upsertEdge(Vertex fromVertex, Vertex toVertex, String label) {
		Edge retEdge = null;
		String edgeID = fromVertex.property(Constants.ID).value() + "" + toVertex.property(Constants.ID).value();
		// try to find the vertex
		GraphTraversal<Edge, Edge> gt = g.traversal().E().has(Constants.ID, edgeID);
		if(gt.hasNext()) {
			retEdge = gt.next();
			Integer count = (Integer)retEdge.property(Constants.COUNT).value();
			count++;
			retEdge.property(Constants.COUNT, count);
		}
		else {
			retEdge = fromVertex.addEdge(label, toVertex, Constants.ID, edgeID, Constants.COUNT, 1);
		}

		return retEdge;
	}

	/****************************** END AGGREGATION METHODS *********************************/

	

	

	
}


