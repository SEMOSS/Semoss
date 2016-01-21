package prerna.ds;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
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
		super(array);
	}
	
	public PrimaryKeyTinkerFrame() {
		super();
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
	
	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Object> rowRawData) {
		 
		//only handling two right now, everything more is an error
		//order the columns so that the newer column is the second column
		Set<String> columns = rowCleanData.keySet();
		int i = 0;
		String[] operatingColumns = new String[2];
		for(String column : this.headerNames) {
			if(columns.contains(column)) {
				operatingColumns[i] = column;
				i++;
			}
		}
		
		String firstColumn = operatingColumns[0];
		String secondColumn = operatingColumns[1];
		
		Vertex vertex = upsertVertex(firstColumn, rowCleanData.get(firstColumn), rowRawData.get(firstColumn).toString());
		Iterator<Vertex> it = vertex.vertices(Direction.IN);
		
		Vertex toVertex = upsertVertex(secondColumn, rowCleanData.get(secondColumn), rowRawData.get(secondColumn).toString());
		while(it.hasNext()) {
			Vertex primKey = it.next();
			upsertEdge(primKey, toVertex, PRIM_KEY);
		}
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
		
		for(String key : newEdgeHash.keySet()) {
			//update the edge hash
			primKeyEdges.addAll(newEdgeHash.get(key));
			primKeyEdges.add(key);
			
			//update the headers
			if(!ArrayUtilityMethods.arrayContainsValue(this.headerNames, key)) {
				newHeaders.add(key);
			}
			
			for(String s : newEdgeHash.get(key)) {
				if(!ArrayUtilityMethods.arrayContainsValue(this.headerNames, s)) {
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


