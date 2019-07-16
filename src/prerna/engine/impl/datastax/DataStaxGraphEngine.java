package prerna.engine.impl.datastax;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.codehaus.jackson.map.ObjectMapper;

import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.dse.graph.api.DseGraph;

import prerna.engine.impl.AbstractEngine;
import prerna.query.interpreters.GremlinNoEdgeBindInterpreter;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class DataStaxGraphEngine extends AbstractEngine {
	
	private GraphTraversalSource graphTraversalSession;
	private Map<String, String> typeMap = new HashMap<String, String>();
	private Map<String, String> nameMap = new HashMap<String, String>();

	@Override
	public void openDB(String propFile) {
		super.openDB(propFile);
		this.prop = Utility.loadProperties(propFile);
		String host = this.prop.getProperty("HOST");
		String port = this.prop.getProperty("PORT");
		String username = this.prop.getProperty("USERNAME");
		String password = this.prop.getProperty("PASSWORD");
		String graphName = this.prop.getProperty("GRAPH_NAME");
		// get the type map
		String typeMapStr = this.prop.getProperty("TYPE_MAP");
		// get the name map
		String nameMapStr = this.prop.getProperty("NAME_MAP");

		DseCluster dseCluster = null;
		if(username != null && password != null) {
			dseCluster = DseCluster.builder().addContactPoint(host).withCredentials(username, password)
					.withPort(Integer.parseInt(port)).withGraphOptions(new GraphOptions().setGraphName(graphName))
					.build();
		} else {
			dseCluster = DseCluster.builder().addContactPoint(host).withPort(Integer.parseInt(port))
					.withGraphOptions(new GraphOptions().setGraphName(graphName)).build();
		}
		if(dseCluster != null) {
			DseSession dseSession = dseCluster.connect();
			if (typeMapStr != null && !typeMapStr.trim().isEmpty()) {
				try {
					this.typeMap = new ObjectMapper().readValue(typeMapStr, Map.class);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(nameMapStr != null && !nameMapStr.trim().isEmpty()) {
				try {
					this.nameMap = new ObjectMapper().readValue(nameMapStr, Map.class);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			this.graphTraversalSession = DseGraph.traversal(dseSession);
		} else {
			NounMetadata noun = new NounMetadata("Unable to establish connection", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
	}
	
	public GraphTraversalSource getGraphTraversalSource() {
		return this.graphTraversalSession;
	}
	
	public Map<String, String> getTypeMap() {
		return this.typeMap;
	}
	
	public Map<String, String> getNameMap() {
		return this.nameMap;
	}
	
	public IQueryInterpreter getQueryInterpreter() {
		return new GremlinNoEdgeBindInterpreter(this.graphTraversalSession, this.typeMap, this.nameMap);
	}
	
	@Override
	public ENGINE_TYPE getEngineType() {
		return ENGINE_TYPE.DATASTAX_GRAPH;
	}
	
	
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	
	@Override
	public Object execQuery(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void insertData(String query) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeData(String query) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

}
