package prerna.ds.datastax;

import java.io.IOException;
import java.util.Map;
import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.codehaus.jackson.map.ObjectMapper;

import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.dse.graph.api.DseGraph;

import prerna.engine.impl.AbstractEngine;
import prerna.query.interpreters.DataStaxInterpreter;
import prerna.query.interpreters.IQueryInterpreter2;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class DataStaxGraphEngine extends AbstractEngine {
	
	private GraphTraversalSource graphTraversalSession;
	private Map<String, String> typeMap;
	

	@Override
	public void openDB(String propFile) {
		super.openDB(propFile);
		this.prop = Utility.loadProperties(propFile);
		String host = this.prop.getProperty("HOST");
		String port = this.prop.getProperty("PORT");
		String username = this.prop.getProperty("USERNAME");
		String password = this.prop.getProperty("PASSWORD");
		String graphName = this.prop.getProperty("GRAPH_NAME");
		// node type to property value map
		String typeMapStr = this.prop.getProperty("TYPE_MAP");

		DseCluster dseCluster = null;
		if (username != null && password != null) {
			dseCluster = DseCluster.builder().addContactPoint(host).withCredentials(username, password)
					.withPort(Integer.parseInt(port)).withGraphOptions(new GraphOptions().setGraphName(graphName))
					.build();
		} else {
			dseCluster = DseCluster.builder().addContactPoint(host).withPort(Integer.parseInt(port))
					.withGraphOptions(new GraphOptions().setGraphName(graphName)).build();
		}
		if (dseCluster != null) {
			DseSession dseSession = dseCluster.connect();
			if (typeMapStr != null && !typeMapStr.trim().isEmpty()) {
				try {
					this.typeMap = new ObjectMapper().readValue(typeMapStr, Map.class);
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
	
	public IQueryInterpreter2 getQueryInterpreter2() {
		return new DataStaxInterpreter(this.graphTraversalSession);
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
