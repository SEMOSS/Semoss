package prerna.ds.datastax;

import java.util.Vector;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import com.datastax.driver.dse.graph.GraphOptions;
import com.datastax.dse.graph.api.DseGraph;

import prerna.engine.impl.AbstractEngine;
import prerna.query.interpreters.IQueryInterpreter2;
import prerna.util.Utility;

public class DataStaxGraphEngine extends AbstractEngine{
	
	private GraphTraversalSource graphTraversalSession;

	@Override
	public void openDB(String propFile) {
		super.openDB(propFile);
		this.prop = Utility.loadProperties(propFile);
		String host = this.prop.getProperty("HOST");
		String port = this.prop.getProperty("PORT");
		String graphName = this.prop.getProperty("GRAPH_NAME");
		
		DseCluster dseCluster = DseCluster.builder().addContactPoint(host).withPort(Integer.parseInt(port)).withGraphOptions(new GraphOptions().setGraphName(graphName)).build();
		DseSession dseSession = dseCluster.connect();
		this.graphTraversalSession = DseGraph.traversal(dseSession);
	}
	
	public GraphTraversalSource getGraphTraversalSource() {
		return this.graphTraversalSession;
	}
	
	public IQueryInterpreter2 getQueryInterpreter2() {
		return new DataStaxInterpreter(this.graphTraversalSession);
	}
	
	@Override
	public ENGINE_TYPE getEngineType() {
		return ENGINE_TYPE.DATASTAX_GRAPH;
	}
	
	
	
	
	
	
	
	
	
	

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

	@Override
	public Vector<Object> getCleanSelect(String query) {
		// TODO Auto-generated method stub
		return null;
	}

}
