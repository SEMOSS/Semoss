package prerna.engine.impl.tinker;

import java.io.IOException;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import prerna.ds.TinkerFrame;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.util.DIHelper;
import prerna.util.MyGraphIoRegistry;

public class TinkerEngine extends AbstractEngine {

	private static final Logger LOGGER = LogManager.getLogger(BigDataEngine.class.getName());

	private TinkerGraph g = null;
	
	public void openDB(String propFile)
	{
		try
		{			
			super.openDB(propFile);
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			String fileName = baseFolder + "/" + prop.getProperty("tinker.file");

			// user kyro to de-serialize the cached graph
			Builder<GryoIo> builder = IoCore.gryo();
			builder.graph(this.g);
			IoRegistry kryo = new MyGraphIoRegistry();
			builder.registry(kryo);
			GryoIo yes = builder.create();
			yes.readGraph(fileName);
			
		} catch(IOException e) {
			e.printStackTrace();
		}
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
	public ENGINE_TYPE getEngineType() {
		// TODO Auto-generated method stub
		return null;
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
		try {
			long startTime = System.currentTimeMillis();
			
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			String fileName = baseFolder + "/" + prop.getProperty("tinker.file");
			
			Builder<GryoIo> builder = IoCore.gryo();
			builder.graph(g);
			IoRegistry kryo = new MyGraphIoRegistry();;
			builder.registry(kryo);
			GryoIo yes = builder.create();
			yes.writeGraph(fileName);
			
			long endTime = System.currentTimeMillis();
			LOGGER.info("Successfully saved TinkerFrame to file: "+fileName+ "("+(endTime - startTime)+" ms)");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Vector<Object> getCleanSelect(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Generate the tinker vertex for a specific instance
	 * @param type		The type of the vertex
	 * @param data		The instance value
	 * @return
	 */
	protected Vertex upsertVertex(Object[] args)
	{
		String type = args[0] + "";
		Object data = args[1];
		
		Vertex retVertex = null;
		GraphTraversal<Vertex, Vertex> gt = g.traversal().V().has(TinkerFrame.TINKER_ID, type + ":" + data);
		if(gt.hasNext()) {
			retVertex = gt.next();
		} else {
			retVertex = g.addVertex(TinkerFrame.TINKER_ID, type + ":" + data, TinkerFrame.TINKER_TYPE, type, TinkerFrame.TINKER_NAME, data);// push the actual value as well who knows when you would need it
		}
		return retVertex; 
	}
	
}
