package prerna.engine.impl.tinker;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.query.interpreters.CypherInterpreter;
import prerna.query.interpreters.IQueryInterpreter;

public class Neo4jEngine extends AbstractEngine {
	private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jEngine.class);
	private GraphDatabaseService db ;
	
//	public Neo4jEngine(String path) {
//		 db = new GraphDatabaseFactory().newEmbeddedDatabase( new File(path) );
//	}

	@Override
	public void openDB(String propFile) {
		super.openDB(propFile);
		String neo4jFile = SmssUtilities.getNeo4jFile(prop).getAbsolutePath();
		try {
			LOGGER.info("Opening neo4j graph: " + neo4jFile);
//			g = Neo4jGraph.open(neo4jFile);
			 db = new GraphDatabaseFactory().newEmbeddedDatabase( new File(neo4jFile) );

			LOGGER.info("Done neo4j opening graph: " + neo4jFile);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public ENGINE_TYPE getEngineType() {
		return IEngine.ENGINE_TYPE.NEO4J;
	}
	
	@Override
	public Object execQuery(String query) {
		
		try {
			Transaction ignored = db.beginTx();
			Result result = db.execute(query);
			List<String> columns = result.columns();
			System.out.println("***************************************************");

			while (result.hasNext()) {
				Map<String, Object> row = result.next();
				for(String col: columns) {
					Object val = row.get(col);
					System.out.println("Column:" + col + " Value: " + val);
				}
				System.out.println("***************************************************");
				for(int i= 0; i < 5; i ++) {
					System.out.println("***************************************************");

				}
			}
			result.close();
			ignored.close();

		} finally {

		}
		
		return null;
	}
	
	@Override
	public IQueryInterpreter getQueryInterpreter() {
		return new CypherInterpreter();
	}
	
	public GraphDatabaseService getGraphDatabaseService() {
		return db;
	}
	
	public static void main(String[] args) {
		String neoDb = "C:\\Users\\SEMOSS\\Downloads\\graph";
//		Neo4jEngine neo = new Neo4jEngine(neoDb);
//		neo.execQuery("match(a:Device_node)-[b:Netflow]->(c:Device_node) return a.ID,a.Device,a.IP_address,a.geo_lat,a.geo_lng,a.owner,b.conn_state,b.diff,b.dtg,b.id_resp_p,b.orig_ip_bytes,b.proto,b.resp_ip_bytes,b.service,b.ts,c.ID,c.Device,c.IP_address,c.geo_lat,c.geo_lng,c.owner");
		//neo.execQuery("MATCH (a:Emitter_node)-[b:propagate]->(c:Platform_node) RETURN a.Emitter_Lat,a.Emitter_Long,a.Emitter_Name,a.Freq_Max_MHz,a.Freq_Min_MHz,a.PRI,a.Type,b.Freq_MHz,b.Time_Stamp,c.Platform_Lat,c.Platform_Long,c.Platform_Elevation");
		System.exit(0);
	}

	@Override
	public void insertData(String query) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeData(String query) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		// TODO Auto-generated method stub
		return null;
	}
}
