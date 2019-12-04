package prerna.engine.impl.neo4j;

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

public class Neo4jEmbeddedEngine extends AbstractEngine {
	private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jEmbeddedEngine.class);
	private GraphDatabaseService db;

	@Override
	public void openDB(String propFile) {
		super.openDB(propFile);
		String neo4jFile = SmssUtilities.getNeo4jFile(prop).getAbsolutePath();
		try {
			LOGGER.info("Opening neo4j graph: " + neo4jFile);
			db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(neo4jFile));
			LOGGER.info("Done neo4j opening graph: " + neo4jFile);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public ENGINE_TYPE getEngineType() {
		return IEngine.ENGINE_TYPE.NEO4J_EMBEDDED;
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
				for (String col : columns) {
					Object val = row.get(col);
					System.out.println("Column:" + col + " Value: " + val);
				}
				System.out.println("***************************************************");
				for (int i = 0; i < 5; i++) {
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

	@Override
	public void insertData(String query) throws Exception {

	}

	@Override
	public void removeData(String query) throws Exception {

	}

	@Override
	public void commit() {

	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		return null;
	}
}