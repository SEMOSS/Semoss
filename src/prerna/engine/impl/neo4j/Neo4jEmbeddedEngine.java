package prerna.engine.impl.neo4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.engine.api.IDatabase;
import prerna.engine.impl.AbstractDatabase;
import prerna.engine.impl.SmssUtilities;
import prerna.query.interpreters.CypherInterpreter;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.util.Constants;
import prerna.util.Utility;

public class Neo4jEmbeddedEngine extends AbstractDatabase {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jEmbeddedEngine.class);
	private GraphDatabaseService db;
	protected Map<String, String> typeMap = new HashMap<String, String>();
	protected Map<String, String> nameMap = new HashMap<String, String>();
	protected boolean useLabel = false;

	@Override
	public void openDB(String propFile) {
		super.openDB(propFile);
		// get type map
		String typeMapStr = this.prop.getProperty(Constants.TYPE_MAP);
		if (typeMapStr != null && !typeMapStr.trim().isEmpty()) {
			try {
				this.typeMap = new ObjectMapper().readValue(typeMapStr, Map.class);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// get the name map
		String nameMapStr = this.prop.getProperty(Constants.NAME_MAP);
		if (nameMapStr != null && !nameMapStr.trim().isEmpty()) {
			try {
				this.nameMap = new ObjectMapper().readValue(nameMapStr, Map.class);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (prop.containsKey(Constants.TINKER_USE_LABEL)) {
			String booleanStr = prop.get(Constants.TINKER_USE_LABEL).toString();
			useLabel = Boolean.parseBoolean(booleanStr);
		}

		String neo4jFile = SmssUtilities.getNeo4jFile(prop).getAbsolutePath();
		try {
			LOGGER.info("Opening neo4j graph: " + Utility.cleanLogString(neo4jFile));
			db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(neo4jFile));
			LOGGER.info("Done neo4j opening graph: " + Utility.cleanLogString(neo4jFile));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public ENGINE_TYPE getEngineType() {
		return IDatabase.ENGINE_TYPE.NEO4J_EMBEDDED;
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
		CypherInterpreter interp = new CypherInterpreter(this.typeMap, this.nameMap);
		interp.setUseLabel(this.useLabel);
		return interp;
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