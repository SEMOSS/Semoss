package prerna.engine.impl.vector;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyUtils;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.Insight;

public class OpenSearchVectorDatabaseEngine extends AbstractVectorDatabaseEngine {
	
	private static final Logger classLogger = LogManager.getLogger(OpenSearchVectorDatabaseEngine.class);
	private static final String OPEN_SEARCH_INIT_SCRIPT = "${VECTOR_SEARCHER_NAME} = vector_database.OpenSearchConnector(embedder_engine_id = '${EMBEDDER_ENGINE_ID}', username = '${USERNAME}', password = '${PASSWORD}', index_name = '${INDEX_NAME}', hosts = ['${HOSTS}'], distance_method = '${DISTANCE_METHOD}')";

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
	}
	
	@Override
	protected String[] getServerStartCommands() {
		return (AbstractVectorDatabaseEngine.TOKENIZER_INIT_SCRIPT+OPEN_SEARCH_INIT_SCRIPT).split(PyUtils.PY_COMMAND_SEPARATOR);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Map<String, Object>> nearestNeighbor(String question, Number limit, Map <String, Object> parameters) {
		StringBuilder callMaker = new StringBuilder();
		
		checkSocketStatus();
		
		Insight insight = getInsight(parameters.get(INSIGHT));
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		
		// Create the call to search against the connection 
		callMaker.append(this.vectorDatabaseSearcher).append(".knn_search(");
		
		// make the question arg
		callMaker.append("question=\"\"\"")
				 .append(question.replace("\"", "\\\""))
				 .append("\"\"\"");

		callMaker.append(", insight_id='")
				 .append(insight.getInsightId())
				 .append("'");
		callMaker.append(", limit='")
		 .append(limit.toString())
		 .append("'");
		
		// TODO  add in fields, coulmns to return 
		// close the method
 		callMaker.append(")");
 		classLogger.info("Running >>>" + callMaker.toString());
		Object output = pyt.runScript(callMaker.toString(), insight);
		return (List<Map<String, Object>>) output;
	}
	
	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight, Map<String, Object> parameters) throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.OPENSEARCH;
	}	
	
}