package prerna.reactor.vector;

import java.util.HashMap;
import java.util.List;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RerankReactor extends AbstractReactor {

	public RerankReactor() {
		this.keysToGet = new String [] {ReactorKeysEnum.FUNCTION.getKey(), ReactorKeysEnum.LIMIT.getKey()};
		this.keyRequired = new int [] {1, 0};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		String engineId = this.keyValue.get(ReactorKeysEnum.FUNCTION.getKey());
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Function " + engineId + " does not exist or user does not have access to it.");
		}
		
		IFunctionEngine functionEngine = Utility.getFunctionEngine(engineId);
				
		Object question = this.insight.getVar(AbstractVectorDatabaseEngine.LATEST_VECTOR_SEARCH_STATEMENT);
		Object vectorSearchResults = getVectorSearchResults();
		int limit = getLimit();
		
		// create the input params for gaas_gpt_reranker
		HashMap<String, Object> rerankMap = new HashMap<>();
		rerankMap.put("question", question);
		rerankMap.put("vector_search_results", vectorSearchResults);
		rerankMap.put("limit", limit);
		
		Object result = functionEngine.execute(rerankMap);
		
		return new NounMetadata(result, PixelDataType.VECTOR);
	}
	
	/**
	 * Get the vector database semantic search results from the noun store
	 * 
	 * @return
	 */
	private Object getVectorSearchResults() {
		GenRowStruct grs = this.store.getNoun(PixelDataType.VECTORDB.getKey());		
		if (grs == null) {
			throw new IllegalArgumentException("Reranker is unable to find vector search results");
		}
		
		return grs.get(0);
	}
	
	/**
	 * Determine the input limit for the number of results returned from the reranker.
	 * The default value is 5.
	 * 
	 * @return
	 */
	private int getLimit() {
		// try the key
		GenRowStruct numGrs = store.getNoun(keysToGet[2]);
		if(numGrs != null && !numGrs.isEmpty()) {
			return ((Number) numGrs.get(0)).intValue();
		}
		
		// try the cur row
		List<Object> allNumericInputs = this.curRow.getAllNumericColumns();
		if(allNumericInputs != null && !allNumericInputs.isEmpty()) {
			return ((Number) allNumericInputs.get(0)).intValue();
		}
		
		// default to 5
		return 5;
	}
}
