package prerna.reactor.vector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.reactor.AbstractReactor;
import prerna.reactor.vector.VectorDatabaseParamOptionsEnum.VectorQueryParamOptions;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class VectorDatabaseQueryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(VectorDatabaseQueryReactor.class);

	public VectorDatabaseQueryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.COMMAND.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				ReactorKeysEnum.FILTERS.getKey(), ReactorKeysEnum.META_FILTERS.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		String engineId = getString(ReactorKeysEnum.ENGINE.getKey());

		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Vector db " + engineId + " does not exist or user does not have access to it.");
		}

		IVectorDatabaseEngine eng = Utility.getVectorDatabase(engineId);
		if (eng == null) {
			throw new SemossPixelException("Unable to find engine");
		}

		String embeddingsEngineId = eng.getSmssProp().getProperty(Constants.EMBEDDER_ENGINE_ID);
		if (embeddingsEngineId == null
				|| !SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), embeddingsEngineId)) {
			throw new IllegalArgumentException("Embeddings model " + embeddingsEngineId
					+ " does not exist or user does not have access to this model");
		}

		String searchStatement = Utility.decodeURIComponent(getString(ReactorKeysEnum.COMMAND.getKey()));
		int limit = getInt(ReactorKeysEnum.LIMIT.getKey(), 5);
		Map<String, Object> paramMap = getMap(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}

		// add the insightId so Model Engine calls can be made for python
		List<IQueryFilter> filters = getFilters(ReactorKeysEnum.FILTERS.getKey());
		if (filters != null) {
			paramMap.put(AbstractVectorDatabaseEngine.FILTERS_KEY, filters);
		}
		filters = getFilters(ReactorKeysEnum.META_FILTERS.getKey());
		if (filters != null) {
			paramMap.put(AbstractVectorDatabaseEngine.METADATA_FILTERS_KEY, filters);
		}

		Object output = eng.nearestNeighbor(this.insight, searchStatement, limit, paramMap);
		return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	/**
	 * 
	 * @param key
	 * @return
	 */
	private List<IQueryFilter> getFilters(String key) {
		AbstractQueryStruct qs;
		GenRowStruct filterGrs = store.getGenRowStruct(key);
		if (filterGrs != null && !filterGrs.isEmpty()) {
			List<NounMetadata> filterInputs = filterGrs.getNounsOfType(PixelDataType.QUERY_STRUCT);
			if (filterInputs != null && !filterInputs.isEmpty()) {
				qs = (AbstractQueryStruct) filterInputs.get(0).getValue();
				List<IQueryFilter> filters = qs.getCombinedFilters().getFilters();
				return filters;
			}
		}
		return null;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			StringBuilder finalDescription = new StringBuilder("Param Options depend on the engine implementation");

			for (VectorQueryParamOptions entry : VectorQueryParamOptions.values()) {
				finalDescription.append("\n").append("\t\t\t\t\t")
						.append(entry.getVectorDbType().getVectorDatabaseName()).append(":");

				for (String paramKey : entry.getParamOptionsKeys()) {
					finalDescription.append("\n").append("\t\t\t\t\t\t").append(paramKey).append("\t").append("-")
							.append("\t").append("(").append(entry.getRequirementStatus(paramKey)).append(")")
							.append(" ").append(VectorDatabaseParamOptionsEnum.getDescriptionFromKey(paramKey));
				}
			}
			return finalDescription.toString();
		}

		return super.getDescriptionForKey(key);
	}
}
