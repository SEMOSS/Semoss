package prerna.reactor.vector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class VectorDatabaseQueryReactor extends AbstractReactor {

	public VectorDatabaseQueryReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.COMMAND.getKey(), 
				ReactorKeysEnum.LIMIT.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				ReactorKeysEnum.FILTERS.getKey()
		};
		this.keyRequired = new int[] {1, 1, 0, 0, 0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Vector db " + engineId + " does not exist or user does not have access to this model");
		}
		
		String question = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[1]));
		int limit = getLimit();

		Map<String, Object> paramMap = getMap();
		if(paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		
		IVectorDatabaseEngine eng = Utility.getVectorDatabase(engineId);

		if (eng == null) {
			throw new SemossPixelException("Unable to find engine");
		}
		
		// add the insightId so Model Engine calls can be made for python
		VectorDatabaseTypeEnum vectorDbType = eng.getVectorDatabaseType();
		if (vectorDbType == VectorDatabaseTypeEnum.FAISS) {
			paramMap.put("insight", this.insight);
		}
		
		List<IQueryFilter> filters = getFilters();
		if (filters != null) {
			paramMap.put("filters", filters);
		}
		
		Object output = eng.nearestNeighbor(question, limit, paramMap);
		return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);	
	}
	
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(keysToGet[3]);
        if(mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if(mapInputs != null && !mapInputs.isEmpty()) {
                return (Map<String, Object>) mapInputs.get(0).getValue();
            }
        }
        List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
        if(mapInputs != null && !mapInputs.isEmpty()) {
            return (Map<String, Object>) mapInputs.get(0).getValue();
        }
        return null;
    }
	
	//returns how much do we need to collect
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
	
	private List<IQueryFilter> getFilters() {
		AbstractQueryStruct qs;
		GenRowStruct filterGrs = store.getNoun(ReactorKeysEnum.FILTERS.getKey());
		if(filterGrs != null && !filterGrs.isEmpty()) {
            List<NounMetadata> filterInputs = filterGrs.getNounsOfType(PixelDataType.QUERY_STRUCT);
            if(filterInputs != null && !filterInputs.isEmpty()) {
            	qs = (AbstractQueryStruct) filterInputs.get(0).getValue();
            	List<IQueryFilter> filters = qs.getCombinedFilters().getFilters();
            	return filters;
        	}
        }
		return null;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			StringBuilder finalDescription = new StringBuilder("Param Options depend on the engine implementation");
			
			HashMap<String, List<String[]>> implementations = new HashMap<String, List<String []>>();
			
			// what are the key options for a given engine implementation
			implementations.put(
					VectorDatabaseTypeEnum.FAISS.getVectorDatabaseName(), 
					Arrays.asList(
							new String [] {VectorDatabaseTypeEnum.ParamValueOptions.COLUMNS_TO_RETURN.getKey(), "Optional"}, 
							new String [] {VectorDatabaseTypeEnum.ParamValueOptions.RETURN_THRESHOLD.getKey(), "Optional"}, 
							new String [] {VectorDatabaseTypeEnum.ParamValueOptions.ASCENDING.getKey(), "Optional"}
					)
			);
			
			for (Entry<String, List<String[]>> entry : implementations.entrySet()) {
				finalDescription.append("\n")
								.append("\t\t\t\t\t")
								.append(entry.getKey())
								.append(":");
				
				for (String[] option : entry.getValue()) {
					
					String paramKey = option[0];
					
					finalDescription.append("\n")
									.append("\t\t\t\t\t\t")
									.append(paramKey)
									.append("\t")
									.append("-")
									.append("\t")
									.append("(").append(option[1]).append(")")
									.append(" ")
									.append(VectorDatabaseTypeEnum.ParamValueOptions.getDescriptionFromKey(paramKey));
				}
			}
			return finalDescription.toString();
		}
	
		return super.getDescriptionForKey(key);
	}
}
